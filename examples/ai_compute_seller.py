"""
================================================================================
 ai_compute_seller.py -- SELL YOUR HARDWARE: local LLM inference on m2m-ledger
================================================================================
A provider node that rents out the compute of the machine it runs on. It
lists `local_llm_inference` on the marketplace; buyers attach a text prompt;
the node forwards it to a local Ollama instance, streams the generation time
as it thinks, and delivers the completion — earning per token produced.

  * Consumers request `local_llm_inference:<prompt>`.
  * The handler POSTs the prompt to Ollama's /api/generate on this machine,
    so the SELLER's GPU/CPU and the SELLER's installed model do the work.
  * While Ollama generates, the handler streams empty progress ticks (the
    buyer pays for compute time, live). When generation completes, ONE
    result chunk carries the text + token accounting; under mode="count"
    param=1 that single delivery settles and completes the session.
  * Pricing is dynamic: a per-request floor plus a per-token component,
    reported back so the buyer sees exactly what the inference cost.

RUN
    ollama serve                        # if not already running
    ollama pull llama3.1                # or any model you want to sell
    export OLLAMA_MODEL=llama3.1
    python3 ai_compute_seller.py        # connects to the live global broker

Same Ed25519 identity, same supervisor/backoff shape as weather_seller.py.
No prompt filtering, no sandboxing — pure plumbing, by directive.
================================================================================
"""

import asyncio
import json
import logging
import os
import sys
import threading
import time
import urllib.request
from pathlib import Path

os.environ.setdefault("M2M_TRACE", "0")
sys.stdout.reconfigure(line_buffering=True)

try:
    from m2m_ledger import Agent
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    from m2m_ledger import Agent

logging.getLogger().setLevel(logging.WARNING)

# The M2M Broker infrastructure is fully managed and live: no local servers.
BROKER_URL = os.environ.get("M2M_BROKER_URL", "wss://m2m-broker.onrender.com")
RESOURCE_NAMESPACE = "local_llm_inference"

# Ollama locale del VENDITORE: e' il suo hardware a lavorare.
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "llama3.1")
OLLAMA_TIMEOUT_SEC = float(os.environ.get("OLLAMA_TIMEOUT", "120"))

# Prezzo dinamico: pavimento per-richiesta + componente per-token generato.
PRICE_PER_REQUEST = float(os.environ.get("PRICE_PER_REQUEST", "0.002"))
PRICE_PER_TOKEN = float(os.environ.get("PRICE_PER_TOKEN", "0.00002"))
# Il listino pubblico usa i campi standard dell'Order Book (per-sec/per-KB);
# la tariffa a token e' descritta nella description ed emessa nel risultato.
PRICE_PER_SEC = 0.003
PRICE_PER_KB = 0.01
BACKOFF_START, BACKOFF_CAP = 1.0, 10.0

if os.name == "nt":
    os.system("")
DIM, BOLD, RESET = "\033[2m", "\033[1m", "\033[0m"
GOLD, CYAN, GREEN, RED = "\033[33m", "\033[36m", "\033[32m", "\033[31m"


def run_ollama(prompt: str) -> dict:
    """POST bloccante a Ollama /api/generate (stream=false). Ritorna un dict
    denso col testo e la contabilita' dei token, o {'error': ...} -- il
    buyer (e la sua AI) devono VEDERE il fallimento, non un crash."""
    payload = json.dumps({"model": OLLAMA_MODEL, "prompt": prompt,
                          "stream": False}).encode("utf-8")
    req = urllib.request.Request(OLLAMA_URL, data=payload,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=OLLAMA_TIMEOUT_SEC) as r:
            data = json.loads(r.read().decode("utf-8"))
    except Exception as exc:
        return {"error": "ollama_failure", "detail": f"{type(exc).__name__}: {exc}"}

    prompt_tok = int(data.get("prompt_eval_count", 0))
    gen_tok = int(data.get("eval_count", 0))
    cost = round(PRICE_PER_REQUEST + gen_tok * PRICE_PER_TOKEN, 8)
    return {
        "model": data.get("model", OLLAMA_MODEL),
        "response": data.get("response", ""),
        "tokens": {"prompt": prompt_tok, "generated": gen_tok,
                   "total": prompt_tok + gen_tok},
        "pricing": {"per_request": PRICE_PER_REQUEST, "per_token": PRICE_PER_TOKEN,
                    "quoted_cost": cost},
        "eval_duration_sec": round(data.get("eval_duration", 0) / 1e9, 3),
        "seller_hardware": True,
    }


# ==============================================================================
# Un'inferenza alla volta (il broker garantisce un cliente per sessione).
# Il lavoro gira in un thread: lo stato condiviso e' interrogato dall'handler
# a ogni tick, cosi' il buyer paga il TEMPO di calcolo mentre Ollama genera.
# ==============================================================================
class InferenceJob:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.done = False
        self.result = None
        self.t0 = time.perf_counter()

    def start(self, prompt: str) -> None:
        self.reset()
        threading.Thread(target=self._work, args=(prompt,), daemon=True).start()

    def _work(self, prompt: str) -> None:
        try:
            self.result = run_ollama(prompt)
        except Exception as exc:                       # mai un thread muto
            self.result = {"error": "worker_crash", "detail": f"{type(exc).__name__}: {exc}"}
        finally:
            self.done = True


def make_handler(job: InferenceJob):
    """cursor: None -> avvia l'inferenza sul thread; poi tick vuoti di
    progress finche' Ollama macina; infine UN chunk col testo generato."""
    def handler(cursor, resource: str):
        if cursor is None:
            prompt = resource.split(":", 1)[1] if ":" in resource else ""
            preview = (prompt[:60] + "…") if len(prompt) > 61 else prompt
            print(f"\n{GOLD}{BOLD}◆ INFERENCE REQUEST{RESET}  {DIM}model={OLLAMA_MODEL}{RESET}")
            print(f"  {CYAN}prompt{RESET} {preview!r}")
            print(f"  {DIM}dispatching to local Ollama — buyer pays for compute time{RESET}")
            job.start(prompt)
            return [], {"phase": "generating"}
        if not job.done:
            return [], cursor                          # progress: paga il tempo
        result = job.result or {"error": "no_result"}
        if "error" in result:
            print(f"  {RED}✗ {result['error']}: {result.get('detail', '')[:70]}{RESET}")
        else:
            tk = result["tokens"]
            print(f"{GREEN}{BOLD}  ✓ GENERATED{RESET}  {tk['generated']} tokens in "
                  f"{result['eval_duration_sec']}s  "
                  f"({DIM}quoted ${result['pricing']['quoted_cost']:.6f}{RESET})")
            print(f"  {DIM}shipping completion through the signed channel…{RESET}")
        return [result], {"phase": "delivered"}
    return handler


async def compute_supervisor() -> None:
    job = InferenceJob()
    seller = Agent(name="AI-Compute-Node", balance=0.0, broker_url=BROKER_URL)
    seller.will_provide(
        f"{RESOURCE_NAMESPACE}:all",                    # wildcard: qualsiasi prompt
        make_handler(job),
        price_per_sec=PRICE_PER_SEC,
        price_per_kb=PRICE_PER_KB,
        description=f"Local {OLLAMA_MODEL} inference on seller hardware - "
                    f"request {RESOURCE_NAMESPACE}:<prompt> "
                    f"(${PRICE_PER_REQUEST}/req + ${PRICE_PER_TOKEN}/token)",
    )
    await seller.ensure_identity()

    print(f"\n{BOLD}{GOLD}╔══════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{GOLD}║   AI COMPUTE NODE  ·  renting local inference on m2m-ledger  ║{RESET}")
    print(f"{BOLD}{GOLD}╚══════════════════════════════════════════════════════════════╝{RESET}")
    print(f"  {CYAN}passport{RESET}  {seller.passport_id[:16]}…  {DIM}(Ed25519){RESET}")
    print(f"  {CYAN}model{RESET}     {OLLAMA_MODEL}  {DIM}(via {OLLAMA_URL}){RESET}")
    print(f"  {CYAN}pricing{RESET}   ${PRICE_PER_REQUEST}/request + ${PRICE_PER_TOKEN}/token")
    print(f"  {CYAN}broker{RESET}    {BROKER_URL}\n")

    backoff, session_n = BACKOFF_START, 0
    while True:
        session_n += 1
        print(f"{DIM}── listed on the order book · waiting for a buyer "
              f"(session #{session_n}) ──{RESET}")
        result = await seller.run()
        kind, why = result.get("type"), result.get("reason", "")
        if kind in ("complete", "halted") and result.get("ticks", 0) > 0:
            backoff = BACKOFF_START
            print(f"{GREEN}{BOLD}💰 SETTLED{RESET}  earned {GREEN}${result.get('earned', 0):.6f}{RESET} "
                  f"over {result.get('ticks', 0)} ticks · lifetime "
                  f"{GREEN}${seller.balance:.6f}{RESET}\n")
            await asyncio.sleep(0.2)
            continue
        print(f"{DIM}broker unreachable or session dropped ({kind}/{why}) — "
              f"retrying in {backoff:.0f}s…{RESET}")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, BACKOFF_CAP)


if __name__ == "__main__":
    try:
        asyncio.run(compute_supervisor())
    except KeyboardInterrupt:
        print(f"\n{DIM}AI Compute Node shutting down.{RESET}")
    sys.exit(0)