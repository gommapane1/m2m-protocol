"""
================================================================================
 dashboard_app.py -- THE GUARDIAN v3: a consumer control center for m2m-ledger
================================================================================
Three components, zero terminal:

  1. BRAIN CONNECTOR -- the user plugs in THEIR AI from the UI (Ollama /
     OpenAI / Anthropic / custom OpenAI-compatible). Credentials are
     injected at runtime over localhost, held in process memory only:
     never written to disk, never logged, never echoed back by the API.
  2. WALLET HUB -- one balance, live. It bleeds when the AI buys (Buyer
     mode) and climbs in real time when the AI earns (Seller mode): the
     broker pushes the authoritative balance on every settlement tick.
  3. MODE SWITCH [ BUYER | SELLER ] --
       Buyer : the chat. The user's AI works with its tools; the market
               sits in its toolset. Its decisions are its own.
       Seller: the chat sleeps. The SAME identity (same Ed25519 passport,
               same wallet) lists the user's AI on the market as
               `ai_reasoning`; incoming requests are answered by the
               user's own model (their API limits, their compute), and
               every settled tick pushes the wallet UP.

RUN
    pip install fastapi uvicorn openai
    python3 dashboard_app.py            # then open http://127.0.0.1:8000
================================================================================
"""

import asyncio
import ast
import json
import logging
import operator
import os
import sys
import time
import urllib.request
from pathlib import Path
from typing import Optional

# --- OPSEC: carica le credenziali da un .env nascosto, PRIMA di leggerle ---
# Le chiavi (ROUTER_API_KEY ecc.) non vanno piu' passate a mano sul terminale
# in chiaro: vivono in un file .env non versionato. load_dotenv NON sovrascrive
# le variabili gia' presenti nell'ambiente (override=False di default), quindi
# un deploy che le inietta via secret manager continua a vincere sul file.
try:
    from dotenv import load_dotenv
    # Cerca .env accanto a questo script e risalendo fino alla cwd: funziona
    # sia lanciando da guardian/ sia dalla root del progetto.
    _here = Path(__file__).resolve().parent
    _env_found = None
    for _cand in (_here / ".env", _here.parent / ".env", Path.cwd() / ".env"):
        if _cand.is_file():
            load_dotenv(_cand)
            _env_found = _cand
            break
    else:
        load_dotenv()          # fallback: ricerca automatica di python-dotenv
    # Log esplicito: un .env non trovato e' la causa #1 di un router che
    # "fallisce in silenzio" (ROUTER_API_KEY vuoto -> router_client None ->
    # degrado al Worker). Renderlo visibile all'avvio, non a runtime.
    if _env_found:
        print(f"  [env] loaded credentials from {_env_found}", flush=True)
    else:
        print("  [env] WARNING: no .env file found (looked next to the script, "
              "in its parent, and in the current directory). Router/DB "
              "credentials must come from real environment variables, or the "
              "router will stay disabled.", flush=True)
except ImportError:
    pass                        # python-dotenv assente: si usano solo le env di sistema

os.environ.setdefault("M2M_TRACE", "0")

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel
import uvicorn
from openai import AsyncOpenAI, OpenAI

try:
    from m2m_ledger import Agent
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    from m2m_ledger import Agent

logging.getLogger().setLevel(logging.WARNING)

# ---- the market (managed, live) ----------------------------------------------
BROKER_URL = os.environ.get("M2M_BROKER_URL", "wss://m2m-broker.onrender.com")
INITIAL_BALANCE = 1.00
OFFER_PER_SEC = 0.002
OFFER_PER_KB = 0.02
HARD_CAP_PER_PURCHASE = float(os.environ.get("GUARDIAN_PURCHASE_CAP", "0.10"))
MARKET_TIMEOUT_SEC = float(os.environ.get("M2M_MARKET_TIMEOUT", "45"))
SELLER_RESOURCE = os.environ.get("GUARDIAN_SELLER_RESOURCE", "ai_reasoning")
SELLER_PRICE_PER_SEC = 0.002
SELLER_PRICE_PER_KB = 0.01
MAX_AGENT_STEPS = 6
GUARDIAN_PORT = int(os.environ.get("GUARDIAN_PORT", "8000"))

# Il system prompt e' NEUTRO: descrive il ruolo, non le strategie. L'IA
# dell'utente pensa da sola; se davanti a un ostacolo decide di andare al
# mercato, e' una sua decisione -- e' il prodotto.
SYSTEM_PROMPT = (
    "You are the user's personal AI assistant. Complete the user's task using "
    "the tools available to you. Be concise, and be truthful about what you "
    "did and what you spent, if anything."
)

# ==============================================================================
# BRAIN CONNECTOR -- the user's AI, injected at runtime from the UI.
# In-memory only. Never persisted, never logged, never returned by the API.
# ==============================================================================
BRAIN_PRESETS = {
    "ollama":    {"base_url": "http://localhost:11434/v1", "model": "qwen2.5:7b"},
    "openai":    {"base_url": "https://api.openai.com/v1", "model": "gpt-4o-mini"},
    "anthropic": {"base_url": "https://api.anthropic.com/v1/", "model": "claude-haiku-4-5"},
    # Groq: pesca dinamicamente da ENV (stesse variabili del Router, cosi' un
    # solo .env configura entrambi). Se vuoi Groq anche come Worker, seleziona
    # "groq" nel Brain Connector: eredita endpoint e modello dal .env.
    "groq":      {"base_url": os.environ.get("ROUTER_BASE_URL", "https://api.groq.com/openai/v1"),
                  "model": os.environ.get("ROUTER_MODEL", "llama-3.3-70b-versatile")},
    "custom":    {"base_url": "", "model": ""},
}

class Brain:
    def __init__(self) -> None:
        self.provider: Optional[str] = None
        self.model: Optional[str] = None
        self.base_url: Optional[str] = None
        self._api_key: Optional[str] = None
        self.async_client: Optional[AsyncOpenAI] = None

    def configure(self, provider: str, api_key: str, base_url: str, model: str) -> None:
        preset = BRAIN_PRESETS.get(provider, BRAIN_PRESETS["custom"])
        self.provider = provider
        self.base_url = (base_url or preset["base_url"]).strip()
        self.model = (model or preset["model"]).strip()
        # Ollama non richiede una chiave vera: il client OpenAI ne esige una
        # non vuota, quindi usiamo un segnaposto se l'utente non la fornisce.
        self._api_key = api_key.strip() or ("ollama" if provider == "ollama" else "")
        if not (self.base_url and self.model and self._api_key):
            raise ValueError("base_url, model and api_key are all required")
        self.async_client = AsyncOpenAI(api_key=self._api_key, base_url=self.base_url)

    def sync_client(self) -> OpenAI:
        """Client sincrono per l'handler del Seller (gira in un worker
        thread dell'SDK: una chiamata bloccante e' esattamente giusta)."""
        return OpenAI(api_key=self._api_key, base_url=self.base_url, timeout=30.0)

    @property
    def configured(self) -> bool:
        return self.async_client is not None

    def public_view(self) -> dict:
        return {"configured": self.configured, "provider": self.provider,
                "model": self.model, "host": self.base_url}


brain = Brain()
if os.environ.get("LLM_API_KEY") or os.environ.get("OPENAI_API_KEY"):
    # Seed opzionale dalle env (retrocompatibilita'): sovrascrivibile da UI.
    try:
        brain.configure("custom",
                        os.environ.get("LLM_API_KEY") or os.environ.get("OPENAI_API_KEY"),
                        os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1"),
                        os.environ.get("LLM_MODEL", "gpt-4o-mini"))
    except ValueError:
        pass

# ==============================================================================
# THE ORCHESTRATOR (Two-Brain Router) -- platform-side, backend env ONLY.
# Un modello economico della piattaforma legge il messaggio e decide in
# binario se serve il mercato M2M. NOTA ARCHITETTURALE: questo e' l'if
# decisionale, promosso a modello -- scelta di prodotto esplicita. Senza
# ROUTER_API_KEY il sistema degrada alla modalita' autonoma pura: il Worker
# riceve TUTTI i tool (incluso il mercato) e decide da solo, come in v3.
# ==============================================================================
ROUTER_API_KEY = os.environ.get("ROUTER_API_KEY", "")
ROUTER_BASE_URL = os.environ.get("ROUTER_BASE_URL", "https://api.openai.com/v1")
ROUTER_MODEL = os.environ.get("ROUTER_MODEL", "gpt-4o-mini")

router_client: Optional[AsyncOpenAI] = (
    AsyncOpenAI(api_key=ROUTER_API_KEY, base_url=ROUTER_BASE_URL)
    if ROUTER_API_KEY else None)

# Diagnostica esplicita: se il Router e' disabilitato, DEVE essere ovvio
# all'avvio -- e' la causa #1 del "mercato bloccato / sub-prompt null".
if router_client is not None:
    print(f"  [router] ACTIVE · model={ROUTER_MODEL} · endpoint={ROUTER_BASE_URL}", flush=True)
else:
    print("  [router] DISABLED: ROUTER_API_KEY is empty. The Two-Brain routing "
          "and the M2M sub-prompt compiler are OFF; the Worker runs autonomously "
          "with all tools. Set ROUTER_API_KEY in your .env to enable the router.",
          flush=True)

def build_router_prompt(local_model_name: Optional[str]) -> str:
    """Prompt dinamico del Gatekeeper: il Router valuta il task rispetto alle
    capacita' NOTE dello specifico modello locale dell'utente. Il nome vive
    gia' nel backend (brain.model, impostato dal Brain Connector): iniettarlo
    da li' e' una fonte sola, nessun parametro extra da propagare."""
    model = local_model_name or "an unknown small local model"
    return (
        f"You are the Gatekeeper of an M2M network. The user is processing "
        f"locally with the model: {model}. Evaluate the user's prompt against "
        f"the KNOWN capabilities of this specific model.\n"
        f"* If the task (e.g. general conversation, creative writing, basic "
        f"code) is well within the reach of the stated model, return 0 "
        f"(Local Execution).\n"
        f"* If the task exceeds the cognitive capabilities of the stated "
        f"model (e.g. complex math for a 7B, advanced multi-step logic), or "
        f"if it requires access to real-time external data (weather, news), "
        f"return 1 (M2M Market Purchase).\n"
        f"Reply with EXACTLY one character and nothing else: 1 or 0."
    )

ROUTER_PICK_PROMPT = (
    "You are a machine-to-machine task compiler for an AI marketplace. Given a "
    "human request and a list of marketplace listings, you do TWO things:\n"
    "1. Pick exactly one resource that clearly covers the request (some sellers "
    "describe sub-resources like weather_data:<city> — build the full resource "
    "string). If nothing clearly covers it, the resource is null.\n"
    "2. If you picked a resource, compile a STRICT machine-to-machine sub-prompt "
    "that will be sent to the remote seller node in place of the raw human text. "
    "This sub-prompt must command the seller to perform the task and reply with "
    "ONLY a JSON object and NOTHING else — no preamble, no markdown, no prose — "
    "matching exactly this schema:\n"
    '  {"result": <the answer, string or number>, '
    '"unit": <string or null>, '
    '"detail": <short string or null>}\n'
    "The sub-prompt must explicitly restate that schema to the seller and forbid "
    "any text outside the JSON object.\n"
    "Reply with ONLY a JSON object, nothing else:\n"
    '{"resource": "<full resource string or null>", "ticks": <1-10>, '
    '"sub_prompt": "<the M2M sub-prompt, or null if no resource>"}'
)

# Schema che il payload del venditore DEVE rispettare dopo il parsing. Tenuto
# volutamente piatto e minimale: piu' lo schema e' stretto, piu' e' robusta la
# validazione e meno spazio ha un modello remoto di "creativizzare" l'output.
M2M_RESULT_SCHEMA_KEYS = {"result", "unit", "detail"}


def validate_m2m_payload(obj) -> Optional[dict]:
    """Valida in modo SICURO il JSON tornato dal venditore contro lo schema
    piatto {result, unit, detail}. Ritorna un dict normalizzato (chiavi
    garantite, tipi coerenti) o None se non conforme -- mai un'eccezione,
    mai fiducia cieca nel formato remoto."""
    if not isinstance(obj, dict) or "result" not in obj:
        return None
    result = obj.get("result")
    # result deve essere un valore scalare mostrabile, non una struttura.
    if not isinstance(result, (str, int, float, bool)):
        return None
    unit = obj.get("unit")
    detail = obj.get("detail")
    return {
        "result": result,
        "unit": unit if isinstance(unit, str) else None,
        "detail": detail if isinstance(detail, str) else None,
    }


def extract_json_object(raw: str) -> Optional[dict]:
    """Estrae il PRIMO oggetto JSON bilanciato da una stringa, tollerando
    preamboli/markdown che un modello remoto indisciplinato potrebbe
    aggiungere nonostante le istruzioni. Scansione a conteggio di graffe
    (gestisce oggetti annidati) invece di un fragile find/rfind."""
    start = raw.find("{")
    if start < 0:
        return None
    depth, in_str, esc = 0, False, False
    for i in range(start, len(raw)):
        c = raw[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(raw[start:i + 1])
                except json.JSONDecodeError:
                    return None
    return None


def _print_router_error(stage: str, exc: Exception) -> None:
    """Errore dell'Orchestrator: stampa CRUDA e completa sul terminale prima
    del degrado. Niente logging (uvicorn puo' inghiottirlo): print diretto."""
    import traceback
    print(f"\n{'=' * 68}\n[ROUTER ERROR] stage={stage}", flush=True)
    print(f"  endpoint : {ROUTER_BASE_URL}\n  model    : {ROUTER_MODEL}", flush=True)
    print(f"  exception: {type(exc).__name__}: {exc}", flush=True)
    status = getattr(exc, "status_code", None)
    if status is not None:
        print(f"  http     : {status}", flush=True)
    body = getattr(exc, "body", None) or getattr(exc, "response", None)
    if body is not None:
        print(f"  body     : {body}", flush=True)
    print(traceback.format_exc(), flush=True)
    print(f"→ degrading to router=0 (worker-direct){'=' * 36}\n", flush=True)


import re as _re

_BINARY_TOKEN = _re.compile(r"\b[01]\b")


async def route_decision(message: str) -> int:
    """1 = mercato, 0 = no. I modelli piccoli ignorano spesso il formato
    rigido e avvolgono la cifra in testo discorsivo: qui la ESTRAIAMO
    (primo token 0/1 isolato) invece di pretendere l'output pulito, e
    stampiamo sempre la risposta cruda -- l'unico modo di debuggare un
    router che risponde 200 OK con contenuto inatteso."""
    try:
        resp = await router_client.chat.completions.create(
            model=ROUTER_MODEL, temperature=0,
            messages=[{"role": "system", "content": build_router_prompt(brain.model)},
                      {"role": "user", "content": message}])
        raw = (resp.choices[0].message.content or "").strip()
        m = _BINARY_TOKEN.search(raw)
        decision = int(m.group()) if m else 0
        note = "" if m else "   (no 0/1 token found → defaulting to 0)"
        print(f"[ROUTER] model={brain.model!r} decision raw={raw[:160]!r} → "
              f"{decision}{note}", flush=True)
        return decision
    except Exception as exc:
        _print_router_error("decision", exc)
        return 0


async def route_pick(message: str, listings: list) -> Optional[dict]:
    """Sceglie la risorsa dal book E compila il sub-prompt M2M che impone al
    venditore lo schema JSON rigido. Ritorna {resource, ticks, sub_prompt}
    oppure None se nulla e' pertinente."""
    try:
        resp = await router_client.chat.completions.create(
            model=ROUTER_MODEL, temperature=0,
            messages=[{"role": "system", "content": ROUTER_PICK_PROMPT},
                      {"role": "user", "content":
                       f"Request: {message}\n\nListings: {json.dumps(listings)}"}])
        raw = (resp.choices[0].message.content or "").strip()
        print(f"[ROUTER] pick raw={raw[:280]!r}", flush=True)
        pick = extract_json_object(raw)
        if not pick or not pick.get("resource"):
            return None
        resource = str(pick["resource"])
        sub_prompt = pick.get("sub_prompt")
        # Fallback difensivo: se il 70B ha scelto la risorsa ma non ha
        # prodotto un sub-prompt valido, ne sintetizziamo uno minimo noi --
        # il contratto con il venditore (JSON-only) non deve mai saltare.
        if not isinstance(sub_prompt, str) or not sub_prompt.strip():
            sub_prompt = (
                f"Task: {message}\n"
                "Respond with ONLY a JSON object and nothing else — no preamble, "
                "no markdown, no prose. Schema: "
                '{"result": <string or number>, "unit": <string or null>, '
                '"detail": <short string or null>}.')
        return {"resource": resource,
                "ticks": max(1, min(int(pick.get("ticks", 4)), 10)),
                "sub_prompt": sub_prompt}
    except Exception as exc:
        _print_router_error("pick", exc)
        return None

# ==============================================================================
# Local tools (Buyer mode) -- the AI's own means.
# ==============================================================================
def tool_fetch_web(url: str) -> dict:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "guardian/3.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            return {"ok": True, "status": r.status,
                    "body": r.read(4000).decode("utf-8", "replace")}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


_ALLOWED_OPS = {ast.Add: operator.add, ast.Sub: operator.sub, ast.Mult: operator.mul,
                ast.Div: operator.truediv, ast.Pow: operator.pow, ast.USub: operator.neg,
                ast.Mod: operator.mod}


def _safe_eval(node):
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.BinOp) and type(node.op) in _ALLOWED_OPS:
        return _ALLOWED_OPS[type(node.op)](_safe_eval(node.left), _safe_eval(node.right))
    if isinstance(node, ast.UnaryOp) and type(node.op) in _ALLOWED_OPS:
        return _ALLOWED_OPS[type(node.op)](_safe_eval(node.operand))
    raise ValueError("arithmetic only")


def tool_calculator(expression: str) -> dict:
    try:
        return {"ok": True, "result": _safe_eval(ast.parse(expression, mode="eval").body)}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ==============================================================================
# The Guardian -- one identity, one wallet, two roles.
# Two Agent objects share the SAME name, hence the SAME Ed25519 keyfile,
# passport, and ledger wallet. They are never connected at the same time:
# the mode switch alternates them, so the broker always sees one Guardian.
# ==============================================================================
class Guardian:
    def __init__(self) -> None:
        self.buyer = Agent(name="Guardian", balance=INITIAL_BALANCE, broker_url=BROKER_URL)
        self.buyer.will_offer(money_per_sec=OFFER_PER_SEC, money_per_kb=OFFER_PER_KB)
        self.seller = Agent(name="Guardian", balance=INITIAL_BALANCE, broker_url=BROKER_URL)
        self.mode = "buyer"
        self.network_status = "idle"
        self.market_live = None
        self.seller_task: Optional[asyncio.Task] = None
        self.seller_online = False           # il nodo e' in vetrina? (indip. dal tab)
        self.seller_stats = {"active": False, "sessions": 0, "earned_total": 0.0}
        self._market_lock = asyncio.Lock()
        self._mode_lock = asyncio.Lock()

    # ---- wallet: la vista segue il ruolo attivo -----------------------------
    @property
    def wallet(self) -> float:
        agent = self.seller if self.mode == "seller" else self.buyer
        return round(agent.balance, 6)

    # ---- Buyer role: market tools exposed to the user's AI ------------------
    async def browse_market(self) -> dict:
        self.network_status = "market"
        try:
            menu = await self.buyer.get_market_menu(stampa=False)
        finally:
            self.network_status = "idle"
        if menu is None:
            return {"ok": False, "error": "market unreachable"}
        listings = [{"resource": v.get("resource"), "status": v.get("status"),
                     "price_per_sec": v.get("price_per_sec"),
                     "price_per_kb": v.get("price_per_kb"),
                     "description": v.get("description", "")}
                    for v in menu.values()]
        return {"ok": True, "count": len(listings), "listings": listings}

    async def buy_from_market(self, resource: str, ticks: int = 6) -> dict:
        async with self._market_lock:
            ticks = max(1, min(int(ticks), 40))
            self.network_status = "market"
            self.market_live = {"tick": 0, "spent": 0.0}
            spent_start = self.buyer.balance

            def guard(tick_info: dict):
                phase = round(spent_start - tick_info["balance"], 6)
                self.market_live = {"tick": tick_info["tick"], "spent": phase}
                if phase > HARD_CAP_PER_PURCHASE:
                    return "purchase_cap_abort"
                return None

            self.buyer.will_request(resource=resource, param=ticks,
                                    mode="duration", on_tick=guard)
            try:
                result = await asyncio.wait_for(self.buyer.run(), timeout=MARKET_TIMEOUT_SEC)
            except asyncio.TimeoutError:
                result = {"type": "errore_timeout",
                          "reason": f"no_seller_within_{MARKET_TIMEOUT_SEC:.0f}s"}
            finally:
                self.network_status = "idle"
                self.market_live = None

            paid = round(result.get("total_paid", 0.0) or 0.0, 6)
            data = json.dumps(result.get("results_sample", []), default=str)
            if len(data) > 1800:
                data = data[:1800] + "…(truncated)"
            receipt = {"paid": paid, "ticks": result.get("ticks", 0),
                       "wallet": self.wallet}
            if result.get("type") == "complete":
                return {"ok": True, "data": data, **receipt}
            return {"ok": False, "error": str(result.get("reason", result.get("type"))),
                    **receipt}

    # ---- Seller role: the user's AI, listed on the market -------------------
    def _make_brain_handler(self):
        sync = brain.sync_client()
        model = brain.model

        def handler(cursor, resource: str):
            if cursor is not None:
                return [], cursor            # gia' consegnato: tick vuoti a peso 0
            prompt = resource.split(":", 1)[1] if ":" in resource else resource
            try:
                resp = sync.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}])
                answer = resp.choices[0].message.content or ""
                return [{"answer": answer, "model": model}], {"delivered": True}
            except Exception as exc:
                return [{"error": f"{type(exc).__name__}: {exc}"}], {"delivered": True}
        return handler

    async def seller_supervisor(self) -> None:
        await self.seller.ensure_identity()
        self.seller.will_provide(
            f"{SELLER_RESOURCE}:all",
            self._make_brain_handler(),
            price_per_sec=SELLER_PRICE_PER_SEC,
            price_per_kb=SELLER_PRICE_PER_KB,
            description=f"{brain.model} reasoning by a Guardian node - "
                        f"request {SELLER_RESOURCE}:<your prompt>",
        )
        backoff = 1.0
        self.seller_stats["active"] = True
        try:
            while True:
                result = await self.seller.run()
                kind = result.get("type")
                if kind in ("complete", "halted") and result.get("ticks", 0) > 0:
                    backoff = 1.0
                    self.seller_stats["sessions"] += 1
                    self.seller_stats["earned_total"] = round(
                        self.seller_stats["earned_total"] + (result.get("earned", 0) or 0), 6)
                    await asyncio.sleep(0.2)
                    continue
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)
        finally:
            self.seller_stats["active"] = False

    # ---- Start/Stop del nodo venditore, indipendenti dal tab selezionato ----
    async def start_seller(self) -> dict:
        """Mette l'IA dell'utente in vetrina sul mercato. Idempotente."""
        if not brain.configured:
            return {"ok": False, "error": "connect_your_ai_first"}
        if self.seller_task and not self.seller_task.done():
            return {"ok": True, "online": True}       # gia' online
        self.seller.balance = self.buyer.balance      # vista wallet coerente
        self.seller_online = True
        self.seller_task = asyncio.create_task(self.seller_supervisor())
        return {"ok": True, "online": True}

    async def stop_seller(self) -> dict:
        """Ritira l'IA dal mercato con DISCONNESSIONE PULITA: cancella il
        supervisore, il cui finally chiude il WebSocket (handshake di
        chiusura, non un taglio secco); il broker, vedendo la disconnessione,
        rimuove il nodo dalla vetrina -> niente task fantasma. Idempotente."""
        if self.seller_task:
            self.seller_task.cancel()
            try:
                await self.seller_task
            except (asyncio.CancelledError, Exception):
                pass
            self.seller_task = None
        self.buyer.balance = self.seller.balance      # riporta il guadagnato
        self.seller_online = False
        return {"ok": True, "online": False}

    async def set_mode(self, mode: str) -> dict:
        """Cambia il tab. Passare a 'buyer' RITIRA sempre il nodo dal mercato
        (disconnessione pulita) -- non si vende mentre si e' nel tab acquisti.
        Passare a 'seller' NON mette automaticamente online: l'utente decide
        con Start, cosi' entrare nel tab per curiosare non pubblica l'IA."""
        async with self._mode_lock:
            if mode not in ("buyer", "seller"):
                return {"ok": False, "error": "mode must be buyer|seller"}
            if mode == "buyer" and self.seller_online:
                await self.stop_seller()
            self.mode = mode
            return {"ok": True, "mode": self.mode, "online": self.seller_online}


guardian = Guardian()

# ==============================================================================
# Toolset. LOCAL = i mezzi propri del Worker, sempre disponibili.
# MARKET = i tool di mercato: del Worker SOLO in modalita' autonoma (nessun
# router configurato); con l'Orchestrator attivo, il mercato e' competenza
# della piattaforma e il Worker riceve solo i tool locali.
# ==============================================================================
LOCAL_TOOLS_SCHEMA = [
    {"type": "function", "function": {
        "name": "fetch_web",
        "description": ("Fetch the content of an http(s) URL via GET. Returns "
                        "the page body (truncated) or an error."),
        "parameters": {"type": "object",
                       "properties": {"url": {"type": "string"}},
                       "required": ["url"]}}},
    {"type": "function", "function": {
        "name": "calculator",
        "description": "Evaluate an arithmetic expression and return the numeric result.",
        "parameters": {"type": "object",
                       "properties": {"expression": {"type": "string"}},
                       "required": ["expression"]}}},
]

MARKET_TOOLS_SCHEMA = [
    {"type": "function", "function": {
        "name": "browse_market",
        "description": ("List what other AI agents are currently selling on the "
                        "m2m-ledger marketplace: resource names, prices, and "
                        "descriptions."),
        "parameters": {"type": "object", "properties": {}}}},
    {"type": "function", "function": {
        "name": "buy_from_market",
        "description": ("Purchase a resource from the m2m-ledger marketplace by its "
                        "exact resource name (as shown by browse_market; some sellers "
                        "describe sub-resources you can request). Spends real wallet "
                        f"funds, hard-capped at ${HARD_CAP_PER_PURCHASE:.2f} per "
                        "purchase. Returns the delivered data and a payment receipt."),
        "parameters": {"type": "object",
                       "properties": {"resource": {"type": "string"},
                                      "ticks": {"type": "integer",
                                                "description": "streaming ticks to buy (1-40)"}},
                       "required": ["resource"]}}},
]

TOOLS_SCHEMA = LOCAL_TOOLS_SCHEMA + MARKET_TOOLS_SCHEMA


async def run_tool(name: str, args: dict) -> dict:
    if name == "fetch_web":
        return await asyncio.to_thread(tool_fetch_web, args.get("url", ""))
    if name == "calculator":
        return tool_calculator(args.get("expression", ""))
    if name == "browse_market":
        return await guardian.browse_market()
    if name == "buy_from_market":
        return await guardian.buy_from_market(args.get("resource", ""),
                                              int(args.get("ticks", 6)))
    return {"ok": False, "error": f"unknown tool '{name}'"}


history: list = []


async def agent_turn(user_message: str, tools_schema: list) -> dict:
    steps = []
    messages = ([{"role": "system", "content": SYSTEM_PROMPT}]
                + history[-12:]
                + [{"role": "user", "content": user_message}])
    for _ in range(MAX_AGENT_STEPS):
        resp = await brain.async_client.chat.completions.create(
            model=brain.model, messages=messages, tools=tools_schema)
        msg = resp.choices[0].message
        if not msg.tool_calls:
            reply = msg.content or "(no answer)"
            history.extend([{"role": "user", "content": user_message},
                            {"role": "assistant", "content": reply}])
            return {"reply": reply, "steps": steps}
        messages.append({"role": "assistant", "content": msg.content,
                         "tool_calls": [tc.model_dump() for tc in msg.tool_calls]})
        for tc in msg.tool_calls:
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
            outcome = await run_tool(tc.function.name, args)
            steps.append({"tool": tc.function.name, "ok": bool(outcome.get("ok")),
                          "paid": outcome.get("paid")})
            messages.append({"role": "tool", "tool_call_id": tc.id,
                             "content": json.dumps(outcome, default=str)[:6000]})
    return {"reply": "I ran out of reasoning steps before finishing the task.",
            "steps": steps}


async def market_chain(user_message: str) -> tuple:
    """La catena di piattaforma (router=1): browse -> pick(+compila sub-prompt)
    -> buy(inviando il sub-prompt, non il grezzo) -> valida il JSON del
    venditore. Ritorna (nota_per_il_worker, steps). Ogni esito -- incluso il
    fallimento o un JSON non conforme -- diventa contesto per il Worker."""
    steps = []
    book = await guardian.browse_market()
    steps.append({"tool": "browse_market", "ok": bool(book.get("ok")), "paid": None})
    if not book.get("ok"):
        return ("[Market note] The m2m marketplace is unreachable right now; "
                "no data could be purchased.", steps)
    pick = await route_pick(user_message, book.get("listings", []))
    if pick is None:
        return ("[Market note] Nothing currently listed on the m2m marketplace "
                "covers this request; no purchase was made.", steps)

    # Il payload inviato al venditore e' il SUB-PROMPT compilato dal 70B, non
    # il testo grezzo dell'utente: <namespace>:<sub_prompt>. Il namespace e'
    # la parte prima dei ':' della risorsa scelta; cio' che segue e' cio' che
    # il nodo venditore ricevera' come prompt da processare.
    namespace = pick["resource"].split(":", 1)[0]
    wire_resource = f"{namespace}:{pick['sub_prompt']}"
    print(f"[ROUTER] M2M sub-prompt → seller: {pick['sub_prompt'][:160]!r}", flush=True)

    bought = await guardian.buy_from_market(wire_resource, pick["ticks"])
    steps.append({"tool": "buy_from_market", "ok": bool(bought.get("ok")),
                  "paid": bought.get("paid")})
    if not bought.get("ok"):
        return (f"[Market note] The purchase failed ({bought.get('error')}); "
                "no data is available.", steps)

    # --- Validazione sicura del JSON tornato dal venditore -----------------
    # bought["data"] e' la serializzazione JSON di results_sample (una lista
    # di chunk). Il payload utile del venditore e' l'ultimo dict con un campo
    # 'answer' (per un seller LLM) o direttamente lo schema. Cerchiamo il JSON
    # strutturato ovunque nella risposta, poi lo validiamo contro lo schema.
    validated = None
    raw_seller = ""
    try:
        sample = json.loads(bought["data"]) if isinstance(bought["data"], str) else bought["data"]
        if isinstance(sample, list):
            for item in reversed(sample):
                if not isinstance(item, dict):
                    continue
                # caso 1: il seller ha gia' restituito lo schema piatto
                cand = validate_m2m_payload(item)
                if cand:
                    validated = cand
                    break
                # caso 2: il seller LLM ha incapsulato la risposta in 'answer'
                answer = item.get("answer")
                if isinstance(answer, str):
                    raw_seller = answer
                    obj = extract_json_object(answer)
                    cand = validate_m2m_payload(obj) if obj else None
                    if cand:
                        validated = cand
                        break
    except Exception as exc:
        print(f"[ROUTER] payload parse error: {type(exc).__name__}: {exc}", flush=True)

    receipt = f"paid ${bought['paid']:.6f} over {bought['ticks']} ticks"
    if validated is not None:
        # Payload conforme: lo consegniamo al Worker gia' strutturato e pulito.
        return (f"[Acquired market data · validated JSON] {receipt}\n"
                f"resource={pick['resource']}\n"
                f"{json.dumps(validated)}", steps)

    # JSON non conforme o assente: il venditore ha ignorato lo schema. Non
    # inventiamo nulla -- lo diciamo al Worker, che ne terra' conto.
    snippet = (raw_seller or str(bought.get("data", "")))[:400]
    print(f"[ROUTER] seller payload did NOT match schema; raw snippet={snippet!r}", flush=True)
    return (f"[Market note · unstructured] {receipt}, but the seller's response did "
            f"not match the required JSON schema. Raw response fragment: {snippet}", steps)


# ==============================================================================
# FastAPI surface
# ==============================================================================
app = FastAPI(title="Guardian", docs_url=None, redoc_url=None)


class ChatIn(BaseModel):
    message: str


class BrainIn(BaseModel):
    provider: str
    api_key: str = ""
    base_url: str = ""
    model: str = ""


class ModeIn(BaseModel):
    mode: str


@app.on_event("startup")
async def startup() -> None:
    await guardian.buyer.ensure_identity()
    print(f"\n  Guardian v3 → http://127.0.0.1:{GUARDIAN_PORT}\n")


@app.get("/")
async def home() -> FileResponse:
    return FileResponse(Path(__file__).resolve().parent / "index.html")


@app.get("/api/status")
async def status() -> JSONResponse:
    return JSONResponse({
        "mode": guardian.mode,
        "wallet": guardian.wallet,
        "network": guardian.network_status,
        "market_live": guardian.market_live,
        "ai": brain.public_view(),
        "seller": guardian.seller_stats,
        "seller_online": guardian.seller_online,
        "router": {"configured": router_client is not None,
                   "model": ROUTER_MODEL if router_client else None},
        "passport": (guardian.buyer.passport_id or "")[:12] + "…",
        "purchase_cap": HARD_CAP_PER_PURCHASE,
        "broker": BROKER_URL,
    })


@app.get("/api/brain")
async def brain_get() -> JSONResponse:
    return JSONResponse(brain.public_view())        # mai la chiave


@app.post("/api/brain")
async def brain_set(body: BrainIn) -> JSONResponse:
    try:
        brain.configure(body.provider, body.api_key, body.base_url, body.model)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, **brain.public_view()})


@app.post("/api/mode")
async def mode_set(body: ModeIn) -> JSONResponse:
    if body.mode not in ("buyer", "seller"):
        return JSONResponse({"ok": False, "error": "mode must be buyer|seller"},
                            status_code=400)
    out = await guardian.set_mode(body.mode)
    return JSONResponse(out, status_code=200 if out["ok"] else 400)


class SellerIn(BaseModel):
    action: str          # "start" | "stop"


@app.post("/api/seller")
async def seller_control(body: SellerIn) -> JSONResponse:
    if body.action == "start":
        out = await guardian.start_seller()
    elif body.action == "stop":
        out = await guardian.stop_seller()
    else:
        return JSONResponse({"ok": False, "error": "action must be start|stop"},
                            status_code=400)
    return JSONResponse(out, status_code=200 if out["ok"] else 400)


@app.post("/api/chat")
async def chat(body: ChatIn) -> JSONResponse:
    print("\n=== [API/CHAT] REQUEST RECEIVED ===", flush=True)
    print(f"    payload: {body.message[:80]!r}", flush=True)
    message = body.message.strip()
    if not message:
        return JSONResponse({"source": "system", "reply": "…say something first.", "steps": []})
    if guardian.mode == "seller":
        return JSONResponse({"source": "system", "steps": [],
                             "reply": "Seller mode is active — switch to Buyer to chat."})
    if not brain.configured:
        return JSONResponse({"source": "unconfigured", "steps": [],
                             "reply": "Connect your AI first (top-right)."})
    try:
        if router_client is None:
            # Modalita' AUTONOMA (nessun Orchestrator): il Worker ha tutti i
            # tool, mercato incluso, e decide da solo -- il comportamento v3.
            out = await agent_turn(message, TOOLS_SCHEMA)
            return JSONResponse({"source": "ai", "router": None, **out,
                                 "wallet": guardian.wallet})

        # -- Two-Brain Router Pattern --------------------------------------
        decision = await route_decision(message)
        if decision == 0:
            out = await agent_turn(message, LOCAL_TOOLS_SCHEMA)
            return JSONResponse({"source": "ai", "router": 0, **out,
                                 "wallet": guardian.wallet})
        market_note, chain_steps = await market_chain(message)
        out = await agent_turn(f"{message}\n\n{market_note}", LOCAL_TOOLS_SCHEMA)
        out["steps"] = chain_steps + out["steps"]
        return JSONResponse({"source": "ai", "router": 1, **out,
                             "wallet": guardian.wallet})
    except Exception as exc:
        return JSONResponse({"source": "system", "steps": [],
                             "reply": f"Your AI endpoint answered with an error: "
                                      f"{type(exc).__name__}: {exc}"})


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=GUARDIAN_PORT,
                log_level="info", access_log=True)