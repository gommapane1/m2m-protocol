"""
================================================================================
 advanced_buyer.py -- THE OVERFLOWING MIND: an AI that pays to forget wisely
================================================================================
An advanced m2m-ledger consumer. This agent's short-term memory (its context
window) is full: a megabyte-scale JSON array of conversation history, tool
traces and raw embeddings. Rather than truncating blindly, it goes to the
market and buys the `compress_cognitive_memory` service:

  1. it serializes its memory, prefixes a one-line transfer header
     ({total_chars, sha256, chunks}) and hands the whole thing to the SDK
     as the DATA leg of the barter (will_offer(data_file=...));
  2. the SDK streams it to the matched provider one Ed25519-signed chunk
     per settlement tick -- watch the kilobytes and the balance move in
     lockstep, that is the protocol's atomicity made visible;
  3. mode="count", param=1: the ONE dense chunk coming back (the distilled
     memory) settles and completes the session automatically.

The broker never parses any of it. It routes signed envelopes and moves
money. The payload is entirely our business -- that is the whole point.

RUN
    python3 advanced_buyer.py           # connects to the live global broker
================================================================================
"""

import asyncio
import hashlib
import json
import logging
import os
import random
import sys
import tempfile
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
RESOURCE = "compress_cognitive_memory"
INITIAL_BALANCE = 1.00
OFFER_PER_SEC = 0.003          # you pay for the refinery's time…
OFFER_PER_KB = 0.005           # …and for the weight of the distilled state shipped back
CHUNK_CHARS = 64 * 1024        # one signed 64 KB envelope per settlement tick
MARKET_TIMEOUT_SEC = float(os.environ.get("M2M_MARKET_TIMEOUT", "120"))
MEMORY_TURNS = int(os.environ.get("M2M_MEMORY_TURNS", "620"))

if os.name == "nt":
    os.system("")
DIM, BOLD, RESET = "\033[2m", "\033[1m", "\033[0m"
GOLD, CYAN, GREEN, RED = "\033[33m", "\033[36m", "\033[32m", "\033[31m"

KB = 1024.0


# ==============================================================================
# The overflowing context window: a big, messy, REALISTIC memory dump.
# Deterministic (seeded) so every demo run ships the same bytes.
# ==============================================================================
def build_massive_memory(turns: int) -> dict:
    rng = random.Random(42)
    topics = ["websocket reconnection", "supabase ledger", "ed25519 signatures",
              "exponential backoff", "order book discovery", "context overflow",
              "micropayment settlement", "binance feed", "stockfish oracle"]
    fillers = ["Let me think through", "The core issue is", "We validated that",
               "Empirically we measured", "The architecture requires",
               "A subtle race condition around", "The invariant we protect is"]
    conversation = []
    for i in range(turns):
        role = "user" if i % 2 == 0 else "assistant"
        topic = rng.choice(topics)
        text = (f"{rng.choice(fillers)} {topic}. " * rng.randint(9, 22)
                + f"[trace#{i:04d}] " + " ".join(rng.choice(topics).split())
                * rng.randint(2, 5))
        turn = {
            "turn": i,
            "role": role,
            "text": text,
            "ts": 1784200000 + i * 37,
            "importance": round(rng.random(), 3),
            # raw 128-dim embedding vectors: the classic context-window ballast
            "embedding": [round(rng.uniform(-1, 1), 5) for _ in range(128)],
        }
        if role == "assistant" and rng.random() < 0.18:
            turn["tool_call"] = {"name": rng.choice(["web_search", "run_code", "read_file"]),
                                 "args": {"query": topic}, "latency_ms": rng.randint(80, 2400)}
        conversation.append(turn)
    return {"agent": "overflowing-mind-v1", "schema": "cognitive_memory/1",
            "conversation": conversation}


def frame_for_transfer(memory: dict) -> tuple[str, dict]:
    """Application-level framing on top of the protocol's data channel:
    one JSON header line + the raw payload. The sha256 lets the refinery
    prove, cryptographically, that it distilled EXACTLY what we sent."""
    payload = json.dumps(memory, separators=(",", ":"))
    header = {
        "m2m_transfer": "v1",
        "total_chars": len(payload),
        "sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "chunks": -(-len(payload) // CHUNK_CHARS),      # ceil
    }
    return json.dumps(header) + "\n" + payload, header


async def main() -> None:
    buyer = Agent(name="Overflowing-Mind", balance=INITIAL_BALANCE, broker_url=BROKER_URL)
    await buyer.ensure_identity()

    print(f"\n{BOLD}{GOLD}╔══════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{GOLD}║   OVERFLOWING MIND  ·  buying memory compression on-market   ║{RESET}")
    print(f"{BOLD}{GOLD}╚══════════════════════════════════════════════════════════════╝{RESET}")
    print(f"  {CYAN}passport{RESET}  {buyer.passport_id[:16]}…  {DIM}(Ed25519){RESET}")
    print(f"  {CYAN}wallet{RESET}    ${INITIAL_BALANCE:.2f}")
    print(f"  {CYAN}broker{RESET}    {BROKER_URL}\n")

    # -- 1. the crisis: measure the overflowing context -----------------------
    print(f"{DIM}  synthesizing the overflowing context window "
          f"({MEMORY_TURNS} turns, embeddings, tool traces)…{RESET}")
    memory = build_massive_memory(MEMORY_TURNS)
    framed, header = frame_for_transfer(memory)
    payload_kb = header["total_chars"] / KB
    print(f"{RED}{BOLD}  ⚠ CONTEXT WINDOW FULL{RESET}  raw memory: "
          f"{BOLD}{payload_kb:,.1f} KB{RESET} "
          f"(~{header['total_chars'] // 4:,} tokens) — offloading to the market.\n")

    # -- 2. the contract: heavy JSON as the data leg of the barter ------------
    with tempfile.NamedTemporaryFile("w", suffix=".m2m.json", delete=False,
                                     encoding="utf-8") as fh:
        fh.write(framed)
        transfer_file = fh.name

    print(f"{GOLD}{BOLD}  ┌─ M2M MARKETPLACE · COGNITIVE OFFLOAD CONTRACT ──────────────┐{RESET}")
    print(f"  {GOLD}│{RESET} resource   {RESOURCE}  (param=1, mode=count)")
    print(f"  {GOLD}│{RESET} payload    {payload_kb:,.1f} KB in {header['chunks']} signed chunks "
          f"of {CHUNK_CHARS // 1024} KB")
    print(f"  {GOLD}│{RESET} integrity  sha256 {header['sha256'][:20]}…")
    print(f"  {GOLD}│{RESET} offer      ${OFFER_PER_SEC}/sec of service + ${OFFER_PER_KB}/KB returned")
    print(f"  {GOLD}{BOLD}  └──────────────────────────────────────────────────────────────┘{RESET}")

    shipped = {"n": 0}

    def upload_ticker(tick_info: dict):
        shipped["n"] = min(tick_info["tick"], header["chunks"])
        on_wire = min(shipped["n"] * CHUNK_CHARS, header["total_chars"]) / KB
        print(f"  {CYAN}▲ TX{RESET} {DIM}tick #{tick_info['tick']:>2} · "
              f"~chunk {shipped['n']}/{header['chunks']} on the wire "
              f"({on_wire:8,.1f} KB) · {GREEN}-${tick_info['spent_this_tick']:.6f}{RESET}"
              f"{DIM} · wallet ${tick_info['balance']:.6f}{RESET}")
        return None

    buyer.will_offer(money_per_sec=OFFER_PER_SEC, money_per_kb=OFFER_PER_KB,
                     data_file=transfer_file, chunk_chars=CHUNK_CHARS)
    buyer.will_request(resource=RESOURCE, param=1, mode="count", on_tick=upload_ticker)

    try:
        result = await asyncio.wait_for(buyer.run(), timeout=MARKET_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        result = {"type": "errore_timeout", "reason": f"no_provider_within_{MARKET_TIMEOUT_SEC:.0f}s"}
    finally:
        os.unlink(transfer_file)

    # -- 3. the goods ----------------------------------------------------------
    sample = [x for x in result.get("results_sample", []) if isinstance(x, dict)]
    distilled = sample[-1] if sample else None

    if result.get("type") == "complete" and distilled and "error" not in distilled:
        paid = result.get("total_paid", 0.0)
        state = distilled["distilled_state"]
        dense_kb = len(json.dumps(distilled).encode("utf-8")) / KB
        print(f"\n{GREEN}{BOLD}  💸 SETTLED{RESET}  paid {GREEN}${paid:.6f}{RESET} across "
              f"{result.get('ticks', 0)} streamed ticks · wallet {GREEN}${buyer.balance:.6f}{RESET}")
        print(f"{CYAN}{BOLD}  🧠 DISTILLED MEMORY RECEIVED{RESET}  "
              f"{payload_kb:,.1f} KB → {dense_kb:.1f} KB "
              f"({payload_kb / dense_kb:,.0f}x denser)")
        print(f"     {DIM}integrity : provider verified sha256 "
              f"{distilled['integrity']['sha256_of_raw'][:20]}… ✓{RESET}")
        print(f"     {DIM}archive   : lossless zlib ratio "
              f"{distilled['cold_storage']['compression_ratio']}x available on request{RESET}")
        print(f"\n  {BOLD}summary{RESET}     {state['summary']}")
        print(f"  {BOLD}top topics{RESET}  {', '.join(state['top_topics'])}")
        print(f"  {BOLD}key moments{RESET}")
        for m in state["key_moments"][:4]:
            print(f"    {DIM}· turn {m['turn']:>3} [{m['role']:<9}]{RESET} {m['text'][:88]}…")
        print(f"\n{BOLD}{GOLD}╔═══════════════════ FINAL LEDGER ═══════════════════╗{RESET}")
        print(f"  uploaded          {payload_kb:,.1f} KB of raw cognition")
        print(f"  received          {dense_kb:.1f} KB of distilled state")
        print(f"  total paid        {GREEN}${paid:.6f}{RESET}")
        print(f"  wallet remaining  {GREEN}${buyer.balance:.6f}{RESET}  (of ${INITIAL_BALANCE:.2f})")
        print(f"  every byte and every cent: one Ed25519-signed, atomically settled channel")
        print(f"{BOLD}{GOLD}╚═════════════════════════════════════════════════════╝{RESET}\n")
    else:
        why = (distilled or {}).get("error") or result.get("reason", result.get("type"))
        print(f"\n{RED}  ✗ offload failed ({why}) — memory retained locally, "
              f"wallet ${buyer.balance:.6f}.{RESET}\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{DIM}Offload aborted (Ctrl+C).{RESET}")
    sys.exit(0)