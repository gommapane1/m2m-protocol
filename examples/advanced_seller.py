"""
================================================================================
 advanced_seller.py -- THE MEMORY REFINERY: buys raw context, sells distilled state
================================================================================
An advanced m2m-protocol provider demonstrating that the protocol is fully
PAYLOAD-AGNOSTIC: the broker routes Ed25519-signed envelopes and settles
micropayments -- it neither knows nor cares that the bytes flowing through
it are a megabyte-scale AI conversation memory.

THE TWO CHANNELS AT WORK (both native to the protocol):
  * consumer -> provider : the DATA leg of the barter. The buyer declares a
    data_file in will_offer(); the SDK streams it one signed chunk per tick;
    this provider receives every chunk through the `on_data` callback.
  * provider -> consumer : the COMPUTE leg. While the upload streams in,
    the handler returns empty progress chunks (the buyer is paying for the
    pipe + the service, tick by tick). Once the transfer integrity-checks
    (sha256), the refinery distills the memory and ships ONE dense result
    chunk -- which, under mode="count" param=1, settles and completes the
    session on its own.

WIRE FRAMING (application-level, on top of the protocol's data channel):
    line 1 : {"m2m_transfer":"v1","total_chars":N,"sha256":...,"chunks":K}\n
    rest   : the raw JSON payload, exactly N characters, hash-verified.

RUN
    python3 advanced_seller.py          # connects to the live global broker
================================================================================
"""

import asyncio
import hashlib
import json
import logging
import os
import re
import sys
import time
import zlib
from collections import Counter
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
PRICE_PER_SEC = 0.003
PRICE_PER_KB = 0.005
BACKOFF_START, BACKOFF_CAP = 1.0, 10.0

if os.name == "nt":
    os.system("")
DIM, BOLD, RESET = "\033[2m", "\033[1m", "\033[0m"
GOLD, CYAN, GREEN, RED = "\033[33m", "\033[36m", "\033[32m", "\033[31m"

KB = 1024.0


def _bar(done: float, total: float, width: int = 26) -> str:
    frac = 0.0 if total <= 0 else min(1.0, done / total)
    filled = int(frac * width)
    return "█" * filled + "░" * (width - filled) + f" {frac * 100:5.1f}%"


# ==============================================================================
# Transfer state machine: fed by on_data (network side), read by the handler
# (compute side). Writes are append-only, reads are snapshot-style.
# ==============================================================================
class MemoryIntake:
    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.buffer: str = ""
        self.header = None
        self.t0 = time.perf_counter()

    # ---- network side: called once per incoming signed data_chunk ----------
    def on_data(self, chunk: str) -> None:
        self.buffer += chunk
        if self.header is None and "\n" in self.buffer:
            head, self.buffer = self.buffer.split("\n", 1)
            self.header = json.loads(head)
            print(f"\n{GOLD}{BOLD}◆ TRANSFER HEADER RECEIVED{RESET}  "
                  f"{DIM}{self.header['total_chars'] / KB:,.1f} KB announced · "
                  f"{self.header['chunks']} chunks · sha256 {self.header['sha256'][:16]}…{RESET}")
            return
        if self.header:
            got = len(self.buffer)
            total = self.header["total_chars"]
            print(f"  {CYAN}▼ RX{RESET} +{len(chunk) / KB:6.1f} KB  "
                  f"{DIM}[{_bar(got, total)}]  {got / KB:8.1f} / {total / KB:,.1f} KB{RESET}")

    # ---- compute side: interrogated by the handler each tick ----------------
    def complete_payload(self):
        if self.header and len(self.buffer) >= self.header["total_chars"]:
            return self.buffer[: self.header["total_chars"]]
        return None


# ==============================================================================
# The actual "heavy compute": a real distillation pass, not a sleep().
# ==============================================================================
def distill(raw_json: str, expected_sha: str, transfer_sec: float) -> dict:
    t0 = time.perf_counter()

    actual_sha = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
    if actual_sha != expected_sha:
        return {"error": "integrity_check_failed",
                "detail": f"sha256 mismatch: expected {expected_sha[:16]}…, got {actual_sha[:16]}…"}
    print(f"{GREEN}  ✓ INTEGRITY VERIFIED{RESET}  sha256 {actual_sha[:16]}… "
          f"({len(raw_json) / KB:,.1f} KB, streamed in {transfer_sec:.1f}s)")

    memory = json.loads(raw_json)
    turns = memory.get("conversation", [])
    raw_bytes = len(raw_json.encode("utf-8"))

    print(f"  {DIM}distilling {len(turns)} turns of cognitive memory…{RESET}")

    # -- lexical topic extraction over the whole history ----------------------
    words = Counter()
    for t in turns:
        for w in re.findall(r"[a-zA-Z]{5,}", t.get("text", "").lower()):
            words[w] += 1
    top_topics = [w for w, _ in words.most_common(8)]

    # -- key-moment selection: importance-flagged + conversation edges --------
    flagged = [t for t in turns if t.get("importance", 0) >= 0.85]
    key_moments = ([{"turn": t["turn"], "role": t["role"], "text": t["text"][:140]}
                    for t in flagged[:6]]
                   or [{"turn": t["turn"], "role": t["role"], "text": t["text"][:140]}
                       for t in turns[:2] + turns[-2:]])

    # -- cold-storage feasibility: REAL zlib pass to measure entropy ----------
    compressed = zlib.compress(raw_json.encode("utf-8"), level=6)
    ratio = raw_bytes / max(1, len(compressed))

    tool_calls = sum(1 for t in turns if t.get("tool_call"))
    distilled = {
        "service": RESOURCE,
        "distilled_state": {
            "summary": (f"{len(turns)}-turn dialogue "
                        f"({sum(1 for t in turns if t['role'] == 'user')} user / "
                        f"{sum(1 for t in turns if t['role'] == 'assistant')} assistant), "
                        f"{tool_calls} tool invocations. "
                        f"Dominant topics: {', '.join(top_topics[:4])}."),
            "top_topics": top_topics,
            "key_moments": key_moments,
            "stats": {
                "turns": len(turns),
                "tool_calls": tool_calls,
                "raw_bytes": raw_bytes,
                "est_tokens": raw_bytes // 4,
            },
        },
        "integrity": {"sha256_of_raw": actual_sha, "verified": True},
        "cold_storage": {
            "codec": "zlib-6",
            "compressed_bytes": len(compressed),
            "compression_ratio": round(ratio, 2),
            "note": "full lossless archive available as a separate purchase",
        },
        "refinery_time_sec": round(time.perf_counter() - t0, 3),
    }
    out_bytes = len(json.dumps(distilled).encode("utf-8"))
    print(f"{GREEN}{BOLD}  ✓ DISTILLATION COMPLETE{RESET}  "
          f"{raw_bytes / KB:,.1f} KB → {out_bytes / KB:.1f} KB dense state  "
          f"({raw_bytes / out_bytes:,.0f}x denser · lossless archive would be "
          f"{len(compressed) / KB:,.1f} KB, ratio {ratio:.1f}x)")
    return distilled


def make_handler(intake: MemoryIntake):
    """cursor is the per-session phase: None -> receiving -> delivered."""
    def handler(cursor, resource: str):
        if cursor is None:
            intake.reset()
            print(f"\n{GOLD}{BOLD}◆ CONTRACT MATCHED{RESET}  {DIM}resource={resource} · "
                  f"the buyer is streaming its memory — billing per second of service{RESET}")
            return [], {"phase": "receiving"}
        payload = intake.complete_payload()
        if payload is None:
            return [], cursor                       # still receiving: progress tick
        transfer_sec = time.perf_counter() - intake.t0
        result = distill(payload, intake.header["sha256"], transfer_sec)
        print(f"  {DIM}shipping distilled state through the signed channel…{RESET}")
        return [result], {"phase": "delivered"}
    return handler


async def refinery_supervisor() -> None:
    intake = MemoryIntake()
    seller = Agent(name="Memory-Refinery", balance=0.0, broker_url=BROKER_URL)
    seller.will_provide(
        RESOURCE,
        make_handler(intake),
        on_data=intake.on_data,       # <- the data leg of the barter, delivered to us
        price_per_sec=PRICE_PER_SEC,
        price_per_kb=PRICE_PER_KB,
        description="Cognitive memory compression - stream raw context in, get distilled state back",
    )
    await seller.ensure_identity()

    print(f"\n{BOLD}{GOLD}╔══════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{GOLD}║   MEMORY REFINERY  ·  context distillation on m2m-protocol     ║{RESET}")
    print(f"{BOLD}{GOLD}╚══════════════════════════════════════════════════════════════╝{RESET}")
    print(f"  {CYAN}passport{RESET}  {seller.passport_id[:16]}…  {DIM}(Ed25519){RESET}")
    print(f"  {CYAN}listing{RESET}   {RESOURCE}  @  ${PRICE_PER_SEC}/sec + ${PRICE_PER_KB}/KB")
    print(f"  {CYAN}broker{RESET}    {BROKER_URL}\n")

    backoff, session_n = BACKOFF_START, 0
    while True:
        session_n += 1
        print(f"{DIM}── listed on the global order book · waiting for a memory-bound "
              f"agent (session #{session_n}) ──{RESET}")
        result = await seller.run()
        kind, why = result.get("type"), result.get("reason", "")
        if kind in ("complete", "halted") and result.get("ticks", 0) > 0:
            backoff = BACKOFF_START
            print(f"{GREEN}{BOLD}💰 SETTLED{RESET}  earned {GREEN}${result.get('earned', 0):.6f}{RESET} "
                  f"over {result.get('ticks', 0)} ticks · received "
                  f"{result.get('data_received_bytes', 0) / KB:,.1f} KB of client data · "
                  f"lifetime balance {GREEN}${seller.balance:.6f}{RESET}\n")
            await asyncio.sleep(0.2)
            continue
        print(f"{DIM}broker unreachable or session dropped ({kind}/{why}) — "
              f"retrying in {backoff:.0f}s…{RESET}")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, BACKOFF_CAP)


if __name__ == "__main__":
    try:
        asyncio.run(refinery_supervisor())
    except KeyboardInterrupt:
        print(f"\n{DIM}Refinery shutting down.{RESET}")
    sys.exit(0)