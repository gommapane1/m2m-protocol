"""
================================================================================
 chess_buyer.py -- THE PLAYER: an autonomous agent that BUYS grandmaster thought
================================================================================
Plays Black in a sharp Caro-Kann (Advance, Tal Variation: 4.h4 h5) against a
local engine. The agent runs on a deliberately starved compute budget --
depth 5 only. When its shallow search cannot separate the top candidate
moves, it does what any rational economic agent does: it goes to the market.

  1. It builds a signed JSON contract: resource "depth_20_analysis:<FEN>",
     mode="count", param=1  ("I am buying exactly ONE premium analysis").
  2. The broker verifies the Ed25519 envelope and matches it with the Oracle.
  3. While Stockfish thinks at depth 20, the session streams: every tick the
     Player pays a few tenths of a cent FOR THE ORACLE'S TIME -- watch the
     balance drain in real time.
  4. The single delivered chunk (the premium move) auto-completes the
     session. The move is played on the board. Capitalism, but for compute.

RUN
    export STOCKFISH_PATH=/usr/games/stockfish
    python3 examples/chess_buyer.py

Requires: pip install chess   +   the Stockfish binary (see chess_seller.py header).
================================================================================
"""

import asyncio
import contextlib
import json
import logging
import os
import shutil
import sys
from pathlib import Path
from typing import Optional

os.environ.setdefault("M2M_TRACE", "0")

# I print sono la demo: line-buffering esplicito cosi' arrivano in tempo
# reale anche quando lo stdout e' una pipe (asciinema, tee, redirect).
sys.stdout.reconfigure(line_buffering=True)   # regia: via i trace SDK, resta solo lo show

import chess
import chess.engine

try:
    from m2m_ledger import Agent, DEFAULT_BROKER_URL, PROTOCOL_VERSION
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    from m2m_ledger import Agent, DEFAULT_BROKER_URL, PROTOCOL_VERSION

logging.getLogger().setLevel(logging.WARNING)

BROKER_URL = os.environ.get("M2M_BROKER_URL", DEFAULT_BROKER_URL)
RESOURCE_NAMESPACE = "depth_20_analysis"
INITIAL_BALANCE = 1.00
LOCAL_DEPTH = int(os.environ.get("M2M_LOCAL_DEPTH", "5"))       # il vincolo di risorse simulato
COMPLEXITY_CP_WINDOW = 30      # top-2 mosse entro 30 centipawn a depth 5 = "non so decidere"
MAX_FULLMOVES = int(os.environ.get("M2M_MAX_FULLMOVES", "14"))  # durata del recording
OFFER_PER_SEC = 0.012          # ~$0.05 per una pensata depth-20 da ~3-4s (misurata)
OFFER_PER_KB = 0.02
HARD_CAP_PER_ANALYSIS = 0.12   # oltre questo, l'agente PIANTA la sessione (cancel volontario)
MARKET_TIMEOUT_SEC = float(os.environ.get("M2M_MARKET_TIMEOUT", "60"))
# ^ tetto TOTALE per un acquisto (match + pensata + settlement). Se nessun
#   Oracle e' in vetrina, senza questo timeout l'agente aspetterebbe un
#   abbinamento per sempre: una demo deve degradare, mai congelarsi.

# La linea book: Caro-Kann Advance, Tal Variation -- tagliente e riconoscibile.
BOOK_UCI = ["e2e4", "c7c6", "d2d4", "d7d5", "e4e5", "c8f5", "h2h4", "h7h5", "c2c4", "e7e6"]
BOOK_NAME = "Caro-Kann · Advance, Tal Variation (4.h4 h5!?)"

if os.name == "nt":
    os.system("")
DIM, BOLD, RESET = "\033[2m", "\033[1m", "\033[0m"
GOLD, CYAN, GREEN, RED = "\033[33m", "\033[36m", "\033[32m", "\033[31m"


def find_stockfish() -> str:
    candidates = [os.environ.get("STOCKFISH_PATH"), shutil.which("stockfish"),
                  "/usr/games/stockfish", "/usr/local/bin/stockfish",
                  "/opt/homebrew/bin/stockfish",
                  r"C:\Program Files\Stockfish\stockfish.exe"]
    for c in candidates:
        if c and Path(c).exists():
            return c
    raise SystemExit(f"{RED}Stockfish binary not found. Install it (apt/brew) or set STOCKFISH_PATH.{RESET}")


def show_board(board: chess.Board) -> None:
    art = board.unicode(empty_square="·") if os.environ.get("M2M_ASCII") != "1" else str(board)
    for line in art.splitlines():
        print(f"     {line}")
    print(f"     {DIM}move {board.fullmove_number} · {'White' if board.turn else 'Black'} to play{RESET}")


class Player:
    def __init__(self) -> None:
        self.engine = chess.engine.SimpleEngine.popen_uci(find_stockfish())
        self.engine.configure({"Threads": 2, "Hash": 128})
        self.agent = Agent(name="Player-Black", balance=INITIAL_BALANCE, broker_url=BROKER_URL)
        self.agent.will_offer(money_per_sec=OFFER_PER_SEC, money_per_kb=OFFER_PER_KB)
        self.purchases = 0
        self.spent_on_market = 0.0

    # ---- il motore LOCALE, volutamente denutrito ---------------------------
    async def local_analysis(self, board: chess.Board):
        """Depth 5, multipv 2: abbastanza per giocare, non abbastanza per
        CAPIRE una posizione tagliente -- il gap tra le prime due mosse e'
        il termometro dell'incertezza."""
        infos = await asyncio.to_thread(
            self.engine.analyse, board, chess.engine.Limit(depth=LOCAL_DEPTH), multipv=2)
        best = infos[0]["pv"][0]
        s0 = infos[0]["score"].pov(board.turn).score(mate_score=10_000)
        s1 = (infos[1]["score"].pov(board.turn).score(mate_score=10_000)
              if len(infos) > 1 else s0 - 10_000)
        return best, s0, abs(s0 - s1)

    # ---- l'acquisto sul mercato --------------------------------------------
    async def buy_premium_move(self, board: chess.Board) -> Optional[dict]:
        fen = board.fen()
        contract_preview = {"resource": f"{RESOURCE_NAMESPACE}:<FEN>", "param": 1,
                            "mode": "count", "offer": {"money_per_sec": OFFER_PER_SEC,
                                                       "money_per_kb": OFFER_PER_KB}}
        print(f"\n{GOLD}{BOLD}  ┌─ M2M MARKETPLACE · PREMIUM COMPUTE REQUEST ─────────────────┐{RESET}")
        print(f"  {GOLD}│{RESET} contract  {DIM}{json.dumps(contract_preview, separators=(',', ':'))}{RESET}")
        print(f"  {GOLD}│{RESET} FEN       {DIM}{fen}{RESET}")
        print(f"  {GOLD}│{RESET} envelope  Ed25519-signed · passport {CYAN}{self.agent.passport_id[:16]}…{RESET}")
        print(f"  {GOLD}│{RESET} hard cap  ${HARD_CAP_PER_ANALYSIS:.2f} per analysis {DIM}(agent aborts beyond this){RESET}")
        print(f"  {GOLD}{BOLD}└──────────────────────────────────────────────────────────────┘{RESET}")

        def budget_guard(tick_info: dict) -> Optional[str]:
            spent = tick_info["spent_this_tick"]
            print(f"  {DIM}⏳ tick #{tick_info['tick']:>2} · the Oracle is thinking · "
                  f"streamed {GREEN}-${spent:.6f}{RESET}{DIM} · balance ${tick_info['balance']:.6f}{RESET}")
            phase_spent = tick_info["total_spent"] - self._spent_before
            if phase_spent > HARD_CAP_PER_ANALYSIS:
                print(f"  {RED}✋ hard budget cap hit (${phase_spent:.4f}) — aborting the session.{RESET}")
                return "budget_cap_abort"          # cancel volontario via protocollo
            return None

        self._spent_before = INITIAL_BALANCE - self.agent.balance
        self.agent.will_request(resource=f"{RESOURCE_NAMESPACE}:{fen}",
                                param=1, mode="count", on_tick=budget_guard)
        try:
            result = await asyncio.wait_for(self.agent.run(), timeout=MARKET_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            # wait_for ha cancellato run(): il finally dell'SDK ha gia' chiuso
            # il socket, e il broker -- vedendo la disconnessione -- ha gia'
            # ripulito l'offerta orfana dal matching pool.
            result = {"type": "errore_timeout", "reason": f"no_oracle_within_{MARKET_TIMEOUT_SEC:.0f}s"}

        kind = result.get("type")
        paid = result.get("total_paid", 0.0) or 0.0
        sample = [x for x in result.get("results_sample", []) if isinstance(x, dict)]
        analysis = sample[-1] if sample else None

        if kind == "complete" and analysis and "error" not in analysis:
            self.purchases += 1
            self.spent_on_market = round(self.spent_on_market + paid, 8)
            print(f"{GREEN}{BOLD}  💸 SETTLED{RESET}  paid {GREEN}${paid:.6f}{RESET} across "
                  f"{result.get('ticks', 0)} streamed ticks · wallet {GREEN}${self.agent.balance:.6f}{RESET}")
            print(f"{CYAN}{BOLD}  🧠 PREMIUM MOVE RECEIVED{RESET}  {BOLD}{analysis['best_move_san']}{RESET} "
                  f"(eval {analysis['eval']}, depth {analysis['depth']}, "
                  f"{analysis['nodes']:,} nodes in {analysis['think_time_sec']}s)")
            print(f"  {DIM}principal variation: {analysis['pv_san']}{RESET}")
            return analysis

        why = (analysis or {}).get("error") or result.get("reason", kind)
        print(f"{RED}  ✗ market buy failed ({why}) — falling back to local depth {LOCAL_DEPTH}.{RESET}")
        return None

    # ---- la partita ----------------------------------------------------------
    async def play(self) -> None:
        await self.agent.ensure_identity()
        board = chess.Board()

        print(f"\n{BOLD}{GOLD}╔══════════════════════════════════════════════════════════════╗{RESET}")
        print(f"{BOLD}{GOLD}║   THE PLAYER  ·  an agent that buys grandmaster thought      ║{RESET}")
        print(f"{BOLD}{GOLD}╚══════════════════════════════════════════════════════════════╝{RESET}")
        print(f"  {CYAN}passport{RESET}  {self.agent.passport_id[:16]}…  {DIM}(Ed25519){RESET}")
        print(f"  {CYAN}wallet{RESET}    ${INITIAL_BALANCE:.2f}   ·   {CYAN}local compute{RESET}  depth {LOCAL_DEPTH} only")
        print(f"  {CYAN}broker{RESET}    {BROKER_URL}")
        print(f"  {CYAN}opening{RESET}   {BOOK_NAME}\n")

        # -- fase 1: la teoria, gratis ---------------------------------------
        for uci in BOOK_UCI:
            mv = chess.Move.from_uci(uci)
            print(f"  {DIM}book{RESET}  {'White' if board.turn else 'Black'} plays "
                  f"{BOLD}{board.san(mv)}{RESET}")
            board.push(mv)
        print()
        show_board(board)

        # -- fase 2: fuori dal libro, dove i soldi parlano ---------------------
        while not board.is_game_over() and board.fullmove_number <= MAX_FULLMOVES:
            if board.turn == chess.WHITE:
                mv, _, _ = await self.local_analysis(board)
                print(f"\n  {DIM}White (sparring engine, depth {LOCAL_DEPTH}) plays{RESET} "
                      f"{BOLD}{board.san(mv)}{RESET}")
                board.push(mv)
                continue

            # --- il nostro agente, a corto di FLOPS --------------------------
            mv, eval_cp, gap = await self.local_analysis(board)
            print(f"\n{CYAN}  ● Black (our agent) · resource constraint: depth {LOCAL_DEPTH} only{RESET}")
            print(f"    local pick {BOLD}{board.san(mv)}{RESET} (eval {eval_cp / 100.0:+.2f}) · "
                  f"top-2 gap {gap}cp · threshold {COMPLEXITY_CP_WINDOW}cp")

            if gap <= COMPLEXITY_CP_WINDOW:
                print(f"{GOLD}    ⚠ POSITION TOO SHARP for shallow search — "
                      f"buying depth-20 from the market.{RESET}")
                analysis = await self.buy_premium_move(board)
                if analysis:
                    bought = chess.Move.from_uci(analysis["best_move_uci"])
                    if bought in board.legal_moves:
                        label = "premium (bought)"
                        mv = bought
                    else:                                   # difesa: mai fidarsi ciecamente del mercato
                        label = f"local d{LOCAL_DEPTH} (premium move was stale)"
                else:
                    label = f"local d{LOCAL_DEPTH} (market fallback)"
            else:
                label = f"local d{LOCAL_DEPTH} (confident)"

            print(f"  {BOLD}▶ Black plays {board.san(mv)}{RESET}  {DIM}[{label}]{RESET}\n")
            board.push(mv)
            show_board(board)

        # -- epilogo -----------------------------------------------------------
        print(f"\n{BOLD}{GOLD}╔═══════════════════ FINAL LEDGER ═══════════════════╗{RESET}")
        print(f"  result            {BOLD}{board.result(claim_draw=True)}{RESET}  "
              f"{DIM}({'game over' if board.is_game_over() else 'demo move cap reached'}){RESET}")
        print(f"  premium analyses  {self.purchases} bought on-market")
        print(f"  market spend      {GREEN}${self.spent_on_market:.6f}{RESET}")
        print(f"  wallet remaining  {GREEN}${self.agent.balance:.6f}{RESET}  (of ${INITIAL_BALANCE:.2f})")
        print(f"  every settlement Ed25519-signed · verified by the broker · "
              f"protocol v{PROTOCOL_VERSION}")
        print(f"{BOLD}{GOLD}╚═════════════════════════════════════════════════════╝{RESET}")
        moves_san = chess.Board().variation_san(board.move_stack)
        print(f"\n  {DIM}game: {moves_san}{RESET}\n")

    def close(self) -> None:
        with contextlib.suppress(Exception):
            self.engine.quit()


async def main() -> None:
    player = Player()
    try:
        await player.play()
    finally:
        player.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{DIM}Player resigned (Ctrl+C).{RESET}")
    sys.exit(0)