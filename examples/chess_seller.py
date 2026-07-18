"""
================================================================================
 chess_seller.py -- THE ORACLE: sells Stockfish depth-20 analysis over m2m-ledger
================================================================================
A provider agent that lists "depth_20_analysis" on the M2M marketplace.
Consumers attach a chess position (FEN) to their signed request; the Oracle
runs Stockfish at depth 20 and streams the session back through the broker:

  * while the engine thinks, the Oracle delivers EMPTY progress chunks each
    tick -- the consumer is paying per second of grandmaster thought
    (money_per_sec), live, tick by tick;
  * when the search completes, ONE final chunk carries the premium move.
    The consumer requested mode="count", param=1, so that single delivered
    item settles and completes the session automatically. No polling, no
    webhooks: the protocol's own accounting is the state machine.

RUN
    export STOCKFISH_PATH=/usr/games/stockfish     # or wherever yours lives
    python3 examples/chess_seller.py

Requires: pip install chess   +   the Stockfish binary
(Debian/Ubuntu: apt install stockfish | macOS: brew install stockfish |
 Windows: download from stockfishchess.org and set STOCKFISH_PATH).
================================================================================
"""

import asyncio
import logging
import os
import shutil
import sys
import threading
import time
import traceback
from pathlib import Path

# Regia del terminale: i log dell'SDK (trace e settlement in italiano) vanno
# silenziati PRIMA dell'import, cosi' sullo schermo resta solo la messa in
# scena inglese pensata per lo screen recording.
os.environ.setdefault("M2M_TRACE", "0")

# I print sono la demo: line-buffering esplicito cosi' arrivano in tempo
# reale anche quando lo stdout e' una pipe (asciinema, tee, redirect).
sys.stdout.reconfigure(line_buffering=True)

import chess
import chess.engine

# Import dal pacchetto src-layout anche SENZA `pip install -e .`. NIENTE
# fallback sul modulo legacy senza firma (vedi storia del silent deadlock).
try:
    from m2m_ledger import Agent, DEFAULT_BROKER_URL, PROTOCOL_VERSION
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    from m2m_ledger import Agent, DEFAULT_BROKER_URL, PROTOCOL_VERSION

logging.getLogger().setLevel(logging.WARNING)

BROKER_URL = os.environ.get("M2M_BROKER_URL", DEFAULT_BROKER_URL)
RESOURCE_NAMESPACE = "depth_20_analysis"          # il consumer chiede "depth_20_analysis:<FEN>"
TARGET_DEPTH = int(os.environ.get("M2M_ORACLE_DEPTH", "20"))
MAX_THINK_SEC = float(os.environ.get("M2M_ORACLE_MAX_THINK", "25"))  # paracadute: mai pensare all'infinito
PRICE_PER_SEC = 0.012                             # listino: ~$0.05 per una pensata da ~3-4s
PRICE_PER_KB = 0.02
BACKOFF_START, BACKOFF_CAP = 1.0, 10.0

# --- colori ANSI (Windows: os.system("") abilita il VT processing) -----------
if os.name == "nt":
    os.system("")
DIM, BOLD, RESET = "\033[2m", "\033[1m", "\033[0m"
GOLD, CYAN, GREEN, RED = "\033[33m", "\033[36m", "\033[32m", "\033[31m"


def find_stockfish() -> str:
    """Auto-discovery del binario: env var > PATH > percorsi tipici."""
    candidates = [os.environ.get("STOCKFISH_PATH"), shutil.which("stockfish"),
                  "/usr/games/stockfish", "/usr/local/bin/stockfish",
                  "/opt/homebrew/bin/stockfish",
                  r"C:\Program Files\Stockfish\stockfish.exe"]
    for c in candidates:
        if c and Path(c).exists():
            return c
    raise SystemExit(f"{RED}Stockfish binary not found. Install it (apt/brew) or set STOCKFISH_PATH.{RESET}")


# ==============================================================================
# Il cervello: UN engine condiviso, UNA analisi alla volta (il broker stesso
# garantisce un cliente per volta: mentre pensa, l'Oracle e' "busy" nell'Order
# Book e non puo' essere abbinato a nessun altro).
# ==============================================================================
class OracleBrain:
    def __init__(self, engine_path: str) -> None:
        self.engine = chess.engine.SimpleEngine.popen_uci(engine_path)
        self.engine.configure({"Threads": min(4, os.cpu_count() or 1), "Hash": 256})
        self.engine_name = self.engine.id.get("name", "Stockfish")
        self._busy = threading.Event()
        self._analysis = None            # handle vivo, per stop() se la sessione muore a meta'

    # ---- API per l'handler SDK (gira in un worker thread, MAI sul loop) ----
    def start_thinking(self, fen: str) -> dict:
        """Avvia la ricerca in un thread dedicato e ritorna subito lo stato
        condiviso che l'handler interroghera' ad ogni tick."""
        state = {"done": False, "depth": 0, "result": None, "t0": time.perf_counter()}
        if self._busy.is_set():
            state["done"] = True
            state["result"] = {"error": "oracle_busy", "detail": "another analysis is still running"}
            return state
        try:
            board = chess.Board(fen)
        except ValueError as exc:
            state["done"] = True
            state["result"] = {"error": "invalid_fen", "detail": str(exc)}
            return state

        self._busy.set()
        threading.Thread(target=self._think, args=(board, state), daemon=True).start()
        return state

    def _think(self, board: chess.Board, state: dict) -> None:
        try:
            # Limit(depth=…, time=…): il motore si ferma al PRIMO limite
            # raggiunto -- il tetto in secondi e' il paracadute che rende
            # IMPOSSIBILE una sessione infinita (il consumer sta pagando a
            # tempo: proteggerlo e' parte del contratto, non gentilezza).
            with self.engine.analysis(board, chess.engine.Limit(depth=TARGET_DEPTH,
                                                                time=MAX_THINK_SEC)) as analysis:
                self._analysis = analysis
                for info in analysis:
                    d = info.get("depth")
                    if d and d > state["depth"]:
                        state["depth"] = d
                        score = info.get("score")
                        ev = score.pov(board.turn) if score else "?"
                        print(f"{DIM}   [engine] depth {d:>2}/{TARGET_DEPTH}  eval {ev}  "
                              f"nodes {info.get('nodes', 0):,}{RESET}")
                final = analysis.info
            elapsed = round(time.perf_counter() - state["t0"], 2)
            best = final["pv"][0]
            pov = final["score"].pov(board.turn)
            san_line, b2 = [], board.copy()
            for mv in final["pv"][:4]:
                san_line.append(b2.san(mv)); b2.push(mv)
            state["result"] = {
                "fen": board.fen(),
                "best_move_uci": best.uci(),
                "best_move_san": board.san(best),
                "eval": f"#{pov.mate()}" if pov.is_mate() else f"{pov.score() / 100.0:+.2f}",
                "depth": final.get("depth", state["depth"]),
                "nodes": final.get("nodes", 0),
                "pv_san": " ".join(san_line),
                "engine": self.engine_name,
                "think_time_sec": elapsed,
            }
        except Exception as exc:                                   # mai un thread che muore zitto
            state["result"] = {"error": "engine_failure", "detail": f"{type(exc).__name__}: {exc}"}
        finally:
            self._analysis = None
            state["done"] = True
            self._busy.clear()

    def abort_pending(self) -> None:
        """Se una sessione e' morta a meta' pensata (consumer sparito), la
        ricerca orfana va fermata SUBITO: l'engine deve tornare libero per
        il prossimo cliente, non tra MAX_THINK_SEC secondi."""
        a = self._analysis
        if a is not None:
            try:
                a.stop()
            except Exception:
                pass

    def quit(self) -> None:
        try:
            self.engine.quit()
        except Exception:
            pass


def make_handler(brain: OracleBrain):
    """
    Contratto SDK: handler(cursor, resource) -> (chunk, cursor), invocato in
    un worker thread una volta per tick (con backpressure sul settlement).

    Macchina a stati via cursor -- e' il protocollo streaming in miniatura:
      cursor None      -> primo tick: estrai la FEN dalla risorsa richiesta,
                          avvia la ricerca, consegna un chunk VUOTO (peso 0:
                          questo tick il cliente paga solo il tempo);
      ricerca in corso -> chunk vuoti di progress, uno per tick;
      ricerca conclusa -> UN chunk con UN elemento: in mode="count" param=1
                          quell'unico elemento fa scattare complete da solo.
    """
    def handler(cursor, resource: str):
        if cursor is None:
            fen = resource.split(":", 1)[1] if ":" in resource else ""
            short = (fen[:38] + "…") if len(fen) > 39 else fen
            print(f"\n{GOLD}{BOLD}◆ INCOMING SIGNED REQUEST{RESET}  {DIM}resource={RESOURCE_NAMESPACE}{RESET}")
            print(f"  {CYAN}FEN{RESET} {short}")
            print(f"  {DIM}spinning up {TARGET_DEPTH}-ply search — client pays per second of thought{RESET}")
            return [], brain.start_thinking(fen)
        if not cursor["done"]:
            return [], cursor                      # progress: il cliente sta pagando il tempo
        result = cursor["result"]
        if "error" in result:
            print(f"  {RED}✗ delivering error to client: {result['error']}{RESET}")
        else:
            print(f"{GREEN}{BOLD}  ✓ ANALYSIS COMPLETE{RESET}  best={BOLD}{result['best_move_san']}{RESET} "
                  f"eval={result['eval']}  depth={result['depth']}  "
                  f"({result['think_time_sec']}s, {result['nodes']:,} nodes)")
            print(f"  {DIM}shipping premium move through the signed channel…{RESET}")
        return [result], cursor
    return handler


async def oracle_supervisor() -> None:
    brain = await asyncio.to_thread(OracleBrain, find_stockfish())

    oracle = Agent(name="Oracle-GM", balance=0.0, broker_url=BROKER_URL)
    oracle.will_provide(
        f"{RESOURCE_NAMESPACE}:all",               # wildcard: soddisfa depth_20_analysis:<qualsiasi FEN>
        make_handler(brain),
        price_per_sec=PRICE_PER_SEC,
        price_per_kb=PRICE_PER_KB,
        description="Stockfish depth-20 grandmaster analysis - pay per second of thought",
    )
    await oracle.ensure_identity()

    print(f"\n{BOLD}{GOLD}╔══════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{GOLD}║   THE ORACLE  ·  Stockfish-as-a-Service on m2m-ledger        ║{RESET}")
    print(f"{BOLD}{GOLD}╚══════════════════════════════════════════════════════════════╝{RESET}")
    print(f"  {CYAN}passport{RESET}  {oracle.passport_id[:16]}…  {DIM}(Ed25519 public key){RESET}")
    print(f"  {CYAN}engine{RESET}    {brain.engine_name}  ·  target depth {TARGET_DEPTH}")
    print(f"  {CYAN}listing{RESET}   {RESOURCE_NAMESPACE}  @  ${PRICE_PER_SEC}/sec + ${PRICE_PER_KB}/KB")
    print(f"  {CYAN}broker{RESET}    {BROKER_URL}\n")

    backoff, session_n = BACKOFF_START, 0
    try:
        while True:
            session_n += 1
            print(f"{DIM}── listed on the order book · waiting for a buyer "
                  f"(session #{session_n}) ──{RESET}")
            try:
                result = await oracle.run()        # per contratto: mai eccezioni di rete
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.error(f"[oracle] unexpected SDK exception:\n{traceback.format_exc()}")
                result = {"type": "errore_connessione", "reason": "sdk_internal"}

            brain.abort_pending()                  # niente ricerche orfane tra una sessione e l'altra

            kind, why = result.get("type"), result.get("reason", "")
            if kind == "complete" and result.get("ticks", 0) > 0:
                backoff = BACKOFF_START
                print(f"{GREEN}{BOLD}💰 SETTLED{RESET}  earned {GREEN}${result.get('earned', 0):.6f}{RESET} "
                      f"over {result.get('ticks', 0)} ticks  ·  "
                      f"lifetime balance {GREEN}${oracle.balance:.6f}{RESET}\n")
                await asyncio.sleep(0.2)
                continue
            if kind == "halted" and result.get("ticks", 0) > 0:
                backoff = BACKOFF_START
                print(f"{GOLD}⚠ session ended early ({why}) — kept "
                      f"${result.get('earned', 0):.6f} for {result.get('ticks', 0)} settled ticks{RESET}\n")
                continue

            print(f"{DIM}broker unreachable or session dropped ({kind}/{why}) — "
                  f"retrying in {backoff:.0f}s…{RESET}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_CAP)
    finally:
        brain.quit()


if __name__ == "__main__":
    try:
        asyncio.run(oracle_supervisor())
    except KeyboardInterrupt:
        print(f"\n{DIM}Oracle shutting down. The market never sleeps — but this node does.{RESET}")
    sys.exit(0)