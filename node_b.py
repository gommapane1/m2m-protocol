"""
================================================================================
 node_b.py -- NODO B: "Oracolo Universale" (multiplexa l'intero mercato)
                       -- versione a concorrenza isolata + auto-riconnessione
================================================================================
Non vende calcolo: si connette all'"All Market Mini Tickers" di Binance
(wss://stream.binance.com:9443/ws/!miniTicker@arr) e ne rivende l'accesso,
filtrato al volo, attraverso il protocollo M2M.

ARCHITETTURA DI CONCORRENZA (il motivo di questa riscrittura)
--------------------------------------------------------------
Due asyncio.Task COMPLETAMENTE indipendenti, creati subito e mai in attesa
l'uno dell'altro:

    [feed-binance]   supervisiona la connessione a Binance e riempie la cache
    [agente-broker]  supervisiona la connessione al broker e serve i client

Ognuno ha il PROPRIO ciclo di auto-riconnessione con backoff esponenziale
(1s -> 2s -> 4s -> 8s -> tetto 10s) e il PROPRIO paracadute anti-eccezione:
un bug o un errore di rete in uno dei due viene loggato CON traceback e
ritentato -- non uccide mai l'altro, non uccide mai il processo, e
soprattutto non muore mai IN SILENZIO (la vecchia versione poteva perdere il
task del feed per un'eccezione non prevista dentro create_task, senza che
nessuno la vedesse mai). L'ordine di avvio dei processi (broker prima o dopo
i nodi) e' irrilevante per costruzione.

    python3 node_b.py
================================================================================
"""

import asyncio
import json
import logging
import os
import sys
import traceback
from pathlib import Path

import websockets

# Import dal pacchetto src-layout anche SENZA `pip install -e .`: se
# m2m_ledger non e' installato, aggiungiamo ./src al path. NIENTE fallback
# sul modulo legacy senza firma: e' esattamente il mix di versioni che ha
# prodotto il deadlock silenzioso (broker pre-crypto che scarta le buste
# firmate senza rispondere) -- meglio un ImportError chiaro che quel limbo.
try:
    from m2m_ledger import Agent, DEFAULT_BROKER_URL, MAX_MESSAGE_SIZE, PROTOCOL_VERSION
    import m2m_ledger.client as _sdk
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    from m2m_ledger import Agent, DEFAULT_BROKER_URL, MAX_MESSAGE_SIZE, PROTOCOL_VERSION
    import m2m_ledger.client as _sdk

BROKER_URL = os.environ.get("M2M_BROKER_URL", DEFAULT_BROKER_URL)
# BINANCE_URL e' sovrascrivibile via env var solo per i test locali (il
# sandbox di sviluppo non raggiunge stream.binance.com): in produzione resta
# sempre il vero endpoint pubblico di Binance.
BINANCE_URL = os.environ.get("BINANCE_URL", "wss://stream.binance.com:9443/ws/!miniTicker@arr")

BACKOFF_INIZIALE_SEC = 1.0
BACKOFF_MASSIMO_SEC = 10.0     # tetto del backoff esponenziale (1, 2, 4, 8, 10, 10, ...)
ATTESA_PRIMO_DATO_SEC = 15.0   # cortesia pre-vendita: quanto aspettare il primo dato di mercato
                               # prima di pubblicare comunque la disponibilita' (non e' un vincolo
                               # di ordine di avvio: e' solo per non vendere chunk vuoti al boot)


class MarketCache:
    """
    Mantiene in memoria l'ultimo prezzo noto di OGNI simbolo mai visto.
    Binance manda solo i simboli "cambiati" ad ogni push: per questo
    aggiorniamo (merge) invece di sovrascrivere, cosi' 'crypto:all' resta un
    quadro completo anche se un simbolo non si muove per qualche secondo.

    Thread-safety: get_chunk() viene invocato in un thread separato
    dall'SDK (Agent._run_as_provider -> asyncio.to_thread). La scrittura
    sostituisce SEMPRE l'intero dict con uno nuovo (mai una mutazione
    in-place): una lettura concorrente vede o lo stato vecchio o quello
    nuovo per intero, mai uno stato a meta'.
    """

    def __init__(self) -> None:
        self._by_symbol: dict = {}
        self.has_data = asyncio.Event()

    def aggiorna(self, items: list) -> int:
        merged = dict(self._by_symbol)          # copia difensiva: vedi nota thread-safety
        for item in items:
            symbol = item.get("s")
            if symbol:
                merged[symbol] = item
        self._by_symbol = merged                # sostituzione atomica del riferimento
        if merged and not self.has_data.is_set():
            self.has_data.set()
        return len(merged)

    def get_chunk(self, cursor, resource: str):
        """Contratto richiesto dall'SDK: (cursor, resource) -> (chunk, nuovo_cursor).
        Gira in un worker thread: SOLO letture su uno snapshot atomico."""
        snapshot = self._by_symbol
        if resource == "crypto:all":
            chunk = list(snapshot.values())
        else:
            symbol = resource.split(":", 1)[1] if ":" in resource else resource
            ticker = snapshot.get(symbol.upper())
            chunk = [ticker] if ticker else []
        return chunk, cursor

    @property
    def n_simboli(self) -> int:
        return len(self._by_symbol)


def _prossimo_backoff(attuale: float) -> float:
    return min(attuale * 2, BACKOFF_MASSIMO_SEC)


# ==============================================================================
# TASK 1 -- supervisore del feed Binance (non tocca MAI il broker)
# ==============================================================================
async def feed_binance_supervisor(cache: MarketCache) -> None:
    """
    Ciclo eterno: connetti -> consuma -> (su QUALSIASI problema) logga,
    aspetta il backoff, riconnetti. Il context switching e' garantito per
    costruzione: ogni iterazione interna attende un frame dal socket
    (`async for raw in ws` = un await per messaggio), quindi l'event loop
    torna libero tra un push e l'altro anche sotto pieno carico del feed.
    """
    backoff = BACKOFF_INIZIALE_SEC
    tentativo = 0
    while True:
        tentativo += 1
        try:
            logging.info(f"[TRACE|feed-binance] sto per connettermi a {BINANCE_URL} (tentativo #{tentativo})...")
            async with websockets.connect(BINANCE_URL, max_size=MAX_MESSAGE_SIZE, open_timeout=10) as ws:
                logging.info(f"[TRACE|feed-binance] connessione stabilita -- in ascolto dei mini-ticker.")
                backoff = BACKOFF_INIZIALE_SEC   # connessione riuscita: il backoff riparte da capo
                tentativo = 0
                async for raw in ws:
                    try:
                        data = json.loads(raw)
                    except json.JSONDecodeError:
                        continue        # UN frame malformato non giustifica il teardown della connessione
                    if not isinstance(data, list):
                        continue
                    n = cache.aggiorna(data)
                    if n and cache.n_simboli == n and not getattr(feed_binance_supervisor, "_primo_log", False):
                        feed_binance_supervisor._primo_log = True
                        logging.info(f"[Oracolo] primo push di mercato ricevuto: {n} simboli in cache.")
        except asyncio.CancelledError:
            raise                        # lo spegnimento del processo deve propagarsi, sempre
        except Exception as exc:
            # Paracadute TOTALE: rete (ConnectionClosed/OSError/TimeoutError),
            # handshake (InvalidStatus del provider), o un bug futuro nel
            # parsing. Qualunque cosa sia, viene LOGGATA (mai piu' task che
            # muoiono in silenzio dentro create_task) e poi si ritenta.
            dettaglio = f"{type(exc).__name__}: {exc}"
            if not isinstance(exc, (websockets.exceptions.WebSocketException, OSError, asyncio.TimeoutError)):
                dettaglio += "\n" + traceback.format_exc()   # un bug vero merita il traceback completo
            logging.warning(f"[feed-binance] feed interrotto ({dettaglio}) -- nuovo tentativo tra {backoff:.0f}s.")
            await asyncio.sleep(backoff)
            backoff = _prossimo_backoff(backoff)


# ==============================================================================
# TASK 2 -- supervisore dell'agente sul broker (non tocca MAI Binance)
# ==============================================================================
async def agente_broker_supervisor(cache: MarketCache) -> None:
    """
    Un Oracolo e' infrastruttura, non uno script usa-e-getta: serve sessione
    dopo sessione, e se il broker e' giu' (o non e' ANCORA su: l'ordine di
    avvio dei processi e' libero) ritenta con backoff esponenziale, per
    sempre, senza mai crashare ne' congelarsi. Stesso Agent per tutte le
    sessioni: stesso passaporto Ed25519, stesso wallet, cosi' gli incassi si
    sommano invece di ripartire da zero ad ogni cliente.
    """
    # Cortesia commerciale, NON vincolo di ordine: se il feed non ha ancora
    # prodotto nulla, aspettiamo qualche secondo prima di metterci in vetrina
    # per non vendere chunk vuoti al primissimo cliente. Se il feed resta
    # muto, si procede comunque: i chunk si riempiranno appena arrivano dati.
    logging.info(f"[TRACE|agente-broker] attendo il primo dato di mercato (max {ATTESA_PRIMO_DATO_SEC:.0f}s)...")
    try:
        await asyncio.wait_for(cache.has_data.wait(), timeout=ATTESA_PRIMO_DATO_SEC)
        logging.info(f"[TRACE|agente-broker] feed attivo -- {cache.n_simboli} simboli in cache: si va in vetrina.")
    except asyncio.TimeoutError:
        logging.info("[TRACE|agente-broker] nessun dato entro la finestra di cortesia -- "
                     "pubblico comunque la disponibilita' (i chunk si popoleranno all'arrivo del feed).")

    node_b = Agent(name="Nodo-B-Oracolo", balance=0.00, broker_url=BROKER_URL)
    node_b.will_provide(resource="crypto:all", handler=cache.get_chunk)   # 'all' = supera 'crypto:*'

    backoff = BACKOFF_INIZIALE_SEC
    tentativo = 0
    sessione_n = 0
    while True:
        sessione_n += 1
        try:
            result = await node_b.run()   # non solleva MAI eccezioni di rete: torna sempre un dict
        except asyncio.CancelledError:
            raise
        except Exception:
            # run() per contratto non dovrebbe mai arrivare qui: se succede
            # e' un bug dell'SDK e DEVE finire nei log col traceback, non
            # ammazzare il task in silenzio.
            logging.error(f"[agente-broker] eccezione inattesa da Agent.run():\n{traceback.format_exc()}")
            result = {"type": "errore_connessione", "reason": "eccezione_interna_sdk"}

        tipo = result.get("type")
        motivo = result.get("reason", tipo)

        if tipo in ("complete", "halted") and result.get("ticks", 0) > 0:
            # Una sessione REALE e' stata servita (per intero o in parte):
            # il mercato funziona, il backoff riparte da capo.
            backoff, tentativo = BACKOFF_INIZIALE_SEC, 0
            print(f"[Oracolo] sessione #{sessione_n} conclusa: {motivo} -- "
                  f"incassati ${result.get('earned', 0):.6f} in {result.get('ticks', 0)} tick "
                  f"-- saldo cumulativo: ${node_b.balance:.6f}")
            await asyncio.sleep(0.2)      # respiro esplicito: cede il loop prima di rimettersi in vetrina
            continue

        # Tutto il resto e' un problema di RAGGIUNGIBILITA' (broker giu',
        # non ancora partito, caduto a meta' handshake o a meta' sessione,
        # protocollo incompatibile): backoff esponenziale e si ritenta.
        tentativo += 1
        logging.info(f"[agente-broker] broker non disponibile o sessione caduta "
                     f"({tipo}/{motivo}) -- tentativo di riconnessione #{tentativo} tra {backoff:.0f}s...")
        await asyncio.sleep(backoff)
        backoff = _prossimo_backoff(backoff)


# ==============================================================================
# ORCHESTRAZIONE -- i due task partono INSIEME e nessuno puo' morire zitto
# ==============================================================================
async def main() -> None:
    logging.info(f"[Oracolo] SDK: {_sdk.__file__} (protocollo v{PROTOCOL_VERSION}) -- broker: {BROKER_URL}")
    cache = MarketCache()

    task_feed = asyncio.create_task(feed_binance_supervisor(cache), name="feed-binance")
    task_broker = asyncio.create_task(agente_broker_supervisor(cache), name="agente-broker")
    logging.info("[Oracolo] task avviati in parallelo: [feed-binance] + [agente-broker] "
                 "(indipendenti: nessuno dei due aspetta l'altro).")

    # I supervisori sono cicli eterni: se uno di loro RITORNA o solleva,
    # e' per definizione un bug -- lo intercettiamo qui, lo urliamo nei log
    # e spegniamo tutto in modo ordinato invece di proseguire zoppi.
    done, pending = await asyncio.wait({task_feed, task_broker}, return_when=asyncio.FIRST_COMPLETED)
    for t in done:
        exc = t.exception() if not t.cancelled() else None
        logging.error(f"[Oracolo] il task '{t.get_name()}' e' terminato in modo inatteso: "
                      f"{exc!r}" + (f"\n{''.join(traceback.format_exception(exc))}" if exc else ""))
    for t in pending:
        t.cancel()
    await asyncio.gather(*pending, return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[Oracolo] arresto richiesto da tastiera: task cancellati, uscita pulita.")
    sys.exit(0)