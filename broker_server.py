import os

from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")


"""
================================================================================
 broker_server.py -- Server centrale indipendente (Broker + Micro-Ledger)
                      -- versione Cloud-Ready (Fase Sandbox / Render.com)
================================================================================
COSA FA
-------
  * Apre un WebSocket server su HOST:PORT (0.0.0.0 + porta dinamica: vedi
    sotto -- e' un requisito rigido di qualunque PaaS, non solo di Render).
  * Istanzia l'unico oggetto Broker (Ledger + matching + regolamento a
    tick): tutta la logica resta nell'SDK m2m_ledger/client.py (protocollo
    v3, buste firmate Ed25519), questo file si limita ad avviarla, esporla
    e tenerla in vita. ATTENZIONE: NON importare mai da m2m_protocol.py
    (versione legacy senza firma) -- vedi il commento sugli import.
  * PERSISTENZA (opzionale): se SUPABASE_URL e SUPABASE_KEY sono impostate
    nell'ambiente, il Ledger scrive su una tabella Postgres reale (vedi
    schema.sql) invece che in un dict RAM -- i saldi sopravvivono ai
    riavvii, incluso lo spin-down automatico del piano Free di Render dopo
    15 minuti di inattivita'. Se le variabili non ci sono (o Supabase non
    risponde), si degrada in modo pulito alla RAM: mai un crash per un
    miglioramento opzionale (vedi m2m_ledger.client.create_ledger()).
  * Risponde 200 OK alle richieste HTTP "semplici" su /healthz, cosi' un
    health-check HTTP (non solo TCP) puo' verificare che il servizio sia
    vivo senza essere scambiato per un tentativo di handshake websocket.
  * Si spegne in modo pulito sia su SIGINT (Ctrl+C locale) sia su SIGTERM
    (il segnale che Render -- e la stragrande maggioranza delle piattaforme
    cloud -- invia per fermare un'istanza ad ogni deploy o manutenzione,
    concedendo tipicamente 30 secondi prima di uccidere il processo a
    forza): niente traceback, niente connessioni tagliate di netto.

PERCHE' 0.0.0.0 E LA PORTA DINAMICA
------------------------------------
In locale, un server in ascolto su "localhost" (127.0.0.1) accetta
connessioni SOLO dallo stesso host. Dentro un container su Render (o quasi
ogni altro PaaS) il traffico pubblico arriva da un layer di rete esterno al
container: se il processo resta in ascolto solo su 127.0.0.1, quel traffico
non lo raggiunge mai, e il deploy risulta "up" ma irraggiungibile.
Analogamente, la porta non la sceglie il nostro codice: la assegna la
piattaforma a runtime tramite la variabile d'ambiente PORT (Render la
imposta di default a 10000, ma va sempre letta dinamicamente, mai
hard-coded) e va cablata di conseguenza sull'URL pubblico che la piattaforma
espone. Per questo:

    HOST = "0.0.0.0"
    PORT = int(os.environ.get("PORT", 8765))

...funziona identico sia in locale (dove PORT non e' definita e si ricade
sul default 8765) sia su Render (dove PORT e' gia' impostata dalla
piattaforma stessa).

COME SI USA IN LOCALE (senza persistenza, RAM pura)
-----------------------------------------------------
    python3 broker_server.py

COME SI USA CON PERSISTENZA SU SUPABASE
------------------------------------------
Poi, in altri due terminali, in qualsiasi ordine:
    python3 node_b.py
    python3 node_a.py
================================================================================
"""

import asyncio
import http
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

# Import dal pacchetto src-layout anche SENZA `pip install -e .`. NIENTE
# fallback sul vecchio m2m_protocol.py: un broker sulla versione pre-firma
# scarta in silenzio le buste Ed25519 dei nodi nuovi e il sistema si blocca
# sull'handshake senza un solo messaggio d'errore (il "silent deadlock").
# Meglio un ImportError rumoroso qui che quel silenzio la'.
try:
    from m2m_ledger.client import Broker, DEFAULT_TICK_INTERVAL, PROTOCOL_VERSION, create_ledger
    import m2m_ledger.client as _sdk
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    from m2m_ledger.client import Broker, DEFAULT_TICK_INTERVAL, PROTOCOL_VERSION, create_ledger
    import m2m_ledger.client as _sdk

HOST = os.environ.get("HOST", "0.0.0.0")           # 0.0.0.0: obbligatorio per essere raggiungibili da fuori il container
PORT = int(os.environ.get("PORT", 8765))            # PORT: nome esatto richiesto da Render; 8765 resta il default solo in locale
TICK_INTERVAL = float(os.environ.get("M2M_TICK_INTERVAL", DEFAULT_TICK_INTERVAL))


def make_http_hook(broker):
    """
    Factory dell'hook process_request di websockets.serve(). E' una factory
    (e non piu' una funzione top-level come il vecchio health_check) perche'
    l'endpoint /orderbook ha bisogno di leggere lo stato del broker -- che
    a import-time non esiste ancora: la closure cattura l'istanza creata
    in main(). Due percorsi HTTP "semplici", tutto il resto prosegue come
    normale handshake websocket (return None):

      GET /healthz    -> 200 "OK" (health-check di Render, come prima)
      GET /orderbook  -> 200 JSON con la fotografia del Dynamic Order Book
                         (stesso identico snapshot servito via websocket ai
                         client autenticati). Pensato per demo e monitoraggio
                         -- un browser o un `curl` bastano per vedere il
                         mercato dal vivo. E' SOLA LETTURA di dati gia'
                         pubblici agli agenti (passaporti = chiavi pubbliche,
                         risorse, listini); chi non vuole esporlo lo spegne
                         con M2M_HTTP_ORDERBOOK=0 senza toccare il codice.

    L'hook gira sull'event loop del broker: order_book_snapshot() e' O(n)
    puro su dict in RAM, senza await ne' lock -- costo trascurabile anche
    con sessioni ad alta frequenza in corso.
    """
    esponi_orderbook = os.environ.get("M2M_HTTP_ORDERBOOK", "1") != "0"

    def hook(connection, request):
        if request.path == "/healthz":
            return connection.respond(http.HTTPStatus.OK, "OK\n")
        if esponi_orderbook and request.path == "/orderbook":
            snapshot = broker.order_book_snapshot()
            body = json.dumps(
                {"server_time": time.time(), "count": len(snapshot), "providers": snapshot},
                indent=2, ensure_ascii=False,
            ) + "\n"
            risposta = connection.respond(http.HTTPStatus.OK, body)
            try:
                risposta.headers["Content-Type"] = "application/json; charset=utf-8"
            except Exception:
                pass  # se la versione di websockets non espone headers mutabili, il JSON arriva comunque
            return risposta
        return None

    return hook


async def main() -> None:
    # TRIPWIRE anti-mismatch: la riga piu' preziosa di tutto il file quando
    # qualcosa "si blocca senza errori". Dichiara NERO SU BIANCO quale file
    # SDK sta girando e con quale versione di protocollo: se qui leggi
    # m2m_protocol.py o una versione diversa da quella dei nodi, hai gia'
    # trovato il colpevole senza aprire un debugger.
    print(f"[Broker] SDK: {_sdk.__file__}  --  protocollo v{PROTOCOL_VERSION} (firme Ed25519 attive)")

    ledger = await create_ledger()  # Supabase se configurato, altrimenti RAM -- vedi m2m_ledger/client.py
    broker = Broker(HOST, PORT, TICK_INTERVAL, ledger=ledger)
    await broker.start(process_request=make_http_hook(broker))
    print(f"Broker M2M pronto su ws://{HOST}:{PORT}  (tick={TICK_INTERVAL}s). "
          f"HTTP: /healthz + /orderbook (Dynamic Order Book, sola lettura)")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        # SIGTERM: il segnale di arresto "pulito" inviato dalle piattaforme
        # cloud (Render compreso) ad ogni deploy/restart/manutenzione.
        # SIGINT: Ctrl+C, per l'uso locale. Stesso percorso di spegnimento
        # per entrambi, cosi' il comportamento e' identico in locale e sul
        # cloud.
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)
        loop.add_signal_handler(signal.SIGINT, stop_event.set)
    except NotImplementedError:
        # add_signal_handler non e' disponibile su Windows: si ripiega sul
        # normale KeyboardInterrupt intercettato piu' sotto, in __main__.
        pass

    await stop_event.wait()
    print("\nSegnale di arresto ricevuto: chiusura pulita del broker in corso...")
    await broker.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Rete di sicurezza per le piattaforme (es. Windows) dove
        # add_signal_handler non e' disponibile.
        pass
    print("Broker arrestato.")
    sys.exit(0)