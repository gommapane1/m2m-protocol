"""
================================================================================
 m2m_ledger/client.py -- M2M Financial Exchange Protocol (PoC -> pacchetto PyPI)
================================================================================
Ruolo di questo file: e' l'SDK. Nasconde TUTTA la complessita' di rete,
matching tra domanda/offerta e regolamento finanziario dietro un'interfaccia
a 3-4 righe per i nodi client (vedi node_a.py / node_b.py).

VINCOLI DI PROGETTO RISPETTATI
-------------------------------
  * Nessuna blockchain, nessun token, nessuno smart contract: il "denaro" e'
    un semplice float Python custodito in un dict centralizzato (simulazione
    fiat interna).
  * Nessun database esterno, nessuna API a pagamento: tutto vive in RAM.
  * asyncio (stdlib) + websockets (libreria open-source gratuita: va
    installata una tantum con `pip install websockets`).

ARCHITETTURA (dalla v2: broker separato)
------------------------------------------
Nella prima versione di questo PoC, il broker veniva auto-eletto dal primo
processo client che partiva (bind della porta come "leader election"). Utile
per un primissimo test locale, ma sbagliato in vista dell'hosting: un broker
la cui vita dipende da quale client capita ad avviarsi per primo non e'
qualcosa che si puo' mettere in produzione ne' rendere resiliente.

Da questa versione, il broker vive in un processo SEPARATO E INDIPENDENTE:
broker_server.py. Questo modulo (m2m_ledger/client.py) definisce comunque le classi
Broker/StreamSession/MicroLedger -- restano "il protocollo" -- ma e'
broker_server.py a istanziarle e a tenerle in vita. I nodi client (Agent) non
fanno piu' alcun tentativo di bind: si limitano a connettersi a un
broker_url (di default ws://localhost:8765, ma in Fase Sandbox potra' essere
un wss://dominio-pubblico/...).

Conseguenza pratica per la resilienza: se un client cade o chiude la
connessione, il broker se ne accorge (timeout sul tick o send fallita) e lo
comunica in modo pulito al peer superstite tramite un normale messaggio di
protocollo ("halted") -- niente piu' eccezioni di rete propagate da un
processo client che si porta via, morendo, anche il server.
================================================================================
"""

import asyncio
import collections
import contextlib
import json
import logging
import os
import re
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import websockets
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("websockets").setLevel(logging.WARNING)  # silenzia il rumore di libreria (connection open/close)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8765
DEFAULT_TICK_INTERVAL = 0.4  # secondi: e' la "granularita' di fiducia" del protocollo
DEFAULT_BROKER_URL = f"ws://{DEFAULT_HOST}:{DEFAULT_PORT}"  # es. wss://mio-broker.example.com in Fase Sandbox

# Finestra di tolleranza per il timestamp firmato di ogni messaggio: oltre
# questa soglia (in secondi, in entrambe le direzioni) un messaggio viene
# rifiutato anche se la firma e' matematicamente valida -- prima difesa,
# semplice, contro il replay di un messaggio firmato catturato in precedenza.
MAX_CLOCK_SKEW_SEC = 30.0

# Il default di websockets (1 MiB) basta per liste di numeri primi, ma un
# intero "all market" di ticker puo' pesare qualche centinaio di KB e
# crescere nel tempo (piu' simboli quotati). Lo alziamo esplicitamente su
# TUTTE le connessioni (broker E client) invece di scoprirlo a runtime con
# un messaggio silenziosamente scartato.
MAX_MESSAGE_SIZE = 8 * 1024 * 1024  # 8 MiB: ampio margine, non illimitato

# Versione del PROTOCOLLO DI FRAMING (non del pacchetto): "3" = buste firmate
# Ed25519. Viaggia dentro l'hello e dentro il welcome: se un client v3 parla
# con un broker pre-v3 (che NON risponde affatto alle buste firmate: le
# scarta in silenzio perche' non trova "type" al livello piu' esterno del
# JSON), il timeout di handshake qui sotto trasforma quello che prima era un
# CONGELAMENTO SILENZIOSO E PERPETUO in un errore visibile e ritentabile.
# E' esattamente il "silent deadlock" osservato sul campo: broker_server.py
# che importava il modulo legacy senza verifica firma + nodi sull'SDK
# firmato = welcome mai inviato, client appeso per sempre su recv().
PROTOCOL_VERSION = "3"

# Tempo massimo concesso al broker per rispondere "welcome" dopo l'hello.
# Un broker sano risponde in millisecondi: se non risponde entro questa
# finestra, o e' irraggiungibile a livello applicativo (proxy che accetta il
# TCP ma non inoltra), o parla un protocollo incompatibile. In entrambi i
# casi la risposta giusta e' un errore esplicito, MAI un'attesa infinita.
HANDSHAKE_TIMEOUT_SEC = float(os.environ.get("M2M_HANDSHAKE_TIMEOUT", "10"))

# Sopra questa soglia, json.loads/json.dumps di un messaggio vengono
# eseguiti in un thread (asyncio.to_thread) invece che sull'event loop:
# misurato empiricamente, la sola serializzazione canonica di un chunk
# 'crypto:all' (~175 KB) costa ~4-5 ms di CPU pura -- inaccettabile inline
# su un loop che deve restare reattivo, irrilevante in un worker thread.
# Sotto la soglia, l'hop verso il thread (~90 microsecondi) costerebbe piu'
# dell'operazione stessa (~40-100 us): si resta inline, ed e' giusto cosi'.
OFFLOAD_JSON_BYTES = 32 * 1024

# ------------------------------------------------------------------------------
# RATE LIMITING CRITTOGRAFICO (per-passaporto, NON per-IP)
# ------------------------------------------------------------------------------
# Difesa DDoS agganciata all'identita' Ed25519, non all'indirizzo di rete:
# un bot dietro mille IP ma con un solo passaporto viene comunque limitato,
# e un IP condiviso (NAT, proxy) non penalizza utenti legittimi distinti.
# Un attaccante che ruota i passaporti paga il costo di generare/firmare
# nuove identita' a ogni richiesta -- il rate limit alza quel costo.
# Token bucket in memoria: RATE_LIMIT_RPS token/secondo, burst fino a
# RATE_LIMIT_BURST. Un messaggio consuma un token; a secco, e' scartato.
RATE_LIMIT_RPS = float(os.environ.get("M2M_RATE_LIMIT_RPS", "10"))
RATE_LIMIT_BURST = float(os.environ.get("M2M_RATE_LIMIT_BURST", str(RATE_LIMIT_RPS)))
RATE_LIMIT_ENABLED = os.environ.get("M2M_RATE_LIMIT", "1") != "0"

# Grace window del matchmaking mirato: quanto un consumer con requisiti
# (es. max_price) attende un provider compatibile PRIMA di ricevere
# no_nodes_available. Sfrutta la macchina di matching esistente -- un
# provider che si connette entro la finestra viene abbinato normalmente.
MATCH_GRACE_SEC = float(os.environ.get("M2M_MATCH_GRACE", "8"))


class RateLimiter:
    """Token bucket per chiave (qui: il passaporto Ed25519). Puro, sincrono,
    O(1) per check -- nessun lock: il broker gira su un unico event loop, i
    check non sono mai concorrenti tra loro."""

    def __init__(self, rps: float = RATE_LIMIT_RPS, burst: float = RATE_LIMIT_BURST) -> None:
        self.rps = rps
        self.burst = burst
        self._buckets: Dict[str, Tuple[float, float]] = {}   # key -> (tokens, last_ts)

    def allow(self, key: str) -> bool:
        """True se c'e' un token (consumandolo), False se la chiave e' a secco."""
        now = time.monotonic()
        tokens, last = self._buckets.get(key, (self.burst, now))
        # Ricarica proporzionale al tempo trascorso, fino al tetto di burst.
        tokens = min(self.burst, tokens + (now - last) * self.rps)
        if tokens < 1.0:
            self._buckets[key] = (tokens, now)
            return False
        self._buckets[key] = (tokens - 1.0, now)
        return True

    def forget(self, key: str) -> None:
        """Libera il bucket di un client disconnesso (igiene memoria)."""
        self._buckets.pop(key, None)


# ------------------------------------------------------------------------------
# Ogni await "di ciclo di vita" (connessione, handshake, attesa peer,
# chiusura) e' preceduto E seguito da una riga di trace: se un processo si
# blocca, l'ULTIMA riga stampata dice esattamente su quale await e' fermo.
# Attivo di default (e' il suo scopo); M2M_TRACE=0 lo silenzia in demo.
# Volutamente NON traccia il percorso caldo per-tick (chunk/settlement):
# quello ha gia' i suoi log di liquidazione e inondarli qui sposterebbe il
# collo di bottiglia proprio sul logging.
_TRACE_ON = os.environ.get("M2M_TRACE", "1").strip().lower() not in ("0", "false", "no", "")


def trace(chi: str, messaggio: str) -> None:
    if _TRACE_ON:
        logging.info(f"[TRACE|{chi}] {messaggio}")


async def _json_loads_smart(raw) -> Any:
    """Parse JSON: inline se piccolo, in un worker thread se grande.
    Vedi OFFLOAD_JSON_BYTES per la motivazione misurata."""
    if len(raw) > OFFLOAD_JSON_BYTES:
        return await asyncio.to_thread(json.loads, raw)
    return json.loads(raw)


_STIMA_BYTES_PER_VOCE = 220  # voce tipica dell'Order Book serializzata (misurata sui test)


async def _json_dumps_smart(obj: Any, voci_stimate: int = 0) -> str:
    """Gemello di _json_loads_smart per la direzione opposta. La taglia di
    un oggetto NON serializzato non si conosce gratis, quindi la decisione
    inline/worker-thread si prende PRIMA, su una stima fornita dal
    chiamante (es. il numero di voci dell'Order Book): con pochi provider
    costa microsecondi e resta inline; con un registro affollato il dumps
    va in un worker thread, perche' il broker e' il componente CONDIVISO
    e il suo event loop non deve pagare la serializzazione di nessuno."""
    if voci_stimate * _STIMA_BYTES_PER_VOCE > OFFLOAD_JSON_BYTES:
        return await asyncio.to_thread(json.dumps, obj)
    return json.dumps(obj)


# ==============================================================================
# 1. IL MICRO-LEDGER
# ==============================================================================
class MicroLedger:
    """
    Registro contabile centralizzato, in memoria, a fonte unica di verita'.

    NON e' un database: e' un semplice dict Python {passaporto: saldo},
    protetto da un asyncio.Lock. Vive per l'intera durata del processo che
    ospita il broker e scompare quando quel processo termina -- scelta
    coerente con un MVP "a costo zero" che non deve persistere nulla su
    disco ne' dipendere da servizi esterni.

    Invarianti garantite dal codice, non dalla fiducia tra le parti:
      1. Nessun saldo puo' MAI andare sotto zero: transfer() rifiuta
         l'operazione anziche' lasciarla "sforare".
      2. Ogni trasferimento e' conservativo: il denaro non si crea ne' si
         distrugge, si sposta soltanto (proprieta' verificabile sommando i
         saldi di tutti i wallet prima e dopo -- vedi summary()).
    """

    def __init__(self) -> None:
        self._balances: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._transactions: List[dict] = []

    async def open_wallet(self, passport_id: str, initial_balance: float = 0.0) -> float:
        """Idempotente: se il passaporto ha gia' un wallet, lo lascia intatto.
        async per coerenza di interfaccia con SupabaseLedger (che deve
        interrogare il database) -- qui non c'e' nulla da attendere
        davvero, ma il chiamante (Broker) puo' trattare le due
        implementazioni in modo intercambiabile."""
        if passport_id not in self._balances:
            self._balances[passport_id] = round(initial_balance, 6)
        return self._balances[passport_id]

    def balance_of(self, passport_id: str) -> float:
        return self._balances.get(passport_id, 0.0)

    async def transfer(self, sender: str, receiver: str, amount: float, memo: str = "") -> bool:
        """
        Sposta atomicamente `amount` da sender a receiver.

        Ritorna False (no-op, nessuna mutazione di stato) se i fondi non
        bastano. E' esattamente qui che vive la meta' "denaro" della
        garanzia trustless: e' FISICAMENTE impossibile spendere dollari
        che non si hanno, quindi impossibile accumulare debito o pagare
        con moneta inesistente. Non serve fiducia: serve solo leggere il
        valore di ritorno di questa funzione.

        Il lock e' difensivo: con il codice attuale (nessun `await` tra il
        controllo del saldo e la sua modifica) non servirebbe, dato che
        l'event loop non puo' interrompere questa sezione a meta'. Resta
        comunque corretto includerlo: e' cio' che rende il metodo
        "corretto per costruzione" anche se in futuro si aggiungesse, ad
        esempio, una scrittura su file di audit con un `await` in mezzo.
        """
        async with self._lock:
            amount = round(amount, 8)
            if amount <= 0:
                return False
            if self._balances.get(sender, 0.0) + 1e-9 < amount:
                return False
            self._balances[sender] = round(self._balances[sender] - amount, 8)
            self._balances[receiver] = round(self._balances.get(receiver, 0.0) + amount, 8)
            self._transactions.append(
                {"ts": time.time(), "from": sender, "to": receiver, "amount": amount, "memo": memo}
            )
            return True

    def summary(self) -> dict:
        """Usata per il log di riconciliazione a fine sessione: dimostra che
        il totale in circolazione e' invariato rispetto all'inizio."""
        return {
            "wallets": dict(self._balances),
            "totale_in_circolazione": round(sum(self._balances.values()), 6),
            "transazioni_liquidate": len(self._transactions),
        }


# ==============================================================================
# 1-bis. IL SUPABASE-LEDGER -- stessa interfaccia di MicroLedger, persistente
# ==============================================================================
class SupabaseLedger:
    """
    Stessa identica interfaccia pubblica di MicroLedger (open_wallet,
    balance_of, transfer, summary): Broker e StreamSession non sanno ne'
    devono sapere quale delle due e' effettivamente in uso.

    Design -- cache in memoria "write-through":
      * balance_of() legge SOLO dalla cache locale, mai dal database. Ogni
        tick del protocollo la interroga almeno due volte (per il messaggio
        di settlement): se andasse su Supabase ad ogni chiamata, la latenza
        di rete diventerebbe il collo di bottiglia del tick_interval.
      * open_wallet()/transfer() scrivono SEMPRE prima sul database (fonte
        di verita' persistente) e SOLO DOPO conferma aggiornano la cache:
        se il processo muore a meta', nessuna transazione "fantasma" resta
        solo in RAM -- quello che il chiamante ha visto come riuscito e'
        gia' su disco (su Postgres, per essere precisi).

    Il trasferimento vero e proprio (transfer) e' delegato alla funzione
    Postgres m2m_transfer(...) via .rpc(...): un aggiornamento a due righe
    (debito+credito) eseguito come UNA sola transazione atomica lato
    database, con FOR UPDATE sulla riga del mittente -- non "due update
    separati e speriamo bene", una vera transazione, come si conviene a un
    ledger finanziario anche in un MVP.
    """

    def __init__(self, client) -> None:
        self._client = client
        self._cache: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._transactions: List[dict] = []

    async def open_wallet(self, passport_id: str, initial_balance: float = 0.0) -> float:
        if passport_id in self._cache:
            return self._cache[passport_id]
        # m2m_get_or_create_wallet: se la riga esiste gia' la lascia intatta
        # (nessun azzeramento accidentale al riavvio del broker con un
        # agente gia' noto), altrimenti la crea col saldo dichiarato ORA da
        # questo agente. Una singola chiamata atomica, non select-poi-insert.
        resp = await self._client.rpc(
            "m2m_get_or_create_wallet",
            {"p_passport": passport_id, "p_initial_balance": round(initial_balance, 6)},
        ).execute()
        saldo = float(resp.data) if resp.data is not None else round(initial_balance, 6)
        self._cache[passport_id] = saldo
        return saldo

    def balance_of(self, passport_id: str) -> float:
        return self._cache.get(passport_id, 0.0)

    async def transfer(self, sender: str, receiver: str, amount: float, memo: str = "") -> bool:
        async with self._lock:
            amount = round(amount, 6)  # precisione della colonna NUMERIC(12,6)
            if amount <= 0:
                return False
            # Controllo veloce lato cache PRIMA di una chiamata di rete: pura
            # ottimizzazione (evita un RPC che sappiamo gia' fallira'). La
            # decisione VERA e definitiva resta quella del database qui sotto,
            # mai la cache da sola.
            if self._cache.get(sender, 0.0) + 1e-9 < amount:
                return False
            try:
                resp = await self._client.rpc(
                    "m2m_transfer",
                    {"p_sender": sender, "p_receiver": receiver, "p_amount": amount},
                ).execute()
                riuscito = bool(resp.data)
            except Exception as exc:
                logging.error(f"[LEDGER] scrittura su Supabase fallita, transazione NON eseguita: {exc}")
                return False
            if not riuscito:
                return False  # la funzione stessa ha rifiutato (fondi insufficienti, verificato di nuovo lato DB)
            self._cache[sender] = round(self._cache.get(sender, 0.0) - amount, 6)
            self._cache[receiver] = round(self._cache.get(receiver, 0.0) + amount, 6)
            self._transactions.append(
                {"ts": time.time(), "from": sender, "to": receiver, "amount": amount, "memo": memo}
            )
            return True

    def summary(self) -> dict:
        return {
            "wallets": dict(self._cache),
            "totale_in_circolazione": round(sum(self._cache.values()), 6),
            "transazioni_liquidate": len(self._transactions),
        }


# ==============================================================================
# 1-ter. L'ASYNCPG-LEDGER -- Postgres nativo, cache write-back + flush batched
# ==============================================================================
class AsyncpgLedger:
    """
    Stessa interfaccia di MicroLedger/SupabaseLedger, ma su Postgres nativo
    via asyncpg (nessuna REST di mezzo). Pensato per il pricing a tick, che
    farebbe altrimenti una scrittura per frazione di centesimo:

      * I saldi vivono in cache RAM (fonte di verita' OPERATIVA durante la
        sessione); balance_of() e transfer() non toccano MAI il DB nel
        percorso caldo del tick -- zero latenza di rete per micro-pagamento.
      * Ogni transfer marca i due passaporti come "dirty". Un task in
        background fa UPDATE dei soli saldi dirty ogni FLUSH_INTERVAL secondi,
        e flush() e' invocato ISTANTANEAMENTE alla disconnessione del client.
        Il DB e' quindi eventualmente-consistente entro pochi secondi, ma
        diventa immediatamente consistente ai confini di sessione (dove conta).
      * Un asyncio.Lock serializza i micro-pagamenti concorrenti prima del
        flush: nessuna race sul saldo, la garanzia trustless (mai sotto zero)
        e' preservata identica a MicroLedger.

    Nota di durabilita' onesta: tra due flush, un saldo aggiornato vive solo
    in RAM. Un crash improvviso del broker puo' perdere fino a FLUSH_INTERVAL
    secondi di micro-pagamenti gia' 'visti' come riusciti dai client. E' il
    trade-off esplicito richiesto per non martellare il DB; le disconnessioni
    ordinate (il caso comune: fine inferenza) flushano subito e non perdono
    nulla.
    """

    FLUSH_INTERVAL = float(os.environ.get("M2M_FLUSH_INTERVAL", "5"))

    def __init__(self, pool) -> None:
        self._pool = pool
        self._cache: Dict[str, float] = {}
        self._dirty: set = set()
        self._lock = asyncio.Lock()
        self._transactions: List[dict] = []
        self._flush_task: Optional[asyncio.Task] = None

    @classmethod
    async def create(cls, dsn: str) -> "AsyncpgLedger":
        """Crea il pool, la tabella se manca, e avvia il task di flush."""
        import asyncpg
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=10)
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS wallets (
                    ed25519_pubkey TEXT PRIMARY KEY,
                    balance NUMERIC(20, 8) NOT NULL DEFAULT 0
                );
                """
            )
        self = cls(pool)
        self._flush_task = asyncio.create_task(self._flush_loop())
        return self

    async def open_wallet(self, passport_id: str, initial_balance: float = 0.0) -> float:
        """Onboarding atomico: se la pubkey esiste, carica il saldo dal DB;
        se non esiste, la crea col saldo iniziale. INSERT ... ON CONFLICT
        DO NOTHING + SELECT, cosi' un riavvio del broker non azzera un
        agente gia' noto (il saldo persistito vince sul valore dichiarato)."""
        if passport_id in self._cache:
            return self._cache[passport_id]
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO wallets (ed25519_pubkey, balance)
                VALUES ($1, $2)
                ON CONFLICT (ed25519_pubkey) DO UPDATE SET balance = wallets.balance
                RETURNING balance;
                """,
                passport_id, round(initial_balance, 8),
            )
        saldo = float(row["balance"])
        self._cache[passport_id] = saldo
        return saldo

    def balance_of(self, passport_id: str) -> float:
        return self._cache.get(passport_id, 0.0)

    async def transfer(self, sender: str, receiver: str, amount: float, memo: str = "") -> bool:
        """Sposta amount in RAM sotto lock (percorso caldo del tick), marca i
        due wallet come dirty per il prossimo flush. La garanzia 'mai sotto
        zero' e' applicata qui, identica a MicroLedger."""
        async with self._lock:
            amount = round(amount, 8)
            if amount <= 0:
                return False
            if self._cache.get(sender, 0.0) + 1e-9 < amount:
                return False
            self._cache[sender] = round(self._cache[sender] - amount, 8)
            self._cache[receiver] = round(self._cache.get(receiver, 0.0) + amount, 8)
            self._dirty.update((sender, receiver))
            self._transactions.append(
                {"ts": time.time(), "from": sender, "to": receiver, "amount": amount, "memo": memo}
            )
            return True

    async def flush(self, passports=None) -> None:
        """UPDATE dei saldi dirty (o solo di `passports`, per il flush mirato
        alla disconnessione). Snapshot sotto lock, poi scrittura fuori dal
        lock in UNA transazione: il percorso caldo non si blocca sul DB."""
        async with self._lock:
            target = (self._dirty if passports is None
                      else self._dirty.intersection(passports))
            if not target:
                return
            snapshot = [(p, self._cache[p]) for p in target]
            self._dirty.difference_update(target)
        try:
            async with self._pool.acquire() as conn:
                await conn.executemany(
                    """
                    INSERT INTO wallets (ed25519_pubkey, balance)
                    VALUES ($1, $2)
                    ON CONFLICT (ed25519_pubkey) DO UPDATE SET balance = EXCLUDED.balance;
                    """,
                    snapshot,
                )
        except Exception as exc:
            # Scrittura fallita: rimarca dirty, il prossimo flush riprovera'.
            logging.error(f"[LEDGER] flush su Postgres fallito, ritento al prossimo giro: {exc}")
            async with self._lock:
                self._dirty.update(p for p, _ in snapshot)

    async def _flush_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.FLUSH_INTERVAL)
                await self.flush()
            except asyncio.CancelledError:
                await self.flush()          # ultimo flush prima di spegnersi
                raise
            except Exception as exc:
                logging.error(f"[LEDGER] errore nel task di flush: {exc}")

    async def aclose(self) -> None:
        """Spegnimento pulito: ferma il task, flush finale, chiudi il pool."""
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self.flush()
        await self._pool.close()

    def summary(self) -> dict:
        return {
            "wallets": dict(self._cache),
            "totale_in_circolazione": round(sum(self._cache.values()), 6),
            "transazioni_liquidate": len(self._transactions),
        }


async def create_ledger():
    """
    Fabbrica del Ledger. Ordine di preferenza:
      1. DATABASE_URL  -> AsyncpgLedger (Postgres nativo, flush batched);
      2. SUPABASE_URL/KEY -> SupabaseLedger (REST, legacy);
      3. nessuna delle due -> MicroLedger (solo RAM).
    Ogni fallimento degrada IN MODO PULITO al livello successivo: la
    persistenza e' un miglioramento, mai un prerequisito per funzionare.
    """
    dsn = os.environ.get("DATABASE_URL")
    if dsn:
        try:
            ledger = await AsyncpgLedger.create(dsn)
            logging.info("[LEDGER] connesso a Postgres via asyncpg: persistenza attiva "
                         "sulla tabella 'wallets' (flush batched).")
            return ledger
        except ImportError:
            logging.warning("[LEDGER] 'asyncpg' non installato (pip install asyncpg): "
                            "provo Supabase o RAM.")
        except Exception as exc:
            logging.warning(f"[LEDGER] connessione Postgres fallita ({exc}): provo Supabase o RAM.")
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")

    if not url or not key:
        logging.warning(
            "[LEDGER] SUPABASE_URL / SUPABASE_KEY non impostate: nessuna persistenza. "
            "I saldi vivranno solo in RAM e si azzereranno al prossimo riavvio "
            "(incluso lo spin-down automatico del piano Free di Render)."
        )
        return MicroLedger()

    try:
        from supabase import create_async_client
    except ImportError:
        logging.warning(
            "[LEDGER] libreria 'supabase' non installata (vedi requirements.txt): "
            "ripiego sul Ledger in sola RAM nonostante le credenziali siano presenti."
        )
        return MicroLedger()

    try:
        client = await create_async_client(url, key)
        # Una query innocua per verificare SUBITO che la tabella esista e le
        # credenziali funzionino, invece di scoprirlo al primo tick di un
        # cliente vero.
        await client.table("m2m_wallets").select("passport").limit(1).execute()
        logging.info("[LEDGER] connesso a Supabase: persistenza attiva sulla tabella 'm2m_wallets'.")
        return SupabaseLedger(client)
    except Exception as exc:
        logging.warning(f"[LEDGER] connessione a Supabase fallita ({exc}): ripiego sul Ledger in sola RAM.")
        return MicroLedger()


# ==============================================================================
# 2. IL BROKER (gestore di connessioni asincrone + matching domanda/offerta)
# ==============================================================================
class Broker:
    """
    Punto d'incontro e custode del MicroLedger. Da questa versione vive in un
    processo indipendente e a lunga vita (vedi broker_server.py): puo'
    servire piu' coppie di agenti in sequenza nel corso della sua vita, non
    solo un singolo scambio.
    """

    def __init__(self, host: str, port: int, tick_interval: float, ledger=None) -> None:
        self.host = host
        self.port = port
        self.tick_interval = tick_interval
        # Se non viene passato esplicitamente un ledger (es. da
        # broker_server.py dopo aver chiamato create_ledger()), il
        # comportamento resta quello originale: RAM pura, zero dipendenze
        # esterne. Broker non sa ne' deve sapere se il ledger che riceve e'
        # in RAM o su Supabase: la stessa identica interfaccia pubblica
        # (open_wallet/balance_of/transfer/summary) copre entrambi i casi.
        self.ledger = ledger if ledger is not None else MicroLedger()
        self.connections: Dict[str, Any] = {}
        self.offers: Dict[str, dict] = {}       # passaporto (consumer) -> contratto offerto
        self.provisions: Dict[str, dict] = {}   # passaporto (provider) -> risorsa fornita
        self.session_of: Dict[str, "StreamSession"] = {}
        self._rate_limiter = RateLimiter() if RATE_LIMIT_ENABLED else None
        self._server = None

    async def start(self, process_request=None) -> None:
        """
        process_request: hook opzionale passato a websockets.serve(), usato
        in Fase Sandbox per rispondere con un 200 OK alle richieste HTTP
        "semplici" (health-check) senza confonderle con handshake websocket
        veri. Vedi broker_server.py per l'implementazione concreta.
        """
        self._server = await websockets.serve(
            self._handle_client, self.host, self.port,
            process_request=process_request, max_size=MAX_MESSAGE_SIZE,
        )
        logging.info(f"[BROKER] in ascolto su ws://{self.host}:{self.port} -- Micro-Ledger inizializzato.")

    async def close(self) -> None:
        """Smette di accettare nuove connessioni e chiude ordinatamente
        quelle aperte (handshake di chiusura websocket, non un taglio secco)
        -- e' quanto basta perche' i client, che gia' gestiscono
        ConnectionClosed, se ne accorgano ed escano puliti anche loro."""
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        # Chiusura pulita del ledger persistente: flush finale + pool chiuso.
        _aclose = getattr(self.ledger, "aclose", None)
        if _aclose is not None:
            try:
                await _aclose()
            except Exception as exc:
                logging.error(f"[BROKER] chiusura del ledger fallita: {exc}")

    # ---- Dynamic Order Book (service discovery) ---------------------------
    @staticmethod
    def _voce_order_book(contract: dict, status: str, adesso: float) -> dict:
        """Proietta un contratto provider in una voce PUBBLICA dell'Order
        Book. Solo campi informativi: mai riferimenti a websocket, code o
        altri oggetti vivi -- la voce deve essere JSON-serializzabile e
        innocua da mostrare a chiunque abbia un passaporto valido.

        I NOMI DEI CAMPI sono in inglese di proposito, a differenza dei
        commenti di questo file: questo dict e' l'UNICA struttura dati che
        attraversa il confine verso un consumer esterno al codice sorgente
        (un altro bot, ma anche un browser su GET /orderbook -- vedi
        broker_server.py). Il resto del sorgente resta in italiano per
        convenzione di questo progetto; il wire format pubblico no."""
        return {
            "resource":       contract.get("resource", "?"),
            "price_per_sec":  contract.get("price_per_sec"),
            "price_per_kb":   contract.get("price_per_kb"),
            "description":    contract.get("description", ""),
            "status":         status,        # "available" | "busy"
            "listed_for_sec": round(adesso - contract.get("_registrato_alle", adesso), 1),
        }

    def order_book_snapshot(self) -> dict:
        """
        Fotografia ISTANTANEA del mercato: {passaporto_provider: voce}.

        NON e' un registro parallelo da tenere sincronizzato: e' una VISTA
        derivata, ad ogni chiamata, dalle due strutture che il broker
        mantiene comunque per il matching --
          * self.provisions  -> provider in vetrina, "available";
          * self.session_of  -> provider dentro una StreamSession, "busy".
        Cosi' la pulizia e' garantita per costruzione dagli stessi punti che
        gia' governano il ciclo di vita: la disconnessione (il finally di
        _handle_client fa pop da provisions) e la fine sessione (il _cleanup
        della StreamSession svuota session_of). Un dizionario separato
        aprirebbe esattamente la classe di bug "ghost offer" gia' vista e
        corretta in passato -- qui non puo' esistere divergenza perche' non
        esiste una seconda copia.

        Funzione PURA e sincrona su dict in RAM: O(n) sui provider, nessun
        await, nessun lock -- chiamarla non puo' bloccare il tick loop.
        """
        adesso = time.time()
        book: Dict[str, dict] = {}
        for pid, contract in self.provisions.items():
            book[pid] = self._voce_order_book(contract, "available", adesso)
        for pid, sess in self.session_of.items():
            if pid == getattr(sess, "provider_id", None) and pid not in book:
                book[pid] = self._voce_order_book(getattr(sess, "provision", {}) or {}, "busy", adesso)
        return book


    @staticmethod
    def _verifica_busta(busta: dict) -> Tuple[bool, Any]:
        """
        Verifica una busta firmata: {"payload", "passport", "timestamp", "signature"}.
        Ritorna (True, payload) se tutto torna, (False, motivo_del_rifiuto) altrimenti.

        Questa e' l'UNICA porta d'ingresso per qualunque messaggio dal
        client al broker: nessun messaggio viene mai elaborato senza essere
        passato di qui prima. Non e' una funzionalita' opzionale (a
        differenza, per dire, della persistenza Supabase): un fallback
        "se manca la firma accetta comunque" vanificherebbe esattamente la
        protezione che questa modifica esiste per introdurre.
        """
        try:
            payload = busta["payload"]
            passport_hex = busta["passport"]
            timestamp = busta["timestamp"]
            signature_hex = busta["signature"]
        except (KeyError, TypeError):
            return False, "busta malformata: manca payload/passport/timestamp/signature"

        try:
            eta = abs(time.time() - float(timestamp))
        except (TypeError, ValueError):
            return False, "timestamp non numerico"
        if eta > MAX_CLOCK_SKEW_SEC:
            return False, f"timestamp fuori dalla finestra di validita' ({eta:.1f}s, limite {MAX_CLOCK_SKEW_SEC}s)"

        try:
            public_key = Ed25519PublicKey.from_public_bytes(bytes.fromhex(passport_hex))
        except (ValueError, TypeError):
            return False, "passaporto non e' una chiave pubblica Ed25519 valida"

        canonico = json.dumps(
            {"payload": payload, "passport": passport_hex, "timestamp": timestamp},
            sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")

        try:
            public_key.verify(bytes.fromhex(signature_hex), canonico)
        except (InvalidSignature, ValueError):
            return False, "firma non valida"

        return True, payload

    async def _handle_client(self, ws) -> None:
        passport_id: Optional[str] = None
        try:
            async for raw in ws:
                try:
                    # Parse fuori dal loop se il messaggio e' grande (es. un
                    # compute_chunk 'crypto:all' da centinaia di KB): il
                    # broker e' il componente CONDIVISO, il suo event loop
                    # non deve mai pagare millisecondi di CPU per un singolo
                    # client. Vedi OFFLOAD_JSON_BYTES.
                    busta = await _json_loads_smart(raw)
                except json.JSONDecodeError:
                    logging.warning("[BROKER] JSON non valido ricevuto: connessione troncata.")
                    break

                # Verifica Ed25519 in un worker thread: su una busta grande
                # la sola ricostruzione canonica del payload costa ~4-5 ms
                # di CPU (misurato) -- inline strozzerebbe il tick loop di
                # TUTTE le sessioni attive, non solo di questa connessione.
                # _verifica_busta e' una funzione pura (nessuno stato
                # condiviso): eseguirla in thread e' sicuro per costruzione.
                ok, risultato = await asyncio.to_thread(self._verifica_busta, busta)
                if not ok:
                    logging.warning(f"[BROKER] messaggio rifiutato da {passport_id or '(sconosciuto)'}: "
                                     f"{risultato} -- connessione troncata.")
                    try:
                        await ws.send(json.dumps({"type": "errore_crittografico", "motivo": risultato}))
                    except websockets.exceptions.ConnectionClosed:
                        pass
                    break  # tronca la connessione: nessun messaggio non autenticato viene mai elaborato

                msg = risultato
                # Da qui in poi il mittente e' CRITTOGRAFICAMENTE garantito
                # essere il detentore della chiave privata di questo
                # passaporto -- non piu' una stringa a piacere dichiarata
                # dal client, come nella versione vulnerabile precedente.
                passport_id = busta["passport"]
                mtype = msg.get("type")

                # --- RATE LIMIT CRITTOGRAFICO (per-passaporto) -------------
                # Applicato DOPO la verifica firma (l'identita' e' certa) e
                # su TUTTO tranne l'hello, che apre il wallet una sola volta.
                # A differenza del rifiuto crittografico, qui NON tronchiamo
                # la connessione: scartiamo il singolo messaggio in eccesso e
                # rispondiamo con un errore, cosi' un client legittimo che va
                # troppo veloce rallenta invece di essere disconnesso.
                if (self._rate_limiter is not None and mtype != "hello"
                        and not self._rate_limiter.allow(passport_id)):
                    logging.warning(f"[BROKER] RATE_LIMIT_EXCEEDED per {passport_id[:12]}… "
                                    f"(mtype={mtype}) -- messaggio scartato.")
                    try:
                        await ws.send(json.dumps({"error": "RATE_LIMIT_EXCEEDED"}))
                    except websockets.exceptions.ConnectionClosed:
                        break
                    continue

                if mtype == "hello":
                    self.connections[passport_id] = ws
                    balance = await self.ledger.open_wallet(passport_id, msg.get("initial_balance", 0.0))
                    logging.info(f"[BROKER] hello da {passport_id[:12]}… (proto v{msg.get('proto', '<pre-3>')})")
                    await ws.send(json.dumps({"type": "welcome", "balance": balance, "proto": PROTOCOL_VERSION}))
                    continue

                if mtype == "get_order_book":
                    # Service discovery. Intercettato QUI, prima del routing
                    # di sessione, di proposito: cosi' anche un agente gia'
                    # impegnato in uno scambio puo' consultare il mercato
                    # senza che la richiesta finisca in una inbox di sessione.
                    # Percorso interamente non bloccante: snapshot sincrono
                    # su dict in RAM + dumps smart (worker thread se il
                    # registro e' affollato) + una singola send. Il tick loop
                    # delle sessioni attive non se ne accorge nemmeno.
                    snapshot = self.order_book_snapshot()
                    risposta = {"type": "order_book", "server_time": time.time(),
                                "count": len(snapshot), "providers": snapshot}
                    await ws.send(await _json_dumps_smart(risposta, voci_stimate=len(snapshot)))
                    trace("BROKER", f"order book servito a {passport_id[:12]}… ({len(snapshot)} voci)")
                    continue

                # Se questo passaporto e' gia' dentro una sessione di streaming
                # attiva, tutto cio' che invia da qui in poi e' traffico di
                # sessione: lo instradiamo alla coda giusta invece di
                # re-interpretarlo come una nuova offerta/disponibilita'.
                session = self.session_of.get(passport_id)
                if session is not None:
                    if mtype == "cancel_volontario" and passport_id == session.consumer_id:
                        # Non instradiamo in coda: l'annullamento deve avere
                        # effetto alla PROSSIMA iterazione del tick loop, non
                        # aspettare che qualcuno consumi la coda giusta (che
                        # potrebbe non succedere mai piu', es. il consumer
                        # non manda dati -- vedi _consumer_data_done).
                        session.request_voluntary_cancel(msg.get("motivo", ""))
                        continue
                    inbox = session.provider_inbox if passport_id == session.provider_id else session.consumer_inbox
                    await inbox.put(msg)
                    continue

                if mtype == "offer":
                    self.offers[passport_id] = msg["contract"]
                    logging.info(f"[BROKER] offerta registrata da {passport_id[:12]}…")
                    await self._try_match()
                elif mtype == "provide":
                    # Copia difensiva + timestamp d'ingresso in vetrina: il
                    # campo "_registrato_alle" (prefisso _ = interno) serve
                    # SOLO all'Order Book per il campo "listed_for_sec";
                    # il matching continua a leggere unicamente "resource".
                    contract = dict(msg["contract"])
                    contract["_registrato_alle"] = time.time()
                    self.provisions[passport_id] = contract
                    logging.info(f"[BROKER] disponibilita' registrata da {passport_id[:12]}… "
                                 f"('{contract.get('resource', '?')}' ora nell'Order Book)")
                    await self._try_match()

        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            if passport_id:
                self.connections.pop(passport_id, None)
                # Un client che si disconnette PRIMA di essere abbinato (es.
                # durante il debug: Ctrl+C ripetuti mentre si aspetta un
                # peer) non deve lasciare un'offerta/disponibilita' "fantasma"
                # nel pool -- altrimenti un tentativo FUTURO puo' essere
                # abbinato per errore a una connessione ormai morta invece
                # che al peer realmente in ascolto in quel momento.
                self.offers.pop(passport_id, None)
                self.provisions.pop(passport_id, None)
                if self._rate_limiter is not None:
                    self._rate_limiter.forget(passport_id)
                # Flush ISTANTANEO del saldo alla disconnessione (fine
                # inferenza o drop): il DB diventa consistente subito ai
                # confini di sessione, senza attendere il task periodico.
                _flush = getattr(self.ledger, "flush", None)
                if _flush is not None:
                    try:
                        await _flush({passport_id})
                    except Exception as exc:
                        logging.error(f"[BROKER] flush su disconnessione fallito per "
                                      f"{passport_id[:12]}…: {exc}")

    @staticmethod
    def _resource_compatible(provided: str, requested: str) -> bool:
        """
        Regola di compatibilita' domanda/offerta, ora dinamica invece che a
        stringa fissa:
          * uguaglianza esatta -> sempre compatibile (comportamento originale,
            es. entrambi 'primes' o entrambi 'crypto:SOLUSDT');
          * un provider che dichiara "<namespace>:all" copre QUALSIASI
            richiesta con lo stesso namespace (es. un Nodo B che offre
            'crypto:all' -- l'intero mercato in cache -- puo' soddisfare
            sia 'crypto:all' sia 'crypto:SOLUSDT', perche' puo' filtrare al
            volo. Il contrario NON vale: un provider che offre solo
            'crypto:SOLUSDT' non puo' soddisfare una richiesta di 'crypto:all',
            perche' non possiede l'intero mercato.
        """
        if provided == requested:
            return True
        if provided.endswith(":all"):
            namespace = provided[: -len("all")]  # es. "crypto:"
            return requested.startswith(namespace)
        return False

    @staticmethod
    def _prezzo_provider(provision: dict) -> float:
        """Prezzo dichiarato nel manifesto firmato (will_provide): somma delle
        due componenti gia' presenti nel contratto. E' la metrica di ordinamento
        per il match economico -- un singolo scalare confrontabile con max_price."""
        return round(float(provision.get("price_per_sec", 0.0) or 0.0)
                     + float(provision.get("price_per_kb", 0.0) or 0.0), 8)

    async def _try_match(self) -> None:
        """Abbina offerta consumer <-> disponibilita' provider. Due fasi:
          1. filtro di COMPATIBILITA': stessa risorsa (_resource_compatible)
             e, se il consumer ha dichiarato max_price, prezzo del provider
             entro il tetto;
          2. SELEZIONE: tra i provider validi, il PIU' ECONOMICO
             (_prezzo_provider). Senza max_price e con un solo candidato, il
             comportamento e' identico a prima -- nessuna regressione.
        Indipendente dall'ordine di arrivo dei due nodi."""
        for consumer_id, offer in list(self.offers.items()):
            request = offer["request"]
            wanted_resource = request["resource"]
            max_price = request.get("max_price")

            # -- fase 1: raccogli i provider compatibili (+ entro il tetto) --
            candidati = []
            for provider_id, provision in list(self.provisions.items()):
                if provider_id == consumer_id:
                    continue
                if not self._resource_compatible(provision["resource"], wanted_resource):
                    continue
                if max_price is not None and self._prezzo_provider(provision) > max_price + 1e-9:
                    continue
                candidati.append((provider_id, provision))

            if not candidati:
                continue  # per questo consumer nessun match ORA: resta in attesa (grace lato client)

            # -- fase 2: il piu' economico vince (tie-break: ordine stabile) --
            provider_id, provision = min(candidati, key=lambda pv: self._prezzo_provider(pv[1]))

            del self.offers[consumer_id]
            del self.provisions[provider_id]
            session = StreamSession(self, consumer_id, provider_id, offer, provision)
            self.session_of[consumer_id] = session
            self.session_of[provider_id] = session
            prezzo = self._prezzo_provider(provision)
            logging.info(f"[BROKER] match trovato: {consumer_id[:12]}… <-> {provider_id[:12]}… "
                         f"('{provision['resource']}' soddisfa '{wanted_resource}'"
                         + (f", prezzo {prezzo} <= max {max_price}, "
                            f"il piu' economico tra {len(candidati)}" if max_price is not None else "")
                         + ")")
            asyncio.create_task(session.run())
            return


# ==============================================================================
# 3. LO STREAM SESSION -- motore di scambio ibrido + regolamento "trustless"
# ==============================================================================
class StreamSession:
    """
    Orchestra UNO scambio ibrido (denaro + dati) <-> (calcolo), tick per
    tick. E' qui che vive la garanzia "trustless / streaming" richiesta:

    Ad ogni tick (ogni `tick_interval` secondi):
      1. Il provider DEVE aver mandato un nuovo chunk di calcolo reale.
      2. SOLO DOPO averlo ricevuto, il ledger sposta il denaro di quel
         singolo tick -- mai in anticipo, mai "a credito".
      3. Il consumer DEVE aver mandato un chunk di dati, finche' il suo
         dataset non e' esaurito (l'esaurimento non e' una violazione: e'
         semplicemente la fine di una delle due gambe del baratto).
      4. Se un qualsiasi passo salta il suo timeout, lo scambio si ferma
         immediatamente: nessun ulteriore pagamento, dato o calcolo viene
         piu' rilasciato da quel momento in poi.

    Perdita massima possibile per una delle due parti: il valore di UN
    singolo tick -- non un centesimo di piu' -- perche' ogni tick
    successivo richiede una NUOVA prova prima di essere liquidato.
    """

    def __init__(self, broker: Broker, consumer_id: str, provider_id: str, offer: dict, provision: dict) -> None:
        self.broker = broker
        self.consumer_id = consumer_id
        self.provider_id = provider_id
        self.provision = provision  # contratto del provider: l'Order Book lo proietta come voce "occupato"
        self.session_id = uuid.uuid4().hex[:8]

        self.rate_per_sec: float = offer["provides"].get("money_per_sec", 0.0)
        self.rate_per_kb: float = offer["provides"].get("money_per_kb", 0.0)
        self.target: Optional[int] = offer["request"]["param"]  # None = nessun limite (streaming illimitato)
        self.resource_name: str = offer["request"]["resource"]
        self.mode: str = offer["request"].get("mode", "count")  # "count": target = unita' consegnate
                                                                   # "duration": target = numero di tick

        self.consumer_inbox: asyncio.Queue = asyncio.Queue()
        self.provider_inbox: asyncio.Queue = asyncio.Queue()

        self.delivered_count = 0
        self.total_paid = 0.0
        self.data_bytes_sent = 0
        self.tick_no = 0
        # Se il consumer non ha MAI dichiarato un dataset (will_offer senza
        # data_file -- es. paga solo in denaro, come il nuovo Nodo A che
        # compra un feed di mercato), non c'e' alcuna prova di dati da
        # attendere fin dal primo tick: e' l'equivalente di "esaurito
        # subito", non "in attesa del primo chunk".
        self._consumer_data_done = "chunks_available" not in offer["provides"]

        # Annullamento VOLONTARIO da parte del consumer (es. un agente che
        # decide autonomamente di rinunciare a un feed troppo costoso): un
        # semplice Event non bloccante, controllato all'inizio di ogni tick.
        # Distinto da timeout/disconnessione -- vedi request_voluntary_cancel().
        self._cancel_event = asyncio.Event()
        self._cancel_reason = "consumer_annullamento_volontario"

    def request_voluntary_cancel(self, reason: str = "") -> None:
        """
        Chiamato in modo sincrono e non bloccante da Broker._handle_client
        quando arriva un messaggio 'cancel_volontario' dal consumer. Non
        tocca lo stato del tick loop direttamente: alza solo un flag che
        il loop controlla alla prossima iterazione -- niente task paralleli,
        niente corsa con un _halt() lanciato da un altro punto del codice.
        """
        if reason:
            self._cancel_reason = f"consumer_annullamento_volontario:{reason}"
        self._cancel_event.set()

    def _still_going(self) -> bool:
        if self._cancel_event.is_set():
            return False
        if self.target is None:
            return True  # streaming illimitato: si ferma solo per fondi/disconnessione/annullamento
        return self.delivered_count < self.target

    @staticmethod
    def _weigh(chunk) -> int:
        """Peso in byte del chunk, per la componente di tariffa 'per KB'.
        Un chunk vuoto (es. simbolo non ancora visto nella cache) pesa 0."""
        if not chunk:
            return 0
        try:
            return len(json.dumps(chunk, default=str).encode("utf-8"))
        except (TypeError, ValueError):
            return 0

    async def run(self) -> None:
        consumer_ws = self.broker.connections.get(self.consumer_id)
        provider_ws = self.broker.connections.get(self.provider_id)
        if consumer_ws is None or provider_ws is None:
            # Difesa in profondita': anche con la pulizia in _handle_client,
            # una race strettissima (disconnessione proprio nell'istante tra
            # il matching e l'avvio di questo task) potrebbe ancora
            # presentarsi. Senza questo controllo, l'accesso diretto al dict
            # solleverebbe un KeyError non gestito DENTRO un task in
            # background: il match sparirebbe nel nulla, entrambi i peer
            # coinvolti resterebbero in attesa per sempre di un "matched"
            # che non arrivera' mai, senza alcun errore visibile nei log.
            logging.warning(
                f"[BROKER] sessione {self.session_id} annullata prima di iniziare: "
                f"un peer si e' gia' disconnesso (consumer={consumer_ws is not None}, "
                f"provider={provider_ws is not None})."
            )
            self._cleanup()
            return

        ok1 = await self._safe_send(consumer_ws, {"type": "matched", "peer": self.provider_id, "target": self.target})
        ok2 = await self._safe_send(provider_ws, {
            "type": "matched", "peer": self.consumer_id, "target": self.target, "mode": self.mode,
            "resource": self.resource_name,  # il provider puo' offrire un superset (es. 'crypto:all'):
                                              # deve sapere ESATTAMENTE cosa ha chiesto QUESTO consumer.
        })
        if not (ok1 and ok2):
            await self._halt("peer_disconnesso_prima_dell_inizio")
            return

        # Finestra di tolleranza prima di dichiarare un nodo "silente". E'
        # un multiplo del tick_interval per assorbire il jitter di rete/CPU
        # senza generare falsi positivi durante il normale funzionamento.
        grace = self.broker.tick_interval * 3

        while self._still_going():
            self.tick_no += 1
            tick_started = time.monotonic()

            # --- 1. prova di calcolo/dati dal provider (obbligatoria) --------
            try:
                compute_msg = await asyncio.wait_for(self.provider_inbox.get(), timeout=grace)
            except asyncio.TimeoutError:
                await self._halt("provider_ha_smesso_di_consegnare_cpu")
                return

            chunk = compute_msg.get("chunk", [])

            # --- 2. liquidazione, SOLO ora, atomica, via ledger -------------
            # La tariffa non e' piu' fissa: e' calcolata AL VOLO su cosa e'
            # stato effettivamente consegnato in QUESTO tick -- una richiesta
            # 'crypto:all' (migliaia di simboli) costa piu' di una singola
            # 'crypto:SOLUSDT' nello stesso identico protocollo, perche' paga
            # anche per il PESO del payload, non solo per il tempo.
            tick_amount = round(
                self.rate_per_sec * self.broker.tick_interval + self.rate_per_kb * (self._weigh(chunk) / 1024.0),
                8,
            )
            paid = await self.broker.ledger.transfer(
                self.consumer_id, self.provider_id, tick_amount,
                memo=f"{self.resource_name}#tick{self.tick_no}",
            )
            if not paid:
                # Il chunk che B ha appena prodotto NON viene ne' pagato ne'
                # inoltrato ad A: resta un costo (gia' sostenuto) a carico di
                # B. delivered_count/total_paid contano solo cio' che e'
                # stato DAVVERO liquidato -- mai lavoro "a credito".
                await self._halt("consumer_fondi_esauriti")
                return
            self.total_paid = round(self.total_paid + tick_amount, 8)
            self.delivered_count += len(chunk) if self.mode == "count" else 1

            # --- 3. prova di dati dal consumer (finche' il dataset non finisce)
            data_chunk = ""
            if not self._consumer_data_done:
                try:
                    data_msg = await asyncio.wait_for(self.consumer_inbox.get(), timeout=grace)
                except asyncio.TimeoutError:
                    await self._halt("consumer_ha_smesso_di_inviare_dati")
                    return
                if data_msg.get("type") == "data_complete":
                    self._consumer_data_done = True
                else:
                    data_chunk = data_msg.get("chunk", "")
                    self.data_bytes_sent += len(data_chunk)

            # --- 4. relay dei payload + ricevuta di liquidazione a entrambi -
            ok = await self._safe_send(consumer_ws, {
                "type": "result_chunk", "chunk": chunk,
                "progress": self.delivered_count, "target": self.target,
            })
            if not ok:
                await self._halt("consumer_disconnesso")
                return

            if data_chunk:
                ok = await self._safe_send(provider_ws, {"type": "data_chunk", "chunk": data_chunk})
                if not ok:
                    await self._halt("provider_disconnesso")
                    return

            settlement = {
                "type": "settlement", "tick": self.tick_no, "amount": tick_amount,
                "consumer_balance": self.broker.ledger.balance_of(self.consumer_id),
                "provider_balance": self.broker.ledger.balance_of(self.provider_id),
            }
            ok1 = await self._safe_send(consumer_ws, settlement)
            ok2 = await self._safe_send(provider_ws, settlement)
            if not (ok1 and ok2):
                await self._halt("un_peer_si_e_disconnesso_durante_la_liquidazione")
                return

            # --- 5. cadenza onesta: il tick_interval E' la granularita' di
            # fiducia del protocollo (vedi spiegazione finale).
            elapsed = time.monotonic() - tick_started
            await asyncio.sleep(max(0.0, self.broker.tick_interval - elapsed))

        if self._cancel_event.is_set():
            # Il consumer ha scelto DELIBERATAMENTE di fermarsi (es. un
            # agente che rinuncia a un feed troppo costoso): non e' un
            # traguardo raggiunto, ma nemmeno un fallimento -- una via di
            # mezzo che merita il proprio motivo esplicito nei log.
            await self._halt(self._cancel_reason)
            return

        await self._complete()

    async def _halt(self, reason: str) -> None:
        payload = {
            "type": "halted", "reason": reason, "ticks": self.tick_no,
            "total_paid": self.total_paid, "delivered": self.delivered_count,
            "target": self.target, "data_bytes_sent": self.data_bytes_sent,
        }
        await self._broadcast(payload)
        logging.info(f"[BROKER] sessione {self.session_id} interrotta: {reason}")
        self._cleanup()

    async def _complete(self) -> None:
        payload = {
            "type": "complete", "reason": "obiettivo_raggiunto", "ticks": self.tick_no,
            "total_paid": self.total_paid, "delivered": self.delivered_count,
            "target": self.target, "data_bytes_sent": self.data_bytes_sent,
        }
        await self._broadcast(payload)
        logging.info(f"[BROKER] sessione {self.session_id} completata con successo.")
        logging.info(f"[BROKER] riconciliazione ledger: {self.broker.ledger.summary()}")
        self._cleanup()

    async def _safe_send(self, ws, payload: dict) -> bool:
        """Invio 'difensivo': se il peer ha gia' chiuso la connessione (crash,
        kill del processo, rete caduta), non lasciamo che un'eccezione non
        gestita faccia esplodere il task della sessione -- la trattiamo come
        un segnale di disconnessione, esattamente come un timeout."""
        try:
            await ws.send(json.dumps(payload))
            return True
        except websockets.exceptions.ConnectionClosed:
            return False

    async def _broadcast(self, payload: dict) -> None:
        for pid in (self.consumer_id, self.provider_id):
            ws = self.broker.connections.get(pid)
            if ws is not None:
                await self._safe_send(ws, payload)

    def _cleanup(self) -> None:
        self.broker.session_of.pop(self.consumer_id, None)
        self.broker.session_of.pop(self.provider_id, None)


# ==============================================================================
# 4. L'AGENT -- l'unica classe che node_a.py / node_b.py devono conoscere
# ==============================================================================
class Agent:
    """
    Facciata pubblica dell'SDK. Un nodo "consumer" (ha soldi/dati, vuole
    calcolo) la usa cosi':

        node = Agent("Nodo-X", balance=1.00, broker_url="ws://localhost:8765")
        node.will_offer(money_per_sec=0.001, data_file="dataset.txt")
        node.will_request(resource="primes", param=10_000)
        result = await node.run()

    Un nodo "provider" (vende CPU) cosi':

        node = Agent("Nodo-Y", broker_url="ws://localhost:8765")
        node.will_provide(resource="primes", handler=my_compute_fn)
        result = await node.run()

    Tutto il resto -- il framing dei messaggi, il matching, il ciclo di
    liquidazione a tick, la gestione delle disconnessioni -- resta nascosto
    qui dentro. .run() non solleva MAI un'eccezione di rete verso il
    chiamante: qualunque interruzione "attesa" (broker irraggiungibile,
    connessione persa, peer che sparisce) torna come un dict di risultato
    con un campo "type" chiaro, esattamente come "complete"/"halted".

    IDENTITA': il "passaporto" non e' piu' una stringa a piacere -- e' la
    chiave pubblica Ed25519 dell'agente, in esadecimale. Alla prima
    esecuzione viene generata una coppia di chiavi e salvata su file
    (keyfile, di default derivato da `name`); alle esecuzioni successive
    la stessa coppia viene ricaricata, cosi' l'identita' (e il saldo che
    la persistenza Supabase associa ad essa) resta la stessa nel tempo.
    Ogni messaggio inviato al broker viene firmato con la chiave privata:
    nessuno puo' spendere o incassare a nome di un passaporto senza
    possedere la chiave privata corrispondente.
    """

    def __init__(self, name: str, balance: float = 0.0,
                 broker_url: str = DEFAULT_BROKER_URL,
                 keyfile: Optional[str] = None) -> None:
        self.name = name
        self.balance = balance
        self.broker_url = broker_url

        # IDENTITA' PIGRA E FUORI DAL LOOP -- il costruttore non tocca piu'
        # ne' il disco ne' la crittografia. Il caricamento/generazione della
        # coppia Ed25519 (lettura/scrittura di .agent_keys.json inclusa)
        # avviene UNA sola volta, dentro run(), via asyncio.to_thread: su un
        # SSD "pulito" costerebbe ~0.2 ms, ma su Windows la PRIMA scrittura
        # di un .json nuovo puo' restare bloccata decine o centinaia di ms
        # dietro la scansione real-time dell'antivirus, e su una cartella
        # sincronizzata (OneDrive/rete) anche secondi: dentro __init__ --
        # cioe' dentro un event loop gia' in esecuzione, come accade in
        # node_b dove l'Agent nasce accanto al task del feed Binance --
        # quel blocco congelerebbe TUTTI i task del processo. In un worker
        # thread, congela solo se' stesso.
        self._keyfile = keyfile or self._nome_file_default(name)
        self._private_key: Optional[Ed25519PrivateKey] = None
        self.passport_id: Optional[str] = None   # disponibile DOPO la prima run() (o ensure_identity())
        self._passport_breve = "(identita' non ancora inizializzata)"
        self._identity_lock = asyncio.Lock()      # due run() concorrenti non devono generare due chiavi

        self._offer_contract: Optional[dict] = None
        self._provision_contract: Optional[dict] = None
        self._provider_fn: Optional[Callable[[Any], Tuple[list, Any]]] = None
        self._on_data: Optional[Callable[[str], None]] = None
        self._data_chunks: Optional[List[str]] = None
        self._data_idx = 0
        self._initial_balance = balance  # fisso alla creazione: serve da riferimento alla Utility Function
                                          # del consumer (es. "non spendere piu' del 50% di QUESTO").
        self._on_tick: Optional[Callable[[dict], Optional[str]]] = None
        self._pending_downgrade: Optional[str] = None

        self._ws = None

    async def ensure_identity(self) -> str:
        """
        Garantisce che la coppia di chiavi Ed25519 sia caricata (o generata
        e salvata su disco) SENZA MAI bloccare l'event loop: tutto l'I/O
        sincrono e la generazione avvengono in un worker thread. Idempotente
        e protetta da lock: chiamabile quante volte si vuole, il lavoro vero
        succede una volta sola. Ritorna il passport_id (chiave pubblica hex).
        """
        if self._private_key is not None:
            return self.passport_id
        async with self._identity_lock:
            if self._private_key is not None:   # double-check: un'altra run() ha gia' fatto il lavoro
                return self.passport_id
            trace(self.name, f"carico/genero identita' Ed25519 da '{self._keyfile}' (worker thread)...")
            self._private_key = await asyncio.to_thread(self._carica_o_genera_chiavi, self._keyfile)
            public_bytes = self._private_key.public_key().public_bytes(
                encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
            )
            self.passport_id = public_bytes.hex()  # la vera identita': la chiave pubblica, non piu' una stringa a piacere
            self._passport_breve = self.passport_id[:12] + "…"  # solo per i log: leggibile a schermo, non usato nel protocollo
            trace(self.name, f"identita' pronta: {self._passport_breve}")
        return self.passport_id

    @staticmethod
    def _nome_file_default(name: str) -> str:
        """File nascosto (convenzione Unix del punto iniziale) e specifico
        per QUESTO nome-agente: se due Agent con nomi diversi girano nella
        stessa cartella (es. node_a.py e node_b.py lanciati dalla stessa
        directory), non devono sovrascriversi a vicenda le chiavi."""
        sicuro = re.sub(r"[^a-zA-Z0-9_-]", "_", name)
        return f".{sicuro}.agent_keys.json"

    @staticmethod
    def _carica_o_genera_chiavi(keyfile: str) -> Ed25519PrivateKey:
        path = Path(keyfile)
        if path.exists():
            dati = json.loads(path.read_text(encoding="utf-8"))
            return Ed25519PrivateKey.from_private_bytes(bytes.fromhex(dati["private_key_hex"]))

        chiave = Ed25519PrivateKey.generate()
        priv_bytes = chiave.private_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )
        pub_bytes = chiave.public_key().public_bytes(
            encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw
        )
        path.write_text(
            json.dumps({"private_key_hex": priv_bytes.hex(), "public_key_hex": pub_bytes.hex()}, indent=2),
            encoding="utf-8",
        )
        try:
            os.chmod(path, 0o600)  # solo il proprietario puo' leggerla -- best-effort, Windows la ignora in gran parte
        except OSError:
            pass
        logging.info(f"[{keyfile}] nuova identita' generata: {pub_bytes.hex()[:12]}…")
        return chiave

    # ---- dichiarazioni (da chiamare PRIMA di run()) --------------------------

    def will_offer(self, *, money_per_sec: float = 0.0, money_per_kb: float = 0.0,
                   data_file: Optional[str] = None, chunk_chars: int = 400) -> "Agent":
        if self._offer_contract is None:
            self._offer_contract = {}
        payload: Dict[str, Any] = {"money_per_sec": money_per_sec, "money_per_kb": money_per_kb}
        if data_file:
            with open(data_file, "r", encoding="utf-8") as f:
                text = f.read()
            self._data_chunks = [text[i:i + chunk_chars] for i in range(0, len(text), chunk_chars)] or [""]
            payload["data_file"] = data_file
            payload["chunks_available"] = len(self._data_chunks)
        self._offer_contract["provides"] = payload
        return self

    def will_request(self, resource: str, param: Any = None, mode: str = "count",
                      on_tick: Optional[Callable[[dict], Optional[str]]] = None,
                      max_price: Optional[float] = None) -> "Agent":
        """
        mode="count" (default): `param` e' un NUMERO DI UNITA' da raccogliere
        (es. 8000 numeri primi) -- comportamento originale.
        mode="duration": `param` e' un NUMERO DI TICK da far durare lo
        streaming (adatto a risorse continue come un feed di mercato, dove
        "quante unita' totali" non ha un significato naturale). `param=None`
        in modalita' "duration" significa "senza limite": lo streaming
        prosegue finche' non finiscono i fondi o qualcuno si disconnette.

        max_price: tetto opzionale al prezzo del provider. Se impostato, il
        broker abbina SOLO provider il cui prezzo dichiarato (price_per_sec
        + price_per_kb, i campi gia' firmati nel manifesto will_provide) e'
        <= max_price, e tra quelli sceglie il PIU' ECONOMICO. Se nessun
        provider compatibile rientra nel tetto entro la grace window, il
        risultato di run() e' {"type": "no_nodes_available"}. Ometterlo
        mantiene il comportamento storico (match sulla sola risorsa).

        on_tick: aggancio opzionale per una Utility Function del consumer.
        Viene chiamato DOPO ogni liquidazione con un dict
        {"tick", "balance", "initial_balance", "spent_this_tick", "total_spent"}.
        Se ritorna una stringa (una nuova resource, es. "crypto:BTCUSDT"),
        l'SDK annulla VOLONTARIAMENTE la sessione corrente (vedi
        request_voluntary_cancel) e il risultato di run() conterra'
        "downgrade_a": <stringa> -- sta al chiamante decidere se e come
        avviare una nuova richiesta. L'SDK fornisce solo l'aggancio: la
        strategia (QUANDO e VERSO COSA fare downgrade) e' logica di
        business del nodo, non del protocollo.
        """
        if self._offer_contract is None:
            self._offer_contract = {}
        req = {"resource": resource, "param": param, "mode": mode}
        if max_price is not None:
            req["max_price"] = float(max_price)
        self._offer_contract["request"] = req
        self._on_tick = on_tick
        return self

    async def buy_data(self, resource: str, *, ticks: int = 10,
                       money_per_sec: float = 0.0002, money_per_kb: float = 0.0001,
                       on_tick: Optional[Callable[[dict], Optional[str]]] = None) -> dict:
        """
        Zucchero sintattico su will_offer()+will_request()+run(), per il
        caso comune "compra N tick di `resource` a queste tariffe, in
        un'unica chiamata" -- pensato per il flusso da Quick Start:

            menu = await agente.get_market_menu(stampa=False)
            risorsa = next(iter(menu))          # o una scelta esplicita
            risultato = await agente.buy_data(menu[risorsa]["resource"], ticks=20)

        Ritorna ESATTAMENTE lo stesso dict di run() (stessi "type"/"reason"/
        "ticks"/"total_paid"/eventuale "downgrade_a"): nessuna forma nuova
        da imparare, buy_data non e' un protocollo parallelo, e' solo il
        modo piu' corto di percorrere quello esistente per un acquisto
        singolo. mode="duration" e' cablato di proposito: e' l'unico che ha
        senso quando cio' che si compra e' un flusso continuo (feed di
        mercato, analisi in tempo reale) invece di un numero fisso di unita'.

        ATTENZIONE -- questo e' il primitivo da "quick start", non quello
        da produzione: UNA sola connessione, NESSUN retry/backoff se il
        broker o il provider non ci sono in quel momento (a differenza del
        ciclo con backoff esponenziale e ripresa dei tick di node_a.py:main()).
        Per una sessione resiliente su piu' fasi, comporre will_offer() +
        will_request() + run() a mano dentro un proprio ciclo di retry
        resta la via consigliata -- vedi node_a.py nel repository.
        """
        self.will_offer(money_per_sec=money_per_sec, money_per_kb=money_per_kb)
        self.will_request(resource=resource, param=ticks, mode="duration", on_tick=on_tick)
        return await self.run()

    def will_provide(self, resource: str, handler: Callable[[Any, str], Tuple[list, Any]], *,
                     price_per_sec: Optional[float] = None,
                     price_per_kb: Optional[float] = None,
                     description: str = "",
                     on_data: Optional[Callable[[str], None]] = None) -> "Agent":
        """
        handler(cursor, resource_richiesta) -> (chunk, nuovo_cursor)

        Il secondo argomento (resource_richiesta) esiste per i provider
        "multiplexati": un Nodo che dichiara di offrire 'crypto:all' (un
        superset) puo' finire abbinato a consumer che hanno chiesto sia
        'crypto:all' sia una risorsa piu' specifica come 'crypto:SOLUSDT' --
        l'handler riceve ESATTAMENTE cosa e' stato richiesto in QUESTA
        sessione, cosi' puo' decidere se restituire tutto o solo un filtro.

        on_data: callback opzionale, SINCRONA e veloce (tipicamente un
        append a un buffer), invocata per OGNI data_chunk che il consumer
        invia come sua meta' del baratto. Il broker ha SEMPRE inoltrato
        questi chunk al provider (il protocollo di rete e' payload-agnostic
        per costruzione); fino a questa versione, pero', il client SDK si
        limitava a contarne i byte e li scartava -- il canale arrivava all'
        80% del percorso e mancava l'ultimo metro fino all'handler. Con
        on_data un provider puo' RICEVERE dati arbitrari dal consumer
        (documenti, memorie conversazionali, dataset), elaborarli, e
        restituire il risultato via handler: e' cio' che rende il baratto
        (denaro+dati) <-> (calcolo) completo in entrambe le direzioni.
        Parametro opzionale: tutti i provider esistenti restano invariati.

        I parametri keyword di listino sono il "listino" PUBBLICO del
        provider, pensato per il Dynamic Order Book (service discovery):
          * price_per_sec / price_per_kb -- tariffe indicative dichiarate;
          * description -- una riga di presentazione per i consumer.
        Sono INFORMATIVI: il regolamento economico dei tick resta governato
        dal contratto del consumer (will_offer), esattamente come prima.
        Tutti opzionali: i provider esistenti continuano a funzionare
        invariati, semplicemente compaiono nell'Order Book senza listino.
        """
        contract: Dict[str, Any] = {"resource": resource}
        if price_per_sec is not None:
            contract["price_per_sec"] = float(price_per_sec)
        if price_per_kb is not None:
            contract["price_per_kb"] = float(price_per_kb)
        if description:
            # Cap difensivo: la description finisce nel registro condiviso
            # e in OGNI risposta get_order_book -- non deve poter diventare
            # un vettore per gonfiare il broker.
            contract["description"] = str(description)[:200]
        self._provision_contract = contract
        self._provider_fn = handler
        self._on_data = on_data
        return self

    async def get_market_menu(self, *, stampa: bool = True) -> Optional[Dict[str, dict]]:
        """
        Service discovery: interroga il broker e restituisce il Dynamic
        Order Book -- {passaporto_provider: {resource, price_*, description,
        status, listed_for_sec}} -- cosi' un consumer puo' DECIDERE cosa
        chiedere con will_request() invece di doverlo sapere a priori.

            menu = await agente.get_market_menu()
            if menu: ...scegli la risorsa e poi will_request(...)

        Ritorna: il dict del registro ({} = mercato vuoto ma broker sano),
        oppure None se il broker non e' raggiungibile / l'handshake fallisce
        (motivo gia' loggato). Con stampa=True mostra anche una tabella
        leggibile -- comoda in demo e nei test manuali.

        Scelte di progetto, per non disturbare il traffico ad alta frequenza:
          * connessione one-shot DEDICATA: non tocca self._ws, quindi non
            interferisce mai con un run() eventualmente in corso su questo
            stesso agente ne' con le sessioni altrui;
          * stesso rito crittografico di run(): hello firmato + welcome con
            timeout -- l'Order Book si mostra solo a chi ha un passaporto
            Ed25519 valido, e un broker legacy silenzioso produce un errore
            diagnostico in HANDSHAKE_TIMEOUT_SEC, mai un congelamento;
          * side-effect free: il saldo riportato dal welcome NON sovrascrive
            self.balance -- consultare il menu non e' un'operazione contabile.
        """
        await self.ensure_identity()
        ws = None
        try:
            trace(self.name, f"[menu] connessione one-shot al broker {self.broker_url}...")
            ws = await websockets.connect(self.broker_url, max_size=MAX_MESSAGE_SIZE)

            busta_hello = await asyncio.to_thread(self._firma_busta, {
                "type": "hello", "passport_id": self.passport_id,
                "initial_balance": self.balance, "proto": PROTOCOL_VERSION,
            })
            await ws.send(busta_hello)
            welcome_raw = await asyncio.wait_for(ws.recv(), timeout=HANDSHAKE_TIMEOUT_SEC)
            welcome = json.loads(welcome_raw)
            if not isinstance(welcome, dict) or welcome.get("type") != "welcome":
                logging.warning(f"[{self.name}] [menu] handshake rifiutato dal broker: {welcome!r}")
                return None

            trace(self.name, "[menu] richiedo l'Order Book...")
            busta_req = await asyncio.to_thread(self._firma_busta, {"type": "get_order_book"})
            await ws.send(busta_req)
            raw = await asyncio.wait_for(ws.recv(), timeout=HANDSHAKE_TIMEOUT_SEC)
            msg = await _json_loads_smart(raw)
            if not isinstance(msg, dict) or msg.get("type") != "order_book":
                logging.warning(f"[{self.name}] [menu] risposta inattesa dal broker: "
                                f"{str(msg)[:120]!r}")
                return None

            providers: Dict[str, dict] = msg.get("providers", {}) or {}
            trace(self.name, f"[menu] Order Book ricevuto: {len(providers)} provider.")
            if stampa:
                self._stampa_menu(providers)
            return providers

        except (ConnectionRefusedError, OSError, asyncio.TimeoutError, TimeoutError,
                json.JSONDecodeError, websockets.exceptions.WebSocketException) as exc:
            # Stessa famiglia di esiti "il broker non c'e' ADESSO" di run():
            # per il chiamante e' un None ritentabile, mai un traceback.
            logging.info(f"[{self.name}] [menu] Order Book non disponibile: "
                         f"{type(exc).__name__}: {exc}")
            return None
        finally:
            if ws is not None:
                with contextlib.suppress(Exception):
                    await ws.close()

    @staticmethod
    def _stampa_menu(providers: Dict[str, dict]) -> None:
        print("\n" + "-" * 78)
        print(f" ORDER BOOK -- {len(providers)} provider sul mercato")
        print("-" * 78)
        if not providers:
            print("  (vuoto: nessun provider in vetrina in questo momento)")
        for pid, voce in providers.items():
            tariffe = []
            if voce.get("price_per_kb") is not None:
                tariffe.append(f"${voce['price_per_kb']}/KB")
            if voce.get("price_per_sec") is not None:
                tariffe.append(f"${voce['price_per_sec']}/s")
            listino = " + ".join(tariffe) if tariffe else "listino n/d"
            descr = voce.get("description") or ""
            print(f"  {pid[:12]}…  {voce.get('resource', '?'):<22} {voce.get('status', '?'):<12} "
                  f"{listino:<22} {descr[:40]}")
        print("-" * 78 + "\n")

    # ---- ciclo di vita ---------------------------------------------------

    async def run(self) -> dict:
        """
        Non solleva mai un'eccezione di rete: broker irraggiungibile e
        connessione persa a meta' scambio tornano come un dict di risultato
        con "type" dedicato, esattamente come "complete"/"halted". Fa
        eccezione asyncio.CancelledError (es. Ctrl+C sul processo stesso):
        quella la lasciamo propagare -- e' cosi' che si comunica "fermati"
        a un task asyncio, e non sta all'SDK deciderne il destino finale.
        La pulizia (chiusura del socket) avviene comunque, in ogni caso,
        grazie al blocco finally.
        """
        try:
            await self.ensure_identity()   # chiavi Ed25519: caricate/generate in un worker thread, mai sul loop

            trace(self.name, f"sto per connettermi al broker: {self.broker_url} ...")
            try:
                self._ws = await websockets.connect(self.broker_url, max_size=MAX_MESSAGE_SIZE)
            except (ConnectionRefusedError, OSError, asyncio.TimeoutError,
                    websockets.exceptions.WebSocketException) as exc:
                # OSError copre rete giu'/DNS; asyncio.TimeoutError l'open_timeout
                # di websockets; WebSocketException gli handshake HTTP falliti --
                # ad es. il 502/timeout del cold-start di Render (piano Free in
                # spin-down): per il chiamante sono tutti lo stesso evento
                # ritentabile, "broker non raggiungibile ADESSO".
                trace(self.name, f"connessione FALLITA: {type(exc).__name__}: {exc}")
                logging.info(f"[{self.name}] impossibile raggiungere il broker su {self.broker_url}: {exc}")
                return {
                    "role": self._role_label(), "type": "errore_connessione",
                    "reason": "broker_non_raggiungibile", "detail": str(exc),
                }
            trace(self.name, "connessione WebSocket stabilita.")

            trace(self.name, "invio hello firmato...")
            await self._send({"type": "hello", "passport_id": self.passport_id,
                              "initial_balance": self.balance, "proto": PROTOCOL_VERSION})
            trace(self.name, f"hello inviato -- attendo welcome (timeout {HANDSHAKE_TIMEOUT_SEC:.0f}s)...")

            # QUESTO wait_for e' il vaccino contro il "silent deadlock": un
            # broker che NON risponde all'hello (perche' parla un protocollo
            # pre-firma e scarta le buste in silenzio, o perche' un proxy
            # accetta il TCP senza inoltrare nulla) prima lasciava il client
            # appeso PER SEMPRE su questo recv(), senza eccezioni ne' log.
            # Ora diventa un esito esplicito e ritentabile entro N secondi.
            try:
                welcome_raw = await asyncio.wait_for(self._ws.recv(), timeout=HANDSHAKE_TIMEOUT_SEC)
            except (asyncio.TimeoutError, TimeoutError):
                trace(self.name, "TIMEOUT sull'attesa del welcome.")
                logging.warning(
                    f"[{self.name}] il broker ha accettato la connessione ma NON ha risposto all'hello "
                    f"entro {HANDSHAKE_TIMEOUT_SEC:.0f}s. Causa tipica: broker su protocollo INCOMPATIBILE "
                    f"(es. versione pre-firma Ed25519 che scarta in silenzio le buste firmate -- "
                    f"controlla che broker_server.py importi m2m_ledger.client, NON il modulo legacy)."
                )
                return {
                    "role": self._role_label(), "type": "errore_handshake",
                    "reason": "welcome_non_ricevuto_timeout",
                    "detail": f"nessuna risposta all'hello entro {HANDSHAKE_TIMEOUT_SEC:.0f}s",
                }

            try:
                welcome = json.loads(welcome_raw)
                if not isinstance(welcome, dict):
                    raise ValueError(f"risposta non-oggetto: {welcome_raw[:120]!r}")
            except (json.JSONDecodeError, ValueError) as exc:
                trace(self.name, f"welcome NON interpretabile: {exc}")
                return {
                    "role": self._role_label(), "type": "errore_handshake",
                    "reason": "welcome_malformato", "detail": str(exc),
                }
            if welcome.get("type") != "welcome":
                # Il broker ha risposto, ma con un rifiuto (es. errore_crittografico
                # per firma non valida o clock skew oltre MAX_CLOCK_SKEW_SEC --
                # frequente su macchine Windows con RTC fuori orario): esito
                # esplicito, non un KeyError criptico su welcome["balance"].
                trace(self.name, f"handshake RIFIUTATO dal broker: {welcome}")
                logging.warning(f"[{self.name}] handshake rifiutato dal broker: {welcome}")
                return {
                    "role": self._role_label(), "type": "errore_handshake",
                    "reason": welcome.get("motivo", welcome.get("type", "rifiuto_sconosciuto")),
                    "detail": welcome,
                }
            proto_broker = welcome.get("proto")
            if proto_broker != PROTOCOL_VERSION:
                logging.warning(f"[{self.name}] ATTENZIONE: broker su protocollo v{proto_broker or '<pre-3>'}, "
                                f"client su v{PROTOCOL_VERSION} -- aggiornare il broker e' fortemente consigliato.")
            self.balance = welcome["balance"]
            trace(self.name, f"welcome ricevuto -- handshake completato (broker proto v{proto_broker}).")
            logging.info(f"[{self.name}] passaporto '{self._passport_breve}' attivo -- wallet aperto: ${self.balance:.4f}")

            is_consumer = self._offer_contract is not None
            is_provider = self._provision_contract is not None
            if is_consumer == is_provider:
                raise RuntimeError(
                    f"[{self.name}] configura l'agente con will_offer()+will_request() "
                    f"OPPURE con will_provide(), non entrambi o nessuno dei due."
                )
            if is_consumer and "request" not in self._offer_contract:
                raise RuntimeError(f"[{self.name}] manca will_request(...): cosa vuoi ricevere in cambio?")

            return await (self._run_as_consumer() if is_consumer else self._run_as_provider())

        except websockets.exceptions.ConnectionClosed:
            # Il broker (o il peer, tramite il broker) e' sparito a meta'
            # scambio: non e' un bug, e' il caso "trustless" per eccellenza.
            # Torniamo un esito pulito invece di far esplodere il chiamante.
            logging.info(f"[{self.name}] connessione con il broker interrotta.")
            return {"role": self._role_label(), "type": "interrotto", "reason": "connessione_persa"}

        finally:
            await self._graceful_close()

    def _role_label(self) -> str:
        if self._offer_contract is not None:
            return "consumer"
        if self._provision_contract is not None:
            return "provider"
        return "sconosciuto"

    def _firma_busta(self, payload: dict) -> str:
        """
        Costruisce e SERIALIZZA la busta firmata per un payload in uscita:
        {"payload", "passport", "timestamp", "signature"} -> stringa JSON
        pronta per ws.send(). Firmiamo una rappresentazione CANONICA (chiavi
        ordinate, separatori fissi) di payload+passport+timestamp: lo stesso
        identico procedimento che il broker rifa' in _verifica_busta per
        controllare la firma -- devono combaciare byte per byte, altrimenti
        anche un messaggio legittimo verrebbe rifiutato.

        E' una funzione PURA e CPU-bound (niente stato mutabile, niente I/O):
        per questo _send la esegue via asyncio.to_thread. Il costo misurato
        per un chunk 'crypto:all' (~175 KB) e' ~4 ms di dumps canonico +
        ~0.6 ms di firma + il dumps esterno: eseguito inline, ad OGNI tick,
        ruberebbe millisecondi all'event loop proprio nel percorso caldo;
        in un worker thread costa un hop da ~90 microsecondi e il loop resta
        libero di servire feed di mercato, heartbeat e sessioni parallele.
        """
        timestamp = time.time()
        canonico = json.dumps(
            {"payload": payload, "passport": self.passport_id, "timestamp": timestamp},
            sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        firma = self._private_key.sign(canonico)
        return json.dumps({"payload": payload, "passport": self.passport_id,
                           "timestamp": timestamp, "signature": firma.hex()})

    async def _send(self, msg: dict) -> None:
        busta_serializzata = await asyncio.to_thread(self._firma_busta, msg)
        await self._ws.send(busta_serializzata)

    async def _graceful_close(self) -> None:
        try:
            if self._ws is not None:
                trace(self.name, "chiusura ordinata del socket verso il broker...")
                await self._ws.close()
                trace(self.name, "socket chiuso.")
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self._ws = None   # igiene per il riuso dello stesso Agent nei loop di riconnessione

    # ---- ruolo: CONSUMER (offre denaro+dati, chiede calcolo) -----------------

    async def _run_as_consumer(self) -> dict:
        self._pending_downgrade = None  # reset esplicito: importante se lo stesso
                                         # Agent viene riusato per una fase successiva
                                         # (es. dopo un downgrade), per non trascinarsi
                                         # dietro la decisione della fase precedente.
        await self._send({"type": "offer", "contract": self._offer_contract})
        trace(self.name, "offerta pubblicata -- in attesa di 'matched' dal broker...")
        logging.info(f"[{self.name}] offerta pubblicata: {self._offer_contract}")
        logging.info(f"[{self.name}] in attesa di un peer compatibile su {self.broker_url} ...")

        # Un buffer LIMITATO (non un elenco che cresce senza fine): per un
        # feed continuo come un mercato intero, un solo chunk puo' contenere
        # migliaia di elementi. Qui teniamo solo un assaggio recente per la
        # diagnostica finale; il conteggio totale (accurato) resta un intero
        # a parte, a costo di memoria trascurabile.
        results_sample = collections.deque(maxlen=200)
        total_items = 0
        final: dict = {}
        settled_ticks = 0     # ultimo tick LIQUIDATO dal broker: e' la verita' contabile
        paid_total = 0.0      # somma delle liquidazioni viste da QUESTA sessione

        # Grace window: applichiamo un timeout SOLO all'attesa del primo
        # 'matched'. Se il consumer ha dichiarato requisiti stringenti
        # (max_price) e nessun provider compatibile+conveniente compare entro
        # la finestra, restituiamo no_nodes_available invece di attendere per
        # sempre. Dopo il match, la ricezione torna bloccante come sempre
        # (una sessione in corso non ha motivo di scadere qui).
        matched_yet = False
        grace_deadline = time.monotonic() + MATCH_GRACE_SEC

        try:
            while True:
                if not matched_yet:
                    residuo = grace_deadline - time.monotonic()
                    if residuo <= 0:
                        trace(self.name, "grace window scaduta senza match -- no_nodes_available.")
                        logging.info(f"[{self.name}] nessun nodo compatibile entro "
                                     f"{MATCH_GRACE_SEC:.0f}s -- no_nodes_available.")
                        return {
                            "role": self._role_label(), "type": "no_nodes_available",
                            "reason": "no_matching_provider_within_grace",
                            "resource": self._offer_contract.get("request", {}).get("resource"),
                            "max_price": self._offer_contract.get("request", {}).get("max_price"),
                            "ticks": 0, "total_paid": 0.0, "results_sample": [],
                        }
                    try:
                        raw = await asyncio.wait_for(self._ws.recv(), timeout=residuo)
                    except (asyncio.TimeoutError, TimeoutError):
                        continue   # ricontrolla il deadline in cima al loop
                else:
                    raw = await self._ws.recv()

                msg = await _json_loads_smart(raw)   # i result_chunk 'crypto:all' pesano centinaia di KB
                if "error" in msg and "type" not in msg:
                    # Rifiuto esplicito del broker (es. RATE_LIMIT_EXCEEDED):
                    # nessun 'type' di protocollo -- lo riportiamo come esito.
                    logging.warning(f"[{self.name}] il broker ha risposto con errore: {msg['error']}")
                    return {"role": self._role_label(), "type": "errore_broker",
                            "reason": msg["error"], "ticks": settled_ticks,
                            "total_paid": paid_total, "results_sample": list(results_sample)}
                mtype = msg["type"]

                if mtype == "matched":
                    matched_yet = True
                    trace(self.name, f"matched ricevuto (peer {str(msg.get('peer'))[:12]}…) -- streaming avviato.")
                    logging.info(f"[{self.name}] abbinato al peer {msg['peer']} -- inizio dello streaming.")
                    await self._send_next_data_chunk()

                elif mtype == "result_chunk":
                    chunk = msg["chunk"]
                    total_items += len(chunk)
                    results_sample.extend(chunk)
                    target_display = msg["target"] if msg["target"] is not None else "senza limite"
                    logging.info(f"[{self.name}] +{len(chunk)} elementi ricevuti (tick {msg['progress']}/{target_display})")

                elif mtype == "settlement":
                    self.balance = msg["consumer_balance"]
                    settled_ticks = msg["tick"]
                    paid_total = round(paid_total + msg["amount"], 8)
                    logging.info(f"[{self.name}] tick #{msg['tick']}: pagato ${msg['amount']:.6f} -- saldo: ${self.balance:.4f}")

                    if self._on_tick is not None:
                        tick_info = {
                            "tick": msg["tick"],
                            "balance": self.balance,
                            "initial_balance": self._initial_balance,
                            "spent_this_tick": msg["amount"],
                            "total_spent": round(self._initial_balance - self.balance, 8),
                        }
                        nuova_risorsa = self._on_tick(tick_info)
                        if nuova_risorsa:
                            self._pending_downgrade = nuova_risorsa
                            await self._send({"type": "cancel_volontario", "motivo": f"downgrade_a_{nuova_risorsa}"})
                            continue  # aspettiamo l'"halted" di conferma dal broker, non serve altro da qui

                    await self._send_next_data_chunk()

                elif mtype in ("complete", "halted"):
                    final = msg
                    break
        except websockets.exceptions.ConnectionClosed:
            # Chiusura BRUSCA a meta' sessione (broker ucciso, rete caduta):
            # la intercettiamo QUI e non piu' solo in run(), perche' qui
            # possediamo la contabilita' locale (tick liquidati, spesa vista)
            # e possiamo consegnarla al chiamante -- e' cio' che permette a
            # un nodo con auto-riconnessione di RIPRENDERE la sessione dal
            # punto giusto invece di ripartire da zero o perdere il conto.
            trace(self.name, "connessione persa a meta' sessione (consumer).")
            final = {"type": "interrotto", "reason": "connessione_persa"}

        if not final:
            # Il ciclo e' terminato SENZA sollevare un'eccezione e SENZA un
            # "complete"/"halted" esplicito: e' il caso di una chiusura
            # PULITA avviata dall'altra parte (es. il broker che si spegne
            # con server.close(), handshake di chiusura regolare -- non un
            # crash). websockets non tratta una chiusura pulita come errore,
            # quindi il ConnectionClosed qui sopra non scatta: sintetizziamo
            # noi un esito coerente invece di restituire un dict incompleto.
            final = {"type": "interrotto", "reason": "connessione_chiusa_dal_broker"}

        # Gli esiti del broker (complete/halted) portano gia' ticks/total_paid
        # autorevoli: setdefault li rispetta. Gli esiti sintetizzati qui
        # (interrotto) ereditano la contabilita' locale appena raccolta.
        final.setdefault("ticks", settled_ticks)
        final.setdefault("total_paid", paid_total)
        trace(self.name, f"sessione consumer terminata: {final.get('type')}/{final.get('reason', '-')} "
                          f"({final.get('ticks', 0)} tick liquidati).")

        result = {"role": "consumer", **final, "results_count": total_items, "results_sample": list(results_sample)[-5:]}
        if self._pending_downgrade:
            result["downgrade_a"] = self._pending_downgrade
        return result

    async def _send_next_data_chunk(self) -> None:
        if self._data_chunks is None:
            return
        if self._data_idx >= len(self._data_chunks):
            await self._send({"type": "data_complete"})
            return
        chunk = self._data_chunks[self._data_idx]
        self._data_idx += 1
        await self._send({"type": "data_chunk", "chunk": chunk})

    # ---- ruolo: PROVIDER (offre calcolo, chiede denaro+dati) -----------------

    async def _run_as_provider(self) -> dict:
        await self._send({"type": "provide", "contract": self._provision_contract})
        logging.info(f"[{self.name}] pronto a fornire '{self._provision_contract['resource']}'.")
        logging.info(f"[{self.name}] in attesa di un peer compatibile su {self.broker_url} ...")

        # "matched" e' un flag A PARTE (non "target is not None"): con le
        # sessioni a durata (mode="duration") il target puo' essere
        # LEGITTIMAMENTE None anche DOPO l'abbinamento (streaming senza
        # limite), quindi non puo' piu' fare da sentinella per "non ancora
        # abbinato".
        state: Dict[str, Any] = {
            "matched": False, "target": None, "mode": "count", "resource": None,
            "earned": 0.0, "data_received": 0, "final": None, "ticks": 0,
        }
        sent_count = 0
        stop_flag = asyncio.Event()
        tick_ack = asyncio.Event()
        tick_ack.set()  # via libera per il primissimo chunk, prima di qualunque settlement

        async def produce_loop() -> None:
            """
            Task separato che produce e invia il flusso di calcolo/dati, UN
            chunk alla volta. Gira in parallelo al loop che legge i messaggi
            in arrivo (settlement, data_chunk, ecc.): sono due meta'
            indipendenti dello stesso streaming bidirezionale.

            Backpressure: dopo aver inviato un chunk, il loop si blocca su
            tick_ack finche' non arriva il "settlement" di QUEL tick. Senza
            questo freno, un provider veloce produrrebbe chunk piu' in
            fretta del ritmo di liquidazione del broker: si accumulerebbero
            in coda, ritardando il rilevamento di un provider caduto.

            La condizione di arresto "ho prodotto abbastanza" vale SOLO in
            modalita' "count" (es. primes: fermati a target unita'); in
            modalita' "duration" (es. un feed di mercato) e' la sessione,
            non il producer, a decidere quando basta -- il producer si
            ferma solo quando arriva stop_flag (settato dal blocco finally
            sotto, alla ricezione di "complete"/"halted").
            """
            nonlocal sent_count
            cursor = None
            while not stop_flag.is_set():
                if not state["matched"]:
                    await asyncio.sleep(0.02)
                    continue
                if state["mode"] == "count" and state["target"] is not None and sent_count >= state["target"]:
                    return
                await tick_ack.wait()
                tick_ack.clear()
                # asyncio.to_thread: il calcolo/recupero dati potrebbe essere
                # pesante o bloccante. Eseguirlo inline bloccherebbe l'intero
                # event loop.
                chunk, cursor = await asyncio.to_thread(self._provider_fn, cursor, state["resource"])
                sent_count += len(chunk)
                await self._send({"type": "compute_chunk", "chunk": chunk})

        producer_task = asyncio.create_task(produce_loop())
        trace(self.name, "in attesa di 'matched' dal broker (producer gia' in standby)...")
        try:
            async for raw in self._ws:
                msg = await _json_loads_smart(raw)
                mtype = msg["type"]

                if mtype == "matched":
                    state["matched"] = True
                    state["target"] = msg["target"]
                    state["mode"] = msg.get("mode", "count")
                    state["resource"] = msg.get("resource", self._provision_contract["resource"])
                    target_display = state["target"] if state["target"] is not None else "senza limite"
                    trace(self.name, "matched ricevuto -- il producer inizia a consegnare.")
                    logging.info(f"[{self.name}] abbinato -- risorsa richiesta: '{state['resource']}', "
                                 f"obiettivo: {target_display} ({state['mode']}).")

                elif mtype == "data_chunk":
                    state["data_received"] += len(msg["chunk"])
                    if self._on_data is not None:
                        # Callback SINCRONA per contratto (tipicamente un
                        # append): un'eccezione qui e' un bug del provider,
                        # non deve abbattere la sessione ne' il tick loop.
                        try:
                            self._on_data(msg["chunk"])
                        except Exception:
                            logging.error(f"[{self.name}] on_data callback ha sollevato:\n"
                                          f"{__import__('traceback').format_exc()}")

                elif mtype == "settlement":
                    self.balance = msg["provider_balance"]
                    state["earned"] += msg["amount"]
                    state["ticks"] = msg["tick"]
                    logging.info(f"[{self.name}] tick #{msg['tick']}: incassato ${msg['amount']:.6f} -- saldo: ${self.balance:.4f}")
                    tick_ack.set()

                elif mtype in ("complete", "halted"):
                    state["final"] = msg
                    break
        except websockets.exceptions.ConnectionClosed:
            # Chiusura brusca a meta' sessione: stessa logica del ramo
            # consumer -- l'esito sintetico porta con se' la contabilita'
            # locale, cosi' il supervisore del nodo puo' distinguere "ho
            # servito N tick e poi e' caduta la linea" da "non e' mai
            # partito niente" e regolare il backoff di conseguenza.
            trace(self.name, "connessione persa a meta' sessione (provider).")
            state["final"] = {"type": "interrotto", "reason": "connessione_persa"}
        finally:
            # Questo blocco gira SEMPRE: sia che si esca dal ciclo con un
            # break "normale" (complete/halted), sia che un'eccezione
            # (ConnectionClosed) o una cancellazione (Ctrl+C) attraversino
            # il metodo. Senza questa garanzia, produce_loop resterebbe un
            # task orfano ancora in esecuzione in background.
            stop_flag.set()
            producer_task.cancel()
            try:
                await producer_task
            except (asyncio.CancelledError, websockets.exceptions.ConnectionClosed):
                pass

        if not state["final"]:
            # Stesso ragionamento del ramo consumer: chiusura pulita
            # dell'altra parte (es. broker in spegnimento), non un'eccezione.
            state["final"] = {"type": "interrotto", "reason": "connessione_chiusa_dal_broker"}
        state["final"].setdefault("ticks", state["ticks"])
        trace(self.name, f"sessione provider terminata: {state['final'].get('type')}/"
                          f"{state['final'].get('reason', '-')} ({state['final'].get('ticks', 0)} tick liquidati).")

        return {
            "role": "provider", **state["final"],
            "earned": round(state["earned"], 6),
            "data_received_bytes": state["data_received"],
        }