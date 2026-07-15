"""
================================================================================
 node_a.py -- NODO A: "Agente Autonomo" (consumer con ragionamento LLM)
================================================================================
Il modulo di ragionamento non e' piu' una formula fissa: ogni 5 tick, il nodo
manda lo stato del proprio budget a un LLM (via Groq, endpoint compatibile
OpenAI) che decide se mantenere il feed corrente o ordinare un downgrade
verso un asset piu' economico. La valutazione gira in BACKGROUND -- il ciclo
dei tick non aspetta mai l'IA: agisce sull'ultima decisione nota e aggiorna
quella decisione appena la richiesta in corso restituisce una risposta.

    python3 node_a.py all         -> compra il feed dell'INTERO mercato
    python3 node_a.py SOLUSDT     -> compra SOLO il feed di Solana
    (nessun argomento)            -> default: 'all'

Se GROQ_API_KEY non e' impostata, il nodo degrada in modo pulito al vecchio
ragionamento deterministico (BudgetReasoner) invece di crashare -- stesso
principio gia' applicato al Ledger con Supabase: la persistenza/l'IA sono
miglioramenti opzionali, non prerequisiti per far funzionare il protocollo.

NOTA SUL MODELLO: il brief originale indicava "llama3-8b-8192". Quel modello
e' stato dismesso da Groq (deprecato il 31 maggio 2025, oggi restituisce un
errore 400 "model_decommissioned") -- e persino il suo successore diretto,
llama-3.1-8b-instant, e' stato a sua volta deprecato il 17 giugno 2026. Il
modello "veloce" attualmente raccomandato da Groq per questa fascia sarebbe
openai/gpt-oss-20b -- ma quel modello ha un bug NOTO e documentato sul
forum ufficiale Groq (aprile 2026): circa il 10% delle richieste in JSON
mode falliscono con errore 400 "json_validate_failed", anche con lo
"structured output garantito". Questa versione usa llama-3.1-8b-instant
su richiesta esplicita: e' un modello la cui deprecazione e' stata
ANNUNCIATA da Groq il 17/6/2026 (raccomandano di migrare... proprio a
openai/gpt-oss-20b, lo stesso appena scartato per il bug qui sopra), ma le
fonti sul suo stato attuale sono contrastanti -- vedi la nota estesa vicino
a MODEL piu' sotto. Il nome del modello resta comunque una costante
facilmente modificabile in un solo punto.

Nota di trasparenza ingegneristica: il PROTOCOLLO (connessioni, matching
dinamico, ledger, regolamento a tick, l'aggancio on_tick e l'annullamento
volontario) resta dentro m2m_protocol.py, INVARIATO rispetto alla versione
precedente: l'aggancio on_tick e' generico per costruzione, un ragionatore
LLM lo usa esattamente come lo usava quello deterministico.
================================================================================
"""

import asyncio
import collections
import contextlib
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Optional

# Import dal pacchetto src-layout anche SENZA `pip install -e .`. NIENTE
# fallback sul modulo legacy senza firma: il mix "broker pre-crypto + nodo
# firmato" e' la ricetta esatta del deadlock silenzioso sull'handshake.
try:
    from m2m_ledger import Agent, DEFAULT_BROKER_URL, PROTOCOL_VERSION
    import m2m_ledger.client as _sdk
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    from m2m_ledger import Agent, DEFAULT_BROKER_URL, PROTOCOL_VERSION
    import m2m_ledger.client as _sdk

BROKER_URL = os.environ.get("M2M_BROKER_URL", DEFAULT_BROKER_URL)
INITIAL_BALANCE = 1.00        # dollari nel wallet simulato
MONEY_RATE_PER_SEC = 0.0002    # piccola tariffa fissa "di connessione"
MONEY_RATE_PER_KB = 0.0001     # tariffa dominante: si paga in base al PESO dei dati di questo tick
DURATION_TICKS = int(os.environ.get("M2M_DURATION_TICKS", "40"))  # tick pianificati per l'intera sessione

BACKOFF_INIZIALE_SEC = 1.0     # auto-riconnessione: 1 -> 2 -> 4 -> 8 -> 10 (tetto)
BACKOFF_MASSIMO_SEC = 10.0

# Esiti di run() che significano "il broker/peer non c'e' ADESSO, ma il
# piano resta valido": si ritenta con backoff, riprendendo dai tick che
# mancano (quelli gia' liquidati restano acquisiti: l'SDK li riporta anche
# negli esiti interrotti, proprio per permettere questa ripresa).
ESITI_RITENTABILI = {"errore_connessione", "errore_handshake", "interrotto"}
# Motivi di "halted" per cui ritentare NON ha senso: la sessione e' morta
# per una causa che una riconnessione non puo' curare.
MOTIVI_HALTED_DEFINITIVI = {"consumer_fondi_esauriti"}

SOGLIA_ALLARME_PCT = 0.50       # oltre questa quota del wallet INIZIALE proiettata, scatta il downgrade
FINESTRA_MEDIA_TICK = 5         # quante liquidazioni recenti mediare per stimare la velocita' di consumo
DOWNGRADE_TARGET = "crypto:BTCUSDT"  # il feed "di riserva", economico, verso cui fare downgrade


# ==============================================================================
# RAGIONATORE DETERMINISTICO -- fallback se GROQ_API_KEY non e' impostata
# ==============================================================================
class BudgetReasoner:
    """
    La Utility Function "classica" del Nodo A, a formula fissa. Resta come
    fallback quando l'IA non e' configurata: ad ogni tick riceve un piccolo
    resoconto e decide se e quando chiedere un downgrade (al massimo una
    volta per fase).
    """

    def __init__(self, risorsa_corrente: str, tick_pianificati: int,
                 soglia_pct: float = SOGLIA_ALLARME_PCT, finestra_media: int = FINESTRA_MEDIA_TICK,
                 downgrade_target: str = DOWNGRADE_TARGET) -> None:
        self.risorsa_corrente = risorsa_corrente
        self.tick_pianificati = tick_pianificati
        self.soglia_pct = soglia_pct
        self.downgrade_target = downgrade_target
        self._spesa_recente = collections.deque(maxlen=finestra_media)
        self._downgrade_gia_deciso = False

    def valuta(self, info: dict) -> Optional[str]:
        tick = info["tick"]
        saldo = info["balance"]
        saldo_iniziale = info["initial_balance"]
        spesa_tick = info["spent_this_tick"]
        spesa_totale = info["total_spent"]

        self._spesa_recente.append(spesa_tick)
        media_per_tick = sum(self._spesa_recente) / len(self._spesa_recente)
        tick_rimanenti = max(0, self.tick_pianificati - tick)
        proiezione_finale = round(spesa_totale + media_per_tick * tick_rimanenti, 6)
        soglia_dollari = round(self.soglia_pct * saldo_iniziale, 6)

        rischio_alto = proiezione_finale > soglia_dollari
        livello_spesa = "alta" if spesa_tick > media_per_tick * 1.2 and spesa_tick > 0 else \
                        ("nulla" if spesa_tick == 0 else "moderata")
        livello_velocita = "critica" if rischio_alto else "sostenibile"

        puo_fare_downgrade = self.risorsa_corrente != self.downgrade_target and not self._downgrade_gia_deciso
        nuova_risorsa = None
        if rischio_alto and puo_fare_downgrade:
            decisione_str = f"Avvio downgrade a '{self.downgrade_target}'"
            nuova_risorsa = self.downgrade_target
            self._downgrade_gia_deciso = True
        elif rischio_alto:
            decisione_str = "Mantengo il feed (nessun downgrade ulteriore disponibile)"
        else:
            decisione_str = "Mantengo il feed"

        print(f"[Agente-A] Analisi budget: Spesa attuale {livello_spesa}. Saldo: ${saldo:.4f}. "
              f"Proiezione a fine sessione: ${proiezione_finale:.4f} (soglia 50%: ${soglia_dollari:.4f}). "
              f"Velocita' di consumo {livello_velocita}. Decisione: {decisione_str}.")

        return nuova_risorsa

    async def chiudi(self) -> None:
        pass  # nessuna risorsa asincrona da ripulire per il ragionatore deterministico


# ==============================================================================
# RAGIONATORE LLM -- Groq (endpoint compatibile OpenAI), non bloccante
# ==============================================================================
class LLMReasoner:
    """
    Stesso contratto di BudgetReasoner (un .valuta(info) -> Optional[str],
    compatibile con Agent.will_request(on_tick=...)), ma la decisione la
    prende un LLM invece di una formula.

    NON BLOCCANTE per costruzione: .valuta() e' una funzione SINCRONA (lo
    richiede l'SDK) che non chiama MAI l'IA direttamente -- ogni
    VALUTA_OGNI_N_TICK tick, se non c'e' gia' una richiesta in volo, lancia
    un asyncio.Task in background e ritorna subito, sempre in base
    all'ULTIMA decisione nota (che puo' avere qualche tick di ritardo
    rispetto all'ultimissimo stato: e' il costo esplicito, accettato in
    cambio di un ciclo tick MAI bloccato in attesa di una risposta di rete).
    """

    # STORIA di questa costante, per chi la trova in futuro:
    #   - "llama3-8b-8192" (brief originale): dismesso da Groq il 31/5/2025,
    #     oggi da errore 400 "model_decommissioned".
    #   - "openai/gpt-oss-20b" (usato nella versione precedente): modello
    #     attivo, ma con un bug NOTO e documentato sul forum Groq (aprile
    #     2026) -- circa il 10% delle richieste in JSON mode falliscono con
    #     "json_validate_failed" ANCHE con lo structured output "garantito".
    #     E' con ogni probabilita' la causa reale dell'errore che hai visto,
    #     non (solo) il markdown.
    #   - "llama-3.1-8b-instant" (questa versione): richiesto esplicitamente.
    #     ATTENZIONE PERO': Groq ha annunciato la sua deprecazione il
    #     17/6/2026 (poche settimane fa), con raccomandazione di migrare a
    #     openai/gpt-oss-20b -- lo stesso modello appena sostituito per il
    #     bug qui sopra. Le fonti sul suo stato ATTUALE sono contrastanti
    #     (pagina di stato Groq lo mostra ancora attivo di recente, pagina
    #     di deprecazione lo da' "in via di ritiro"): se in futuro inizia a
    #     restituire "model_decommissioned", il rimedio e' lo stesso di
    #     sempre, cambiare solo questa riga.
    MODEL = "llama-3.1-8b-instant"
    BASE_URL = "https://api.groq.com/openai/v1"
    VALUTA_OGNI_N_TICK = 5
    TIMEOUT_SEC = 8.0

    SYSTEM_PROMPT = (
        "Sei un Algorithmic Risk Manager per un agente software M2M che acquista "
        "in autonomia un feed di dati di mercato a consumo, pagando in micro-dollari "
        "in base al peso dei dati ricevuti. Il tuo unico obiettivo e' portare a "
        "termine la sessione pianificata SENZA esaurire il budget assegnato. "
        "Ricevi ad ogni valutazione: saldo residuo, saldo iniziale, spesa dell'ultimo "
        "tick, tick correnti e rimanenti sulla sessione, la risorsa attualmente "
        "attiva, e l'asset economico disponibile per un eventuale downgrade. "
        "Se la spesa recente, proiettata sui tick rimanenti, rischia di superare "
        "il 50% del saldo INIZIALE prima della fine della sessione, ordina un "
        "downgrade verso l'asset economico indicato. Se il downgrade e' gia' "
        "avvenuto o la risorsa attiva e' gia' quella economica, mantieni il feed. "
        "Rispondi ESCLUSIVAMENTE con un oggetto JSON valido, nessun testo prima o "
        "dopo, in questo formato esatto: "
        '{"decision": "keep" o "downgrade", '
        '"target_resource": "crypto:XXXUSDT" o null, '
        '"reason": "spiegazione breve e professionale della decisione finanziaria"} '
        "Output strictly raw JSON. Do not include markdown formatting, backticks, "
        "or preamble text. Il tuo output deve iniziare con '{' e finire con '}': "
        "nessun altro carattere prima o dopo, nessun blocco di codice."
    )



    def __init__(self, risorsa_corrente: str, tick_pianificati: int,
                 downgrade_target: str = DOWNGRADE_TARGET) -> None:
        from openai import AsyncOpenAI  # import differito: non deve rompere l'avvio se il pacchetto manca

        self.risorsa_corrente = risorsa_corrente
        self.tick_pianificati = tick_pianificati
        self.downgrade_target = downgrade_target
        self._client = AsyncOpenAI(
            api_key=os.environ.get("GROQ_API_KEY"),
            base_url=self.BASE_URL,
            timeout=self.TIMEOUT_SEC,
            max_retries=1,
        )
        self._spesa_recente = collections.deque(maxlen=FINESTRA_MEDIA_TICK)
        self._task_in_corso: Optional[asyncio.Task] = None
        self._ultima_decisione = {
            "decision": "keep", "target_resource": None,
            "reason": "Nessuna valutazione IA ancora ricevuta -- mantengo il feed per prudenza.",
        }
        self._downgrade_gia_agito = False

    def valuta(self, info: dict) -> Optional[str]:
        tick = info["tick"]
        saldo = info["balance"]
        spesa_tick = info["spent_this_tick"]
        self._spesa_recente.append(spesa_tick)

        # Ogni N tick, se non c'e' gia' una valutazione in volo, ne lanciamo
        # una nuova IN BACKGROUND: non la aspettiamo qui.
        richiesta_gia_in_volo = self._task_in_corso is not None and not self._task_in_corso.done()
        if tick % self.VALUTA_OGNI_N_TICK == 0 and not richiesta_gia_in_volo:
            self._task_in_corso = asyncio.create_task(self._interroga_llm(dict(info)))

        stato_richiesta = "in corso" if richiesta_gia_in_volo else "nessuna richiesta pendente"
        print(f"[Agente-A/LLM] tick {tick}: saldo ${saldo:.4f}, ultima decisione IA: "
              f"'{self._ultima_decisione['decision']}' ({stato_richiesta}) -- {self._ultima_decisione['reason']}")

        # Agiamo SEMPRE sull'ultima decisione nota, mai su una richiesta che
        # stiamo ancora aspettando: questo e' esattamente cio' che rende il
        # ciclo dei tick non bloccante.
        if (self._ultima_decisione.get("decision") == "downgrade"
                and not self._downgrade_gia_agito
                and self.risorsa_corrente != self.downgrade_target):
            target = self._ultima_decisione.get("target_resource") or self.downgrade_target
            self._downgrade_gia_agito = True
            print(f"[Agente-A/LLM] >>> Agisco sulla decisione dell'IA: downgrade verso '{target}' <<<")
            return target

        return None

    async def _interroga_llm(self, info: dict) -> None:
        payload = {
            "saldo_residuo": round(info["balance"], 6),
            "saldo_iniziale": round(info["initial_balance"], 6),
            "spesa_ultimo_tick": round(info["spent_this_tick"], 6),
            "spesa_totale_finora": round(info["total_spent"], 6),
            "tick_corrente": info["tick"],
            "tick_pianificati_totali": self.tick_pianificati,
            "tick_rimanenti": max(0, self.tick_pianificati - info["tick"]),
            "risorsa_attiva": self.risorsa_corrente,
            "asset_downgrade_disponibile": self.downgrade_target,
        }
        messages = [
            {"role": "system", "content": self.SYSTEM_PROMPT},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
        ]

        testo = None
        try:
            # --- TENTATIVO 1: JSON mode vincolato lato Groq -----------------
            # Quando funziona e' la via piu' affidabile (il server stesso
            # garantisce JSON valido). Alcuni modelli Groq, pero', falliscono
            # questo vincolo con una frequenza non trascurabile (bug noto,
            # documentato sul forum Groq: circa 10% delle richieste anche su
            # modelli con "structured output garantito") -- da qui il
            # tentativo 2 qui sotto, non e' un caso raro da ignorare.
            risposta = await self._client.chat.completions.create(
                model=self.MODEL, messages=messages,
                response_format={"type": "json_object"},
                temperature=0.2, max_tokens=200,
            )
            testo = risposta.choices[0].message.content
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            print(f"[Agente-A/LLM] JSON mode fallito ({exc}) -- ritento senza response_format...")
            try:
                # --- TENTATIVO 2: nessun vincolo lato server, ci pensiamo noi ---
                # Ci affidiamo solo alle istruzioni "severe" nel system
                # prompt, poi ripuliamo noi il testo grezzo (vedi
                # _estrai_json): toglie markdown/backtick/preamboli invece
                # di sperare che il modello non li generi.
                risposta = await self._client.chat.completions.create(
                    model=self.MODEL, messages=messages,
                    temperature=0.2, max_tokens=200,
                )
                testo = risposta.choices[0].message.content
            except asyncio.CancelledError:
                raise
            except Exception as exc2:
                print(f"[Agente-A/LLM] anche il tentativo senza response_format e' fallito ({exc2}) "
                      f"-- mantengo l'ultima decisione nota.")
                return

        try:
            decisione = self._estrai_json(testo)
            if decisione.get("decision") not in ("keep", "downgrade"):
                raise ValueError(f"campo 'decision' inatteso: {decisione.get('decision')!r}")

            self._ultima_decisione = {
                "decision": decisione["decision"],
                "target_resource": decisione.get("target_resource"),
                "reason": str(decisione.get("reason", ""))[:300],
            }
            print(f"[Agente-A/LLM] Valutazione IA ricevuta (tick {info['tick']}): {self._ultima_decisione}")
        except Exception as exc:
            # JSON irrecuperabile anche dopo la ripulitura, campo 'decision'
            # mancante/malformato: NON tocchiamo _ultima_decisione -- si
            # continua ad agire sull'ultima valutazione buona nota, mai su
            # un crash del ciclo dei tick.
            print(f"[Agente-A/LLM] risposta IA non interpretabile ({exc}); testo grezzo: {testo!r} "
                  f"-- mantengo l'ultima decisione nota.")

    @staticmethod
    def _estrai_json(testo: str) -> dict:
        """
        Estrae un oggetto JSON da una stringa che potrebbe non essere JSON
        puro: rimuove blocchi di codice markdown (```json ... ```), poi
        cerca il primo oggetto '{...}' nel testo. Lo schema atteso qui e'
        volutamente PIATTO (decision/target_resource/reason, nessun oggetto
        annidato): una regex non-greedy su '{[^{}]*}' basta e resta piu'
        prevedibile di un parser generico per un caso d'uso cosi' ristretto.
        """
        if testo is None:
            raise ValueError("risposta vuota dal modello")
        pulito = testo.strip()

        # 1. magari e' gia' JSON pulito -- il percorso piu' comune quando va bene
        try:
            return json.loads(pulito)
        except json.JSONDecodeError:
            pass

        # 2. via i blocchi di codice markdown, se presenti (```json ... ``` o ``` ... ```)
        senza_backtick = re.sub(r"^```(?:json)?\s*|\s*```$", "", pulito, flags=re.IGNORECASE | re.MULTILINE).strip()
        try:
            return json.loads(senza_backtick)
        except json.JSONDecodeError:
            pass

        # 3. ultima spiaggia: il primo oggetto piatto '{...}' ovunque nel testo
        match = re.search(r"\{[^{}]*\}", pulito, flags=re.DOTALL)
        if match:
            return json.loads(match.group(0))

        raise ValueError(f"nessun oggetto JSON riconoscibile nella risposta: {testo[:200]!r}")

    async def chiudi(self) -> None:
        """Cancella un'eventuale valutazione ancora in volo e chiude il
        client HTTP -- da chiamare ad ogni fine fase/fine programma per non
        lasciare task o connessioni pendenti."""
        if self._task_in_corso is not None and not self._task_in_corso.done():
            self._task_in_corso.cancel()
            try:
                await self._task_in_corso
            except asyncio.CancelledError:
                pass
        await self._client.close()


def crea_reasoner(risorsa_corrente: str, tick_pianificati: int):
    """
    Fabbrica del ragionatore: LLM (Groq) se GROQ_API_KEY e' impostata,
    altrimenti degrado pulito al ragionatore deterministico -- stesso
    principio di m2m_protocol.create_ledger() per Supabase: un miglioramento
    opzionale non deve MAI impedire al protocollo di funzionare.
    """
    if os.environ.get("GROQ_API_KEY"):
        try:
            return LLMReasoner(risorsa_corrente, tick_pianificati)
        except ImportError:
            logging.warning(
                "[Agente-A] libreria 'openai' non installata (vedi requirements.txt): "
                "ripiego sul ragionatore deterministico nonostante GROQ_API_KEY sia presente."
            )
    else:
        logging.warning(
            "[Agente-A] GROQ_API_KEY non impostata: ragionamento LLM disattivato, "
            "uso il ragionatore deterministico (formula a soglia fissa)."
        )
    return BudgetReasoner(risorsa_corrente, tick_pianificati)


def _asset_da_cli() -> str:
    """
    'all' -> 'crypto:all' (l'intero mercato, un vero superset);
    un ticker (es. 'solusdt') -> 'crypto:SOLUSDT' (un simbolo specifico).
    """
    if len(sys.argv) < 2:
        print("Uso: python3 node_a.py <asset>   (es. 'all' oppure 'SOLUSDT')")
        print("Nessun argomento fornito: uso 'all' come default.\n")
        return "crypto:all"
    raw = sys.argv[1].strip()
    return "crypto:all" if raw.lower() == "all" else f"crypto:{raw.upper()}"


def _stampa_riepilogo(fasi: list, node: Agent, tick_fatti: int, titolo: str) -> None:
    print("\n" + "=" * 70)
    print(f"NODO A -- {titolo}")
    if not fasi:
        print("  Nessun tick liquidato.")
    for i, (res, r) in enumerate(fasi, 1):
        print(f"  Fase {i} ({res:<18}): {r.get('reason', '?'):<32} "
              f"{r.get('ticks', 0):>3} tick   ${r.get('total_paid', 0.0):.6f}")
    print(f"  Tick liquidati / pianificati: {tick_fatti} / {DURATION_TICKS}")
    print(f"  Speso in totale             : ${INITIAL_BALANCE - node.balance:.6f}")
    print(f"  Saldo residuo wallet        : ${node.balance:.6f}")
    print("=" * 70)


async def main() -> None:
    risorsa = _asset_da_cli()
    logging.info(f"[Nodo-A] risorsa richiesta: '{risorsa}' -- {DURATION_TICKS} tick pianificati, "
                 f"broker: {BROKER_URL}")

    # UN solo Agent per tutta la vita del processo: stesso passaporto Ed25519,
    # stesso wallet. Ogni riconnessione e' una nuova *sessione* dello stesso
    # agente, non un agente nuovo -- il saldo resta quello del ledger.
    node_a = Agent(name="Nodo-A", balance=INITIAL_BALANCE, broker_url=BROKER_URL)
    node_a.will_offer(money_per_sec=MONEY_RATE_PER_SEC, money_per_kb=MONEY_RATE_PER_KB)

    tick_rimanenti = DURATION_TICKS
    fasi = []                          # una voce per ogni fase che ha prodotto tick
    backoff = BACKOFF_INIZIALE_SEC
    tentativo = 0

    try:
        while tick_rimanenti > 0:
            tentativo += 1
            # Reasoner nuovo a ogni fase: il suo "piano" sono i tick che
            # MANCANO adesso, non quelli della sessione originale.
            reasoner = crea_reasoner(risorsa_corrente=risorsa, tick_pianificati=tick_rimanenti)

            # ---- LE UNICHE RIGHE DI PROTOCOLLO CHE NODO A DEVE SCRIVERE ----
            node_a.will_request(resource=risorsa, param=tick_rimanenti,
                                mode="duration", on_tick=reasoner.valuta)
            try:
                result = await node_a.run()
            finally:
                # Niente task/connessioni IA pendenti tra una fase e l'altra --
                # vale anche se run() viene cancellato (Ctrl+C): sopprimiamo
                # SOLO l'eventuale ri-cancellazione dentro chiudi(); quella
                # originale si ripropaga comunque alla fine del finally.
                with contextlib.suppress(asyncio.CancelledError):
                    await reasoner.chiudi()
            # -----------------------------------------------------------------

            esito = result.get("type", "?")
            motivo = str(result.get("reason", ""))
            ticks_fatti = int(result.get("ticks", 0) or 0)

            if ticks_fatti > 0:
                fasi.append((risorsa, result))
                tick_rimanenti -= ticks_fatti
                backoff = BACKOFF_INIZIALE_SEC   # progresso reale: backoff riparte da 1s

            # -- 1) esiti DEFINITIVI: si chiude qui ---------------------------
            if esito == "complete":
                break
            if esito == "halted" and motivo in MOTIVI_HALTED_DEFINITIVI:
                if ticks_fatti == 0:
                    fasi.append((risorsa, result))   # a riepilogo anche senza tick
                logging.info(f"[Nodo-A] sessione chiusa in modo definitivo: {motivo}")
                break

            # -- 2) DOWNGRADE deliberato: nuova fase, subito ------------------
            nuova_risorsa = result.get("downgrade_a")
            if nuova_risorsa and tick_rimanenti > 0:
                print(f"\n[Agente-A] >>> Downgrade: '{risorsa}' -> '{nuova_risorsa}' "
                      f"({tick_rimanenti} tick rimanenti sul piano) <<<\n")
                risorsa = nuova_risorsa
                backoff = BACKOFF_INIZIALE_SEC
                continue

            # -- 3) tutto il resto e' RITENTABILE con backoff -----------------
            # errore_connessione / errore_handshake / interrotto, ma anche gli
            # halted "curabili" (provider caduto, peer sparito prima del via):
            # il provider ha il suo supervisore che lo riportera' online. Noi
            # aspettiamo e riproviamo con i tick che mancano -- e' questo a
            # rendere IRRILEVANTE l'ordine di avvio dei tre processi.
            if esito in ESITI_RITENTABILI or esito == "halted":
                logging.info(f"[Nodo-A] esito '{esito}' ({motivo or 'n/d'}) al tentativo "
                             f"{tentativo} -- {tick_rimanenti} tick ancora da fare, "
                             f"riprovo tra {backoff:.0f}s...")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MASSIMO_SEC)
                continue

            # Esito mai visto: meglio fermarsi rumorosamente che ciclare alla cieca.
            logging.error(f"[Nodo-A] esito NON riconosciuto dall'SDK: {result!r} -- mi fermo.")
            fasi.append((risorsa, result))
            break

    except asyncio.CancelledError:
        _stampa_riepilogo(fasi, node_a, DURATION_TICKS - tick_rimanenti,
                          "interrotto da tastiera (Ctrl+C) -- riepilogo parziale")
        raise

    _stampa_riepilogo(fasi, node_a, DURATION_TICKS - tick_rimanenti, "Sessione conclusa")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    sys.exit(0)