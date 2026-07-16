"""
m2m-protocol -- protocollo di micro-pagamento Machine-to-Machine per agenti AI.

USO TIPICO (lato sviluppatore esterno: un agente che compra o vende una
risorsa attraverso un broker gia' ospitato da qualcun altro):

    from m2m_ledger import Agent

    node = Agent("il-mio-agente", balance=1.00, broker_url="wss://broker.esempio.com")
    node.will_offer(money_per_sec=0.001, money_per_kb=0.0001)
    node.will_request(resource="crypto:BTCUSDT", param=40, mode="duration")
    result = await node.run()

Questa e' DELIBERATAMENTE l'intera superficie pubblica di questo pacchetto:
chi lo installa da PyPI per costruire un proprio agente non ha bisogno di
altro. Il resto dell'implementazione -- Broker, MicroLedger, SupabaseLedger,
create_ledger -- e' cio' con cui un OPERATORE ospita l'infrastruttura
condivisa (vedi broker_server.py nel repository), non cio' con cui un
cliente del protocollo costruisce il proprio agente. Resta comunque
raggiungibile per chi ne ha davvero bisogno, esplicitamente, via:

    from m2m_ledger.client import Broker, create_ledger, MicroLedger, SupabaseLedger

...ma non e' parte dell'impegno di stabilita' che questo pacchetto assume a
partire dalla v0.1.0: puo' cambiare forma tra una versione e l'altra senza
che questo conti come una modifica "breaking" ai fini del versionamento
semantico, perche' non e' pensato per essere usato al di fuori di questo
stesso repository.
"""

from importlib.metadata import PackageNotFoundError, version

from .client import Agent, DEFAULT_BROKER_URL, MAX_MESSAGE_SIZE, PROTOCOL_VERSION

try:
    __version__ = version("m2m-protocol")
except PackageNotFoundError:
    # Il pacchetto non e' installato (es. si sta eseguendo codice direttamente
    # da un checkout sorgente senza `pip install -e .`): non e' un errore
    # fatale, serve solo un valore sensato per __version__.
    __version__ = "0.0.0+unknown"

__all__ = ["Agent", "DEFAULT_BROKER_URL", "MAX_MESSAGE_SIZE", "PROTOCOL_VERSION", "__version__"]