# m2m-ledger

> **The Financial Layer for Autonomous AI.**

![License](https://img.shields.io/badge/license-MIT-informational)
![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Protocol](https://img.shields.io/badge/protocol-v3%20(Ed25519)-black)
![Async](https://img.shields.io/badge/asyncio-native-green)

`m2m-ledger` is a settlement and discovery protocol for autonomous AI agents. It lets any two agents — a data feed and a trading bot, a GPU node and an inference agent, a specialist model and a generalist orchestrator — find each other, agree on a price, and exchange value in real time, without a human wiring the integration by hand.

---

## The Problem

Autonomous agents can reason, plan, and call tools — but they cannot natively **pay** one another. Every existing option predates the agent economy it is being asked to serve:

- **Static API keys and monthly quotas**, provisioned by a human days before the agent runs a task it could not yet anticipate. Zero autonomy, zero dynamic pricing.
- **Blockchain settlement**, with block-confirmation latency and gas floors that make a $0.0001-per-tick data feed economically incoherent.
- **No discovery layer.** An agent cannot ask the network "who is selling GPU inference right now, and at what price?" That integration is hardcoded once, by an engineer, and never updates itself.

None of this scales to a world where agents provision other agents' capabilities on demand and settle in milliseconds, not invoice cycles.

## The Solution

`m2m-ledger` is the missing financial layer: a protocol where any two agents that can open a WebSocket can discover each other, negotiate through price and resource matching, and stream value against value — data or compute against money — tick by tick, with cryptographic proof attached to every unit delivered.

- **Non-blocking core.** A pure `asyncio` event loop routes every session over WebSockets. CPU-bound work — Ed25519 verification, JSON parsing above 32 KB — is offloaded to worker threads, never the loop: signing a ~174 KB settlement envelope costs ~0.6 ms, verifying one costs ~4.6 ms, and neither blocks any other concurrent session.
- **Cryptographic accountability.** Every message on the wire — offer, provision, data chunk, cancellation — is signed with **Ed25519** and verified broker-side before it is trusted. No valid signature, no processing.
- **Bounded trust.** Settlement is tick-based: a provider is paid only after delivering verifiable proof of work for that tick; a consumer receives data only after that tick clears. Maximum possible loss for either counterparty: the value of one tick.
- **Durable ledger.** Wallet state settles to **Supabase / Postgres** through atomic RPC functions when configured, and degrades cleanly to an in-memory ledger when it isn't. The protocol never depends on persistence to function correctly.
- **Live service discovery.** A **Dynamic Order Book**, maintained by the broker in real time, lets consumers find providers by resource, price, and description — no hardcoded endpoints, no static registry file, no manual pairing.

---

## Quick Start

The topology below is distributed by design — one broker (the market), one or more providers, one or more consumers, each an independent process — but you only ever run the last two. **The M2M Broker infrastructure is fully managed and live. You don't need to spin up any local servers. Just run the agents and connect to the global Order Book.**

### 1. Installation

```bash
pip install git+https://github.com/YOUR-USERNAME/m2m-ledger.git
```

### 2. Initialization & Identity

```python
from m2m_ledger import Agent

agent = Agent(
    name="my-agent",
    balance=1.00,                       # starting balance, simulated wallet
    broker_url="wss://YOUR-RENDER-APP-NAME.onrender.com",  # the live, managed broker — no local server required
)

# Cryptographic identity — an Ed25519 keypair — is generated once and
# reused on every future run, loaded lazily in a background thread the
# first time this agent actually talks to a broker. There is no separate
# "connect" step to call: the keypair is the agent's passport, and every
# session below opens and authenticates the connection for you.
```

### 3. Selling Resources

```python
import asyncio
from m2m_ledger import Agent

def analyze_position(cursor, requested_resource):
    # `cursor` carries your own state between calls. `requested_resource`
    # is exactly what the buyer asked for — useful when one provider
    # multiplexes several resources under a shared namespace.
    chunk = [{"move": "e4", "eval": 0.3}]
    return chunk, cursor

async def main():
    oracle = Agent(name="chess-oracle", broker_url="wss://YOUR-RENDER-APP-NAME.onrender.com")
    oracle.will_provide(
        "chess_analysis",
        analyze_position,
        price_per_kb=0.15,
        description="Real-time chess move analysis",
    )
    while True:                # one matched, metered session per iteration
        await oracle.run()     # blocks until matched, streamed, and settled
        await asyncio.sleep(1) # back on the Order Book for the next buyer

asyncio.run(main())
```

### 4. Buying Resources

```python
import asyncio
from m2m_ledger import Agent

async def main():
    buyer = Agent(name="buyer-agent", balance=1.00, broker_url="wss://YOUR-RENDER-APP-NAME.onrender.com")

    # Live service discovery — no hardcoded resource name required.
    menu = await buyer.get_market_menu()
    resource = next(iter(menu.values()))["resource"]

    result = await buyer.buy_data(resource, ticks=20)
    print(result)
    # {'type': 'complete', 'ticks': 20, 'total_paid': 0.000858, ...}

asyncio.run(main())
```

`buy_data()` is the single-call convenience path for exactly this case. For resilient, multi-phase sessions — automatic reconnection, exponential backoff, mid-session recovery — compose `will_offer()` / `will_request()` / `run()` directly inside your own retry loop; see `node_a.py` in this repository for the reference implementation.

---

## Live Market Dashboard

The broker doubles as a real-time market registry. Every provider that calls `will_provide()` is published — and automatically removed on disconnect — to a **Dynamic Order Book**, reachable two ways:

- **From code**, over the same authenticated WebSocket session, via `get_market_menu()`.
- **From a browser, `curl`, or any HTTP client** — no signature, no SDK required:

```bash
curl https://YOUR-RENDER-APP-NAME.onrender.com/orderbook
```

```json
{
  "server_time": 1784071793.09,
  "count": 1,
  "providers": {
    "319d9849f470bd3aa551bcedc23dce818e6a5a34c7656000105d994a5f7ff03": {
      "resource": "chess_analysis",
      "price_per_sec": null,
      "price_per_kb": 0.15,
      "description": "Real-time chess move analysis",
      "status": "available",
      "listed_for_sec": 1.4
    }
  }
}
```

Same registry, two protocols: bots negotiate over the signed WebSocket channel, humans watch the market over plain HTTP. Zero additional infrastructure.

---

## Security Note

Each agent's Ed25519 private key is written to a local file named `.<agent-name>.agent_keys.json`. This file **is** the agent's cryptographic identity — anyone holding it can sign transactions, drain its wallet, and impersonate it to every broker and counterparty on the network. It is written with owner-only permissions on POSIX systems, but permissions are not a substitute for keeping it out of version control.

Add this to `.gitignore` before your first commit, not after:

```gitignore
.*.agent_keys.json
```

Treat every `.agent_keys.json` file exactly as you would a private key or an API credential: never commit it, never paste its contents into a chat, an issue, or a log line, and rotate the identity (delete the file, let the agent regenerate one) immediately if you suspect it has leaked.

---

## License

MIT — see [LICENSE](LICENSE).
