# m2m-ledger

> **The Financial Layer for Autonomous AI.**

![License](https://img.shields.io/badge/license-MIT-informational)
![Python](https://img.shields.io/badge/python-3.9%2B-blue)
![Protocol](https://img.shields.io/badge/protocol-v3%20(Ed25519)-black)
![Async](https://img.shields.io/badge/asyncio-native-green)

**`m2m-ledger` is a payload-agnostic network where AI agents buy and sell data, memory, or compute power from each other — autonomously, over secure WebSockets.** A chess engine sells analysis. An LLM sells reasoning. One agent's overflowing memory becomes another agent's paid summarization job. The network doesn't know or care which — it just routes signed contracts and settles the money.

```
      SELLER  (Agent)                                BUYER  (Agent)
   will_provide(...)                              will_request(...)
           │                                               │
           │        Ed25519-signed contract, matched        │
           └───────────────────┐         ┌──────────────────┘
                                ▼         ▼
                       ┌─────────────────────────┐
                       │      M2M  BROKER         │
                       │  Order Book · Ledger     │
                       │  fully managed · 24/7    │
                       └─────────────────────────┘
```

---

## ⚡ Zero Setup

**The M2M Broker infrastructure is fully managed and live.** You don't need to spin up any local servers, run a database, or host anything. Install the SDK, write an agent, point it at the network — you're trading in under a minute.

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

### 1. Installation

```bash
pip install git+https://github.com/gommapane1/m2m-ledger.git
```

### 2. Initialization & Identity

```python
from m2m_ledger import Agent

agent = Agent(
    name="my-agent",
    balance=1.00,                                          # starting balance, simulated wallet
    broker_url="wss://m2m-broker.onrender.com",       # the live, managed broker — no local server required
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

def next_primes(cursor, resource):
    # `cursor` carries your own state between calls — here, the last
    # prime we found. Return whatever you want to sell; the protocol
    # doesn't care what's inside the chunk.
    n = cursor or 1
    batch = []
    while len(batch) < 50:
        n += 1
        if all(n % d for d in range(2, int(n ** 0.5) + 1)):
            batch.append(n)
    return batch, n

async def main():
    seller = Agent(name="prime-seller", broker_url="wss://m2m-broker.onrender.com")
    seller.will_provide("primes", next_primes, price_per_kb=0.01,
                        description="A steady stream of prime numbers")
    result = await seller.run()   # blocks until matched, streamed, and settled
    print(result)

asyncio.run(main())
```

### 4. Buying Resources

```python
import asyncio
from m2m_ledger import Agent

async def main():
    buyer = Agent(name="buyer-agent", balance=1.00, broker_url="wss://m2m-broker.onrender.com")

    # Live service discovery — no hardcoded resource name required.
    menu = await buyer.get_market_menu()
    resource = next(iter(menu.values()))["resource"]

    result = await buyer.buy_data(resource, ticks=20)
    print(result)
    # {'type': 'complete', 'ticks': 20, 'total_paid': 0.000858, ...}

asyncio.run(main())
```

`buy_data()` is the single-call convenience path for exactly this case. For custom retry logic — reconnection, backoff, falling back to a local computation when the market doesn't answer in time — compose `will_offer()` / `will_request()` / `run()` directly inside your own loop; see [`examples/chess_buyer.py`](examples/chess_buyer.py) for a working pattern.

---

## 🔌 Bring Your Own AI (BYOAI)

Here's the part that surprises people: **the protocol has no idea what you're selling.** The broker routes signed envelopes and settles micropayments — the callback you hand to `will_provide()` can do anything a normal Python function can do. Query a database. Run a simulation. Or call an LLM.

This is a complete, running seller that turns GPT-4 into a metered, pay-per-call service on the network:

```python
from openai import OpenAI
from m2m_ledger import Agent

client = OpenAI()   # reads OPENAI_API_KEY from the environment

def gpt4_handler(cursor, resource: str):
    prompt = resource.split(":", 1)[1] if ":" in resource else resource
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
    )
    return [{"answer": response.choices[0].message.content}], cursor

seller = Agent(name="gpt4-oracle", broker_url="wss://m2m-broker.onrender.com")
seller.will_provide(
    "gpt4_reasoning:all",     # wildcard: matches gpt4_reasoning:<any prompt>
    gpt4_handler,
    price_per_kb=0.02,
    description="GPT-4 reasoning, billed per response",
)
```

A buyer reaches it exactly like any other resource on the network — no special-casing anywhere in the protocol:

```python
buyer.will_request(resource="gpt4_reasoning:What is the capital of France?", param=1, mode="count")
```

One detail worth knowing: `will_provide()` already runs your handler in a background worker thread. A plain **blocking** `openai` call — no `async`, no extra plumbing — is exactly the right tool here; the event loop stays free for every other session on the broker while GPT-4 thinks.

Swap the ten lines inside `gpt4_handler` for a call to your own model, your vector database, your simulation, your proprietary dataset — the network doesn't change.

---

## 📚 Examples — Pick Your Level

Three pairs of scripts, in increasing order of "how far can this actually go":

| Level | Files | What it shows |
|---|---|---|
| **1 · Basics** | [`examples/simple_seller.py`](examples/simple_seller.py)<br>[`examples/simple_buyer.py`](examples/simple_buyer.py) | The absolute minimum: `will_provide` / `will_offer` + `will_request` / `run()`. Selling and buying a stream of prime numbers. Start here. |
| **2 · Visual demo** | [`examples/chess_seller.py`](examples/chess_seller.py)<br>[`examples/chess_buyer.py`](examples/chess_buyer.py) | An Oracle sells real Stockfish depth-20 analysis to an agent playing a live game in your terminal — a full Order Book match, streamed per-second billing, and a resource-constrained buyer deciding *when* a purchase is worth it. |
| **3 · Heavy-duty** | [`examples/advanced_seller.py`](examples/advanced_seller.py)<br>[`examples/advanced_buyer.py`](examples/advanced_buyer.py) | Streaming **megabytes** of AI conversational memory across the wire — context-window offloading, SHA-256 integrity verification, and a real distillation pass sold back as dense state. The payload-agnostic claim, proven at scale. |

Run any pair the same way — start the `_seller` first, then the `_buyer`; both connect straight to the live network, no local broker required:

```bash
python3 examples/simple_seller.py &
python3 examples/simple_buyer.py
```

---

## Live Market Dashboard

The broker doubles as a real-time market registry. Every provider that calls `will_provide()` is published — and automatically removed on disconnect — to a **Dynamic Order Book**, reachable two ways:

- **From code**, over the same authenticated WebSocket session, via `get_market_menu()`.
- **From a browser, `curl`, or any HTTP client** — no signature, no SDK required:

```bash
curl https://m2m-broker.onrender.com/orderbook
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