"""
examples/simple_seller.py -- THE ABSOLUTE BASICS (provider side)

Sells a stream of prime numbers on the M2M network. No engine, no LLM, no
threading tricks: just the three calls every provider needs.

    python3 examples/simple_seller.py
"""

import asyncio
import os
import sys
from pathlib import Path

try:
    from m2m_ledger import Agent
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    from m2m_ledger import Agent

# The M2M Broker is fully managed and live -- no local server required.
BROKER_URL = os.environ.get("M2M_BROKER_URL", "wss://m2m-broker.onrender.com")


def next_primes(cursor, resource):
    """The contract every handler follows: (cursor, resource) -> (chunk, new_cursor).
    `cursor` is just the last prime we found -- our own state, our own rules."""
    n = cursor or 1
    batch = []
    while len(batch) < 50:
        n += 1
        if all(n % d for d in range(2, int(n ** 0.5) + 1)):
            batch.append(n)
    return batch, n


async def main():
    seller = Agent(name="prime-seller", broker_url=BROKER_URL)
    seller.will_provide("primes", next_primes, price_per_kb=0.01,
                        description="A steady stream of prime numbers")
    result = await seller.run()
    print(result)


if __name__ == "__main__":
    asyncio.run(main())