"""
examples/simple_buyer.py -- THE ABSOLUTE BASICS (consumer side)

Buys 200 prime numbers on the M2M network. Pairs with simple_seller.py.

    python3 examples/simple_buyer.py
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


async def main():
    buyer = Agent(name="prime-buyer", balance=1.00, broker_url=BROKER_URL)
    buyer.will_offer(money_per_kb=0.01)
    buyer.will_request(resource="primes", param=200, mode="count")
    result = await buyer.run()
    print(result)


if __name__ == "__main__":
    asyncio.run(main())