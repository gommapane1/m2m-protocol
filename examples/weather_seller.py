"""
================================================================================
 weather_seller.py -- THE WEATHER ORACLE: sells live weather, no API key needed
================================================================================
A drop-in m2m-ledger provider that closes the demand/supply gap for the
Guardian demo: it lists `weather_data` on the marketplace and serves live
conditions for any city, fetched from Open-Meteo (free, keyless).

  * Consumers request `weather_data:<city>`  (e.g. weather_data:Rome).
  * The handler geocodes the city, fetches current conditions, and delivers
    ONE dense result chunk on the first tick; later ticks stream empty
    (weight 0), so a duration-mode buyer pays seconds, not repeats.
  * A tiny in-memory cache (60s per city) keeps repeated purchases from
    hammering the upstream API.

RUN
    python3 weather_seller.py           # connects to the live global broker

No API key, no extra dependency: stdlib + m2m_ledger only.
================================================================================
"""

import asyncio
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

os.environ.setdefault("M2M_TRACE", "0")
sys.stdout.reconfigure(line_buffering=True)

try:
    from m2m_ledger import Agent
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
    from m2m_ledger import Agent

logging.getLogger().setLevel(logging.WARNING)

# The M2M Broker infrastructure is fully managed and live: no local servers.
BROKER_URL = os.environ.get("M2M_BROKER_URL", "wss://m2m-broker.onrender.com")
RESOURCE_NAMESPACE = "weather_data"
PRICE_PER_SEC = 0.002
PRICE_PER_KB = 0.01
CACHE_TTL_SEC = 60.0
BACKOFF_START, BACKOFF_CAP = 1.0, 10.0

# Endpoint Open-Meteo (keyless). Override via env per test/self-hosting.
GEOCODE_BASE = os.environ.get("WEATHER_GEOCODE_BASE",
                              "https://geocoding-api.open-meteo.com/v1/search")
FORECAST_BASE = os.environ.get("WEATHER_FORECAST_BASE",
                               "https://api.open-meteo.com/v1/forecast")

# https://open-meteo.com/en/docs -- WMO weather interpretation codes
WMO = {0: "clear sky", 1: "mainly clear", 2: "partly cloudy", 3: "overcast",
       45: "fog", 51: "light drizzle", 61: "light rain", 63: "rain",
       65: "heavy rain", 71: "light snow", 80: "rain showers",
       95: "thunderstorm"}

if os.name == "nt":
    os.system("")
DIM, BOLD, RESET = "\033[2m", "\033[1m", "\033[0m"
GOLD, CYAN, GREEN, RED = "\033[33m", "\033[36m", "\033[32m", "\033[31m"

_cache: dict = {}          # city_lower -> (timestamp, result_dict)


def _http_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "m2m-weather-oracle/1.0"})
    with urllib.request.urlopen(req, timeout=8) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_weather(city: str) -> dict:
    """Geocoding -> forecast -> dict denso. Errori come dict {'error': ...}:
    il buyer (e la SUA AI) devono vedere il fallimento, non un crash."""
    key = city.strip().lower()
    if not key:
        return {"error": "empty_city", "detail": "request weather_data:<city>"}
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < CACHE_TTL_SEC:
        return hit[1]
    try:
        geo = _http_json(f"{GEOCODE_BASE}?{urllib.parse.urlencode({'name': city, 'count': 1})}")
        results = geo.get("results") or []
        if not results:
            return {"error": "unknown_city", "detail": f"no geocoding match for '{city}'"}
        place = results[0]
        lat, lon = place["latitude"], place["longitude"]
        fc = _http_json(f"{FORECAST_BASE}?{urllib.parse.urlencode({'latitude': lat, 'longitude': lon, 'current': 'temperature_2m,weather_code'})}")
        cur = fc.get("current") or {}
        code = int(cur.get("weather_code", -1))
        result = {
            "city": place.get("name", city),
            "country": place.get("country", ""),
            "latitude": lat, "longitude": lon,
            "temperature_c": cur.get("temperature_2m"),
            "conditions": WMO.get(code, f"wmo_code_{code}"),
            "observed_at": cur.get("time"),
            "source": "open-meteo.com",
        }
        _cache[key] = (time.time(), result)
        return result
    except Exception as exc:
        return {"error": "upstream_failure", "detail": f"{type(exc).__name__}: {exc}"}


def handler(cursor, resource: str):
    """Primo tick: il dato (1 elemento). Tick successivi: chunk vuoti a peso
    zero -- un buyer in mode=duration paga i secondi, non copie ripetute."""
    if cursor is None:
        city = resource.split(":", 1)[1] if ":" in resource else ""
        print(f"\n{GOLD}{BOLD}◆ SIGNED REQUEST{RESET}  city={CYAN}{city or '?'}{RESET}")
        result = fetch_weather(city)
        if "error" in result:
            print(f"  {RED}✗ {result['error']}: {result['detail'][:70]}{RESET}")
        else:
            print(f"  {GREEN}✓ {result['city']}: {result['temperature_c']}°C, "
                  f"{result['conditions']}{RESET}")
        return [result], {"delivered": True}
    return [], cursor


async def oracle_supervisor() -> None:
    seller = Agent(name="Weather-Oracle", balance=0.0, broker_url=BROKER_URL)
    seller.will_provide(
        f"{RESOURCE_NAMESPACE}:all",
        handler,
        price_per_sec=PRICE_PER_SEC,
        price_per_kb=PRICE_PER_KB,
        description="Live weather by city - request weather_data:<city> (e.g. weather_data:Rome)",
    )
    await seller.ensure_identity()

    print(f"\n{BOLD}{GOLD}╔══════════════════════════════════════════════════════════════╗{RESET}")
    print(f"{BOLD}{GOLD}║   WEATHER ORACLE  ·  live conditions on m2m-ledger           ║{RESET}")
    print(f"{BOLD}{GOLD}╚══════════════════════════════════════════════════════════════╝{RESET}")
    print(f"  {CYAN}passport{RESET}  {seller.passport_id[:16]}…  {DIM}(Ed25519){RESET}")
    print(f"  {CYAN}listing{RESET}   {RESOURCE_NAMESPACE}  @  ${PRICE_PER_SEC}/sec + ${PRICE_PER_KB}/KB")
    print(f"  {CYAN}upstream{RESET}  open-meteo.com  {DIM}(keyless){RESET}")
    print(f"  {CYAN}broker{RESET}    {BROKER_URL}\n")

    backoff, session_n = BACKOFF_START, 0
    while True:
        session_n += 1
        print(f"{DIM}── listed on the order book · waiting for a buyer "
              f"(session #{session_n}) ──{RESET}")
        result = await seller.run()
        kind, why = result.get("type"), result.get("reason", "")
        if kind in ("complete", "halted") and result.get("ticks", 0) > 0:
            backoff = BACKOFF_START
            print(f"{GREEN}{BOLD}💰 SETTLED{RESET}  earned {GREEN}${result.get('earned', 0):.6f}{RESET} "
                  f"over {result.get('ticks', 0)} ticks · lifetime "
                  f"{GREEN}${seller.balance:.6f}{RESET}\n")
            await asyncio.sleep(0.2)
            continue
        print(f"{DIM}broker unreachable or session dropped ({kind}/{why}) — "
              f"retrying in {backoff:.0f}s…{RESET}")
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, BACKOFF_CAP)


if __name__ == "__main__":
    try:
        asyncio.run(oracle_supervisor())
    except KeyboardInterrupt:
        print(f"\n{DIM}Weather Oracle shutting down.{RESET}")
    sys.exit(0)