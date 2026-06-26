"""
common/scenarios.py
===================
Generates live weather readings for a chosen SCENARIO so the realtime app shows
visible movement every few seconds (the real Open-Meteo API only updates every
~15 minutes, which is too slow to "feel" real-time in a demo).

Scenarios:
  - normal    : seasonal baseline per city + small noise
  - heatwave  : temperatures climb high (>40C), low humidity, clear/sunny
  - storm     : temperature drops, high humidity + wind, thunderstorm/rain codes
  - cold_snap : temperatures drop sharply

Each call returns one message per city with a fresh per-second event_time, so
silver gets new rows and gold updates over time.
"""

import random
from datetime import datetime, timezone

from . import weather_rules as wr

# Per-city seasonal baseline temperature (deg C) for the "normal" scenario.
_BASELINE = {
    "Delhi": 39, "Mumbai": 31, "Bengaluru": 27,
    "Chennai": 36, "Kolkata": 33, "Gurugram": 38,
}


def _reading(city: str, scenario: str) -> dict:
    base = _BASELINE.get(city, 32)

    if scenario == "heatwave":
        temp = base + random.uniform(4, 9)        # push well above 40 for big cities
        humidity = random.randint(12, 28)
        wind = random.uniform(1, 6)
        code = random.choice([0, 0, 1])           # clear / mostly clear
    elif scenario == "storm":
        temp = base - random.uniform(6, 12)        # cools down in a storm
        humidity = random.randint(80, 99)
        wind = random.uniform(30, 60)
        code = random.choice([95, 61, 63, 65])     # thunderstorm / rain
    elif scenario == "cold_snap":
        temp = random.uniform(4, 14)
        humidity = random.randint(50, 80)
        wind = random.uniform(5, 20)
        code = random.choice([3, 45, 51])          # overcast / fog / drizzle
    else:  # normal
        temp = base + random.uniform(-2.5, 2.5)
        humidity = random.randint(35, 70)
        wind = random.uniform(2, 15)
        code = random.choice([0, 1, 2, 3])

    return {
        "city": city,
        # per-second timestamp -> every tick is a new event_time
        "event_time": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "temperature_c": round(temp, 1),
        "humidity_pct": humidity,
        "wind_speed_kmh": round(wind, 1),
        "weather_code": code,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def generate(scenario: str = "normal") -> list[dict]:
    """Return one reading per known city for the given scenario."""
    return [_reading(city, scenario) for city in wr.CITIES]
