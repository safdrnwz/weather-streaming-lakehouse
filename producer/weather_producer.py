"""
producer/weather_producer.py
============================
Polls Open-Meteo for several cities every minute and publishes one JSON message
per city to the Kafka topic `weather-raw`.

This is the "live source" of the streaming pipeline. Run it in its own terminal:
    python producer/weather_producer.py

Stop with Ctrl+C.
"""

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from kafka import KafkaProducer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import config as cfg  # noqa: E402
from common import weather_rules as wr  # noqa: E402


def fetch_city(name: str, lat: float, lon: float) -> dict | None:
    """Call Open-Meteo for one city and shape it into our message format."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
        "timezone": cfg.TIMEZONE,
    }
    try:
        resp = requests.get(cfg.OPEN_METEO_URL, params=params, timeout=10)
        resp.raise_for_status()
        cur = resp.json()["current"]
    except (requests.RequestException, KeyError, ValueError) as exc:
        print(f"[producer] WARN fetch failed for {name}: {exc}")
        return None

    # Shape into our flat message (this is what silver expects).
    return {
        "city": name,
        "event_time": cur["time"],                 # local time, e.g. 2026-06-25T12:30
        "temperature_c": cur.get("temperature_2m"),
        "humidity_pct": cur.get("relative_humidity_2m"),
        "wind_speed_kmh": cur.get("wind_speed_10m"),
        "weather_code": cur.get("weather_code"),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=cfg.KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    print(f"[producer] sending to topic '{cfg.KAFKA_TOPIC}' every {cfg.POLL_SECONDS}s "
          f"for cities: {', '.join(wr.CITIES)}")

    while True:
        sent = 0
        for name, (lat, lon) in wr.CITIES.items():
            msg = fetch_city(name, lat, lon)
            if msg is not None:
                # key = city so all readings of a city land in the same partition
                producer.send(cfg.KAFKA_TOPIC, key=name, value=msg)
                sent += 1
        producer.flush()
        print(f"[producer] {datetime.now().strftime('%H:%M:%S')} sent {sent} messages")
        time.sleep(cfg.POLL_SECONDS)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[producer] stopped")
