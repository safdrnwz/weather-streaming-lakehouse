"""Shared pytest fixtures for the weather pipeline tests."""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def raw_messages() -> pd.DataFrame:
    """A small batch of raw Kafka messages, including some dirty rows."""
    return pd.DataFrame([
        # normal readings
        {"city": "Gurugram", "event_time": "2026-06-25T12:30", "temperature_c": 37.6,
         "humidity_pct": 32, "wind_speed_kmh": 2.5, "weather_code": 0, "fetched_at": "2026-06-25T12:31:00+05:30"},
        {"city": "Mumbai", "event_time": "2026-06-25T12:30", "temperature_c": 31.0,
         "humidity_pct": 70, "wind_speed_kmh": 12.0, "weather_code": 3, "fetched_at": "2026-06-25T12:31:00+05:30"},
        {"city": "Delhi", "event_time": "2026-06-25T12:30", "temperature_c": 41.2,
         "humidity_pct": 20, "wind_speed_kmh": 5.0, "weather_code": 0, "fetched_at": "2026-06-25T12:31:00+05:30"},
        # duplicate (same city+event_time as Gurugram above) -> must collapse to 1
        {"city": "Gurugram", "event_time": "2026-06-25T12:30", "temperature_c": 37.6,
         "humidity_pct": 32, "wind_speed_kmh": 2.5, "weather_code": 0, "fetched_at": "2026-06-25T12:31:30+05:30"},
        # invalid: humidity 250 -> must be dropped
        {"city": "Chennai", "event_time": "2026-06-25T12:30", "temperature_c": 33.0,
         "humidity_pct": 250, "wind_speed_kmh": 8.0, "weather_code": 61, "fetched_at": "2026-06-25T12:31:00+05:30"},
        # invalid: temperature 999 -> must be dropped
        {"city": "Kolkata", "event_time": "2026-06-25T12:30", "temperature_c": 999.0,
         "humidity_pct": 80, "wind_speed_kmh": 6.0, "weather_code": 95, "fetched_at": "2026-06-25T12:31:00+05:30"},
    ])
