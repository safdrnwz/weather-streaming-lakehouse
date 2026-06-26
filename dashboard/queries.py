"""
dashboard/queries.py
====================
Pure data-access functions for the dashboard -- each returns a pandas DataFrame
read from the weather_warehouse. No Streamlit here, so these can be unit-tested.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import db as dbcfg  # noqa: E402


def _read(sql: str) -> pd.DataFrame:
    """Run a SQL query and return a DataFrame (empty DF on any missing table)."""
    try:
        return pd.read_sql(sql, dbcfg.engine())
    except Exception:
        return pd.DataFrame()


def city_latest() -> pd.DataFrame:
    """Latest reading per city (gold.city_latest)."""
    df = _read("SELECT * FROM gold.city_latest ORDER BY city")
    if not df.empty:
        df["event_time"] = pd.to_datetime(df["event_time"])
    return df


def daily_stats() -> pd.DataFrame:
    """Per-city per-day stats (gold.daily_stats)."""
    df = _read("SELECT * FROM gold.daily_stats ORDER BY day, city")
    if not df.empty:
        df["day"] = pd.to_datetime(df["day"])
    return df


def hourly_stats() -> pd.DataFrame:
    """Per-city per-hour stats (gold.hourly_stats)."""
    df = _read("SELECT * FROM gold.hourly_stats ORDER BY hour_start, city")
    if not df.empty:
        df["hour_start"] = pd.to_datetime(df["hour_start"])
    return df


def recent_readings(hours: int = 24) -> pd.DataFrame:
    """Raw silver readings from the last N hours (for the trend line chart)."""
    df = _read(
        "SELECT city, event_time, temperature_c, humidity_pct, condition "
        "FROM silver.weather_readings "
        f"WHERE event_time >= now() - interval '{int(hours)} hours' "
        "ORDER BY event_time"
    )
    if not df.empty:
        df["event_time"] = pd.to_datetime(df["event_time"])
    return df


def latest_full() -> pd.DataFrame:
    """Latest full reading per city from silver (includes wind) for gauges/map."""
    df = _read(
        "SELECT DISTINCT ON (city) city, event_time, temperature_c, humidity_pct, "
        "wind_speed_kmh, condition "
        "FROM silver.weather_readings ORDER BY city, event_time DESC"
    )
    if not df.empty:
        df["event_time"] = pd.to_datetime(df["event_time"])
    return df
