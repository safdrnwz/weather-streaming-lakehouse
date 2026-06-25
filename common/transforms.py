"""
common/transforms.py
====================
PURE transformation functions (pandas) -- no Kafka, no Spark, no database.

WHY pure: the streaming job applies these to each micro-batch, AND the unit
tests run them on small samples with no infrastructure. One tested code path,
used in production and in tests.

Message shape produced by producer/weather_producer.py (one Kafka message):
    {
      "city": "Gurugram",
      "event_time": "2026-06-25T12:30",     # local time from Open-Meteo
      "temperature_c": 37.6,
      "humidity_pct": 32,
      "wind_speed_kmh": 2.5,
      "weather_code": 0,
      "fetched_at": "2026-06-25T12:31:05+05:30"
    }
"""

from datetime import datetime, timezone

import pandas as pd

from . import weather_rules as wr


def transform_silver(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Raw messages -> clean silver rows.

    Steps: type-cast, add `condition` from weather_code, validate ranges (drop
    bad), drop duplicates on (city, event_time), add ingested_at.
    """
    if raw_df.empty:
        return raw_df.assign(condition=[], ingested_at=[]) if False else raw_df.copy()

    df = raw_df.copy()

    # 1) types
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df["temperature_c"] = pd.to_numeric(df["temperature_c"], errors="coerce")
    df["humidity_pct"] = pd.to_numeric(df["humidity_pct"], errors="coerce")
    df["wind_speed_kmh"] = pd.to_numeric(df["wind_speed_kmh"], errors="coerce")
    df["weather_code"] = pd.to_numeric(df["weather_code"], errors="coerce")

    # 2) derive condition from the WMO code
    df["condition"] = df["weather_code"].apply(wr.code_to_condition)

    # 3) drop rows missing a city or a timestamp (cannot key them)
    df = df.dropna(subset=["city", "event_time"])

    # 4) data-quality gate: keep only sane readings
    mask = df.apply(
        lambda r: wr.is_valid_reading(r["temperature_c"], r["humidity_pct"], r["wind_speed_kmh"]),
        axis=1,
    )
    df = df[mask]

    # 5) de-duplicate on the business key (latest fetched wins if present)
    if "fetched_at" in df.columns:
        df = df.sort_values("fetched_at").drop_duplicates(subset=["city", "event_time"], keep="last")
    else:
        df = df.drop_duplicates(subset=["city", "event_time"])

    # 6) lineage
    df["ingested_at"] = datetime.now(timezone.utc)

    cols = ["city", "event_time", "temperature_c", "humidity_pct",
            "wind_speed_kmh", "weather_code", "condition", "ingested_at"]
    return df[cols].reset_index(drop=True)


def build_gold_city_latest(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Latest reading per city (one row per city)."""
    if silver_df.empty:
        return silver_df.copy()
    idx = silver_df.sort_values("event_time").groupby("city")["event_time"].idxmax()
    latest = silver_df.loc[idx, ["city", "event_time", "temperature_c", "humidity_pct", "condition"]]
    return latest.sort_values("city").reset_index(drop=True)


def build_gold_hourly(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Per (city, hour) average/max/min temperature and reading count."""
    if silver_df.empty:
        return silver_df.copy()
    df = silver_df.copy()
    df["hour_start"] = df["event_time"].dt.floor("h")
    g = (df.groupby(["city", "hour_start"])
           .agg(avg_temp=("temperature_c", "mean"),
                max_temp=("temperature_c", "max"),
                min_temp=("temperature_c", "min"),
                readings=("temperature_c", "count"))
           .reset_index())
    g["avg_temp"] = g["avg_temp"].round(2)
    return g.sort_values(["city", "hour_start"]).reset_index(drop=True)


def build_gold_daily(silver_df: pd.DataFrame) -> pd.DataFrame:
    """Per (city, date) stats + a simple heatwave flag (max temp > 40 C)."""
    if silver_df.empty:
        return silver_df.copy()
    df = silver_df.copy()
    df["day"] = df["event_time"].dt.date
    g = (df.groupby(["city", "day"])
           .agg(avg_temp=("temperature_c", "mean"),
                max_temp=("temperature_c", "max"),
                min_temp=("temperature_c", "min"),
                readings=("temperature_c", "count"))
           .reset_index())
    g["avg_temp"] = g["avg_temp"].round(2)
    g["heatwave"] = g["max_temp"] > 40.0
    return g.sort_values(["city", "day"]).reset_index(drop=True)
