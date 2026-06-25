"""
common/db.py
============
Database access for the warehouse (weather_warehouse). Provides:
  - a SQLAlchemy engine
  - schema creation (bronze/silver/gold) with self-healing of gold primary keys
  - a helper to ensure the per-DAY partition of the silver table exists
  - INSERT helpers (silver append/upsert) and INCREMENTAL gold upserts (MERGE)

DESIGN:
  - bronze: every message is appended (full history, see spark_streaming.py).
  - silver: RANGE-partitioned by event_time (DATE first); insert with
    ON CONFLICT (city, event_time) DO NOTHING (idempotent, keeps history).
  - gold: INCREMENTAL -- only the keys touched by the current batch are
    recomputed and upserted (ON CONFLICT DO UPDATE). The rest of gold is left
    untouched (no full-table rebuild).
"""

from datetime import date, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, create_engine, text

from . import config as cfg


def engine():
    """SQLAlchemy engine connected to the warehouse database."""
    url = (f"postgresql+psycopg2://{cfg.PGUSER}:{cfg.PGPASSWORD}"
           f"@{cfg.PGHOST}:{cfg.PGPORT}/{cfg.WAREHOUSE_DB}")
    return create_engine(url)


def ensure_schemas_and_tables():
    """Create schemas + parent tables once (idempotent). Runs the DDL file.

    Also self-heals: an older version of this project rebuilt the gold tables
    each batch with pandas `to_sql`, which creates tables WITHOUT a primary key.
    The incremental upserts need the PK for `ON CONFLICT`, so any gold table that
    exists without one is dropped here and recreated (with its PK) from the DDL.
    Gold can always be rebuilt from silver, so dropping it is safe.
    """
    from pathlib import Path
    ddl = (Path(__file__).resolve().parents[1] / "warehouse" / "ddl.sql").read_text()
    # Remove comment lines FIRST (a ';' inside a comment must not split a
    # statement), then split the remaining SQL into individual statements.
    sql_only = "\n".join(
        ln for ln in ddl.splitlines() if not ln.strip().startswith("--")
    )
    with engine().begin() as conn:
        # 1) make sure the schemas exist (so the checks below can run)
        for sch in (cfg.BRONZE_SCHEMA, cfg.SILVER_SCHEMA, cfg.GOLD_SCHEMA):
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {sch}"))

        # 2) self-heal: drop any gold table that exists WITHOUT a primary key
        for tbl in ("city_latest", "hourly_stats", "daily_stats"):
            exists = conn.execute(
                text("SELECT to_regclass(:q)"),
                {"q": f"{cfg.GOLD_SCHEMA}.{tbl}"},
            ).scalar()
            if not exists:
                continue
            has_pk = conn.execute(
                text("SELECT 1 FROM information_schema.table_constraints "
                     "WHERE table_schema = :s AND table_name = :t "
                     "AND constraint_type = 'PRIMARY KEY'"),
                {"s": cfg.GOLD_SCHEMA, "t": tbl},
            ).first()
            if not has_pk:
                conn.execute(text(f"DROP TABLE {cfg.GOLD_SCHEMA}.{tbl}"))

        # 3) run the DDL (CREATE ... IF NOT EXISTS recreates anything dropped, WITH PKs)
        for stmt in sql_only.split(";"):
            if stmt.strip():
                conn.execute(text(stmt))


def ensure_silver_partition(day: date):
    """Create the silver partition for a given day if it does not exist.

    WHY: a RANGE-partitioned parent has no storage of its own; each day needs
    its own child partition before rows for that day can be inserted. Created on
    demand (idempotent) so the streaming job never fails on a new day.
    """
    start = day
    end = day + timedelta(days=1)
    part = f"weather_readings_{day.strftime('%Y_%m_%d')}"
    sql = (
        f"CREATE TABLE IF NOT EXISTS {cfg.SILVER_SCHEMA}.{part} "
        f"PARTITION OF {cfg.SILVER_SCHEMA}.weather_readings "
        f"FOR VALUES FROM ('{start} 00:00:00+05:30') TO ('{end} 00:00:00+05:30')"
    )
    with engine().begin() as conn:
        conn.execute(text(sql))


# -----------------------------------------------------------------------------
# Helpers to make pandas/numpy values safe for psycopg2.
# WHY: to_dict("records") yields numpy scalars / NaN / pandas Timestamps that
#   some psycopg2 versions cannot adapt. Convert to native Python first.
# -----------------------------------------------------------------------------
def _native(v):
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return None if np.isnan(v) else float(v)
    if isinstance(v, (np.bool_, bool)):
        return bool(v)
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v


def _records(df, cols):
    """DataFrame -> list of dicts with native Python values (psycopg2-safe)."""
    return [{c: _native(r[c]) for c in cols} for _, r in df.iterrows()]


def upsert_silver(df):
    """Insert silver rows, ignoring duplicates on (city, event_time).

    WHY ON CONFLICT DO NOTHING: streaming can re-deliver a message; the primary
    key (city, event_time) keeps the table idempotent (no double rows). History
    is preserved -- nothing is deleted.
    """
    if df.empty:
        return
    cols = ["city", "event_time", "temperature_c", "humidity_pct",
            "wind_speed_kmh", "weather_code", "condition", "ingested_at"]
    placeholders = ", ".join(f":{c}" for c in cols)
    sql = text(
        f"INSERT INTO {cfg.SILVER_SCHEMA}.weather_readings ({', '.join(cols)}) "
        f"VALUES ({placeholders}) ON CONFLICT (city, event_time) DO NOTHING"
    )
    with engine().begin() as conn:
        conn.execute(sql, _records(df, cols))


def read_silver_for_keys(cities, days):
    """Read silver rows for the given cities + days (to recompute touched buckets).

    WHY: incremental gold recomputes only the (city, day) buckets that the new
    batch touched -- but each bucket must be recomputed from ALL its silver rows
    (e.g. an hourly average needs every reading in that hour), so we fetch the
    full set of rows for those cities and days, not just the batch.
    """
    q = text(
        f"SELECT * FROM {cfg.SILVER_SCHEMA}.weather_readings "
        f"WHERE city IN :cities AND event_time::date IN :days"
    ).bindparams(bindparam("cities", expanding=True), bindparam("days", expanding=True))
    sub = pd.read_sql(q, engine(), params={"cities": list(cities), "days": [str(d) for d in days]})
    if not sub.empty:
        sub["event_time"] = pd.to_datetime(sub["event_time"])
    return sub


def upsert_city_latest(df):
    """Incremental upsert of gold.city_latest (one row per city, move forward only)."""
    if df.empty:
        return
    cols = ["city", "event_time", "temperature_c", "humidity_pct", "condition"]
    sql = text(
        f"INSERT INTO {cfg.GOLD_SCHEMA}.city_latest ({', '.join(cols)}) "
        f"VALUES (:city, :event_time, :temperature_c, :humidity_pct, :condition) "
        f"ON CONFLICT (city) DO UPDATE SET "
        f"  event_time = EXCLUDED.event_time, "
        f"  temperature_c = EXCLUDED.temperature_c, "
        f"  humidity_pct = EXCLUDED.humidity_pct, "
        f"  condition = EXCLUDED.condition "
        f"WHERE EXCLUDED.event_time > {cfg.GOLD_SCHEMA}.city_latest.event_time"
    )
    with engine().begin() as conn:
        conn.execute(sql, _records(df, cols))


def upsert_hourly(df):
    """Incremental upsert of gold.hourly_stats for the (city, hour) buckets given."""
    if df.empty:
        return
    cols = ["city", "hour_start", "avg_temp", "max_temp", "min_temp", "readings"]
    sql = text(
        f"INSERT INTO {cfg.GOLD_SCHEMA}.hourly_stats ({', '.join(cols)}) "
        f"VALUES (:city, :hour_start, :avg_temp, :max_temp, :min_temp, :readings) "
        f"ON CONFLICT (city, hour_start) DO UPDATE SET "
        f"  avg_temp = EXCLUDED.avg_temp, max_temp = EXCLUDED.max_temp, "
        f"  min_temp = EXCLUDED.min_temp, readings = EXCLUDED.readings"
    )
    with engine().begin() as conn:
        conn.execute(sql, _records(df, cols))


def upsert_daily(df):
    """Incremental upsert of gold.daily_stats for the (city, day) buckets given."""
    if df.empty:
        return
    cols = ["city", "day", "avg_temp", "max_temp", "min_temp", "readings", "heatwave"]
    sql = text(
        f"INSERT INTO {cfg.GOLD_SCHEMA}.daily_stats ({', '.join(cols)}) "
        f"VALUES (:city, :day, :avg_temp, :max_temp, :min_temp, :readings, :heatwave) "
        f"ON CONFLICT (city, day) DO UPDATE SET "
        f"  avg_temp = EXCLUDED.avg_temp, max_temp = EXCLUDED.max_temp, "
        f"  min_temp = EXCLUDED.min_temp, readings = EXCLUDED.readings, "
        f"  heatwave = EXCLUDED.heatwave"
    )
    with engine().begin() as conn:
        conn.execute(sql, _records(df, cols))