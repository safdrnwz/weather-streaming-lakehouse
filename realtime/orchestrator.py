"""
realtime/orchestrator.py
========================
The real-time medallion app. Three independent stages run on their own
schedules (a scheduler, not a single stream, is the right tool for per-layer
cadences):

    API / scenario  --(every 10s)-->  BRONZE   (raw, append-only)
    BRONZE          --(every 30s)-->  SILVER   (clean, validated, date-partitioned)
    SILVER          --(every 60s)-->  GOLD     (current per city + hourly/daily, incremental)

Run it with:
    python realtime/orchestrator.py

Set the data source + scenario via .env (SOURCE_MODE, SCENARIO) or env vars:
    SOURCE_MODE=scenario SCENARIO=heatwave python realtime/orchestrator.py
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import config as cfg  # noqa: E402
from common import db as dbcfg  # noqa: E402
from common import scenarios, transforms  # noqa: E402


def _now():
    return datetime.now().strftime("%H:%M:%S")


# --- STAGE 1: API / scenario -> bronze (every 10s) ---------------------------
def ingest_bronze():
    """Fetch readings and append them to bronze (raw, every message kept)."""
    if cfg.SOURCE_MODE == "api":
        messages = _fetch_from_api()
    else:
        messages = scenarios.generate(cfg.SCENARIO)
    dbcfg.insert_bronze(messages)
    print(f"[{_now()}] BRONZE  +{len(messages)} raw rows "
          f"(source={cfg.SOURCE_MODE}/{cfg.SCENARIO})")


def _fetch_from_api():
    """Real Open-Meteo fetch for each city (used when SOURCE_MODE=api)."""
    import requests

    from common import weather_rules as wr
    out = []
    for name, (lat, lon) in wr.CITIES.items():
        try:
            r = requests.get(cfg.OPEN_METEO_URL, timeout=10, params={
                "latitude": lat, "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code",
                "timezone": cfg.TIMEZONE,
            })
            cur = r.json()["current"]
            out.append({
                "city": name, "event_time": cur["time"],
                "temperature_c": cur.get("temperature_2m"),
                "humidity_pct": cur.get("relative_humidity_2m"),
                "wind_speed_kmh": cur.get("wind_speed_10m"),
                "weather_code": cur.get("weather_code"),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as exc:  # noqa: BLE001
            print(f"[{_now()}] API warn {name}: {exc}")
    return out


# --- STAGE 2: bronze -> silver (every 30s) -----------------------------------
def bronze_to_silver():
    """Read unprocessed bronze rows, clean them, write silver, mark processed."""
    ids, messages = dbcfg.fetch_unprocessed_bronze()
    if not messages:
        print(f"[{_now()}] SILVER  (nothing new)")
        return
    silver_df = transforms.transform_silver(pd.DataFrame(messages))
    if not silver_df.empty:
        for day in sorted({ts.date() for ts in silver_df["event_time"]}):
            dbcfg.ensure_silver_partition(day)
        dbcfg.upsert_silver(silver_df)
    dbcfg.mark_bronze_processed(ids)
    print(f"[{_now()}] SILVER  {len(messages)} bronze -> {len(silver_df)} clean rows")


# --- STAGE 3: silver -> gold (every 60s) -------------------------------------
def silver_to_gold():
    """Recompute today's gold aggregates from silver and upsert (incremental)."""
    today = datetime.now(timezone.utc).date()
    cities = list(_distinct_cities_today(today))
    if not cities:
        print(f"[{_now()}] GOLD    (no silver yet)")
        return
    sub = dbcfg.read_silver_for_keys(cities, [today])
    if sub.empty:
        return
    dbcfg.upsert_city_latest(transforms.build_gold_city_latest(sub))
    dbcfg.upsert_hourly(transforms.build_gold_hourly(sub))
    dbcfg.upsert_daily(transforms.build_gold_daily(sub))
    print(f"[{_now()}] GOLD    refreshed for {len(cities)} cities")


def _distinct_cities_today(day):
    from sqlalchemy import text
    with dbcfg.engine().begin() as conn:
        rows = conn.execute(
            text(f"SELECT DISTINCT city FROM {cfg.SILVER_SCHEMA}.weather_readings "
                 f"WHERE event_time::date = :d"),
            {"d": str(day)},
        ).fetchall()
    return {r[0] for r in rows}


def main():
    dbcfg.ensure_schemas_and_tables()
    print("=" * 60)
    print("REALTIME MEDALLION APP")
    print(f"  bronze every {cfg.BRONZE_EVERY_SECONDS}s | "
          f"silver every {cfg.SILVER_EVERY_SECONDS}s | "
          f"gold every {cfg.GOLD_EVERY_SECONDS}s")
    print(f"  source = {cfg.SOURCE_MODE}  scenario = {cfg.SCENARIO}")
    print("  Ctrl+C to stop")
    print("=" * 60)

    sched = BlockingScheduler(timezone="UTC")
    sched.add_job(ingest_bronze, "interval", seconds=cfg.BRONZE_EVERY_SECONDS, id="bronze")
    sched.add_job(bronze_to_silver, "interval", seconds=cfg.SILVER_EVERY_SECONDS, id="silver")
    sched.add_job(silver_to_gold, "interval", seconds=cfg.GOLD_EVERY_SECONDS, id="gold")

    ingest_bronze()  # run one bronze tick immediately so data starts flowing
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        print("\n[stopped]")


if __name__ == "__main__":
    main()
