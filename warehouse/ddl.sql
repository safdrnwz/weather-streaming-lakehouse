-- =============================================================================
-- warehouse/ddl.sql -- weather_warehouse schema (bronze / silver / gold)
-- =============================================================================
-- Design: time-series data, so the silver table is RANGE-partitioned by
-- event_time (DATE first). All cities for a day live inside that day's
-- partition; a (city, event_time) index makes per-city filters fast.
-- The streaming job creates each day's partition on demand (see common/db.py).
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- BRONZE: raw readings, stored as-is (append-only). `processed` marks rows the
-- silver job has already consumed (so bronze->silver runs incrementally).
CREATE TABLE IF NOT EXISTS bronze.raw_messages (
    id          bigserial PRIMARY KEY,
    raw_json    jsonb       NOT NULL,     -- the full message exactly as received
    processed   boolean     NOT NULL DEFAULT false,
    ingested_at timestamptz NOT NULL DEFAULT now()
);

-- SILVER: one clean row per (city, event_time). Partitioned by DATE.
CREATE TABLE IF NOT EXISTS silver.weather_readings (
    city            text        NOT NULL,
    event_time      timestamptz NOT NULL,
    temperature_c   numeric,
    humidity_pct    int,
    wind_speed_kmh  numeric,
    weather_code    int,
    condition       text,
    ingested_at     timestamptz NOT NULL,
    PRIMARY KEY (city, event_time)
) PARTITION BY RANGE (event_time);

CREATE INDEX IF NOT EXISTS idx_silver_city_time
    ON silver.weather_readings (city, event_time);

-- GOLD: latest reading per city (one row per city).
CREATE TABLE IF NOT EXISTS gold.city_latest (
    city          text PRIMARY KEY,
    event_time    timestamptz,
    temperature_c numeric,
    humidity_pct  int,
    condition     text
);

-- GOLD: per (city, hour) rolling stats.
CREATE TABLE IF NOT EXISTS gold.hourly_stats (
    city       text,
    hour_start timestamptz,
    avg_temp   numeric,
    max_temp   numeric,
    min_temp   numeric,
    readings   int,
    PRIMARY KEY (city, hour_start)
);

-- GOLD: per (city, date) stats + heatwave flag.
CREATE TABLE IF NOT EXISTS gold.daily_stats (
    city     text,
    day      date,
    avg_temp numeric,
    max_temp numeric,
    min_temp numeric,
    readings int,
    heatwave boolean,
    PRIMARY KEY (city, day)
);
