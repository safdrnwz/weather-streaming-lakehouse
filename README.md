# 🌦️ Real-Time Weather Lakehouse (Kafka + PySpark + Medallion → Postgres)

A **streaming** medallion pipeline. Live weather for several Indian cities is
polled from the free **Open-Meteo** API, pushed through **Kafka**, processed by
**PySpark Structured Streaming**, and stored in a **Postgres** warehouse using
the **Bronze → Silver → Gold** pattern.

Everything runs **locally — no cloud**.

```
Open-Meteo (6 cities, every 60s)
      │  producer/weather_producer.py
      ▼
  Kafka topic  "weather-raw"            (Docker, KRaft mode, no Zookeeper)
      │  PySpark Structured Streaming (foreachBatch)
      ▼
  BRONZE   bronze.raw_messages          (raw JSON, append-only audit)
      ▼
  SILVER   silver.weather_readings      (parsed + validated, DATE-partitioned)
      ▼
  GOLD     gold.city_latest / hourly_stats / daily_stats
```

---

## 🧱 Why these tools (and not pandas)

Streaming is where the three engines work **together**, not as alternatives:

- **Kafka** = the streaming source (a continuous feed of weather messages).
- **PySpark Structured Streaming** = the consumer that reads Kafka in
  micro-batches, with checkpointing and exactly-once semantics.
- **Postgres** = the warehouse where clean results live for SQL queries.

pandas is **not** a streaming engine (no native Kafka, batch/in-memory only), so
it is the wrong tool to *drive* a stream. Here pandas appears only inside each
Spark micro-batch to apply the shared transform logic — fine because each batch
is tiny (a few cities per minute).

---

## 🗄️ Database design — date-first, city-indexed

Time-series data grows forever, so the silver table is **RANGE-partitioned by
`event_time` (DATE first)**. All cities for a day live inside that day's
partition, with a `(city, event_time)` index for fast per-city filters.

```
silver.weather_readings                 (parent, partitioned by DATE)
├── silver.weather_readings_2026_06_25   ← all cities for 25 Jun
├── silver.weather_readings_2026_06_26   ← all cities for 26 Jun
└── ...                                  (created automatically each day)
```

**Why date-first:** dropping old data = drop one partition; time-range queries
("last hour", "today") only scan relevant partitions; city is a tiny dimension
(6 values) so it belongs as a column + index, not a partition. The streaming job
creates each day's partition on demand (`common/db.py: ensure_silver_partition`).

Gold tables are keyed by their grain: `city_latest` (one row per city),
`hourly_stats` and `daily_stats` (keyed by `(city, time-bucket)`).

---

## 📁 Structure

```
weather_streaming_lakehouse/
├── .env.example
├── docker/docker-compose.yml        # local Kafka (KRaft, no Zookeeper)
├── producer/weather_producer.py     # Open-Meteo -> Kafka (every 60s)
├── pipeline/spark_streaming.py      # Kafka -> bronze/silver/gold (Structured Streaming)
├── warehouse/ddl.sql                # schemas + partitioned tables
├── common/
│   ├── config.py                    # env settings
│   ├── weather_rules.py             # cities, weather_code->condition, validation
│   ├── transforms.py                # PURE transforms (unit-tested)
│   └── db.py                        # engine, schema/partition helpers, upsert
├── tests/test_transforms.py         # unit tests (no infra needed)
├── requirements.txt
└── README.md
```

---

## 🚀 How to run (local, no cloud)

**Prerequisites:** Docker Desktop (for Kafka), Python 3.12, Java (for Spark), and
Postgres with a database `weather_warehouse` (`CREATE DATABASE weather_warehouse;`).

```bash
# 1) install deps
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt

# 2) settings
cp .env.example .env      # set PGPASSWORD

# 3) start Kafka (Docker)
docker compose -f docker/docker-compose.yml up -d

# 4) start the producer (terminal A) -- streams weather into Kafka
python producer/weather_producer.py

# 5) start the streaming pipeline (terminal B) -- Kafka -> bronze/silver/gold
python pipeline/spark_streaming.py
```

Then query results in Postgres:
```sql
SELECT * FROM gold.city_latest ORDER BY city;
SELECT * FROM gold.daily_stats WHERE heatwave;
SELECT tableoid::regclass AS partition, * FROM silver.weather_readings ORDER BY event_time DESC LIMIT 20;
```

> On the first run Spark downloads the Kafka connector jar from Maven, so keep
> internet ON the first time.

---

## ✅ What is tested

- **Pure transforms** (`tests/`): weather_code→condition, validation gate
  (drops bad readings), de-duplication, gold aggregations + heatwave flag.
  Run: `pytest -q`.
- **Database path**: DDL execution, automatic date-partition creation, idempotent
  upsert (`ON CONFLICT DO NOTHING`), and gold refresh — all verified against a
  real Postgres.
- **Lint**: `ruff check .` is clean.

**Not executed in this build environment** (need Docker + a running broker):
the Kafka producer and the Spark Structured Streaming job. The code follows the
standard patterns — run them on your machine and share any error.

---

## 🔑 Key streaming concepts shown

- Kafka producer/consumer, topics, message keys (city → consistent partitioning)
- Spark Structured Streaming with `foreachBatch` + `checkpointLocation`
- Incremental writes with idempotency (re-delivered messages don't duplicate)
- Medallion layers on a **live** source (vs a static batch)
- Time-partitioned warehouse modeling for time-series data
