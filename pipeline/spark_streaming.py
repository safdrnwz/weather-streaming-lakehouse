"""
pipeline/spark_streaming.py
===========================
The streaming medallion job (PySpark Structured Streaming):

    Kafka topic "weather-raw"
        -> BRONZE  (raw messages appended to bronze.raw_messages)
        -> SILVER  (parsed/validated rows -> silver.weather_readings, date-partitioned)
        -> GOLD    (city_latest / hourly_stats / daily_stats refreshed from silver)

HOW IT WORKS
  Spark reads Kafka as a streaming source. For each micro-batch we use
  `foreachBatch`: the batch is small (a handful of cities per minute), so we
  convert it to pandas and reuse the SAME tested pure transforms from
  common/transforms.py, then write to Postgres. This keeps one tested code path
  in production. At higher volume you would push the transforms into Spark SQL.

RUN (after Kafka is up and the producer is running):
    python pipeline/spark_streaming.py
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from common import config as cfg  # noqa: E402
from common import db as dbcfg  # noqa: E402
from common import transforms  # noqa: E402

# Spark needs the Kafka source connector (auto-downloaded on first run).
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


def get_spark() -> SparkSession:
    import os
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder
        .appName("weather-streaming-medallion")
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("FATAL")
    return spark


def process_batch(batch_df, batch_id: int):
    """Called by Spark for every micro-batch. batch_df has a 'value' string col."""
    # Collect the small batch to the driver as a list of JSON strings.
    rows = [r["value"] for r in batch_df.select(F.col("value").cast("string")).collect()]
    if not rows:
        return

    messages = []
    for raw in rows:
        try:
            messages.append(json.loads(raw))
        except json.JSONDecodeError:
            continue  # skip malformed message

    # --- BRONZE: store raw messages as-is (audit trail) ---
    with dbcfg.engine().begin() as conn:
        from sqlalchemy import text
        conn.execute(
            text(f"INSERT INTO {cfg.BRONZE_SCHEMA}.raw_messages (raw_json, ingested_at) "
                 f"VALUES (:j, now())"),
            [{"j": json.dumps(m)} for m in messages],
        )

    # --- SILVER: clean + validate using the shared pure transform ---
    raw_df = pd.DataFrame(messages)
    silver_df = transforms.transform_silver(raw_df)
    if not silver_df.empty:
        # ensure each day's partition exists, then upsert
        for day in sorted({ts.date() for ts in silver_df["event_time"]}):
            dbcfg.ensure_silver_partition(day)
        dbcfg.upsert_silver(silver_df)

    # --- GOLD: INCREMENTAL -- only recompute the buckets this batch touched ---
    if not silver_df.empty:
        update_gold_incremental(silver_df)

    print(f"[stream] batch {batch_id}: {len(messages)} msgs -> "
          f"{len(silver_df)} silver rows ({datetime.now().strftime('%H:%M:%S')})")


def update_gold_incremental(batch_silver):
    """Recompute only the (city, day) buckets touched by this batch, then upsert.

    WHY incremental: instead of rebuilding the whole gold table every batch, we
    take the cities + days present in the batch, re-read just those rows from
    silver (a bucket's stats need all its rows), recompute, and MERGE the result
    into gold. Untouched gold rows stay exactly as they were.
    """
    cities = sorted(batch_silver["city"].unique())
    days = sorted({ts.date() for ts in batch_silver["event_time"]})

    # Fetch the full silver rows for the touched cities+days (correct aggregates).
    sub = dbcfg.read_silver_for_keys(cities, days)
    if sub.empty:
        return

    dbcfg.upsert_city_latest(transforms.build_gold_city_latest(sub))
    dbcfg.upsert_hourly(transforms.build_gold_hourly(sub))
    dbcfg.upsert_daily(transforms.build_gold_daily(sub))


def main():
    # Make sure schemas + parent tables exist before streaming starts.
    dbcfg.ensure_schemas_and_tables()

    spark = get_spark()
    # Read the Kafka topic as a streaming source.
    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.KAFKA_BOOTSTRAP)
        .option("subscribe", cfg.KAFKA_TOPIC)
        .option("startingOffsets", "earliest")   # read from the start of the topic
        .option("failOnDataLoss", "false")        # do not crash if the topic is new/recreated
        .load()
    )

    query = (
        stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", cfg.CHECKPOINT_DIR)  # fault tolerance
        .start()
    )
    print("[stream] running -- Ctrl+C to stop")
    query.awaitTermination()


if __name__ == "__main__":
    main()
