"""
common/config.py
================
Loads configuration from the project-root .env. Keeps Kafka, Open-Meteo, and
database settings in one place (no hardcoding).
"""

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

# --- Kafka ---
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather-raw")

# --- Open-Meteo ---
OPEN_METEO_URL = os.getenv("OPEN_METEO_URL", "https://api.open-meteo.com/v1/forecast")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))   # poll every minute
TIMEZONE = os.getenv("TIMEZONE", "Asia/Kolkata")

# --- Postgres warehouse ---
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = os.getenv("PGPORT", "5432")
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "postgres")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "weather_warehouse")

BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.getenv("SILVER_SCHEMA", "silver")
GOLD_SCHEMA = os.getenv("GOLD_SCHEMA", "gold")

# Spark streaming checkpoint dir (local; required for fault tolerance).
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", str(ROOT / "checkpoints"))
