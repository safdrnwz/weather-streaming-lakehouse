"""Unit tests for the pure weather transforms (no Kafka/Spark/DB)."""

from common import transforms
from common import weather_rules as wr


# --- weather_rules ---
def test_code_to_condition_known():
    assert wr.code_to_condition(0) == "Clear"
    assert wr.code_to_condition(3) == "Cloudy"
    assert wr.code_to_condition(61) == "Rain"
    assert wr.code_to_condition(95) == "Thunderstorm"


def test_code_to_condition_unknown():
    assert wr.code_to_condition(123) == "Unknown"
    assert wr.code_to_condition(None) == "Unknown"


def test_valid_reading_ranges():
    assert wr.is_valid_reading(37.6, 32, 2.5) is True
    assert wr.is_valid_reading(999, 80, 6) is False     # temp too high
    assert wr.is_valid_reading(33, 250, 8) is False      # humidity too high
    assert wr.is_valid_reading(30, 50, -1) is False      # negative wind


# --- silver transform ---
def test_silver_drops_invalid_and_dupes(raw_messages):
    out = transforms.transform_silver(raw_messages)
    # 6 raw -> drop 2 invalid (Chennai, Kolkata) -> 4; collapse 1 Gurugram dupe -> 3
    assert len(out) == 3
    assert set(out["city"]) == {"Gurugram", "Mumbai", "Delhi"}


def test_silver_adds_condition(raw_messages):
    out = transforms.transform_silver(raw_messages).set_index("city")
    assert out.loc["Gurugram", "condition"] == "Clear"
    assert out.loc["Mumbai", "condition"] == "Cloudy"


def test_silver_unique_key(raw_messages):
    out = transforms.transform_silver(raw_messages)
    assert not out.duplicated(subset=["city", "event_time"]).any()


# --- gold transforms ---
def test_gold_city_latest_one_row_per_city(raw_messages):
    silver = transforms.transform_silver(raw_messages)
    latest = transforms.build_gold_city_latest(silver)
    assert len(latest) == latest["city"].nunique() == 3


def test_gold_daily_heatwave_flag(raw_messages):
    silver = transforms.transform_silver(raw_messages)
    daily = transforms.build_gold_daily(silver).set_index("city")
    # Delhi 41.2 C -> heatwave True; Mumbai 31 C -> False
    assert bool(daily.loc["Delhi", "heatwave"]) is True
    assert bool(daily.loc["Mumbai", "heatwave"]) is False


def test_gold_hourly_buckets(raw_messages):
    silver = transforms.transform_silver(raw_messages)
    hourly = transforms.build_gold_hourly(silver)
    assert {"city", "hour_start", "avg_temp", "max_temp", "min_temp", "readings"} <= set(hourly.columns)


# --- scenarios (realtime app data generator) ---
def test_scenario_generates_one_reading_per_city():
    from common import scenarios
    from common import weather_rules as wr
    rows = scenarios.generate("normal")
    assert len(rows) == len(wr.CITIES)
    assert {"city", "event_time", "temperature_c", "weather_code"} <= set(rows[0])


def test_heatwave_is_hot_and_valid():
    import pandas as pd

    from common import scenarios, transforms
    rows = scenarios.generate("heatwave")
    df = transforms.transform_silver(pd.DataFrame(rows))
    # heatwave readings are valid and run hot (every reading above 30C)
    assert not df.empty
    assert (df["temperature_c"] > 30).all()
