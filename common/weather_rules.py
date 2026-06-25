"""
common/weather_rules.py
=======================
Shared domain rules for the weather pipeline (single source of truth):
  - the list of cities we poll
  - WMO weather_code -> human-readable condition mapping
  - validation ranges for a "sane" reading

WHY a shared module: the producer, the streaming transforms, and the tests all
use the same constants/functions, so a rule is defined exactly once.
"""

# Cities to poll from Open-Meteo (name -> latitude, longitude).
# WHY several cities: more rows per minute and cross-city variety, even when a
#   single city's value changes slowly.
CITIES = {
    "Delhi": (28.61, 77.21),
    "Mumbai": (19.076, 72.877),
    "Bengaluru": (12.97, 77.59),
    "Chennai": (13.08, 80.27),
    "Kolkata": (22.57, 88.36),
    "Gurugram": (28.46, 77.03),
}


def code_to_condition(code) -> str:
    """Map a WMO weather code (Open-Meteo `weather_code`) to a readable bucket."""
    try:
        c = int(code)
    except (TypeError, ValueError):
        return "Unknown"
    if c == 0:
        return "Clear"
    if c in (1, 2, 3):
        return "Cloudy"
    if c in (45, 48):
        return "Fog"
    if c in (51, 53, 55, 56, 57):
        return "Drizzle"
    if c in (61, 63, 65, 66, 67):
        return "Rain"
    if c in (71, 73, 75, 77):
        return "Snow"
    if c in (80, 81, 82):
        return "Rain showers"
    if c in (85, 86):
        return "Snow showers"
    if c in (95, 96, 99):
        return "Thunderstorm"
    return "Unknown"


# Validation ranges -- a reading outside these is treated as bad data.
# WHY: data-quality gate. A temperature of 500 C or humidity of 250% is clearly
#   corrupt and must not pollute the gold aggregates.
TEMP_MIN_C, TEMP_MAX_C = -60.0, 60.0
HUMIDITY_MIN, HUMIDITY_MAX = 0, 100
WIND_MIN = 0.0


def is_valid_reading(temperature_c, humidity_pct, wind_speed_kmh) -> bool:
    """Return True only if all three measurements are within sane ranges."""
    try:
        t, h, w = float(temperature_c), float(humidity_pct), float(wind_speed_kmh)
    except (TypeError, ValueError):
        return False
    return (TEMP_MIN_C <= t <= TEMP_MAX_C
            and HUMIDITY_MIN <= h <= HUMIDITY_MAX
            and w >= WIND_MIN)
