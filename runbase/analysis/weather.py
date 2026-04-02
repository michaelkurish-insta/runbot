"""Fetch historical weather data from Open-Meteo API.

Uses the free Open-Meteo Historical Weather API (no API key required).
Returns temperature (°F), humidity (%), and conditions for a given
lat/lon/datetime.
"""

import json
import urllib.request
import urllib.error
from datetime import datetime

# WMO Weather interpretation codes
# https://open-meteo.com/en/docs
WMO_CODES = {
    0: "Clear",
    1: "Mainly Clear",
    2: "Partly Cloudy",
    3: "Cloudy",
    45: "Fog",
    48: "Rime Fog",
    51: "Light Drizzle",
    53: "Drizzle",
    55: "Heavy Drizzle",
    56: "Freezing Drizzle",
    57: "Heavy Freezing Drizzle",
    61: "Light Rain",
    63: "Rain",
    65: "Heavy Rain",
    66: "Freezing Rain",
    67: "Heavy Freezing Rain",
    71: "Light Snow",
    73: "Snow",
    75: "Heavy Snow",
    77: "Snow Grains",
    80: "Light Showers",
    81: "Showers",
    82: "Heavy Showers",
    85: "Light Snow Showers",
    86: "Heavy Snow Showers",
    95: "Thunderstorm",
    96: "Thunderstorm w/ Hail",
    99: "Heavy Thunderstorm w/ Hail",
}


def fetch_weather(lat: float, lon: float, dt: datetime,
                  verbose: bool = False) -> dict | None:
    """Fetch weather for a location and time from Open-Meteo.

    Args:
        lat: Latitude
        lon: Longitude
        dt: Datetime of the activity (used to pick the closest hour)
        verbose: Print debug info

    Returns:
        {"temperature_f": float, "humidity_pct": float, "weather_conditions": str}
        or None on failure.
    """
    date_str = dt.strftime("%Y-%m-%d")
    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat:.5f}&longitude={lon:.5f}"
        f"&start_date={date_str}&end_date={date_str}"
        f"&hourly=temperature_2m,relative_humidity_2m,weather_code"
        f"&temperature_unit=fahrenheit"
        f"&timezone=auto"
    )

    if verbose:
        print(f"    Weather API: {lat:.4f},{lon:.4f} on {date_str} hour={dt.hour}")

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError,
            OSError) as e:
        if verbose:
            print(f"    Weather fetch failed: {e}")
        return None

    hourly = data.get("hourly")
    if not hourly:
        if verbose:
            print("    Weather: no hourly data in response")
        return None

    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", [])
    humids = hourly.get("relative_humidity_2m", [])
    codes = hourly.get("weather_code", [])

    if not times:
        return None

    # Pick the hour closest to dt.hour
    target_hour = dt.hour
    best_idx = 0
    best_diff = 999
    for i, t in enumerate(times):
        # times are like "2025-01-15T14:00"
        try:
            h = int(t.split("T")[1].split(":")[0])
        except (IndexError, ValueError):
            continue
        diff = abs(h - target_hour)
        if diff < best_diff:
            best_diff = diff
            best_idx = i

    temp = temps[best_idx] if best_idx < len(temps) else None
    humid = humids[best_idx] if best_idx < len(humids) else None
    code = codes[best_idx] if best_idx < len(codes) else None

    if temp is None:
        return None

    conditions = WMO_CODES.get(code, f"Code {code}") if code is not None else ""

    result = {
        "temperature_f": round(temp, 1),
        "humidity_pct": round(humid, 1) if humid is not None else None,
        "weather_conditions": conditions,
    }

    if verbose:
        print(f"    Weather: {result['temperature_f']}°F  "
              f"{result['humidity_pct']}%  {result['weather_conditions']}")

    return result
