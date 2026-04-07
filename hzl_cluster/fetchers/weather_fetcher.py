"""
Weather fetcher — pulls forecast from Open-Meteo API.
Free, no API key required. Saves JSON to staging.
"""
import json
import logging
import os
from datetime import datetime
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError

logger = logging.getLogger("hzl.fetcher.weather")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

def fetch_weather(
    staging_dir: str,
    latitude: float = 40.7128,   # NYC default
    longitude: float = -74.0060,
    days: int = 3,
    simulate: bool = False,
) -> dict:
    """
    Fetch weather forecast and save to staging directory.

    Returns: {"success": bool, "file": str or None, "summary": str}
    """
    os.makedirs(staging_dir, exist_ok=True)

    if simulate:
        # Return fake data for testing without network
        data = {
            "fetched_at": datetime.now().isoformat(),
            "latitude": latitude,
            "longitude": longitude,
            "current": {"temperature": 72.0, "unit": "fahrenheit", "condition": "Clear sky"},
            "daily": [
                {"date": "2026-04-07", "high": 78, "low": 55, "condition": "Sunny"},
                {"date": "2026-04-08", "high": 65, "low": 50, "condition": "Partly cloudy"},
                {"date": "2026-04-09", "high": 70, "low": 52, "condition": "Clear"},
            ],
        }
        outpath = os.path.join(staging_dir, "weather.json")
        with open(outpath, "w") as f:
            json.dump(data, f, indent=2)
        return {"success": True, "file": outpath, "summary": "72°F, Clear sky"}

    # Real fetch
    params = (
        f"?latitude={latitude}&longitude={longitude}"
        f"&daily=temperature_2m_max,temperature_2m_min,weathercode"
        f"&current=temperature_2m,weathercode"
        f"&temperature_unit=fahrenheit"
        f"&forecast_days={days}"
        f"&timezone=America%2FNew_York"
    )
    url = OPEN_METEO_URL + params

    try:
        req = Request(url, headers={"User-Agent": "HazelOS/1.0"})
        with urlopen(req, timeout=10) as resp:
            raw = json.loads(resp.read().decode())
    except (URLError, json.JSONDecodeError, OSError) as e:
        logger.error(f"Weather fetch failed: {e}")
        return {"success": False, "file": None, "summary": f"Fetch failed: {e}"}

    # Parse into clean format
    WMO_CODES = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Foggy", 48: "Depositing rime fog",
        51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
        61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
        71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
        80: "Slight rain showers", 81: "Moderate rain showers", 82: "Violent rain showers",
        95: "Thunderstorm", 96: "Thunderstorm with slight hail", 99: "Thunderstorm with heavy hail",
    }

    current_temp = raw.get("current", {}).get("temperature_2m", "?")
    current_code = raw.get("current", {}).get("weathercode", 0)
    current_condition = WMO_CODES.get(current_code, "Unknown")

    daily_data = []
    daily = raw.get("daily", {})
    dates = daily.get("time", [])
    highs = daily.get("temperature_2m_max", [])
    lows = daily.get("temperature_2m_min", [])
    codes = daily.get("weathercode", [])

    for i in range(min(len(dates), days)):
        daily_data.append({
            "date": dates[i],
            "high": highs[i] if i < len(highs) else None,
            "low": lows[i] if i < len(lows) else None,
            "condition": WMO_CODES.get(codes[i] if i < len(codes) else 0, "Unknown"),
        })

    data = {
        "fetched_at": datetime.now().isoformat(),
        "latitude": latitude,
        "longitude": longitude,
        "current": {
            "temperature": current_temp,
            "unit": "fahrenheit",
            "condition": current_condition,
        },
        "daily": daily_data,
    }

    outpath = os.path.join(staging_dir, "weather.json")
    with open(outpath, "w") as f:
        json.dump(data, f, indent=2)

    summary = f"{current_temp}°F, {current_condition}"
    logger.info(f"Weather fetched: {summary}")
    return {"success": True, "file": outpath, "summary": summary}
