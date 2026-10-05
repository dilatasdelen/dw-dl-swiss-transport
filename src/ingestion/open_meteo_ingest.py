import os
import json
import requests
from datetime import datetime, timezone

API_URL = "https://api.open-meteo.com/v1/forecast"

# One point per canton covered by the project (canton capital / main city)
LOCATIONS = {
    "ZH": {"lat": 47.3769, "lon": 8.5417},   # Zuerich
    "ZG": {"lat": 47.1662, "lon": 8.5155},   # Zug
    "LU": {"lat": 47.0502, "lon": 8.3093},   # Luzern
    "SZ": {"lat": 47.0207, "lon": 8.6530},   # Schwyz
}

VARIABLES = "temperature_2m,precipitation,rain,snowfall,wind_speed_10m,weather_code"


def fetch_weather(save_dir="data/raw/weather"):
    os.makedirs(save_dir, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    results = {}

    for canton, coords in LOCATIONS.items():
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "current": VARIABLES,
            "timezone": "UTC",
        }
        response = requests.get(API_URL, params=params)
        response.raise_for_status()
        results[canton] = response.json()

    filepath = os.path.join(save_dir, f"weather_{timestamp}.json")
    with open(filepath, "w") as f:
        json.dump(results, f, indent=2)

    print(f"Saved weather for {len(results)} cantons to {filepath}")
    return filepath


if __name__ == "__main__":
    fetch_weather()
