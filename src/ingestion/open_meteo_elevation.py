import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone

API_URL = "https://api.open-meteo.com/v1/elevation"
STOPS_PATH = "data/raw/stops.txt"
OUTPUT_PATH = "data/raw/elevation/station_elevation.csv"

BATCH_SIZE = 100

PAUSE_SECONDS = 80

# Rough bounding box around ZH, ZG, LU and SZ, so we don't request elevation
# for all of Switzerland. The exact canton filter is done later in canton_join.py
BBOX = {"lat_min": 46.75, "lat_max": 47.70, "lon_min": 7.83, "lon_max": 9.03}


def load_stations(stops_path=STOPS_PATH):
    # stops.txt starts with a byte order mark, so utf-8-sig keeps "stop_id" readable
    stops = pd.read_csv(stops_path, dtype={"didok": str}, encoding="utf-8-sig")

    # Every platform points to a parent station row (location_type = 1) with the
    # same didok number, so the parent rows are already one row per physical station, so we keep the rows,
    # which only has the location_type = 1, meaning a main station without any platforms
    stations = stops[stops["location_type"] == 1]
    stations = stations[["didok", "stop_id", "stop_name", "stop_lat", "stop_lon"]].rename(columns={
        "stop_id": "parent_stop_id",
        "stop_name": "station_name",
        "stop_lat": "lat",
        "stop_lon": "lon",
    })

    # limiting it to Zurich, Zug, Luzern and Schwyz
    in_bbox = (
        stations["lat"].between(BBOX["lat_min"], BBOX["lat_max"])
        & stations["lon"].between(BBOX["lon_min"], BBOX["lon_max"])
    )
    
    return stations[in_bbox].reset_index(drop=True)


def fetch_elevation(lats, lons, max_retries=3):
    params = {
        "latitude": ",".join(f"{lat:.5f}" for lat in lats),
        "longitude": ",".join(f"{lon:.5f}" for lon in lons),
    }

    for attempt in range(max_retries + 1):
        try:
            response = requests.get(API_URL, params=params, timeout=30)
        except requests.exceptions.ConnectionError:
            # Too many requests: wait a minute and try the same batch again
            if attempt == max_retries:
                raise
            print("Connection dropped, waiting 2 minutes...")
            time.sleep(120)
            continue
         # Too many requests: wait and try the same batch again
        if response.status_code == 429 and attempt < max_retries:
            print("Rate limited by Open-Meteo, waiting 2 minutes...")
            time.sleep(120)
            continue

        response.raise_for_status()
        return response.json()["elevation"]


def fetch_station_elevation(stops_path=STOPS_PATH, output_path=OUTPUT_PATH):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    stations = load_stations(stops_path)

    # If an earlier run stopped halfway, skip the stations that are already saved
    if os.path.exists(output_path):
        done = pd.read_csv(output_path, dtype={"didok": str})["didok"]
        stations = stations[~stations["didok"].isin(done)].reset_index(drop=True)
        print(f"{len(done)} stations already saved, {len(stations)} left")

    print(f"Requesting elevation for {len(stations)} stations...")

    for start in range(0, len(stations), BATCH_SIZE):
        batch = stations.iloc[start:start + BATCH_SIZE].copy()
        batch["elevation_m"] = fetch_elevation(batch["lat"], batch["lon"])
        batch["retrieved_at"] = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

        # Append each batch right away, so progress is kept if the script stops
        write_header = not os.path.exists(output_path)
        batch.to_csv(output_path, mode="a", header=write_header, index=False)
        print(f"Saved {start + len(batch)}/{len(stations)} stations")

        if start + BATCH_SIZE < len(stations):
            time.sleep(PAUSE_SECONDS)

    print(f"Done, elevation is saved in {output_path}")
    return output_path


if __name__ == "__main__":
    fetch_station_elevation()