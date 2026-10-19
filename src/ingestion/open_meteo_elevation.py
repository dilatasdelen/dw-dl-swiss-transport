import argparse
import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone

API_URL = "https://api.open-meteo.com/v1/elevation"
OUTPUT_PATH = "data/raw/elevation/station_elevation.csv"

BATCH_SIZE = 100

PAUSE_SECONDS = 80

# Reuse the point-in-polygon result from canton_join.py. The reference must
# contain one explicit GTFS Static version, not a directory of mixed versions.
CACHE_KEYS = [
    "didok", "parent_stop_id", "lat", "lon", "feed_version",
    "boundary_release", "boundary_crs", "study_canton_code",
]


def load_stations(canton_reference_path):
    """Load study-area parent stations from one polygon-assigned reference."""
    stops = pd.read_parquet(canton_reference_path)
    versions = stops["feed_version"]
    if versions.isna().any() or versions.nunique() != 1:
        raise ValueError("Select a canton reference containing exactly one feed_version.")

    selected = (
        pd.to_numeric(stops["location_type"], errors="coerce").eq(1)
        & stops["canton_join_status"].eq("matched")
        & stops["in_study_area"].eq(True)
    )
    stations = stops.loc[selected, [
        "didok", "stop_id", "stop_name", "stop_lat", "stop_lon",
        "feed_version", "boundary_release", "boundary_crs", "study_canton_code",
    ]].rename(columns={
        "stop_id": "parent_stop_id",
        "stop_name": "station_name",
        "stop_lat": "lat",
        "stop_lon": "lon",
    })

    if stations["didok"].isna().any():
        raise ValueError("Selected parent stations have missing didok identifiers.")
    for column in ("didok", "feed_version", "boundary_release"):
        stations[column] = stations[column].astype(str)
    return stations.reset_index(drop=True)


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


def fetch_station_elevation(canton_reference_path, output_path=OUTPUT_PATH):
    stations = load_stations(canton_reference_path)

    # If an earlier run stopped halfway, skip the stations that are already saved
    if os.path.exists(output_path):
        cached = pd.read_csv(output_path, dtype={
            "didok": str, "feed_version": str, "boundary_release": str,
        }, float_precision="round_trip")
        if not set(CACHE_KEYS).issubset(cached.columns):
            raise ValueError(
                "Existing elevation CSV predates the polygon reference. "
                "Preserve it separately and rebuild the default station list."
            )
        matches = cached.merge(
            stations[CACHE_KEYS], on=CACHE_KEYS, how="left",
            indicator=True, validate="many_to_one",
        )
        if not matches["_merge"].eq("both").all():
            raise ValueError(
                "Existing elevation CSV does not match this polygon reference "
                "and its station coordinates. Preserve it separately and rebuild."
            )
        done = cached["didok"]
        stations = stations[~stations["didok"].isin(done)].reset_index(drop=True)
        print(f"{len(done)} stations already saved, {len(stations)} left")

    print(f"Requesting elevation for {len(stations)} stations...")
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

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
    parser = argparse.ArgumentParser(
        description="Collect station elevation using the polygon-filtered study area."
    )
    parser.add_argument("--canton-reference", required=True,
                        help="Parquet file for one GTFS Static feed_version.")
    parser.add_argument("--output-path", default=OUTPUT_PATH)
    args = parser.parse_args()
    fetch_station_elevation(args.canton_reference, args.output_path)
