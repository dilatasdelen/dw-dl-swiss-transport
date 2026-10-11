"""
Ist-Daten processing: turn the large daily Ist-Daten CSV files into small Parquet files
that only contain our study area, with the delay already calculated.

The data:
- One CSV per operating day, about 550 MB, all of Switzerland plus trains abroad.
- One row per trip per stop, with the scheduled time (ANKUNFTSZEIT) and the
  actual or forecast time (AN_PROGNOSE). Delay = AN_PROGNOSE - ANKUNFTSZEIT.
- AN_PROGNOSE_STATUS tells how good that time is:
  REAL = measured, GESCHAETZT = estimated, PROGNOSE = forecast only, UNBEKANNT = no data.
- BPUIC is the station number, the same as "didok" in stops.txt and our elevation table.

What this script does, for each day file:
1. Read it in chunks (never the whole file at once, to save memory)
2. Keep only stations in our study area
3. Calculate arrival and departure delay in seconds
4. Save one Parquet file per day in the same folder layout the GTFS-RT data will use
"""

import os
import glob
import pandas as pd

RAW_DIR = "data/raw/istdaten"
# The elevation table already holds every station in our study area (one row per didok)
STATIONS_PATH = "data/raw/elevation/station_elevation.csv"
# Same layout for both sources, so they can be read as one table later:
# data/curated/stop_events/source=istdaten/date=2026-09-01/part.parquet
SAVE_DIR = "data/curated/stop_events/source=istdaten"

# Read 500,000 rows at a time, so a 550 MB file never has to fit in memory at once
CHUNK_SIZE = 500_000

# Only the columns we need (the operator names etc. are dropped)
COLUMNS = [
    "BETRIEBSTAG", "FAHRT_BEZEICHNER", "LINIEN_TEXT", "VERKEHRSMITTEL_TEXT",
    "ZUSATZFAHRT_TF", "FAELLT_AUS_TF", "BPUIC", "HALTESTELLEN_NAME",
    "ANKUNFTSZEIT", "AN_PROGNOSE", "AN_PROGNOSE_STATUS",
    "ABFAHRTSZEIT", "AB_PROGNOSE", "AB_PROGNOSE_STATUS", "DURCHFAHRT_TF",
]


def load_station_ids(stations_path=STATIONS_PATH):
    stations = pd.read_csv(stations_path, dtype={"didok": str})
    return set(stations["didok"])


def to_datetime(column):
    # Scheduled times look like "01.09.2026 06:41", forecasts like "01.09.2026 06:41:00".
    # dayfirst=True because the date is written day.month.year
    return pd.to_datetime(column, dayfirst=True, format="mixed", errors="coerce")


def clean_chunk(chunk, station_ids):
    # Keep only stations in our study area (BPUIC = didok)
    chunk = chunk[chunk["BPUIC"].isin(station_ids)]
    if chunk.empty:
        return chunk

    scheduled_arrival = to_datetime(chunk["ANKUNFTSZEIT"])
    scheduled_departure = to_datetime(chunk["ABFAHRTSZEIT"])

    # Delay in seconds = actual/forecast time - scheduled time.
    # Stays empty when there is no actual time (e.g. status UNBEKANNT)
    arrival_delay = (to_datetime(chunk["AN_PROGNOSE"]) - scheduled_arrival).dt.total_seconds()
    departure_delay = (to_datetime(chunk["AB_PROGNOSE"]) - scheduled_departure).dt.total_seconds()

    # Same column names the GTFS-RT data will get, so both sources fit in one table
    events = pd.DataFrame({
        "operating_date": to_datetime(chunk["BETRIEBSTAG"]).dt.date,
        "trip_id": chunk["FAHRT_BEZEICHNER"],
        "didok": chunk["BPUIC"],
        "station_name": chunk["HALTESTELLEN_NAME"],
        "line": chunk["LINIEN_TEXT"],
        "transport_type": chunk["VERKEHRSMITTEL_TEXT"],
        "scheduled_arrival": scheduled_arrival,
        "scheduled_departure": scheduled_departure,
        "arrival_delay_s": arrival_delay.round().astype("Int64"),
        "departure_delay_s": departure_delay.round().astype("Int64"),
        "arrival_status": chunk["AN_PROGNOSE_STATUS"],
        "departure_status": chunk["AB_PROGNOSE_STATUS"],
        "is_cancelled": chunk["FAELLT_AUS_TF"] == "true",
        "is_additional": chunk["ZUSATZFAHRT_TF"] == "true",
        "is_passing": chunk["DURCHFAHRT_TF"] == "true",
    })

    # Drop rows that tell us nothing: no delay at all and not cancelled
    has_delay = events["arrival_delay_s"].notna() | events["departure_delay_s"].notna()
    return events[has_delay | events["is_cancelled"]]


def process_file(filepath, station_ids, save_dir=SAVE_DIR):
    kept = []
    total_rows = 0

    # Ist-Daten uses ";" as separator. Everything is read as text first,
    # so station numbers and times are not changed by automatic type guessing
    for chunk in pd.read_csv(filepath, sep=";", usecols=COLUMNS, dtype=str,
                             encoding="utf-8", chunksize=CHUNK_SIZE):
        total_rows += len(chunk)
        kept.append(clean_chunk(chunk, station_ids))

    events = pd.concat(kept, ignore_index=True)

    # One Parquet file per operating day (a file normally holds exactly one day)
    for day, day_events in events.groupby("operating_date"):
        day_dir = os.path.join(save_dir, f"date={day}")
        os.makedirs(day_dir, exist_ok=True)
        day_events.to_parquet(os.path.join(day_dir, "part.parquet"), index=False)

    print(f"{filepath}: {total_rows} rows -> {len(events)} rows in study area")


def process_all(raw_dir=RAW_DIR, save_dir=SAVE_DIR):
    station_ids = load_station_ids()
    files = sorted(glob.glob(os.path.join(raw_dir, "*.csv")))
    if not files:
        raise RuntimeError(f"No Ist-Daten .csv files found in {raw_dir}")

    for filepath in files:
        # Skip days that are already processed, so the script can be stopped and restarted.
        # Ist-Daten file names start with the date, e.g. 2026-09-01_istdaten.csv
        day = os.path.basename(filepath)[:10]
        if os.path.exists(os.path.join(save_dir, f"date={day}", "part.parquet")):
            print(f"{filepath}: already processed, skipping")
            continue
        process_file(filepath, station_ids, save_dir)


if __name__ == "__main__":
    process_all()