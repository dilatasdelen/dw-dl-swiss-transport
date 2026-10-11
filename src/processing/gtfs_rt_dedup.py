"""
GTFS-RT processing: keep only the settled (newest) delay per stop event and save it
in the same layout and with the same columns as the Ist-Daten history.

The data:
- Each .pb file is one snapshot: every predicted delay in Switzerland at one moment.
- A "stop event" is one trip at one stop on one day, e.g. the Basel -> Lugano train
  (trip_id) on 2026-10-10 (start_date) at Luzern (stop_sequence 3).
- We poll every 60 seconds, so the same stop event appears in many snapshots with a
  changing prediction (648 s -> 660 s -> 700 s). Only the newest one is kept, the
  older predictions are never saved.
- RT only gives the delay, not the scheduled time. The scheduled time comes from the
  GTFS Static timetable (stop_times.txt), the station from stops.txt, the line from routes.txt.

The result is written next to the Ist-Daten files:
data/curated/stop_events/source=gtfs_rt/date=2026-10-10/part.parquet
"""

import os
import glob
import pandas as pd
from google.transit import gtfs_realtime_pb2  # decodes the binary .pb files

RAW_DIR = "data/raw/gtfs_rt"
# Folder with the GTFS Static timetable files (stops.txt, stop_times.txt, routes.txt)
GTFS_DIR = "data/raw"
# Same list of study-area stations the Ist-Daten script uses
STATIONS_PATH = "data/raw/elevation/station_elevation.csv"
SAVE_DIR = "data/curated/stop_events/source=gtfs_rt"

# stop_times.txt is large, so it is read in chunks like the Ist-Daten files
CHUNK_SIZE = 1_000_000

# The feed stores statuses as numbers (e.g. 3 = cancelled)
TRIP_STATUS = {0: "SCHEDULED", 1: "ADDED", 2: "UNSCHEDULED", 3: "CANCELED"}
STOP_STATUS = {0: "SCHEDULED", 1: "SKIPPED", 2: "NO_DATA"}


def read_snapshot(filepath):
    """Open ONE .pb snapshot and turn it into a list of rows (one row per trip per stop)."""
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(filepath, "rb") as f:
        feed.ParseFromString(f.read())

    rows = []
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        trip = entity.trip_update.trip

        for stop in entity.trip_update.stop_time_update:
            rows.append({
                "trip_id": trip.trip_id,
                "start_date": trip.start_date,
                "stop_sequence": stop.stop_sequence,
                "route_id": trip.route_id,
                "stop_id": stop.stop_id,
                "trip_status": TRIP_STATUS.get(trip.schedule_relationship),
                "stop_status": STOP_STATUS.get(stop.schedule_relationship),
                # Empty (None) when missing, because 0 would wrongly mean "on time"
                "arrival_delay_s": stop.arrival.delay if stop.arrival.HasField("delay") else None,
                "departure_delay_s": stop.departure.delay if stop.departure.HasField("delay") else None,
            })

    return rows


def settle(files):
    """Read all snapshots from oldest to newest and keep only the newest row per stop event."""

    # One "drawer" per stop event: (trip, day, stop number).
    # A newer snapshot overwrites the older row in the same drawer,
    # so only the settled value is left and old predictions are never stored
    latest = {}
    for filepath in files:
        for row in read_snapshot(filepath):
            key = (row["trip_id"], row["start_date"], row["stop_sequence"])
            latest[key] = row
        print(f"Read {filepath} ({len(latest)} unique stop events so far)")

    return pd.DataFrame(latest.values())


def read_gtfs(name, **kwargs):
    """Read one GTFS Static file (e.g. "stops.txt"). Everything as text, BOM removed."""
    return pd.read_csv(os.path.join(GTFS_DIR, name), dtype=str, encoding="utf-8-sig", **kwargs)


def add_stations(events):
    """stop_id (platform) -> didok (station) and station name, keep only the study area."""
    stops = read_gtfs("stops.txt", usecols=["stop_id", "stop_name", "didok"])
    events = events.merge(stops, on="stop_id", how="left")

    study_area = set(pd.read_csv(STATIONS_PATH, dtype={"didok": str})["didok"])
    return events[events["didok"].isin(study_area)]


def add_scheduled_times(events):
    """Look up the planned arrival and departure in stop_times.txt."""
    needed_trips = set(events["trip_id"])
    parts = []

    # Only keep the timetable rows of trips we actually have, chunk by chunk
    for chunk in read_gtfs("stop_times.txt", chunksize=CHUNK_SIZE,
                           usecols=["trip_id", "stop_sequence", "arrival_time", "departure_time"]):
        parts.append(chunk[chunk["trip_id"].isin(needed_trips)])

    stop_times = pd.concat(parts, ignore_index=True)
    stop_times["stop_sequence"] = stop_times["stop_sequence"].astype(int)
    events = events.merge(stop_times, on=["trip_id", "stop_sequence"], how="left")

    # GTFS times are counted from midnight of the operating day and can go past 24:00
    # (e.g. "25:10:00" = 01:10 the next morning), so they are added as a duration
    day = pd.to_datetime(events["start_date"], format="%Y%m%d")
    for col, gtfs_col in [("scheduled_arrival", "arrival_time"), ("scheduled_departure", "departure_time")]:
        events[col] = day + pd.to_timedelta(events[gtfs_col].replace("", None), errors="coerce")

    return events


def add_lines(events):
    """route_id -> line name (e.g. IR, S5) and transport type, from routes.txt."""
    routes = read_gtfs("routes.txt", usecols=["route_id", "route_short_name", "route_desc"])
    routes = routes.rename(columns={"route_short_name": "line", "route_desc": "transport_type"})
    return events.merge(routes, on="route_id", how="left")


def to_stop_events(events):
    """Same columns, in the same order, as the Ist-Daten Parquet files."""
    return pd.DataFrame({
        "operating_date": pd.to_datetime(events["start_date"], format="%Y%m%d").dt.date,
        "trip_id": events["trip_id"],
        "didok": events["didok"],
        "station_name": events["stop_name"],
        "line": events["line"],
        "transport_type": events["transport_type"],
        "scheduled_arrival": events["scheduled_arrival"],
        "scheduled_departure": events["scheduled_departure"],
        "arrival_delay_s": events["arrival_delay_s"].astype("Int64"),
        "departure_delay_s": events["departure_delay_s"].astype("Int64"),
        # RT_FINAL = the last prediction we captured (Ist-Daten uses REAL, PROGNOSE ...)
        "arrival_status": events["arrival_delay_s"].notna().map({True: "RT_FINAL", False: None}),
        "departure_status": events["departure_delay_s"].notna().map({True: "RT_FINAL", False: None}),
        "is_cancelled": (events["trip_status"] == "CANCELED") | (events["stop_status"] == "SKIPPED"),
        "is_additional": events["trip_status"] == "ADDED",
        "is_passing": False,  # not available in GTFS-RT
        # No "source" column: the folder name source=gtfs_rt already says it
    })


def process(raw_dir=RAW_DIR, save_dir=SAVE_DIR):
    # File names contain the capture time, so sorting puts the snapshots in time order
    files = sorted(glob.glob(os.path.join(raw_dir, "*.pb")))
    if not files:
        raise RuntimeError(f"No .pb snapshots found in {raw_dir}")

    events = settle(files)
    events = add_stations(events)
    events = add_scheduled_times(events)
    events = add_lines(events)
    events = to_stop_events(events)

    # One Parquet file per operating day, same layout as Ist-Daten.
    # Running the script again with more snapshots simply replaces the day's file
    for day, day_events in events.groupby("operating_date"):
        day_dir = os.path.join(save_dir, f"date={day}")
        os.makedirs(day_dir, exist_ok=True)
        day_events.to_parquet(os.path.join(day_dir, "part.parquet"), index=False)
        print(f"Saved {len(day_events)} settled stop events to {day_dir}")


if __name__ == "__main__":
    process()