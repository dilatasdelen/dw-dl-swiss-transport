"""Backfill hourly ERA5 weather for the GTFS study area into S3 Bronze."""

import json
import sys
from datetime import date, datetime, timedelta, timezone
from math import floor
from time import sleep
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"
START_UTC = date(2025, 11, 8)  # Includes midnight on November 9 in Switzerland.
END_UTC = date(2026, 11, 8)
WINDOW_DAYS = 14
UNITS = {
    "temperature_2m": "°C",
    "precipitation": "mm",
    "rain": "mm",
    "snowfall": "cm",
    "wind_speed_10m": "km/h",
    "weather_code": "wmo code",
}


def put_json(s3, bucket, key, data):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json",
    )


def load_stop_grid(bucket, version):
    """Map each study stop to the nearest native ERA5 cell centre."""
    path = f"s3://{bucket}/silver/reference/gtfs_static_stops/feed_version={version}/"
    stops = (
        SparkSession.builder.getOrCreate().read.parquet(path)
        .where("in_study_area = true")
        .select("stop_id", "stop_lat", "stop_lon")
        .distinct()
        .collect()
    )

    mapping = {}
    for stop in stops:
        lat, lon = stop.stop_lat, stop.stop_lon
        if lat is None or lon is None or not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError(f"Invalid coordinates for study stop {stop.stop_id}")
        cell = (floor(lat * 4 + 0.5) / 4, floor(lon * 4 + 0.5) / 4)
        if mapping.setdefault(stop.stop_id, cell) != cell:
            raise ValueError(f"Conflicting ERA5 cells for stop {stop.stop_id}")

    if not mapping:
        raise ValueError(f"No study stops found in {path}")
    return mapping


def fetch_window(lat, lon, start, end):
    """Verify the returned cell, UTC timestamps, units, and values."""
    request = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(UNITS),
        "timezone": "GMT",
        "models": "era5",
        "cell_selection": "nearest",
        "elevation": "nan",
    }
    url = f"{ENDPOINT}?{urlencode(request)}"
    with urlopen(url, timeout=60) as response:
        raw = response.read()

    data = json.loads(raw)
    hourly = data["hourly"]
    hours = ((end - start).days + 1) * 24
    first = datetime.combine(start, datetime.min.time())
    expected = [
        (first + timedelta(hours=i)).isoformat(timespec="minutes")
        for i in range(hours)
    ]

    if (data["latitude"], data["longitude"]) != (lat, lon):
        raise ValueError("Open-Meteo returned a different ERA5 cell")
    if data["utc_offset_seconds"] != 0 or hourly["time"] != expected:
        raise ValueError("Missing or unexpected UTC hours")

    for variable, unit in UNITS.items():
        values = hourly[variable]
        if (
            data["hourly_units"][variable].casefold() != unit.casefold()
            or len(values) != hours
            or None in values
        ):
            raise ValueError(f"Missing values or unexpected units for {variable}")

    return raw, {
        "status": "success",
        "data_product": "historical_reanalysis",
        "source_url": url,
        "request": request,
        "hourly_units": data["hourly_units"],
        "hour_count": hours,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def main():
    args = getResolvedOptions(sys.argv, ["DATA_BUCKET", "FEED_VERSION"])
    bucket, version = args["DATA_BUCKET"], args["FEED_VERSION"]
    s3 = boto3.client("s3")
    prefix = f"bronze/open_meteo/weather_historical/model=era5/feed_version={version}"
    mapping = load_stop_grid(bucket, version)
    cells = sorted(set(mapping.values()))

    # Keep the stop-to-cell lookup beside the raw responses.
    put_json(s3, bucket, f"{prefix}/stop_to_grid.json", mapping)

    # Reuse windows with both a response and a success manifest.
    existing = {
        obj["Key"]
        for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=f"{prefix}/"
        )
        for obj in page.get("Contents", [])
        if obj["Size"] > 0
    }

    ready_through = datetime.now(timezone.utc).date() - timedelta(days=6)
    saved = reused = deferred = 0
    failures = []
    rate_limited = False

    # Keep fixed window boundaries so later runs can reuse the same S3 keys.
    for offset in range(0, (END_UTC - START_UTC).days + 1, WINDOW_DAYS):
        start = START_UTC + timedelta(days=offset)
        end = min(start + timedelta(days=WINDOW_DAYS - 1), END_UTC)

        for lat, lon in cells:
            folder = f"{prefix}/grid={lat:.2f}_{lon:.2f}/start={start}/end={end}"
            raw_key, meta_key = f"{folder}/response.json", f"{folder}/ingestion.json"

            if raw_key in existing and meta_key in existing:
                reused += 1
                continue
            if end > ready_through or rate_limited:
                deferred += 1
                continue

            try:
                raw, metadata = fetch_window(lat, lon, start, end)
            except (URLError, TimeoutError, ValueError, KeyError) as error:
                failures.append({"window": folder, "error": str(error)})
                rate_limited = isinstance(error, HTTPError) and error.code == 429
            else:
                # Write the original bytes before publishing the success manifest.
                s3.put_object(
                    Bucket=bucket, Key=raw_key, Body=raw, ContentType="application/json"
                )
                put_json(s3, bucket, meta_key, metadata)
                saved += 1
            sleep(1)

    report = {
        "status": "failed" if failures else "partial" if deferred else "complete",
        "feed_version": version,
        "study_stops": len(mapping),
        "grid_cells": len(cells),
        "start_date_utc": START_UTC.isoformat(),
        "end_date_utc": END_UTC.isoformat(),
        "saved_windows": saved,
        "reused_windows": reused,
        "deferred_windows": deferred,
        "failures": failures,
    }
    report_key = f"quality/open_meteo_weather_historical/feed_version={version}/report.json"
    put_json(s3, bucket, report_key, report)
    print(json.dumps(report), flush=True)
    if failures:
        raise RuntimeError(f"Weather requests failed. See s3://{bucket}/{report_key}")


if __name__ == "__main__":
    main()