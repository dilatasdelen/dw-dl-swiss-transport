"""
    bronze/open_meteo/weather/feed_version=<V>/date=<D>/hour=<H>/grid=<LAT>_<LON>/response.json
    bronze/open_meteo/weather_historical/model=era5/feed_version=<V>/grid=<LAT>_<LON>/
        start=<D>/end=<D>/response.json + ingestion.json
    bronze/open_meteo/weather_historical/model=era5/feed_version=<V>/stop_to_grid.json
Outputs:
    silver/open_meteo_weather/data_product=forecast/issued_date=<D>/issued_hour=<H>/part.parquet
    silver/open_meteo_weather/data_product=historical_reanalysis/window_start=<D>/part.parquet
    silver/open_meteo_weather/stop_to_location/feed_version=<V>/part.parquet
    quality/open_meteo_weather/<same partition>/report.json   (written last = "processed")

Each run converts only Bronze units that are complete and have no report yet:
- a forecast hour when all grid cells are present, or when it is older than
  FORECAST_GRACE_HOURS (then the missing cells are reported);
- a historical window when all grid cells have a response and a success manifest.
Incomplete historical windows (e.g. ERA5 publication delay) are picked up by a later run.

Weather is stored per grid location (one row per location and valid hour; forecasts keep
one row per issued snapshot so revisions stay traceable). Stops reach their location through
the stop_to_location table.
"""

import io
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from time import perf_counter


BRONZE_FORECAST = "bronze/open_meteo/weather"
BRONZE_HISTORICAL = "bronze/open_meteo/weather_historical/model=era5"
SILVER_PREFIX = "silver/open_meteo_weather"
QUALITY_PREFIX = "quality/open_meteo_weather"
FORECAST_GRACE_HOURS = 3

# Open-Meteo variable -> (Silver column, expected unit)
VARIABLES = {
    "temperature_2m": ("temperature_2m_c", "°C"),
    "precipitation": ("precipitation_mm", "mm"),
    "rain": ("rain_mm", "mm"),
    "snowfall": ("snowfall_cm", "cm"),
    "wind_speed_10m": ("wind_speed_10m_kmh", "km/h"),
    "weather_code": ("weather_code", "wmo code"),
}
WEATHER_COLUMNS = [
    ("location_id", "string"), ("latitude", "double"), ("longitude", "double"),
    ("weather_time_utc", "timestamp"), ("temperature_2m_c", "double"),
    ("precipitation_mm", "double"), ("rain_mm", "double"), ("snowfall_cm", "double"),
    ("wind_speed_10m_kmh", "double"), ("weather_code", "int64"),
    ("data_product", "string"), ("model", "string"), ("issued_hour_utc", "timestamp"),
    ("lead_hours", "int64"), ("retrieved_at_utc", "timestamp"), ("feed_version", "string"),
    ("bronze_object_key", "string"), ("processed_at_utc", "timestamp"),
]
GRID = re.compile(r"grid=(-?\d+\.\d+)_(-?\d+\.\d+)/")
FORECAST_KEY = re.compile(r"date=(\d{4}-\d{2}-\d{2})/hour=(\d{2})/grid=[^/]+/response\.json$")
HISTORICAL_KEY = re.compile(r"grid=[^/]+/start=(\d{4}-\d{2}-\d{2})/end=(\d{4}-\d{2}-\d{2})/"
                            r"(response|ingestion)\.json$")


def location_id(lat, lon):
    return f"{float(lat):.2f}_{float(lon):.2f}"


def parse_time(value):
    """Open-Meteo returns naive GMT times like 2026-11-09T19:00."""
    return datetime.fromisoformat(value).replace(tzinfo=None)


def parse_response(raw, lat, lon, base):
    """One Open-Meteo JSON -> list of row dicts. Returns (rows, problems)."""
    data = json.loads(raw)
    problems = []
    if data.get("utc_offset_seconds") != 0:
        problems.append("not_utc")
    units, hourly = data.get("hourly_units", {}), data.get("hourly", {})
    times = hourly.get("time") or []
    for variable, (_, unit) in VARIABLES.items():
        if (units.get(variable) or "").casefold() != unit.casefold():
            problems.append(f"unit_{variable}")
        if len(hourly.get(variable) or []) != len(times):
            problems.append(f"length_{variable}")
    if problems or not times:
        return [], problems or ["no_hours"]

    rows = []
    for i, value in enumerate(times):
        row = dict(base, location_id=location_id(lat, lon), latitude=float(lat),
                   longitude=float(lon), weather_time_utc=parse_time(value))
        for variable, (column, _) in VARIABLES.items():
            row[column] = hourly[variable][i]
        if row["weather_code"] is not None:
            row["weather_code"] = int(row["weather_code"])
        if row["issued_hour_utc"] is not None:
            row["lead_hours"] = int((row["weather_time_utc"] - row["issued_hour_utc"]).total_seconds() // 3600)
        rows.append(row)
    return rows, []


def to_table(rows):
    import pyarrow as pa

    types = {"string": pa.string(), "double": pa.float64(), "int64": pa.int64(),
             "timestamp": pa.timestamp("us")}
    schema = pa.schema([(name, types[kind]) for name, kind in WEATHER_COLUMNS])
    return pa.Table.from_pylist([{name: r.get(name) for name, _ in WEATHER_COLUMNS} for r in rows],
                                schema=schema)


def check_unique(rows, key_fields):
    seen, duplicates = set(), 0
    for row in rows:
        key = tuple(row[f] for f in key_fields)
        duplicates += key in seen
        seen.add(key)
    return duplicates


def main():
    import boto3
    import pyarrow.parquet as pq
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATA_BUCKET", "FEED_VERSION"])
    bucket, version = args["DATA_BUCKET"], args["FEED_VERSION"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", version):
        raise ValueError("Unsupported FEED_VERSION label")
    s3 = boto3.client("s3")
    started = perf_counter()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    processed_at = now

    def keys(prefix):
        return [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix) for o in page.get("Contents", [])]

    def read(key):
        obj = s3.get_object(Bucket=bucket, Key=key)
        return obj["Body"].read(), obj.get("Metadata", {})

    def write(partition, rows, report):
        """Data first, report last: the report marks the partition as processed."""
        if rows:
            buffer = io.BytesIO()
            pq.write_table(to_table(rows), buffer, compression="snappy")
            s3.put_object(Bucket=bucket, Key=f"{SILVER_PREFIX}/{partition}/part.parquet",
                          Body=buffer.getvalue(), ServerSideEncryption="AES256")
        s3.put_object(Bucket=bucket, Key=f"{QUALITY_PREFIX}/{partition}/report.json",
                      ContentType="application/json", ServerSideEncryption="AES256",
                      Body=(json.dumps(report, indent=2, default=str) + "\n").encode())

    done = {k[len(QUALITY_PREFIX) + 1:-len("/report.json")]
            for k in keys(f"{QUALITY_PREFIX}/") if k.endswith("/report.json")}

    # 1. Stop -> weather location mapping (once per feed version)
    historical_root = f"{BRONZE_HISTORICAL}/feed_version={version}"
    stop_to_grid = json.loads(read(f"{historical_root}/stop_to_grid.json")[0])
    locations = sorted({location_id(lat, lon) for lat, lon in stop_to_grid.values()})
    mapping_partition = f"stop_to_location/feed_version={version}"
    summary = {"feed_version": version, "grid_locations": len(locations)}
    if mapping_partition not in done:
        import pyarrow as pa
        table = pa.table({
            "feed_version": [version] * len(stop_to_grid),
            "stop_id": list(stop_to_grid.keys()),
            "location_id": [location_id(*cell) for cell in stop_to_grid.values()],
            "latitude": [float(cell[0]) for cell in stop_to_grid.values()],
            "longitude": [float(cell[1]) for cell in stop_to_grid.values()],
        })
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        s3.put_object(Bucket=bucket, Key=f"{SILVER_PREFIX}/{mapping_partition}/part.parquet",
                      Body=buffer.getvalue(), ServerSideEncryption="AES256")
        s3.put_object(Bucket=bucket, Key=f"{QUALITY_PREFIX}/{mapping_partition}/report.json",
                      ContentType="application/json", ServerSideEncryption="AES256",
                      Body=json.dumps({"status": "success", "stops": len(stop_to_grid),
                                       "locations": len(locations)}, indent=2).encode())

    # 2. Historical reanalysis: one partition per window, only when every location is complete
    windows = defaultdict(lambda: defaultdict(set))
    for key in keys(f"{historical_root}/grid="):
        match, grid = HISTORICAL_KEY.search(key), GRID.search(key)
        if match and grid:
            windows[(match.group(1), match.group(2))][location_id(*grid.groups())].add(match.group(3))
    hist_done, hist_waiting = 0, []
    for (start, end), files in sorted(windows.items()):
        partition = f"data_product=historical_reanalysis/window_start={start}"
        if partition in done:
            continue
        complete = [loc for loc in locations if files.get(loc) == {"response", "ingestion"}]
        if len(complete) < len(locations):
            hist_waiting.append({"window_start": start, "complete_locations": len(complete)})
            continue
        rows, problems = [], {}
        for loc in locations:
            folder = f"{historical_root}/grid={loc}/start={start}/end={end}"
            meta = json.loads(read(f"{folder}/ingestion.json")[0])
            raw, _ = read(f"{folder}/response.json")
            lat, lon = (float(x) for x in loc.split("_"))
            parsed, issues = parse_response(raw, lat, lon, {
                "data_product": "historical_reanalysis", "model": "era5",
                "issued_hour_utc": None, "lead_hours": None, "feed_version": version,
                "retrieved_at_utc": parse_time(meta["retrieved_at_utc"][:19]),
                "bronze_object_key": f"{folder}/response.json", "processed_at_utc": processed_at})
            rows += parsed
            if issues:
                problems[loc] = issues
        duplicates = check_unique(rows, ["location_id", "weather_time_utc"])
        if duplicates:
            raise RuntimeError(f"Window {start}: {duplicates} duplicate location/hour rows")
        write(partition, rows, {"status": "success", "data_product": "historical_reanalysis",
                                "window_start": start, "window_end": end, "rows": len(rows),
                                "locations": len(locations), "invalid_responses": problems,
                                "processed_at_utc": processed_at})
        hist_done += 1

    # 3. Forecast: one partition per issued hour (snapshot), complete or past the grace period
    hours = defaultdict(set)
    for key in keys(f"{BRONZE_FORECAST}/feed_version={version}/date="):
        match, grid = FORECAST_KEY.search(key), GRID.search(key)
        if match and grid:
            hours[(match.group(1), match.group(2))].add(location_id(*grid.groups()))
    fc_done, fc_waiting = 0, []
    for (day, hour), present in sorted(hours.items()):
        partition = f"data_product=forecast/issued_date={day}/issued_hour={hour}"
        if partition in done:
            continue
        issued = datetime.fromisoformat(f"{day}T{hour}:00")
        missing = [loc for loc in locations if loc not in present]
        if missing and now - issued < timedelta(hours=FORECAST_GRACE_HOURS):
            fc_waiting.append(f"{day}T{hour}")
            continue
        rows, problems = [], {}
        for loc in sorted(present):
            key = (f"{BRONZE_FORECAST}/feed_version={version}/date={day}/hour={hour}/"
                   f"grid={loc}/response.json")
            raw, meta = read(key)
            lat, lon = (float(x) for x in loc.split("_"))
            retrieved = meta.get("retrieved-at-utc")
            parsed, issues = parse_response(raw, lat, lon, {
                "data_product": "forecast", "model": meta.get("model-selection", "best_match"),
                "issued_hour_utc": issued, "lead_hours": None, "feed_version": version,
                "retrieved_at_utc": parse_time(retrieved[:19]) if retrieved else None,
                "bronze_object_key": key, "processed_at_utc": processed_at})
            rows += parsed
            if issues:
                problems[loc] = issues
        duplicates = check_unique(rows, ["location_id", "weather_time_utc", "issued_hour_utc"])
        if duplicates:
            raise RuntimeError(f"Forecast {day}T{hour}: {duplicates} duplicate rows")
        write(partition, rows, {"status": "success", "data_product": "forecast",
                                "issued_hour_utc": issued, "rows": len(rows),
                                "locations_present": len(present), "locations_missing": missing,
                                "invalid_responses": problems, "processed_at_utc": processed_at})
        fc_done += 1

    summary.update({
        "historical_windows_processed": hist_done, "historical_windows_waiting": hist_waiting,
        "forecast_hours_processed": fc_done, "forecast_hours_waiting": fc_waiting,
        "total_job_script_seconds": round(perf_counter() - started, 3),
    })
    s3.put_object(Bucket=bucket, Key=f"{QUALITY_PREFIX}/runs/{now:%Y%m%dT%H%M%S}Z.json",
                  ContentType="application/json", ServerSideEncryption="AES256",
                  Body=(json.dumps(summary, indent=2, default=str) + "\n").encode())
    print(json.dumps(summary, default=str))


if __name__ == "__main__":
    main()