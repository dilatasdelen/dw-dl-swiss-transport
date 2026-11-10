"""Build the parent-station elevation lookup in AWS Glue."""

import json
import math
import sys
from collections import Counter
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

CANTONS = {"ZH", "ZG", "LU", "SZ"}
SOURCE = "Open-Meteo / Copernicus DEM GLO-90"


def coordinate_key(latitude, longitude):
    # Match the eight-decimal coordinate keys used by the Bronze collector.
    return round(float(latitude), 8), round(float(longitude), 8)


def read_json(s3, bucket, key):
    body = s3.get_object(Bucket=bucket, Key=key)["Body"]
    return json.loads(body.read())


def read_elevations(s3, bucket, prefix):
    """Pair each raw elevation with the request coordinate at the same index."""
    elevations = {}
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    for page in pages:
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".meta.json"):
                continue
            meta = read_json(s3, bucket, obj["Key"])
            if meta["status"] != "success":
                continue

            response_key = meta["response_key"]
            values = read_json(s3, bucket, response_key)["elevation"]
            fetched_at = datetime.fromisoformat(meta["fetched_at_utc"])
            for point, value in zip(meta["coordinates"], values, strict=True):
                if value is None or not math.isfinite(value):
                    continue
                key = coordinate_key(point["latitude"], point["longitude"])
                # Prefer the newest valid result when a coordinate was fetched again.
                previous = elevations.get(key)
                if previous is None or fetched_at > previous[1]:
                    elevations[key] = (float(value), fetched_at, response_key)
    return elevations


def main():
    args = getResolvedOptions(sys.argv, ["DATA_BUCKET", "FEED_VERSION", "OUTPUT_PREFIX"])
    bucket, version = args["DATA_BUCKET"], args["FEED_VERSION"]
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    partition = f"feed_version={version}"
    reference = f"silver/reference/gtfs_static_stops/{partition}/"
    bronze = f"bronze/open_meteo/elevation/{partition}/"
    output = f"s3://{bucket}/{args['OUTPUT_PREFIX'].rstrip('/')}/{partition}/run_id={run_id}/"
    report_key = f"quality/stop_elevation_stations/{partition}/run_id={run_id}/report.json"

    s3 = boto3.client("s3")
    s3.head_object(Bucket=bucket, Key=reference + "_PREPARED_SUCCESS")
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # Read the versioned stop-to-canton reference and keep the station hierarchy.
    stops = spark.read.parquet(f"s3://{bucket}/{reference}").select(
        "feed_version", "stop_id", "stop_name", "didok", "stop_lat", "stop_lon",
        "study_canton_code", "canton_join_status", "in_study_area",
        "boundary_release", "boundary_crs", "location_type", "parent_station",
    ).collect()
    if {row.feed_version for row in stops} != {version}:
        raise ValueError(f"Expected one GTFS Static version: {version}")

    parents = [row for row in stops if row.location_type == "1"]
    stations = [
        row for row in parents
        if row.in_study_area and row.canton_join_status == "matched"
        and row.study_canton_code in CANTONS
    ]
    station_ids = Counter(row.stop_id for row in stations if row.stop_id)
    didok_ids = Counter(row.didok for row in stations if row.didok)
    platforms = [
        row for row in stops if row.in_study_area and row.location_type in (None, "", "0")
    ]

    # Each parent gets one row. Stations at the same coordinate share a cached value.
    elevations = read_elevations(s3, bucket, bronze)
    rows, missing_elevations = [], []
    for station in stations:
        elevation = elevations.get(coordinate_key(station.stop_lat, station.stop_lon))
        if elevation is None:
            missing_elevations.append(station.stop_id)
            continue
        rows.append((
            version, station.stop_id, station.didok, station.stop_name,
            station.stop_lat, station.stop_lon, station.study_canton_code,
            station.boundary_release, station.boundary_crs,
            *elevation, SOURCE,
        ))

    canton_counts = Counter(row.study_canton_code for row in stations)
    report = {
        "feed_version": version,
        "output_path": output,
        "stations_by_canton": dict(canton_counts),
        "missing_cantons": sorted(CANTONS - canton_counts.keys()),
        "parent_canton_status_counts": dict(Counter(row.canton_join_status for row in parents)),
        "unassigned_parent_ids": [
            row.stop_id for row in parents if row.canton_join_status != "matched"
        ],
        "missing_parent_ids": sum(not row.stop_id for row in stations),
        "stations_without_didok": [row.stop_id for row in stations if not row.didok],
        "duplicate_parent_ids": [key for key, count in station_ids.items() if count > 1],
        "shared_didok_ids": [key for key, count in didok_ids.items() if count > 1],
        "missing_boundary_lineage": sum(
            not row.boundary_release or not row.boundary_crs for row in stations
        ),
        "missing_elevations": missing_elevations,
        "platforms_without_parent": sum(not row.parent_station for row in platforms),
        "platforms_with_parent_outside_lookup": sum(
            bool(row.parent_station) and row.parent_station not in station_ids
            for row in platforms
        ),
    }
    blocked = (
        report["missing_cantons"] or report["missing_parent_ids"] or report["duplicate_parent_ids"]
        or report["missing_boundary_lineage"] or missing_elevations
    )

    # Publish the quality report even when validation blocks the Parquet output.
    if not blocked:
        schema = (
            "feed_version string, parent_stop_id string, didok string, station_name string, "
            "latitude double, longitude double, study_canton_code string, "
            "boundary_release string, boundary_crs string, elevation_m double, "
            "retrieved_at_utc timestamp, elevation_response_key string, elevation_source string"
        )
        spark.createDataFrame(rows, schema).write.mode("errorifexists").parquet(output)

    report["status"] = "failed" if blocked else "success"
    report["published_rows"] = 0 if blocked else len(rows)
    s3.put_object(
        Bucket=bucket, Key=report_key, Body=json.dumps(report, indent=2).encode("utf-8"),
        ContentType="application/json", ServerSideEncryption="AES256",
    )
    print(json.dumps({"status": report["status"], "report": f"s3://{bucket}/{report_key}"}))
    if blocked:
        raise RuntimeError(f"Station lookup validation failed: s3://{bucket}/{report_key}")


if __name__ == "__main__":
    main()