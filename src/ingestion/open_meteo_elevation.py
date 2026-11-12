"""Collect stop elevations from Open-Meteo into Bronze and Silver using Glue."""

import json
import math
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

API_URL = "https://api.open-meteo.com/v1/elevation"
USER_AGENT = "HSLU-DWDL-DataVault/1.0"

# Open-Meteo supports up to 100 coordinate pairs per elevation request.
BATCH_SIZE = 100
MAX_ATTEMPTS = 4
BATCH_PAUSE_SECONDS = 10

STOPS_PREFIX = "silver/reference/gtfs_static_stops"
BRONZE_PREFIX = "bronze/open_meteo/elevation"
SILVER_PREFIX = "silver/reference/stop_elevation"
QUALITY_PREFIX = "quality/open_meteo_elevation"
ATTRIBUTION = "Open-Meteo; Copernicus DEM GLO-90"

s3 = boto3.client("s3")


def now_utc():
    return datetime.now(timezone.utc)


def coordinate_key(latitude, longitude):
    return round(float(latitude), 8), round(float(longitude), 8)


def valid_coordinate(latitude, longitude):
    try:
        latitude, longitude = float(latitude), float(longitude)
    except (TypeError, ValueError):
        return False

    return (
        math.isfinite(latitude)
        and math.isfinite(longitude)
        and -90 <= latitude <= 90
        and -180 <= longitude <= 180
    )


def put_json(bucket, key, value):
    body = (json.dumps(value, indent=2, ensure_ascii=False) + "\n").encode()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )


def list_keys(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]


def load_bronze_cache(bucket, bronze_prefix):
    """Load successful response values so reruns only request missing points."""
    cache = {}

    for metadata_key in list_keys(bucket, bronze_prefix):
        if not metadata_key.endswith(".meta.json"):
            continue

        metadata = json.loads(
            s3.get_object(Bucket=bucket, Key=metadata_key)["Body"].read()
        )
        if metadata.get("status") != "success":
            continue

        response = s3.get_object(
            Bucket=bucket, Key=metadata["response_key"]
        )["Body"].read()
        elevations = json.loads(response)["elevation"]
        coordinates = metadata["coordinates"]

        if len(elevations) != len(coordinates):
            raise ValueError(f"Bad cached response: {metadata_key}")

        for point, elevation in zip(coordinates, elevations):
            if elevation is None:
                continue

            key = coordinate_key(point["latitude"], point["longitude"])
            cache[key] = (elevation, metadata["fetched_at_utc"])

    return cache


def fetch_batch(bucket, bronze_prefix, run_id, batch_number, coordinates):
    """Fetch one batch, preserve its response, and retry temporary errors."""
    query = urllib.parse.urlencode({
        "latitude": ",".join(f"{lat:.8f}" for lat, _ in coordinates),
        "longitude": ",".join(f"{lon:.8f}" for _, lon in coordinates),
    })
    url = f"{API_URL}?{query}"

    run_prefix = f"{bronze_prefix}run_id={run_id}/"
    batch_prefix = f"{run_prefix}batch={batch_number:05d}/"
    metadata_key = f"{run_prefix}batch={batch_number:05d}.meta.json"

    last_error = None
    http_status = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = urllib.request.Request(
            url, headers={"User-Agent": USER_AGENT}
        )

        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                raw_response = response.read()
                http_status = response.status

        except urllib.error.HTTPError as error:
            last_error = error
            http_status = error.code

            if error.code == 429:
                retry_after = error.headers.get("Retry-After")
                delay = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else 60 * 2 ** (attempt - 1)
                )
            elif 500 <= error.code < 600:
                delay = 10 * 2 ** (attempt - 1)
            else:
                break  # Other 4xx errors usually need a code or request fix.

            if attempt < MAX_ATTEMPTS:
                print(f"HTTP {error.code}; batch {batch_number}, wait {delay}s")
                time.sleep(delay)
            continue

        except (urllib.error.URLError, TimeoutError, OSError) as error:
            last_error = error
            if attempt < MAX_ATTEMPTS:
                delay = 10 * 2 ** (attempt - 1)
                print(f"Request error; batch {batch_number}, wait {delay}s")
                time.sleep(delay)
            continue

        # Keep the raw response in Bronze before parsing it.
        response_key = (
            f"{batch_prefix}attempt={attempt:02d}_response.json"
        )
        s3.put_object(
            Bucket=bucket,
            Key=response_key,
            Body=raw_response,
            ContentType="application/json",
            ServerSideEncryption="AES256",
        )

        try:
            elevations = json.loads(raw_response)["elevation"]
            if len(elevations) != len(coordinates):
                raise ValueError("API response count differs from request count")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            last_error = error
            break

        fetched_at = now_utc().isoformat()
        put_json(bucket, metadata_key, {
            "source": "Open-Meteo Elevation API",
            "attribution": ATTRIBUTION,
            "status": "success",
            "http_status": http_status,
            "attempts": attempt,
            "fetched_at_utc": fetched_at,
            "coordinates": [
                {"latitude": lat, "longitude": lon}
                for lat, lon in coordinates
            ],
            "response_key": response_key,
        })
        return elevations, fetched_at

    # Record failures so they are visible and retryable in a later run.
    put_json(bucket, metadata_key, {
        "source": "Open-Meteo Elevation API",
        "status": "failed",
        "http_status": http_status,
        "attempts": attempt,
        "failed_at_utc": now_utc().isoformat(),
        "coordinates": [
            {"latitude": lat, "longitude": lon}
            for lat, lon in coordinates
        ],
        "error": str(last_error)[:500] if last_error else "Unknown error",
    })
    return None, None


def stop_rows_from_reference(spark, bucket, feed_version):
    """Read the matched study-area stops for exactly one Static version."""
    path = f"s3://{bucket}/{STOPS_PREFIX}/feed_version={feed_version}/"
    source = spark.read.parquet(path)

    versions = [
        row["feed_version"]
        for row in source.select("feed_version").distinct().collect()
    ]
    if versions != [feed_version]:
        raise ValueError(f"Expected {feed_version}; found {versions}")

    selected = (
        source
        .filter(
            (F.col("in_study_area") == F.lit(True))
            & (F.col("canton_join_status") == F.lit("matched"))
        )
        .select("feed_version", "stop_id", "stop_lat", "stop_lon")
    )
    stops = [row.asDict() for row in selected.collect()]

    if not stops:
        raise ValueError(f"No study-area stops found for {feed_version}")
    if any(stop["stop_id"] is None for stop in stops):
        raise ValueError("Stop reference contains a null stop_id")

    # The upstream job checks uniqueness too; keep this as a lookup safeguard.
    stop_ids = [str(stop["stop_id"]) for stop in stops]
    if len(stop_ids) != len(set(stop_ids)):
        raise ValueError("Stop reference contains duplicate stop_id values")

    return stops


def main():
    args = getResolvedOptions(
        sys.argv, ["JOB_NAME", "DATA_BUCKET", "FEED_VERSION"]
    )
    bucket = args["DATA_BUCKET"]
    version = args["FEED_VERSION"]

    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", version):
        raise ValueError(f"Invalid feed version: {version}")

    silver_prefix = f"{SILVER_PREFIX}/feed_version={version}/"
    bronze_prefix = f"{BRONZE_PREFIX}/feed_version={version}/"
    marker_key = silver_prefix + "_PREPARED_SUCCESS"

    # 1. Stop if this version already has a success marker.
    # The marker means the Silver lookup and quality report were written.
    try:
        s3.head_object(Bucket=bucket, Key=marker_key)
        print(f"Already prepared: {version}")
        return
    except ClientError as error:
        if error.response["Error"]["Code"] not in {
            "404", "NoSuchKey", "NotFound"
        }:
            raise

    # 2. Load the matching, prepared GTFS stop reference.
    # Keep only matched stops inside the four-canton study area.

    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    stops = stop_rows_from_reference(spark, bucket, version)

    # 3. Validate and deduplicate coordinates.
    # Several stops can share a coordinate, so request each unique pair once.
    invalid_stops = [
        stop for stop in stops
        if not valid_coordinate(stop["stop_lat"], stop["stop_lon"])
    ]
    coordinates = sorted({
        coordinate_key(stop["stop_lat"], stop["stop_lon"])
        for stop in stops
        if valid_coordinate(stop["stop_lat"], stop["stop_lon"])
    })

    # 4. Reuse successful Bronze responses from earlier runs.
    # Only coordinates without a cached elevation need another API request.
    run_id = now_utc().strftime("%Y%m%dT%H%M%S%fZ")
    cache = load_bronze_cache(bucket, bronze_prefix)
    cache_hits = sum(point in cache for point in coordinates)
    missing = [point for point in coordinates if point not in cache]

    failed_batches = []
    batches_requested = 0

    # 5. Request the missing elevations in batches.
    # Save each raw API response and its metadata in Bronze before using it.
    for start in range(0, len(missing), BATCH_SIZE):
        batch_number = start // BATCH_SIZE
        batch = missing[start:start + BATCH_SIZE]
        batches_requested += 1

        elevations, fetched_at = fetch_batch(
            bucket, bronze_prefix, run_id, batch_number, batch
        )
        if elevations is None:
            failed_batches.append(batch_number)
        else:
            for point, elevation in zip(batch, elevations):
                if elevation is not None:
                    cache[point] = (elevation, fetched_at)

        print(json.dumps({
            "event": "batch_finished",
            "batch": batch_number,
            "batch_size": len(batch),
            "status": "failed" if elevations is None else "success",
        }))

        if start + BATCH_SIZE < len(missing):
            time.sleep(BATCH_PAUSE_SECONDS)

    # 6. Check completeness before publishing Silver.
    # Failed requests, invalid coordinates, or missing elevations must be reported.
    missing_values = sum(
        cache.get(point, (None, None))[0] is None for point in coordinates
    )
    unresolved = [point for point in coordinates if point not in cache]

    report_key = (
        f"{QUALITY_PREFIX}/feed_version={version}/"
        f"run_id={run_id}/report.json"
    )
    report = {
        "feed_version": version,
        "run_id": run_id,
        "status": "success",
        "stop_count": len(stops),
        "unique_valid_coordinates": len(coordinates),
        "cached_coordinates_reused": cache_hits,
        "api_batches_requested": batches_requested,
        "invalid_stop_coordinates": len(invalid_stops),
        "missing_elevation_values": missing_values,
        "failed_batches": failed_batches,
        "finished_at_utc": now_utc().isoformat(),
    }

    # Do not publish a complete Silver lookup if any stops lack valid elevation.
    if failed_batches or unresolved or invalid_stops or missing_values:
        report["status"] = "failed"
        put_json(bucket, report_key, report)
        raise RuntimeError(
            f"Elevation lookup incomplete; see s3://{bucket}/{report_key}"
        )

    # 7. Map elevations back to stops and write the versioned Silver lookup.
    # The output has one row per stop, even when stops share coordinates.
    output_rows = []
    for stop in stops:
        key = coordinate_key(stop["stop_lat"], stop["stop_lon"])
        elevation, fetched_at = cache[key]
        output_rows.append({
            "feed_version": version,
            "stop_id": str(stop["stop_id"]),
            "stop_lat": float(stop["stop_lat"]),
            "stop_lon": float(stop["stop_lon"]),
            "elevation_m": float(elevation),
            "fetched_at_utc": datetime.fromisoformat(fetched_at),
        })

    schema = StructType([
        StructField("feed_version", StringType(), False),
        StructField("stop_id", StringType(), False),
        StructField("stop_lat", DoubleType(), False),
        StructField("stop_lon", DoubleType(), False),
        StructField("elevation_m", DoubleType(), False),
        StructField("fetched_at_utc", TimestampType(), False),
    ])

    output_path = f"s3://{bucket}/{silver_prefix}run_id={run_id}/"

    # 8. Write Silver first, then the quality report, and the success marker last.
    # Downstream jobs can rely on the marker to identify a completed version.
    spark.createDataFrame(output_rows, schema).write.mode(
        "errorifexists"
    ).parquet(output_path)

    report["silver_output_path"] = output_path
    put_json(bucket, report_key, report)

    # Write the success marker last so downstream jobs only see completed data.
    put_json(bucket, marker_key, {
        "feed_version": version,
        "run_id": run_id,
        "output_path": output_path,
        "quality_report": f"s3://{bucket}/{report_key}",
        "prepared_at_utc": now_utc().isoformat(),
    })

    print(json.dumps({
        "event": "job_completed",
        "feed_version": version,
        "run_id": run_id,
        "stop_count": len(output_rows),
        "quality_report": f"s3://{bucket}/{report_key}",
    }))


if __name__ == "__main__":
    main()