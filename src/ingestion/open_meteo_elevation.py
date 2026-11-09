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
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType,
)

API_URL = "https://api.open-meteo.com/v1/elevation"
STOPS_PREFIX = "silver/reference/gtfs_static_stops"
BRONZE_PREFIX = "bronze/open_meteo/elevation"
SILVER_PREFIX = "silver/reference/stop_elevation"
QUALITY_PREFIX = "quality/open_meteo_elevation"
BATCH_SIZE = 100
MAX_ATTEMPTS = 4

s3 = boto3.client("s3")


def utc_now():
    return datetime.now(timezone.utc)


def coord_key(lat, lon):
    return (round(float(lat), 8), round(float(lon), 8))


def put_json(bucket, key, value):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=(json.dumps(value, ensure_ascii=False) + "\n").encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )


def object_keys(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    return [
        obj["Key"]
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
        for obj in page.get("Contents", [])
    ]


def load_cached_elevations(bucket, bronze_version_prefix):
    """Reuse successful raw responses from earlier attempts for this version."""
    cache = {}
    for meta_key in object_keys(bucket, bronze_version_prefix):
        if not meta_key.endswith(".meta.json"):
            continue
        metadata = json.loads(
            s3.get_object(Bucket=bucket, Key=meta_key)["Body"].read()
        )
        if metadata.get("status") != "success":
            continue

        raw = s3.get_object(
            Bucket=bucket, Key=metadata["response_key"]
        )["Body"].read()
        elevations = json.loads(raw)["elevation"]
        coordinates = metadata["coordinates"]
        if len(coordinates) != len(elevations):
            raise ValueError(f"Coordinate/elevation count mismatch: {meta_key}")

        for point, elevation in zip(coordinates, elevations):
            cache[coord_key(point["latitude"], point["longitude"])] = (
                elevation, metadata["fetched_at_utc"]
            )
    return cache


def fetch_batch(bucket, version_prefix, run_id, batch_number, coordinates):
    params = {
        "latitude": ",".join(f"{p[0]:.8f}" for p in coordinates),
        "longitude": ",".join(f"{p[1]:.8f}" for p in coordinates),
    }
    url = API_URL + "?" + urllib.parse.urlencode(params)
    response_key = (
        f"{version_prefix}run_id={run_id}/"
        f"batch={batch_number:05d}.json"
    )
    meta_key = response_key.replace(".json", ".meta.json")
    last_error = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            request = urllib.request.Request(
                url, headers={"User-Agent": "HSLU-DWDL-DataVault/1.0"}
            )
            with urllib.request.urlopen(request, timeout=60) as response:
                raw = response.read()

            # Preserve the API response in Bronze before interpreting it.
            s3.put_object(
                Bucket=bucket, Key=response_key, Body=raw,
                ContentType="application/json",
                ServerSideEncryption="AES256",
            )
            values = json.loads(raw)["elevation"]
            if len(values) != len(coordinates):
                raise ValueError("API returned a different number of elevations")

            fetched_at = utc_now().isoformat()
            put_json(bucket, meta_key, {
                "source": "Open-Meteo Elevation API",
                "attribution": "Open-Meteo and Copernicus DEM GLO-90",
                "status": "success",
                "fetched_at_utc": fetched_at,
                "coordinates": [
                    {"latitude": lat, "longitude": lon}
                    for lat, lon in coordinates
                ],
                "response_key": response_key,
                "http_status": 200,
            })
            return values, fetched_at

        except urllib.error.HTTPError as error:
            last_error = error
            if attempt == MAX_ATTEMPTS:
                break

            if error.code == 429:
                retry_after = error.headers.get("Retry-After")
                delay = (
                    int(retry_after)
                    if retry_after and retry_after.isdigit()
                    else 60 * (2 ** (attempt - 1))
                )
            elif 500 <= error.code < 600:
                delay = 10 * (2 ** (attempt - 1))
            else:
                break

            print(f"HTTP {error.code}; retrying in {delay}s")
            time.sleep(delay)

        except Exception as error:
            last_error = error
            if attempt < MAX_ATTEMPTS:
                delay = 10 * (2 ** (attempt - 1))
                print(f"Request error; retrying in {delay}s")
                time.sleep(delay)

    put_json(bucket, meta_key, {
        "source": "Open-Meteo Elevation API",
        "status": "failed",
        "coordinates": [
            {"latitude": lat, "longitude": lon}
            for lat, lon in coordinates
        ],
        "attempts": MAX_ATTEMPTS,
        "error": str(last_error)[:500],
        "failed_at_utc": utc_now().isoformat(),
    })
    return None, None


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "DATA_BUCKET", "FEED_VERSION"])
    bucket = args["DATA_BUCKET"]
    version = args["FEED_VERSION"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", version):
        raise ValueError(f"Invalid feed version: {version}")

    silver_version = f"{SILVER_PREFIX}/feed_version={version}/"
    bronze_version = f"{BRONZE_PREFIX}/feed_version={version}/"
    marker_key = silver_version + "_PREPARED_SUCCESS"

    if any(k == marker_key for k in object_keys(bucket, silver_version)):
        print(f"Already prepared: {version}")
        return
    if object_keys(bucket, silver_version):
        raise RuntimeError(
            "Silver output exists without a success marker; inspect before rerunning."
        )

    sc = SparkContext.getOrCreate()
    spark = GlueContext(sc).spark_session

    stops_uri = f"s3://{bucket}/{STOPS_PREFIX}/feed_version={version}/"
    stops_df = spark.read.parquet(stops_uri)
    versions = [r[0] for r in stops_df.select("feed_version").distinct().collect()]
    if versions != [version]:
        raise ValueError(f"Expected only feed_version={version}; found {versions}")

    stops_df = stops_df.filter(
        (F.col("in_study_area") == True)
        & (F.col("canton_join_status") == "matched")
    ).select("feed_version", "stop_id", "stop_lat", "stop_lon")

    stops = [row.asDict() for row in stops_df.collect()]
    if not stops:
        raise ValueError(f"No study-area stops found for {version}")

    run_id = utc_now().strftime("%Y%m%dT%H%M%SZ")
    cache = load_cached_elevations(bucket, bronze_version)

    unique_coordinates = set()
    invalid_count = 0
    for stop in stops:
        lat, lon = stop["stop_lat"], stop["stop_lon"]
        if lat is None or lon is None or not math.isfinite(lat) or not math.isfinite(lon):
            invalid_count += 1
            continue
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            unique_coordinates.add(coord_key(lat, lon))
        else:
            invalid_count += 1

    missing = sorted(unique_coordinates - set(cache))
    failures = []
    api_calls = 0

    for start in range(0, len(missing), BATCH_SIZE):
        batch = missing[start:start + BATCH_SIZE]
        number = start // BATCH_SIZE
        values, fetched_at = fetch_batch(
            bucket, bronze_version, run_id, number, batch
        )
        api_calls += 1
        if values is None:
            failures.append(number)
        else:
            for coordinate, elevation in zip(batch, values):
                cache[coordinate] = (elevation, fetched_at)

        if start + BATCH_SIZE < len(missing):
            time.sleep(10)

    report = {
        "feed_version": version,
        "run_id": run_id,
        "unique_valid_coordinates": len(unique_coordinates),
        "cached_coordinates_reused": len(unique_coordinates) - len(missing),
        "api_batches_requested": api_calls,
        "invalid_stop_coordinates": invalid_count,
        "failed_batches": failures,
        "status": "failed" if failures else "success",
        "finished_at_utc": utc_now().isoformat(),
    }
    report_key = (
        f"{QUALITY_PREFIX}/feed_version={version}/run_id={run_id}/report.json"
    )
    put_json(bucket, report_key, report)

    if failures:
        raise RuntimeError(
            f"Elevation collection failed for batches {failures}; "
            f"see s3://{bucket}/{report_key}. Rerun to reuse successful Bronze responses."
        )

    output = []
    for stop in stops:
        lat, lon = stop["stop_lat"], stop["stop_lon"]
        key = None
        if lat is not None and lon is not None:
            if math.isfinite(lat) and math.isfinite(lon) and -90 <= lat <= 90 and -180 <= lon <= 180:
                key = coord_key(lat, lon)
        elevation, fetched_at = cache.get(key, (None, None))
        output.append({
            "feed_version": version,
            "stop_id": str(stop["stop_id"]),
            "stop_lat": lat,
            "stop_lon": lon,
            "elevation_m": elevation,
            "fetched_at_utc": (
                datetime.fromisoformat(fetched_at) if fetched_at else None
            ),
        })

    schema = StructType([
        StructField("feed_version", StringType(), False),
        StructField("stop_id", StringType(), False),
        StructField("stop_lat", DoubleType(), True),
        StructField("stop_lon", DoubleType(), True),
        StructField("elevation_m", DoubleType(), True),
        StructField("fetched_at_utc", TimestampType(), True),
    ])
    output_uri = silver_version + f"run_id={run_id}/"
    spark.createDataFrame(output, schema).write.mode("error").parquet(
        f"s3://{bucket}/{output_uri}"
    )
    put_json(bucket, marker_key, {
        "feed_version": version,
        "output_path": f"s3://{bucket}/{output_uri}",
        "quality_report": f"s3://{bucket}/{report_key}",
        "prepared_at_utc": utc_now().isoformat(),
    })


if __name__ == "__main__":
    main()