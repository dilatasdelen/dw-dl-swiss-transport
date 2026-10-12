"""S3-triggered Lambda: one raw GTFS-RT snapshot -> decoded NDJSON staging.

This is the default pilot path because it processes one object at a time and
does not require a Glue job for protobuf decoding.  The output is deliberately
named ``staging``: rows are not eligible for the analytical warehouse until
they have been matched to the corresponding GTFS Static version.

Package ``gtfs_rt_snapshot.py`` beside this file in the Lambda ZIP.
"""

import gzip
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3

try:
    # Lambda ZIP deployment: package gtfs_rt_snapshot.py beside this handler.
    from gtfs_rt_snapshot import decode_snapshot
except ImportError:
    # Repository execution and tests.
    from src.models.gtfs_rt_snapshot import decode_snapshot


s3 = boto3.client("s3")

DATA_BUCKET = os.environ["DATA_BUCKET"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/gtfs_rt/").strip("/") + "/"
STAGING_PREFIX = (
    os.environ.get("STAGING_PREFIX", "staging/gtfs_rt_prediction_snapshot")
    .strip("/")
)
QUALITY_PREFIX = (
    os.environ.get("QUALITY_PREFIX", "quality/gtfs_rt_snapshot_decode")
    .strip("/")
)

HASH_PATTERN = re.compile(r"_([0-9a-f]{64})\.pb$")
TIMESTAMP_PATTERN = re.compile(r"/(\d{8}T\d{12}Z)_[0-9a-f]{64}\.pb$")
PARTITION_PATTERN = re.compile(r"ingest_date=([^/]+)/hour=([^/]+)/")


def lambda_handler(event, context):
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])
        if bucket != DATA_BUCKET or not key.startswith(RAW_PREFIX) or not key.endswith(".pb"):
            continue
        results.append(process_object(bucket, key))
    return {"processed": len(results), "results": results}


def process_object(bucket, key):
    partition_match = PARTITION_PATTERN.search(key)
    timestamp_match = TIMESTAMP_PATTERN.search(f"/{key}")
    hash_match = HASH_PATTERN.search(key)
    if not partition_match or not timestamp_match:
        raise ValueError(f"Raw object key does not follow the storage contract: {key}")

    ingest_date, ingest_hour = partition_match.groups()
    ingested_at = datetime.strptime(
        timestamp_match.group(1), "%Y%m%dT%H%M%S%fZ"
    ).replace(tzinfo=timezone.utc)
    expected_hash = hash_match.group(1) if hash_match else None
    base_name = key.rsplit("/", 1)[-1][:-3]

    staging_key = (
        f"{STAGING_PREFIX}/ingest_date={ingest_date}/hour={ingest_hour}/"
        f"{base_name}.jsonl.gz"
    )
    quality_key = (
        f"{QUALITY_PREFIX}/ingest_date={ingest_date}/hour={ingest_hour}/"
        f"{base_name}.json"
    )

    response = s3.get_object(Bucket=bucket, Key=key)
    payload = response["Body"].read()
    rows, quality = decode_snapshot(
        payload=payload,
        raw_object_key=key,
        ingested_at_utc=ingested_at,
        expected_sha256=expected_hash,
    )
    quality.update({"ingest_date": ingest_date, "hour": ingest_hour})

    s3.put_object(
        Bucket=bucket,
        Key=quality_key,
        Body=(json.dumps(quality, default=_json_default) + "\n").encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    if quality["parse_status"] != "success" or not quality["payload_hash_valid"]:
        raise RuntimeError(
            f"Refusing staging write for {key}: "
            f"parse_status={quality['parse_status']}, "
            f"payload_hash_valid={quality['payload_hash_valid']}"
        )

    compressed = gzip.compress(
        b"".join(
            (json.dumps(row, default=_json_default) + "\n").encode("utf-8")
            for row in rows
        )
    )
    s3.put_object(
        Bucket=bucket,
        Key=staging_key,
        Body=compressed,
        ContentType="application/x-ndjson",
        ContentEncoding="gzip",
        ServerSideEncryption="AES256",
        Metadata={
            "source-raw-key": key[:1024],
            "payload-sha256": quality["payload_sha256"],
        },
    )

    result = {
        "raw_object_key": key,
        "staging_object_key": staging_key,
        "quality_object_key": quality_key,
        "event_row_count": quality["event_row_count"],
        "feed_timestamp": quality["feed_timestamp"],
        "feed_version": quality["feed_version"],
    }
    print(json.dumps(result))
    return result


def _json_default(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")
