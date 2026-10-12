"""AWS Lambda collector for the Swiss GTFS Realtime protobuf feed.

Deployment dependency:
    gtfs-realtime-bindings==3.0.0

The function always writes the successful HTTP response to the immutable raw
zone before attempting protobuf parsing.  A parse failure therefore remains
reproducible and is visible in the audit record.
"""

import gzip
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3
from google.transit import gtfs_realtime_pb2


s3 = boto3.client("s3")
secrets_manager = boto3.client("secretsmanager")

RAW_BUCKET = os.environ["RAW_BUCKET"]
API_SECRET_NAME = os.environ["API_SECRET_NAME"]
GTFS_RT_URL = os.environ["GTFS_RT_URL"]
USER_AGENT = os.environ["USER_AGENT"]

_cached_api_key = None


def get_api_key():
    global _cached_api_key

    if _cached_api_key:
        return _cached_api_key

    response = secrets_manager.get_secret_value(SecretId=API_SECRET_NAME)
    secret_text = response["SecretString"]

    try:
        secret_value = json.loads(secret_text)
    except json.JSONDecodeError:
        _cached_api_key = secret_text
        return _cached_api_key

    _cached_api_key = secret_value["api_key"]
    return _cached_api_key


def make_prefix(base, captured_at):
    return (
        f"{base}/gtfs_rt/"
        f"ingest_date={captured_at:%Y-%m-%d}/"
        f"hour={captured_at:%H}"
    )


def save_audit(audit, captured_at, request_id):
    prefix = make_prefix("audit", captured_at)
    timestamp = captured_at.strftime("%Y%m%dT%H%M%S%fZ")
    key = f"{prefix}/{timestamp}_{request_id}.json"
    audit["audit_object_key"] = key

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=(json.dumps(audit, ensure_ascii=False) + "\n").encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    return key


def parse_feed_metadata(payload):
    """Return header metadata without filtering or changing the raw payload."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)
    return {
        "feed_timestamp": (
            int(feed.header.timestamp)
            if feed.header.HasField("timestamp")
            else None
        ),
        "feed_version": (
            feed.header.feed_version
            if feed.header.HasField("feed_version")
            else None
        ),
        "entity_count": len(feed.entity),
    }


def lambda_handler(event, context):
    started_at = datetime.now(timezone.utc)
    timer_started = time.perf_counter()
    request_id = context.aws_request_id
    stage = "request"

    audit = {
        "source": "swiss_gtfs_rt",
        "status": "started",
        "scheduled_time": event.get("time") if isinstance(event, dict) else None,
        "request_started_at": started_at.isoformat(),
        "response_received_at": None,
        "http_status": None,
        "network_byte_count": None,
        "payload_byte_count": None,
        "payload_sha256": None,
        "feed_timestamp": None,
        "feed_version": None,
        "entity_count": None,
        "retry_count": 0,
        "raw_object_key": None,
        "final_url": None,
        "error_stage": None,
        "error_class": None,
        "error_detail": None,
        "lambda_request_id": request_id,
    }

    try:
        request = urllib.request.Request(
            GTFS_RT_URL,
            headers={
                "Authorization": f"Bearer {get_api_key()}",
                "User-Agent": USER_AGENT,
                "Accept": "application/octet-stream",
                "Accept-Encoding": "gzip",
            },
            method="GET",
        )

        with urllib.request.urlopen(request, timeout=45) as response:
            network_payload = response.read()
            content_encoding = response.headers.get("Content-Encoding", "").lower()
            payload = (
                gzip.decompress(network_payload)
                if content_encoding == "gzip"
                else network_payload
            )
            audit["http_status"] = response.status
            audit["final_url"] = response.geturl()

        received_at = datetime.now(timezone.utc)
        payload_hash = hashlib.sha256(payload).hexdigest()
        timestamp = received_at.strftime("%Y%m%dT%H%M%S%fZ")
        raw_key = (
            f"{make_prefix('raw', received_at)}/"
            f"{timestamp}_{payload_hash}.pb"
        )

        stage = "raw_write"
        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=raw_key,
            Body=payload,
            ContentType="application/octet-stream",
            ServerSideEncryption="AES256",
            Metadata={"source": "swiss_gtfs_rt", "sha256": payload_hash},
        )

        audit.update(
            {
                "response_received_at": received_at.isoformat(),
                "network_byte_count": len(network_payload),
                "payload_byte_count": len(payload),
                "payload_sha256": payload_hash,
                "raw_object_key": raw_key,
            }
        )

        stage = "protobuf_parse"
        audit.update(parse_feed_metadata(payload))
        audit["status"] = "success"
        audit["duration_ms"] = round(
            (time.perf_counter() - timer_started) * 1000, 2
        )

        audit_key = save_audit(audit, received_at, request_id)
        print(json.dumps(audit, ensure_ascii=False))
        return {
            "statusCode": 200,
            "rawObjectKey": raw_key,
            "auditObjectKey": audit_key,
            "sha256": payload_hash,
            "bytes": len(payload),
            "feedTimestamp": audit["feed_timestamp"],
            "feedVersion": audit["feed_version"],
            "entityCount": audit["entity_count"],
        }

    except Exception as error:
        failed_at = datetime.now(timezone.utc)
        audit.update(
            {
                "status": "failed",
                "response_received_at": audit["response_received_at"]
                or failed_at.isoformat(),
                "error_stage": stage,
                "error_class": type(error).__name__,
                "error_detail": _safe_error_detail(error),
                "duration_ms": round(
                    (time.perf_counter() - timer_started) * 1000, 2
                ),
            }
        )
        if isinstance(error, urllib.error.HTTPError):
            audit["http_status"] = error.code

        try:
            save_audit(audit, failed_at, request_id)
        except Exception as audit_error:
            audit["audit_write_error"] = str(audit_error)[:500]

        print(json.dumps(audit, ensure_ascii=False))
        raise


def _safe_error_detail(error):
    if isinstance(error, urllib.error.HTTPError):
        try:
            return error.read(500).decode("utf-8", errors="replace")
        except Exception:
            return str(error)[:500]
    return str(error)[:500]
