import gzip
import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3


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

    response = secrets_manager.get_secret_value(
        SecretId=API_SECRET_NAME
    )

    secret_text = response["SecretString"]

    try:
        secret_value = json.loads(secret_text)
    except json.JSONDecodeError:
        # Secret을 plain text로 저장한 경우도 처리
        _cached_api_key = secret_text
        return _cached_api_key

    _cached_api_key = secret_value["api_key"]
    return _cached_api_key


def make_prefix(base, captured_at):
    date_value = captured_at.strftime("%Y-%m-%d")
    hour_value = captured_at.strftime("%H")

    return (
        f"{base}/gtfs_rt/"
        f"ingest_date={date_value}/"
        f"hour={hour_value}"
    )


def save_audit(audit, captured_at, request_id):
    prefix = make_prefix("audit", captured_at)
    timestamp = captured_at.strftime("%Y%m%dT%H%M%S%fZ")
    key = f"{prefix}/{timestamp}_{request_id}.json"

    audit["audit_object_key"] = key

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=json.dumps(
            audit,
            ensure_ascii=False,
            indent=2
        ).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256"
    )

    return key


def lambda_handler(event, context):
    started_at = datetime.now(timezone.utc)
    timer_started = time.perf_counter()

    audit = {
        "source": "swiss_gtfs_rt",
        "status": "started",
        "scheduled_time": (
            event.get("time")
            if isinstance(event, dict)
            else None
        ),
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
        "error_class": None,
        "error_detail": None,
        "lambda_request_id": context.aws_request_id
    }

    try:
        api_key = get_api_key()

        request = urllib.request.Request(
            GTFS_RT_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "User-Agent": USER_AGENT,
                "Accept": "application/octet-stream",
                "Accept-Encoding": "gzip"
            },
            method="GET"
        )

        # urllib는 일반적인 HTTP 301/302 redirect를 따라갑니다.
        with urllib.request.urlopen(
            request,
            timeout=45
        ) as response:
            network_payload = response.read()
            content_encoding = (
                response.headers
                .get("Content-Encoding", "")
                .lower()
            )

            if content_encoding == "gzip":
                payload = gzip.decompress(network_payload)
            else:
                payload = network_payload

            audit["http_status"] = response.status
            audit["final_url"] = response.geturl()

        received_at = datetime.now(timezone.utc)
        payload_hash = hashlib.sha256(payload).hexdigest()

        raw_prefix = make_prefix("raw", received_at)
        timestamp = received_at.strftime("%Y%m%dT%H%M%S%fZ")
        raw_key = (
            f"{raw_prefix}/"
            f"{timestamp}_{payload_hash}.pb"
        )

        s3.put_object(
            Bucket=RAW_BUCKET,
            Key=raw_key,
            Body=payload,
            ContentType="application/octet-stream",
            ServerSideEncryption="AES256",
            Metadata={
                "source": "swiss_gtfs_rt",
                "sha256": payload_hash
            }
        )

        audit.update({
            "status": "success",
            "response_received_at": received_at.isoformat(),
            "network_byte_count": len(network_payload),
            "payload_byte_count": len(payload),
            "payload_sha256": payload_hash,
            "raw_object_key": raw_key,
            "duration_ms": round(
                (time.perf_counter() - timer_started) * 1000,
                2
            )
        })

        audit_key = save_audit(
            audit,
            received_at,
            context.aws_request_id
        )

        print(json.dumps(audit, ensure_ascii=False))

        return {
            "statusCode": 200,
            "rawObjectKey": raw_key,
            "auditObjectKey": audit_key,
            "sha256": payload_hash,
            "bytes": len(payload)
        }

    except urllib.error.HTTPError as error:
        failed_at = datetime.now(timezone.utc)

        audit.update({
            "status": "failed",
            "response_received_at": failed_at.isoformat(),
            "http_status": error.code,
            "error_class": type(error).__name__,
            "error_detail": (
                error.read(500)
                .decode("utf-8", errors="replace")
            ),
            "duration_ms": round(
                (time.perf_counter() - timer_started) * 1000,
                2
            )
        })

        try:
            save_audit(
                audit,
                failed_at,
                context.aws_request_id
            )
        except Exception as audit_error:
            audit["audit_write_error"] = str(audit_error)

        print(json.dumps(audit, ensure_ascii=False))
        raise

    except Exception as error:
        failed_at = datetime.now(timezone.utc)

        audit.update({
            "status": "failed",
            "response_received_at": failed_at.isoformat(),
            "error_class": type(error).__name__,
            "error_detail": str(error)[:500],
            "duration_ms": round(
                (time.perf_counter() - timer_started) * 1000,
                2
            )
        })

        try:
            save_audit(
                audit,
                failed_at,
                context.aws_request_id
            )
        except Exception as audit_error:
            audit["audit_write_error"] = str(audit_error)

        print(json.dumps(audit, ensure_ascii=False))
        raise
