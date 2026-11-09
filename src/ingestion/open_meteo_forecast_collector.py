"""Collect hourly Open-Meteo forecast snapshots for the study area."""

import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode
from urllib.request import urlopen

import boto3
from botocore.exceptions import ClientError

ENDPOINT = "https://api.open-meteo.com/v1/forecast"
MODEL = "best_match"
FORECAST_HOURS = 24
VARIABLES = (
    "temperature_2m",
    "precipitation",
    "rain",
    "snowfall",
    "wind_speed_10m",
    "weather_code",
)
s3 = boto3.client("s3")


def fetch_forecast(latitude, longitude):
    """Fetch one location and validate its hourly forecast before saving."""
    query = urlencode({
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ",".join(VARIABLES),
        "forecast_hours": FORECAST_HOURS,
        "timezone": "GMT",
        "models": MODEL,
    })
    url = f"{ENDPOINT}?{query}"
    with urlopen(url, timeout=10) as response:
        raw = response.read()

    # Check the parsed data while keeping the original response bytes for Bronze.
    data = json.loads(raw)
    hourly = data["hourly"]
    times = [datetime.fromisoformat(value) for value in hourly["time"]]
    if (
        data["utc_offset_seconds"] != 0
        or len(times) != FORECAST_HOURS
        or any(end - start != timedelta(hours=1) for start, end in zip(times, times[1:]))
    ):
        raise ValueError("Expected 24 consecutive UTC forecast hours")

    for variable in VARIABLES:
        values = hourly[variable]
        unit = data["hourly_units"].get(variable)
        if not unit or len(values) != len(times) or None in values:
            raise ValueError(f"Missing units or values for {variable}")

    return raw, url


def lambda_handler(event, context):
    bucket = os.environ["DATA_BUCKET"]
    feed_version = os.environ["FEED_VERSION"]
    now = datetime.now(timezone.utc)
    current_hour = now.replace(minute=0, second=0, microsecond=0)

    # Scheduler retries reuse the original UTC hour. Manual tests use the current hour.
    slot = current_hour
    if scheduled_time := event.get("scheduled_time"):
        scheduled = datetime.fromisoformat(scheduled_time).astimezone(timezone.utc)
        slot = scheduled.replace(minute=0, second=0, microsecond=0)

    # A late retry would fetch a newer forecast and label it with the old hour.
    if slot != current_hour:
        raise ValueError(f"Forecast snapshot hour is no longer current: {slot.isoformat()}")

    # Reuse the KAN-33 stop lookup. Several stops can share one request location.
    lookup_key = (
        "bronze/open_meteo/weather_historical/model=era5/"
        f"feed_version={feed_version}/stop_to_grid.json"
    )
    with s3.get_object(Bucket=bucket, Key=lookup_key)["Body"] as body:
        locations = sorted({tuple(cell) for cell in json.load(body).values()})
    if not locations:
        raise ValueError(f"No study locations in s3://{bucket}/{lookup_key}")

    # On a retry, request forecasts only for locations missing from this hour.
    prefix = (
        f"bronze/open_meteo/weather/feed_version={feed_version}/"
        f"date={slot:%Y-%m-%d}/hour={slot:%H}/"
    )
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    existing = {obj["Key"] for page in pages for obj in page.get("Contents", [])}

    saved = 0
    failures = []
    for latitude, longitude in locations:
        key = f"{prefix}grid={latitude:.2f}_{longitude:.2f}/response.json"
        if key in existing:
            continue

        try:
            raw, url = fetch_forecast(latitude, longitude)

            # Save the raw JSON and its metadata together. The conditional write
            # protects the first snapshot if two invocations overlap.
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=raw,
                ContentType="application/json",
                IfNoneMatch="*",
                Metadata={
                    "status": "success",
                    "data-product": "forecast",
                    "model-selection": MODEL,
                    "source-url": url,
                    "scheduled-hour-utc": slot.isoformat(),
                    "retrieved-at-utc": datetime.now(timezone.utc).isoformat(),
                },
            )
            saved += 1
        except ClientError as error:
            # S3 returns 412 when another invocation has already saved this key.
            if error.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
                raise
        except (OSError, ValueError, KeyError) as error:
            failures.append(f"{latitude},{longitude}: {error}")

    # Keep successful snapshots and report any gaps in CloudWatch.
    result = {
        "prefix": prefix,
        "locations": len(locations),
        "saved": saved,
        "failures": failures,
    }
    print(json.dumps(result), flush=True)
    if failures:
        raise RuntimeError(f"{len(failures)} forecast locations failed. See CloudWatch logs.")
    return result