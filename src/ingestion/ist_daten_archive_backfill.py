"""Backfill Ist-Daten v2 daily CSVs into S3 Bronze for an inclusive date range."""

import hashlib
import json
import re
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import PurePosixPath
from urllib.parse import urljoin, urlsplit
from urllib.request import Request, urlopen
from zipfile import ZipFile
from urllib.error import HTTPError

import boto3
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

ARCHIVE_PAGE = "https://archive.opentransportdata.swiss/actual_data_archive.htm"
CATALOG_API = (
    "https://api.opentransportdata.swiss/ckan-api/"
    "package_show?id=ist-daten-v2"
)
SECRET_ID = "swiss-transit/ckan-api"
PREFIX = "bronze/ist_daten/version=v2"
CHUNK_SIZE = 8 * 1024 * 1024
CSV_DATE = re.compile(r"^(\d{4}-\d{2}-\d{2}).*\.csv$", re.IGNORECASE)
ZIP_MONTH = re.compile(r"ist-daten-v2-(\d{4}-\d{2})\.zip", re.IGNORECASE)


class ArchiveLinks(HTMLParser):
    """Index v2 archive links by month."""

    def __init__(self):
        super().__init__()
        self.by_month = {}

    def handle_starttag(self, tag, attrs):
        href = dict(attrs).get("href") if tag == "a" else None
        if href:
            url = urljoin(ARCHIVE_PAGE, href)
            match = ZIP_MONTH.fullmatch(PurePosixPath(urlsplit(url).path).name)
            if match:
                self.by_month[match.group(1)] = url


def open_source(url, token=None):
    headers = {"User-Agent": "ist-daten-bronze-ingest"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return urlopen(Request(url, headers=headers), timeout=120)


def completed(s3, bucket, day):
    """Accept the CSV key recorded by either the daily job or the backfill."""
    date_prefix = f"{PREFIX}/service_date={day}/"
    uri_prefix = f"s3://{bucket}/"
    try:
        result = s3.get_object(Bucket=bucket, Key=f"{date_prefix}ingestion.json")
        with result["Body"] as body:
            record = json.load(body)
        uri = record.get("s3_uri", "")
        if record.get("status") != "success" or not uri.startswith(uri_prefix + date_prefix):
            return False

        obj = s3.head_object(Bucket=bucket, Key=uri[len(uri_prefix):])
        return obj["ContentLength"] == record.get("size_bytes")
    except ClientError as error:
        if error.response["Error"]["Code"] in {"NoSuchKey", "404", "NotFound"}:
            return False
        raise


def save_csv(s3, bucket, day, source, source_url, expected_size=None, member=None):
    """Copy the original bytes and publish the success manifest after upload."""
    date_prefix = f"{PREFIX}/service_date={day}"
    key = f"{date_prefix}/IstDaten.csv"
    digest = hashlib.sha256()

    with tempfile.TemporaryFile() as temp:
        while chunk := source.read(CHUNK_SIZE):
            temp.write(chunk)
            digest.update(chunk)
        size = temp.tell()
        if not size or (expected_size is not None and size != expected_size):
            raise RuntimeError(f"{day}: empty or incomplete CSV from {source_url}")

        temp.seek(0)
        s3.upload_fileobj(temp, bucket, key, ExtraArgs={"ContentType": "text/csv"})

    record = {
        "service_date": day,
        "source_dataset": "ist-daten-v2",
        "source_url": source_url,
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "success",
        "s3_uri": f"s3://{bucket}/{key}",
        "size_bytes": size,
        "sha256": digest.hexdigest(),
    }
    if member is not None:
        record["archive_member"] = member

    s3.put_object(
        Bucket=bucket,
        Key=f"{date_prefix}/ingestion.json",
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json",
    )
    print(f"{day}: saved {size} bytes", flush=True)


def ingest_archive(s3, bucket, url, pending):
    """Download one ZIP to disk and extract only the outstanding dates."""
    print(f"Downloading {url}", flush=True)
    with tempfile.TemporaryFile() as temp:
        with open_source(url) as source:
            while chunk := source.read(CHUNK_SIZE):
                temp.write(chunk)
        temp.seek(0)
        with ZipFile(temp) as archive:
            
            print(f"ZIP members for {url}: {archive.namelist()}", flush=True)
                        
            for entry in archive.infolist():
                match = CSV_DATE.fullmatch(PurePosixPath(entry.filename).name)
                if match and match[1] in pending:
                    day = match[1]
                    with archive.open(entry) as source:
                        save_csv(s3, bucket, day, source, url, entry.file_size, entry.filename)
                    pending.remove(day)


def ingest_catalog(s3, bucket, pending):
    """Use CKAN for outstanding dates available as individual daily files."""
    secret = boto3.client("secretsmanager").get_secret_value(SecretId=SECRET_ID)
    token = json.loads(secret["SecretString"])["ckan_api_key"]
    with open_source(CATALOG_API, token) as response:
        catalog = json.load(response)
    if not catalog.get("success"):
        raise RuntimeError("Ist-Daten catalog lookup failed")

    for resource in catalog["result"]["resources"]:
        raw_name = resource.get("name", "")
        names = raw_name.values() if isinstance(raw_name, dict) else [raw_name]
        for name in names:
            match = CSV_DATE.fullmatch(name) if isinstance(name, str) else None
            if match and match[1] in pending:
                day = match[1]
                url = resource["url"]
                with open_source(url) as source:
                    length = source.headers.get("Content-Length")
                    size = int(length) if length is not None else None
                    save_csv(s3, bucket, day, source, url, size)
                pending.remove(day)
                break


def main():
    args = getResolvedOptions(sys.argv, ["DATA_BUCKET", "START_DATE", "END_DATE"])
    bucket = args["DATA_BUCKET"]
    start = date.fromisoformat(args["START_DATE"])
    end = date.fromisoformat(args["END_DATE"])
    if start > end:
        raise ValueError("START_DATE must be on or before END_DATE")

    s3 = boto3.client("s3")
    days = [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
    pending = {day for day in days if not completed(s3, bucket, day)}
    print(f"{len(days) - len(pending)} already complete, {len(pending)} pending", flush=True)

    if pending:
        # The official archive publishes v2 ZIPs at these year/month paths.
        for month in sorted({day[:7] for day in pending}):
            archive_url = (
                f"https://archive.opentransportdata.swiss/istdaten/{month[:4]}/"
                f"ist-daten-v2-{month}.zip"
            )
            try:
                ingest_archive(s3, bucket, archive_url, pending)
            except HTTPError as error:
                if error.code != 404:
                    raise
                print(f"{month}: no monthly ZIP; checking CKAN next", flush=True)

    if pending:
        ingest_catalog(s3, bucket, pending)

    # The official October 2026 ZIP has no CSV for this service date.
    known_source_gaps = {"2026-10-19"}

    unexpected_missing = pending - known_source_gaps
    if unexpected_missing:
        missing = ", ".join(sorted(unexpected_missing))
        raise RuntimeError(f"No source file found for: {missing}")

    if pending:
        print(f"Known source gaps: {', '.join(sorted(pending))}", flush=True)

    print(f"Backfill complete: {len(days) - len(pending)}/{len(days)} dates in Bronze",
          flush=True)


if __name__ == "__main__":
    main()