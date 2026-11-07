import csv
import hashlib
import io
import json
import os
import re
import sys
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from time import perf_counter


BRONZE_PREFIX = "bronze/gtfs_static"
SILVER_PREFIX = "silver/reference/gtfs_static_stop_times"
QUALITY_PREFIX = "quality/gtfs_static_stop_times"
WORK_PREFIX = "tmp/gtfs_static_prepare"
SUCCESS_MARKER = "_PREPARED_SUCCESS"

OUTPUT_SCHEMA = [
    ("feed_version", "string"),
    ("trip_id", "string"),
    ("route_id", "string"),
    ("stop_id", "string"),
    ("stop_sequence", "bigint"),
    ("scheduled_arrival_seconds", "bigint"),
    ("scheduled_departure_seconds", "bigint"),
]
TIME_PATTERN = r"^(\d{1,3}):([0-5]\d):([0-5]\d)$"
SEQUENCE_PATTERN = r"^\d+$"


def validate_arguments(bucket, version):
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid DATA_BUCKET")
    # Same label rule as glue_staging_silver.py, so both jobs accept the same versions
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", version):
        raise ValueError("Unsupported FEED_VERSION label")


def storage_paths(bucket, version):
    return {
        "bronze": f"{BRONZE_PREFIX}/feed_version={version}/",
        "silver": f"{SILVER_PREFIX}/feed_version={version}/",
        "quality": f"{QUALITY_PREFIX}/feed_version={version}/",
        "work": f"{WORK_PREFIX}/feed_version={version}/",
        "silver_uri": f"s3://{bucket}/{SILVER_PREFIX}/feed_version={version}/",
        "work_uri": f"s3://{bucket}/{WORK_PREFIX}/feed_version={version}/",
    }


def normalise_version(value):
    """'2026-10-17', '20261017' -> '20261017'; anything without a date -> None."""
    match = re.search(r"(20\d{2})-?(\d{2})-?(\d{2})", value or "")
    return "".join(match.groups()) if match else None


def zip_filename(source_url, version):
    name = os.path.basename(urllib.parse.urlparse(source_url or "").path)
    if re.fullmatch(r"[A-Za-z0-9_.-]{1,200}\.zip", name or ""):
        return name
    return f"gtfs_static_{version}.zip"


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def read_feed_info(archive):
    """Return feed_info.txt's first row as a dict, or None when the file is absent."""
    if "feed_info.txt" not in archive.namelist():
        return None
    with archive.open("feed_info.txt") as raw:
        rows = list(csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig")))
    return {k.strip(): (v or "").strip() for k, v in rows[0].items()} if rows else {}


def version_evidence(version, archive, zip_name):
    """Check the ZIP against the requested version. A feed_info mismatch is fatal."""
    feed_info = read_feed_info(archive)
    evidence = {
        "requested_feed_version": version,
        "zip_filename": zip_name,
        "zip_filename_date": normalise_version(zip_name),
        "feed_info_present": feed_info is not None,
        "feed_info": feed_info,
    }
    stated = (feed_info or {}).get("feed_version")
    if stated:
        if normalise_version(stated) != normalise_version(version) and stated != version:
            raise ValueError(
                f"ZIP feed_info.txt states feed_version={stated!r}, "
                f"but FEED_VERSION={version!r} was requested"
            )
        evidence["feed_info_check"] = "matched"
    else:
        evidence["feed_info_check"] = "not_available"
    return evidence


def prepare_frames(stop_times, trips, version):
    """Build the Silver timetable reference and the counts needed for validation.

    Pure Spark logic (no AWS calls) so it can be tested locally.
    """
    from pyspark.sql import functions as F

    def clean(col):
        # Empty strings and whitespace become NULL; values stay strings
        return F.when(F.trim(F.col(col)) != "", F.trim(F.col(col)))

    def to_seconds(col):
        value = clean(col)
        seconds = (
            F.regexp_extract(value, TIME_PATTERN, 1).cast("long") * 3600
            + F.regexp_extract(value, TIME_PATTERN, 2).cast("long") * 60
            + F.regexp_extract(value, TIME_PATTERN, 3).cast("long")
        )
        # Missing -> NULL (never 0). Invalid formats are counted and rejected below
        return F.when(value.rlike(TIME_PATTERN), seconds)

    def invalid_time(col):
        return clean(col).isNotNull() & ~clean(col).rlike(TIME_PATTERN)

    st = stop_times.select(
        clean("trip_id").alias("trip_id"),
        clean("stop_id").alias("stop_id"),
        clean("stop_sequence").alias("stop_sequence_raw"),
        clean("arrival_time").alias("arrival_raw"),
        clean("departure_time").alias("departure_raw"),
        invalid_time("arrival_time").alias("arrival_invalid"),
        invalid_time("departure_time").alias("departure_invalid"),
        to_seconds("arrival_time").alias("scheduled_arrival_seconds"),
        to_seconds("departure_time").alias("scheduled_departure_seconds"),
    )
    tr = trips.select(clean("trip_id").alias("trip_id"), clean("route_id").alias("route_id"))

    joined = st.join(tr, "trip_id", "left")
    reference = joined.select(
        F.lit(version).cast("string").alias("feed_version"),
        F.col("trip_id"),
        F.col("route_id"),
        F.col("stop_id"),
        F.when(F.col("stop_sequence_raw").rlike(SEQUENCE_PATTERN),
               F.col("stop_sequence_raw").cast("long")).alias("stop_sequence"),
        F.col("scheduled_arrival_seconds"),
        F.col("scheduled_departure_seconds"),
    )

    # Counted on stop_times itself, so a broken trips.txt cannot inflate them
    stats = st.agg(
        F.count(F.lit(1)).alias("stop_times_rows"),
        F.sum(F.col("trip_id").isNull().cast("int")).alias("missing_trip_id"),
        F.sum(F.col("stop_id").isNull().cast("int")).alias("missing_stop_id"),
        F.sum((~F.coalesce(F.col("stop_sequence_raw").rlike(SEQUENCE_PATTERN), F.lit(False)))
              .cast("int")).alias("invalid_stop_sequence"),
        F.sum(F.col("arrival_invalid").cast("int")).alias("invalid_arrival_format"),
        F.sum(F.col("departure_invalid").cast("int")).alias("invalid_departure_format"),
        F.sum(F.col("arrival_raw").isNull().cast("int")).alias("null_arrival_times"),
        F.sum(F.col("departure_raw").isNull().cast("int")).alias("null_departure_times"),
        F.sum((F.col("scheduled_arrival_seconds") >= 86400).cast("int")).alias("arrivals_after_24h"),
        F.sum((F.col("scheduled_departure_seconds") >= 86400).cast("int")).alias("departures_after_24h"),
    ).first().asDict()
    stats = {k: int(v or 0) for k, v in stats.items()}
    # stop_times rows whose trip_id has no route in trips.txt
    stats["missing_route_id"] = st.join(tr.dropDuplicates(["trip_id"]), "trip_id", "left") \
        .filter(F.col("route_id").isNull()).count()
    stats["trips_rows"] = tr.count()
    stats["duplicate_trip_ids_in_trips"] = tr.groupBy("trip_id").count().filter("count > 1").count()
    return reference, stats


def blocking_problems(stats):
    """Rules that must hold before anything is written to Silver."""
    problems = []
    for key in ["missing_trip_id", "missing_stop_id", "invalid_stop_sequence",
                "invalid_arrival_format", "invalid_departure_format",
                "duplicate_trip_ids_in_trips", "missing_route_id"]:
        if stats.get(key):
            problems.append(f"{key}={stats[key]}")
    if not stats.get("stop_times_rows"):
        problems.append("stop_times.txt has no rows")
    return problems


def verify_output(frame, version, expected_rows):
    """Acceptance checks on the Parquet as written: columns, types, version, uniqueness."""
    actual_schema = [(f.name, f.dataType.simpleString()) for f in frame.schema.fields]
    if actual_schema != OUTPUT_SCHEMA:
        raise RuntimeError(f"Output schema mismatch: {actual_schema}")
    versions = [r[0] for r in frame.select("feed_version").distinct().collect()]
    if versions != [version]:
        raise RuntimeError(f"Output feed_version mismatch: {versions}")
    rows = frame.count()
    if rows != expected_rows:
        raise RuntimeError(f"Output row count {rows} != expected {expected_rows}")
    duplicates = (frame.groupBy("feed_version", "trip_id", "stop_sequence")
                  .count().filter("count > 1").count())
    if duplicates:
        raise RuntimeError(f"{duplicates} duplicate feed_version+trip_id+stop_sequence keys")
    return {"schema": actual_schema, "feed_versions": versions, "rows": rows,
            "duplicate_keys": duplicates}


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "DATA_BUCKET", "FEED_VERSION", "SOURCE_URL", "USER_AGENT",
        "REPLACE_INCOMPLETE",
    ])
    bucket, version = args["DATA_BUCKET"], args["FEED_VERSION"]
    source_url = args["SOURCE_URL"].strip()
    if source_url.lower() in {"", "none", "-"}:
        source_url = ""
    validate_arguments(bucket, version)
    paths = storage_paths(bucket, version)
    s3 = boto3.client("s3")
    started = perf_counter()

    def keys(prefix):
        return [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix) for o in page.get("Contents", [])]

    # 1. Already prepared? Then this run is a no-op (versions are never rebuilt).
    silver_keys = keys(paths["silver"])
    if paths["silver"] + SUCCESS_MARKER in silver_keys:
        print(json.dumps({"status": "already_prepared", "feed_version": version}))
        return
    if silver_keys:
        if args["REPLACE_INCOMPLETE"].lower() != "true":
            raise ValueError("Incomplete Silver output exists without _PREPARED_SUCCESS. "
                             "Inspect it, then rerun with --REPLACE_INCOMPLETE true.")
        for i in range(0, len(silver_keys), 1000):
            s3.delete_objects(Bucket=bucket, Delete={
                "Objects": [{"Key": k} for k in silver_keys[i:i + 1000]]})

    # 2. Bronze: reuse the preserved ZIP, or download and preserve it once.
    local_zip = "/tmp/gtfs_static.zip"
    bronze_zips = [k for k in keys(paths["bronze"]) if k.endswith(".zip")]
    if len(bronze_zips) > 1:
        raise ValueError(f"More than one ZIP in {paths['bronze']}: {bronze_zips}")
    if bronze_zips:
        zip_key = bronze_zips[0]
        s3.download_file(bucket, zip_key, local_zip)
        bronze_action = "reused_existing"
    else:
        if not source_url:
            raise ValueError("No ZIP in Bronze for this version and no SOURCE_URL given")
        request = urllib.request.Request(source_url, headers={"User-Agent": args["USER_AGENT"]})
        with urllib.request.urlopen(request, timeout=300) as response, open(local_zip, "wb") as out:
            # A permalink redirects to the actual file; keep that file's original name
            final_url = response.geturl().split("?", 1)[0]
            while chunk := response.read(1024 * 1024):
                out.write(chunk)
        zip_key = paths["bronze"] + zip_filename(final_url, version)
        bronze_action = "downloaded"

    if not zipfile.is_zipfile(local_zip):
        raise ValueError("Source is not a valid ZIP archive")
    zip_sha256 = sha256_file(local_zip)
    with zipfile.ZipFile(local_zip) as archive:
        members = set(archive.namelist())
        for required in ["stop_times.txt", "trips.txt"]:
            if required not in members:
                raise ValueError(f"GTFS ZIP does not contain {required}")
        evidence = version_evidence(version, archive, os.path.basename(zip_key))

        if bronze_action == "downloaded":
            # Preserve the original bytes; written only after the ZIP was validated
            s3.upload_file(local_zip, bucket, zip_key, ExtraArgs={
                "ContentType": "application/zip", "ServerSideEncryption": "AES256",
                "Metadata": {"sha256": zip_sha256, "feed-version": version}})
            s3.put_object(Bucket=bucket, Key=paths["bronze"] + "source_manifest.json",
                          ContentType="application/json", ServerSideEncryption="AES256",
                          Body=(json.dumps({
                              "feed_version": version, "source_url": source_url,
                              "resolved_file_url": final_url,
                              "zip_key": zip_key, "sha256": zip_sha256,
                              "bytes": os.path.getsize(local_zip),
                              "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
                          }, indent=2) + "\n").encode())

        # Spark executors cannot read the driver's /tmp, so stage the two members in S3
        for member in ["stop_times.txt", "trips.txt"]:
            with archive.open(member) as source:
                s3.upload_fileobj(source, bucket, paths["work"] + member)

    context = GlueContext(SparkContext.getOrCreate())
    spark = context.spark_session
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    def read_gtfs(member):
        frame = (spark.read.option("header", "true").option("inferSchema", "false")
                 .option("quote", '"').option("escape", '"').option("encoding", "UTF-8")
                 .csv(paths["work_uri"] + member))
        # Remove a UTF-8 byte order mark from the first header, if present
        return frame.toDF(*[c.replace("\ufeff", "").strip() for c in frame.columns])

    reference, stats = prepare_frames(read_gtfs("stop_times.txt"), read_gtfs("trips.txt"), version)
    problems = blocking_problems(stats)
    report = {
        "feed_version": version, "bucket": bucket,
        "bronze_zip_key": zip_key, "bronze_action": bronze_action, "zip_sha256": zip_sha256,
        "version_evidence": evidence, "input_counts": stats,
        "silver_path": paths["silver_uri"], "problems": problems,
        "time_rule": "hours*3600 + minutes*60 + seconds; >24:00 kept; missing -> NULL",
    }

    def write_report(status):
        report["status"] = status
        report["total_job_script_seconds"] = round(perf_counter() - started, 3)
        s3.put_object(Bucket=bucket, Key=paths["quality"] + "report.json",
                      ContentType="application/json", ServerSideEncryption="AES256",
                      Body=(json.dumps(report, default=str, indent=2) + "\n").encode())

    if problems:
        write_report("failed_validation")
        raise ValueError(f"Static timetable failed validation: {problems}")

    reference.coalesce(8).write.mode("errorifexists").parquet(paths["silver_uri"])
    written = spark.read.parquet(paths["silver_uri"])
    report["output_verification"] = verify_output(written, version, stats["stop_times_rows"])

    work_keys = keys(paths["work"])
    if work_keys:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in work_keys]})
    write_report("success")
    job.commit()
    # Written last: Glue job 1 and the version-check Lambda trust only marked versions
    s3.put_object(Bucket=bucket, Key=paths["silver"] + SUCCESS_MARKER, Body=b"")
    print(json.dumps(report, default=str))


if __name__ == "__main__":
    main()
