"""
Input:
    s3://<bucket>/bronze/ist_daten/version=v2/service_date=<YYYY-MM-DD>/IstDaten.csv
    s3://<bucket>/silver/reference/gtfs_static_stops/feed_version=<VERSION>/   (KAN-19)
Output (one folder per service date, replaced as a whole on rerun):
    s3://<bucket>/silver/stop_events/source=istdaten/date=<YYYY-MM-DD>/*.parquet
Report (one per service date):
    s3://<bucket>/quality/istdaten_stop_events/date=<YYYY-MM-DD>/report.json

- Stops are matched to the canton reference by SLOID (= GTFS stop_id), falling back to
  BPUIC (= didok). Only stops in ZH, ZG, LU and SZ are kept; all others are counted.
- Delays are calculated in seconds from the scheduled and provided times. The provided
  time can be REAL, GESCHAETZT, PROGNOSE or UNBEKANNT; that status is kept per event.
- Rows without a delay are kept (no silent filtering); missing times are counted.
- Times are Swiss local time in the source and stored as UTC.

Modes:
    --START_DATE / --END_DATE given  -> process that inclusive range (rerun replaces those dates)
    --START_DATE pending             -> process every Bronze date that has no report yet
"""

import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from time import perf_counter


BRONZE_PREFIX = "bronze/ist_daten/version=v2"
STOPS_PREFIX = "silver/reference/gtfs_static_stops"
SILVER_PREFIX = "silver/stop_events/source=istdaten"
QUALITY_PREFIX = "quality/istdaten_stop_events"
SOURCE_TIMEZONE = "Europe/Zurich"
STUDY_CANTONS = ["ZH", "ZG", "LU", "SZ"]

# Ist-Daten v2 columns used by this job (all read as text)
SOURCE_COLUMNS = [
    "BETRIEBSTAG", "FAHRT_BEZEICHNER", "BETREIBER_ABK", "PRODUKT_ID", "LINIEN_ID",
    "LINIEN_TEXT", "VERKEHRSMITTEL_TEXT", "ZUSATZFAHRT_TF", "FAELLT_AUS_TF", "BPUIC",
    "HALTESTELLEN_NAME", "ANKUNFTSZEIT", "AN_PROGNOSE", "AN_PROGNOSE_STATUS",
    "ABFAHRTSZEIT", "AB_PROGNOSE", "AB_PROGNOSE_STATUS", "DURCHFAHRT_TF", "SLOID",
]

# Proposed shared stop-event schema (Silver), in output order
OUTPUT_COLUMNS = [
    "source_type", "service_date", "trip_id", "operator", "product", "line_id", "line",
    "transport_type", "bpuic", "sloid", "stop_name", "matched_stop_id", "didok",
    "study_canton_code", "stop_mapping_method",
    "scheduled_arrival_utc", "scheduled_departure_utc",
    "provided_arrival_utc", "provided_departure_utc",
    "arrival_delay_s", "departure_delay_s", "arrival_time_status", "departure_time_status",
    "is_cancelled", "is_additional", "is_passing",
    "reference_feed_version", "boundary_release", "bronze_object_key", "processed_at_utc",
]


def validate_arguments(bucket, version, start, end):
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid DATA_BUCKET")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", version):
        raise ValueError("Unsupported FEED_VERSION label")
    if start != "pending":
        first, last = date.fromisoformat(start), date.fromisoformat(end)
        if first > last:
            raise ValueError("START_DATE must be on or before END_DATE")


def date_range(start, end):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    return [(first + timedelta(days=i)).isoformat() for i in range((last - first).days + 1)]


def stop_lookups(stops):
    """Two lookups from the KAN-19 reference: by stop_id (SLOID) and by didok (BPUIC)."""
    from pyspark.sql import functions as F

    by_sloid = stops.select(
        F.col("stop_id").alias("ref_stop_id"),
        F.col("didok").alias("ref_didok"),
        F.col("study_canton_code").alias("ref_canton"),
        F.col("canton_join_status").alias("ref_status"),
    )
    # A didok is usable only when all its matched stops agree on one canton
    by_didok = (
        stops.where(F.col("canton_join_status") == "matched")
        .groupBy(F.col("didok").alias("did_didok"))
        .agg(
            F.countDistinct(F.coalesce(F.col("study_canton_code"), F.lit("outside"))).alias("did_cantons"),
            F.first("study_canton_code", ignorenulls=True).alias("did_canton"),
            F.min("stop_id").alias("did_stop_id"),
        )
    )
    return by_sloid, by_didok


def build_events(raw, stops, feed_version, boundary_release, processed_at):
    """Pure Spark logic (no AWS calls): returns (all classified rows, kept events)."""
    from pyspark.sql import functions as F

    def text(column):
        return F.when(F.trim(F.col(column)) != "", F.trim(F.col(column)))

    def local_time(column):
        # Scheduled times are dd.MM.yyyy HH:mm, provided times dd.MM.yyyy HH:mm:ss
        value = text(column)
        return F.coalesce(F.to_timestamp(value, "dd.MM.yyyy HH:mm:ss"),
                          F.to_timestamp(value, "dd.MM.yyyy HH:mm"))

    def utc(column):
        return F.to_utc_timestamp(local_time(column), SOURCE_TIMEZONE)

    def flag(column):
        return F.lower(F.coalesce(text(column), F.lit("false"))) == "true"

    base = raw.select(
        F.col("bronze_object_key"),
        F.col("file_service_date"),
        F.to_date(text("BETRIEBSTAG"), "dd.MM.yyyy").alias("service_date"),
        text("FAHRT_BEZEICHNER").alias("trip_id"),
        text("BETREIBER_ABK").alias("operator"),
        text("PRODUKT_ID").alias("product"),
        text("LINIEN_ID").alias("line_id"),
        text("LINIEN_TEXT").alias("line"),
        text("VERKEHRSMITTEL_TEXT").alias("transport_type"),
        text("BPUIC").alias("bpuic"),
        text("SLOID").alias("sloid"),
        text("HALTESTELLEN_NAME").alias("stop_name"),
        text("ANKUNFTSZEIT").alias("arrival_raw"),
        text("AN_PROGNOSE").alias("arrival_provided_raw"),
        text("ABFAHRTSZEIT").alias("departure_raw"),
        text("AB_PROGNOSE").alias("departure_provided_raw"),
        utc("ANKUNFTSZEIT").alias("scheduled_arrival_utc"),
        utc("ABFAHRTSZEIT").alias("scheduled_departure_utc"),
        utc("AN_PROGNOSE").alias("provided_arrival_utc"),
        utc("AB_PROGNOSE").alias("provided_departure_utc"),
        F.upper(text("AN_PROGNOSE_STATUS")).alias("arrival_time_status"),
        F.upper(text("AB_PROGNOSE_STATUS")).alias("departure_time_status"),
        flag("FAELLT_AUS_TF").alias("is_cancelled"),
        flag("ZUSATZFAHRT_TF").alias("is_additional"),
        flag("DURCHFAHRT_TF").alias("is_passing"),
    )

    # Rows that cannot be used at all (counted, never written)
    invalid = (
        F.when(F.col("service_date").isNull(), F.lit("invalid_service_date"))
        .when(F.col("service_date").cast("string") != F.col("file_service_date"),
              F.lit("service_date_differs_from_file"))
        .when(F.col("trip_id").isNull(), F.lit("missing_trip_id"))
        .when(F.col("bpuic").isNull() & F.col("sloid").isNull(), F.lit("missing_stop_id"))
        .when((F.col("arrival_raw").isNotNull() & F.col("scheduled_arrival_utc").isNull())
              | (F.col("departure_raw").isNotNull() & F.col("scheduled_departure_utc").isNull())
              | (F.col("arrival_provided_raw").isNotNull() & F.col("provided_arrival_utc").isNull())
              | (F.col("departure_provided_raw").isNotNull() & F.col("provided_departure_utc").isNull()),
              F.lit("unparseable_time"))
        .when(F.col("scheduled_arrival_utc").isNull() & F.col("scheduled_departure_utc").isNull(),
              F.lit("missing_scheduled_times"))
    )
    base = base.withColumn("invalid_reason", invalid)

    by_sloid, by_didok = stop_lookups(stops)
    joined = (
        base.join(F.broadcast(by_sloid), base["sloid"] == by_sloid["ref_stop_id"], "left")
        .join(F.broadcast(by_didok), base["bpuic"] == by_didok["did_didok"], "left")
    )
    sloid_matched = F.col("ref_status") == "matched"
    didok_usable = F.col("did_didok").isNotNull() & (F.col("did_cantons") == 1)
    classified = joined.select(
        "*",
        F.when(sloid_matched, F.lit("sloid"))
         .when(didok_usable, F.lit("bpuic"))
         .otherwise(F.lit("none")).alias("stop_mapping_method"),
        F.when(sloid_matched, F.col("ref_canton"))
         .when(didok_usable, F.col("did_canton")).alias("study_canton_code"),
        F.when(sloid_matched, F.col("ref_stop_id"))
         .when(didok_usable, F.col("did_stop_id")).alias("matched_stop_id"),
        F.when(sloid_matched, F.col("ref_didok"))
         .when(didok_usable, F.col("did_didok")).alias("didok"),
    )
    status = (
        F.when(F.col("invalid_reason").isNotNull(), F.col("invalid_reason"))
        .when(F.col("stop_mapping_method") == "none",
              F.when(F.col("did_cantons") > 1, F.lit("ambiguous_stop")).otherwise(F.lit("unmatched_stop")))
        .when(F.col("study_canton_code").isin(STUDY_CANTONS), F.lit("kept"))
        .otherwise(F.lit("outside_study_area"))
    )
    classified = classified.withColumn("row_status", status)

    kept = (
        classified.where(F.col("row_status") == "kept")
        .withColumn("source_type", F.lit("istdaten"))
        .withColumn("arrival_delay_s",
                    (F.unix_timestamp("provided_arrival_utc") - F.unix_timestamp("scheduled_arrival_utc")).cast("long"))
        .withColumn("departure_delay_s",
                    (F.unix_timestamp("provided_departure_utc") - F.unix_timestamp("scheduled_departure_utc")).cast("long"))
        .withColumn("reference_feed_version", F.lit(feed_version))
        .withColumn("boundary_release", F.lit(boundary_release))
        .withColumn("processed_at_utc", F.lit(processed_at).cast("timestamp"))
        .select(*OUTPUT_COLUMNS)
        # Identical source rows (same trip, stop and times) are written once
        .dropDuplicates(["service_date", "trip_id", "bpuic", "sloid",
                         "scheduled_arrival_utc", "scheduled_departure_utc"])
    )
    return classified, kept


def date_counts(classified, kept):
    """Per service date: input, kept, excluded by reason, mapping methods, missing times."""
    from pyspark.sql import functions as F

    status_rows = classified.groupBy("file_service_date", "row_status").count().collect()
    method_rows = (classified.where(F.col("row_status") == "kept")
                   .groupBy("file_service_date", "stop_mapping_method").count().collect())
    kept_rows = kept.groupBy(F.col("service_date").cast("string").alias("d")).agg(
        F.count(F.lit(1)).alias("written"),
        F.sum(F.col("arrival_delay_s").isNull().cast("int")).alias("arrival_delay_missing"),
        F.sum(F.col("departure_delay_s").isNull().cast("int")).alias("departure_delay_missing"),
        F.sum(F.col("is_cancelled").cast("int")).alias("cancelled"),
    ).collect()
    arrival_status = (kept.groupBy(F.col("service_date").cast("string").alias("d"),
                                   F.coalesce("arrival_time_status", F.lit("NULL")).alias("s"))
                      .count().collect())
    departure_status = (kept.groupBy(F.col("service_date").cast("string").alias("d"),
                                     F.coalesce("departure_time_status", F.lit("NULL")).alias("s"))
                        .count().collect())

    counts = {}
    def entry(day):
        return counts.setdefault(day, {"input_rows": 0, "written_rows": 0,
                                       "duplicates_removed": 0, "row_status_counts": {},
                                       "kept_by_mapping_method": {},
                                       "arrival_time_status_counts": {},
                                       "departure_time_status_counts": {}})
    for row in status_rows:
        item = entry(row["file_service_date"])
        item["input_rows"] += row["count"]
        item["row_status_counts"][row["row_status"]] = row["count"]
    for row in method_rows:
        entry(row["file_service_date"])["kept_by_mapping_method"][row["stop_mapping_method"]] = row["count"]
    for row in kept_rows:
        item = entry(row["d"])
        item.update({"written_rows": row["written"],
                     "duplicates_removed": item["row_status_counts"].get("kept", 0) - row["written"],
                     "arrival_delay_missing": row["arrival_delay_missing"],
                     "departure_delay_missing": row["departure_delay_missing"],
                     "cancelled_rows": row["cancelled"]})
    for rows, field in [(arrival_status, "arrival_time_status_counts"),
                        (departure_status, "departure_time_status_counts")]:
        for row in rows:
            entry(row["d"])[field][row["s"]] = row["count"]
    return counts


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "DATA_BUCKET", "FEED_VERSION", "START_DATE", "END_DATE"])
    bucket, version = args["DATA_BUCKET"], args["FEED_VERSION"]
    start, end = args["START_DATE"].strip().lower(), args["END_DATE"].strip()
    validate_arguments(bucket, version, start, end)
    s3 = boto3.client("s3")
    started = perf_counter()
    processed_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    def keys(prefix):
        return [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix) for o in page.get("Contents", [])]

    def manifest(day):
        try:
            body = s3.get_object(Bucket=bucket,
                                 Key=f"{BRONZE_PREFIX}/service_date={day}/ingestion.json")["Body"]
            return json.load(body)
        except s3.exceptions.NoSuchKey:
            return None

    # 1. Which dates to process: an explicit range, or all Bronze dates without a report
    bronze_days = sorted({m.group(1) for k in keys(f"{BRONZE_PREFIX}/")
                          if (m := re.search(r"service_date=(\d{4}-\d{2}-\d{2})/ingestion\.json$", k))})
    if start == "pending":
        reported = {m.group(1) for k in keys(f"{QUALITY_PREFIX}/")
                    if (m := re.search(r"date=(\d{4}-\d{2}-\d{2})/report\.json$", k))}
        requested = [d for d in bronze_days if d not in reported]
    else:
        requested = date_range(start, end)

    inputs, missing = {}, []
    for day in requested:
        record = manifest(day)
        if record and record.get("status") == "success":
            inputs[day] = record["s3_uri"]
        else:
            missing.append(day)
    if not inputs:
        print(json.dumps({"status": "nothing_to_process", "missing_in_bronze": missing}))
        return

    context = GlueContext(SparkContext.getOrCreate())
    spark = context.spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

    # 2. Stop reference for the recorded Static version
    stops = spark.read.parquet(f"s3://{bucket}/{STOPS_PREFIX}/feed_version={version}/")
    versions = [r[0] for r in stops.select("feed_version").distinct().collect()]
    if versions != [version]:
        raise ValueError(f"Stop reference version mismatch: {versions}")
    boundary_release = stops.select("boundary_release").first()[0]

    # 3. Read the CSVs (semicolon-separated, all text) and remember each row's file
    raw = (spark.read.option("header", "true").option("sep", ";")
           .option("inferSchema", "false").option("encoding", "UTF-8")
           .csv(list(inputs.values())))
    raw = raw.toDF(*[c.replace("﻿", "").strip() for c in raw.columns])
    missing_columns = set(SOURCE_COLUMNS) - set(raw.columns)
    if missing_columns:
        raise ValueError(f"Ist-Daten files lack columns: {sorted(missing_columns)}")
    raw = (raw.select(*SOURCE_COLUMNS)
           .withColumn("bronze_object_key", F.input_file_name())
           .withColumn("file_service_date",
                       F.regexp_extract("bronze_object_key", r"service_date=(\d{4}-\d{2}-\d{2})", 1)))

    classified, kept = build_events(raw, stops, version, boundary_release, processed_at)
    classified.cache()
    kept = kept.cache()

    # 4. Write: each processed date folder is replaced as a whole, so reruns never duplicate
    (kept.withColumn("date", F.col("service_date").cast("string"))
         .repartition("date")
         .write.mode("overwrite").partitionBy("date")
         .parquet(f"s3://{bucket}/{SILVER_PREFIX}/"))

    counts = date_counts(classified, kept)
    for day, source_uri in inputs.items():
        report = {
            "status": "success", "service_date": day, "source_type": "istdaten",
            "bronze_object": source_uri, "reference_feed_version": version,
            "boundary_release": boundary_release,
            "output_path": f"s3://{bucket}/{SILVER_PREFIX}/date={day}/",
            "processed_at_utc": processed_at,
            **counts.get(day, {"input_rows": 0, "written_rows": 0}),
        }
        s3.put_object(Bucket=bucket, Key=f"{QUALITY_PREFIX}/date={day}/report.json",
                      ContentType="application/json", ServerSideEncryption="AES256",
                      Body=(json.dumps(report, indent=2, default=str) + "\n").encode())

    summary = {"status": "success", "processed_dates": len(inputs),
               "missing_in_bronze": missing,
               "total_job_script_seconds": round(perf_counter() - started, 3)}
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
