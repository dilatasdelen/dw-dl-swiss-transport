"""AWS Glue job 2: preserved Silver history -> window-scoped event candidates.

Select the latest valid prediction per event and summarise revisions within
the requested feed-time window, preserving the Silver history.
"""

import json
import re
import sys
from datetime import datetime, timezone
from time import perf_counter


RULE_VERSION = "pilot_latest_valid_feed_v1"
EVENT_KEY_COLUMNS = ["source", "service_date", "trip_id", "stop_id",
                     "effective_stop_sequence", "event_type", "feed_version"]
VALUE_COLUMNS = ["delay_seconds", "published_time_epoch", "uncertainty_seconds"]
HISTORY_COLUMNS = [
    "source", "service_date", "trip_id", "stop_id", "stop_sequence",
    "event_type", "feed_version", "feed_timestamp", "trip_update_timestamp",
    "start_time", "route_id", "entity_is_deleted", "payload_hash_valid",
    "raw_object_key", "payload_sha256", "ingested_at_utc", "lineage_id",
    "prediction_signature", "delay_seconds", "published_time_epoch",
    "uncertainty_seconds", "is_delay_missing", "static_match_status",
    "static_match_reason", "static_stop_sequence", "scheduled_time_seconds",
    "stop_schedule_relationship", "trip_schedule_relationship", "stop_name",
    "didok", "study_canton_code", "boundary_release", "boundary_crs",
]
# Replays may have different representative capture rows and aggregate counts.
# Those fields must not create new predictions or count source captures twice.
REPLAY_CONTENT_COLUMNS = [c for c in HISTORY_COLUMNS if c not in {
    "raw_object_key", "payload_sha256", "ingested_at_utc", "lineage_id"
}]


def utc_second(value):
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
        raise ValueError("Selection window timestamps must explicitly use UTC (Z or +00:00)")
    if parsed.microsecond:
        raise ValueError("Selection boundaries must use whole seconds")
    return int(parsed.timestamp())


def configuration(bucket, run_ids, output_run_id, start, end, rule_version):
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid DATA_BUCKET")
    labels = [label.strip() for label in run_ids.split(",")]
    label_pattern = r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}"
    if not labels or any(not re.fullmatch(label_pattern, label) for label in labels):
        raise ValueError("INPUT_RUN_IDS must be comma-separated job-1 run labels, not paths")
    if len(labels) != len(set(labels)):
        raise ValueError("INPUT_RUN_IDS contains a repeated run label")
    if not re.fullmatch(label_pattern, output_run_id):
        raise ValueError("RUN_ID must be a simple label")
    if rule_version != RULE_VERSION:
        raise ValueError(f"This script implements only {RULE_VERSION}")
    lower, upper = utc_second(start), utc_second(end)
    if lower >= upper:
        raise ValueError("WINDOW_START_UTC must be before WINDOW_END_UTC")
    return {
        "input_roots": [f"s3://{bucket}/experiments/staging_silver_pilot/run_id={label}/" for label in labels],
        "input_run_ids": labels,
        "output_root": f"s3://{bucket}/experiments/event_selection_pilot/run_id={output_run_id}/",
        "start_epoch": lower, "end_epoch": upper, "rule_version": rule_version,
    }


def sql_plan(start_epoch, end_epoch):
    if not isinstance(start_epoch, int) or not isinstance(end_epoch, int) or start_epoch >= end_epoch:
        raise ValueError("Invalid integer selection bounds")
    replay_conflicts = """
        SELECT prediction_signature, COUNT(DISTINCT replay_content_signature) AS content_versions
        FROM history_input GROUP BY prediction_signature
        HAVING COUNT(DISTINCT replay_content_signature) > 1
    """
    replay_ranked = """
        SELECT h.*, ROW_NUMBER() OVER (
          PARTITION BY prediction_signature
          ORDER BY ingested_at_utc, raw_object_key, lineage_id, upstream_run_id
        ) AS replay_rank
        FROM history_input h
    """
    unique = "SELECT * FROM replay_ranked WHERE replay_rank = 1"
    classified = f"""
        SELECT h.*,
          CASE
            WHEN feed_timestamp IS NULL OR feed_timestamp <= 0 THEN 'invalid_feed_timestamp'
            WHEN feed_timestamp < {start_epoch} OR feed_timestamp >= {end_epoch} THEN 'outside_selection_window'
            WHEN COALESCE(event_key_valid, FALSE) = FALSE THEN 'missing_or_invalid_event_key'
            WHEN NULLIF(TRIM(prediction_signature), '') IS NULL
              OR NULLIF(TRIM(lineage_id), '') IS NULL
              OR NULLIF(TRIM(raw_object_key), '') IS NULL
              OR NULLIF(TRIM(payload_sha256), '') IS NULL
              OR ingested_at_utc IS NULL THEN 'missing_lineage'
            WHEN COALESCE(payload_hash_valid, FALSE) = FALSE THEN 'invalid_payload_hash'
            WHEN COALESCE(entity_is_deleted, FALSE) THEN 'deleted_entity'
            WHEN COALESCE(static_match_status, '') <> 'matched' THEN 'no_valid_static_match'
            WHEN scheduled_time_seconds IS NULL THEN 'missing_scheduled_time'
            WHEN event_type IS NULL OR event_type NOT IN ('arrival', 'departure') THEN 'non_delay_event'
            WHEN trip_schedule_relationship IN ('CANCELED', 'DELETED') THEN 'cancelled_trip'
            WHEN stop_schedule_relationship IN ('SKIPPED', 'NO_DATA') THEN 'unavailable_stop_prediction'
            WHEN delay_seconds IS NULL THEN 'missing_published_delay'
            WHEN COALESCE(is_delay_missing, TRUE) THEN 'inconsistent_delay_missing_flag'
            ELSE 'eligible'
          END AS selection_reason
        FROM history_unique h
    """
    eligible = "SELECT * FROM observations WHERE selection_reason = 'eligible'"
    point_conflicts = """
        SELECT event_key, feed_timestamp, COUNT(DISTINCT value_signature) AS value_variants
        FROM eligible GROUP BY event_key, feed_timestamp
        HAVING COUNT(DISTINCT value_signature) > 1
    """
    blocked = """
        SELECT event_key, 'conflicting_values_at_same_feed_time' AS blocking_reason
        FROM point_conflicts GROUP BY event_key
        UNION ALL
        SELECT event_key, 'multiple_trip_start_times' AS blocking_reason
        FROM eligible GROUP BY event_key
        HAVING COUNT(DISTINCT NULLIF(TRIM(start_time), '')) > 1
    """
    safe = """
        SELECT e.* FROM eligible e LEFT JOIN (
          SELECT DISTINCT event_key FROM blocked_events
        ) b ON e.event_key = b.event_key WHERE b.event_key IS NULL
    """
    # Entity/vehicle metadata can create multiple upstream signatures at the
    # same event/feed instant. Equivalent values are one timeline observation.
    point_ranked = """
        SELECT e.*, ROW_NUMBER() OVER (
          PARTITION BY event_key, feed_timestamp
          ORDER BY trip_update_timestamp DESC NULLS LAST, ingested_at_utc DESC,
                   prediction_signature, lineage_id
        ) AS point_rank
        FROM safe_eligible e
    """
    timeline = "SELECT * FROM point_ranked WHERE point_rank = 1"
    changes = """
        SELECT t.*,
          ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY feed_timestamp) AS timeline_number,
          LAG(value_signature) OVER (PARTITION BY event_key ORDER BY feed_timestamp) AS previous_value_signature,
          LAG(delay_seconds) OVER (PARTITION BY event_key ORDER BY feed_timestamp) AS previous_delay_seconds,
          ROW_NUMBER() OVER (PARTITION BY event_key ORDER BY feed_timestamp DESC) AS latest_rank
        FROM timeline t
    """
    summary = """
        SELECT event_key, COUNT(*) AS prediction_observation_count,
          SUM(CASE WHEN timeline_number > 1 AND value_signature <> previous_value_signature THEN 1 ELSE 0 END) AS prediction_revision_count,
          SUM(CASE WHEN timeline_number > 1 AND delay_seconds <> previous_delay_seconds THEN 1 ELSE 0 END) AS delay_revision_count,
          MIN(delay_seconds) AS minimum_published_delay_seconds,
          MAX(delay_seconds) AS maximum_published_delay_seconds,
          MAX(CASE WHEN timeline_number = 1 THEN delay_seconds END) AS first_published_delay_seconds,
          MAX(CASE WHEN latest_rank = 1 THEN delay_seconds END) AS selected_published_delay_seconds,
          MIN(feed_timestamp) AS first_valid_feed_timestamp,
          MAX(feed_timestamp) AS last_valid_feed_timestamp,
          MAX(feed_timestamp) - MIN(feed_timestamp) AS observation_span_seconds,
          STDDEV_POP(delay_seconds) AS published_delay_stddev_seconds
        FROM changes GROUP BY event_key
    """
    observed_summary = f"""
        SELECT event_key, MAX(feed_timestamp) AS last_observed_feed_timestamp,
          SUM(CASE WHEN selection_reason <> 'eligible' THEN 1 ELSE 0 END) AS invalid_prediction_record_count,
          COUNT(DISTINCT prediction_signature) AS distinct_input_prediction_count
        FROM observations
        WHERE event_key_valid = TRUE AND feed_timestamp >= {start_epoch} AND feed_timestamp < {end_epoch}
        GROUP BY event_key
    """
    candidates = """
        SELECT c.*, s.prediction_observation_count, s.prediction_revision_count,
          s.delay_revision_count, s.minimum_published_delay_seconds,
          s.maximum_published_delay_seconds, s.first_published_delay_seconds,
          s.first_valid_feed_timestamp, s.last_valid_feed_timestamp,
          s.observation_span_seconds, s.published_delay_stddev_seconds,
          a.last_observed_feed_timestamp, a.invalid_prediction_record_count,
          a.distinct_input_prediction_count,
          CASE WHEN a.last_observed_feed_timestamp > c.feed_timestamp THEN TRUE ELSE FALSE END AS has_newer_invalid_observation
        FROM changes c
        JOIN revision_summary_base s ON c.event_key = s.event_key
        JOIN observed_summary a ON c.event_key = a.event_key
        WHERE c.latest_rank = 1
    """
    lineage_unique = """
        SELECT prediction_signature, lineage_id, raw_object_key, payload_sha256,
          input_object_key, ingested_at_utc,
          MAX(source_row_count) AS source_row_count,
          COUNT(DISTINCT upstream_run_id) AS upstream_run_occurrences
        FROM lineage_input
        GROUP BY prediction_signature, lineage_id, raw_object_key, payload_sha256,
          input_object_key, ingested_at_utc
    """
    selected_lineage = """
        SELECT e.event_key, e.prediction_signature, l.lineage_id,
          l.raw_object_key, l.payload_sha256, l.input_object_key,
          l.ingested_at_utc, l.source_row_count, l.upstream_run_occurrences
        FROM event_candidates_base e JOIN lineage_unique l
          ON e.prediction_signature = l.prediction_signature
    """
    return {
        "replay_conflicts": replay_conflicts, "replay_ranked": replay_ranked,
        "history_unique": unique, "observations": classified, "eligible": eligible,
        "point_conflicts": point_conflicts, "blocked_events": blocked,
        "safe_eligible": safe, "point_ranked": point_ranked, "timeline": timeline,
        "changes": changes, "revision_summary_base": summary,
        "observed_summary": observed_summary, "event_candidates_base": candidates,
        "lineage_unique": lineage_unique, "selected_capture_lineage": selected_lineage,
    }


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
    from pyspark import StorageLevel
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "DATA_BUCKET", "INPUT_RUN_IDS", "RUN_ID",
        "WINDOW_START_UTC", "WINDOW_END_UTC", "RULE_VERSION",
    ])
    config = configuration(args["DATA_BUCKET"], args["INPUT_RUN_IDS"], args["RUN_ID"],
                           args["WINDOW_START_UTC"], args["WINDOW_END_UTC"], args["RULE_VERSION"])
    bucket = args["DATA_BUCKET"]
    s3 = boto3.client("s3")
    prefix = config["output_root"].split(f"s3://{bucket}/", 1)[1]
    if s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1).get("KeyCount", 0):
        raise ValueError("Output RUN_ID already exists. No overwrite is permitted.")
    upstream = []
    for label, root in zip(config["input_run_ids"], config["input_roots"]):
        key = root.split(f"s3://{bucket}/", 1)[1]
        s3.head_object(Bucket=bucket, Key=key + "_EXPERIMENT_SUCCESS")
        report_object = s3.get_object(Bucket=bucket, Key=key + "report.json")
        report = json.loads(report_object["Body"].read())
        if report.get("status") != "success" or report.get("run_id") != label:
            raise ValueError(f"Upstream run {label} has no matching successful report")
        if report.get("final_published_prediction_selected") is not False:
            raise ValueError("Expected preserved prediction history, not preselected events")
        files = []
        for suffix in ["prediction_history/", "capture_lineage/"]:
            files.extend({"key": obj["Key"], "bytes": obj["Size"], "etag": obj["ETag"],
                          "last_modified": obj["LastModified"].isoformat()}
                         for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key + suffix)
                         for obj in page.get("Contents", []) if obj["Key"].endswith(".parquet"))
            if not any(f["key"].startswith(key + suffix) for f in files):
                raise ValueError(f"Missing completed Parquet dataset: {root}{suffix}")
        upstream.append({"run_id": label, "root": root, "report_etag": report_object["ETag"],
                         "report": report, "objects": files})
    manifest = {"arguments": args, "rule": config, "upstream_runs": upstream,
                "created_at_utc": datetime.now(timezone.utc).isoformat()}
    s3.put_object(Bucket=bucket, Key=prefix + "input_manifest.json",
                  Body=(json.dumps(manifest, indent=2) + "\n").encode(), ContentType="application/json")

    context = GlueContext(SparkContext.getOrCreate())
    spark = context.spark_session
    job = Job(context)
    job.init(args["JOB_NAME"], args)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    spark.conf.set("spark.sql.shuffle.partitions", "32")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    started = perf_counter()
    counts, times = {}, {}

    def materialize(name, frame):
        tick = perf_counter()
        frame = frame.persist(StorageLevel.MEMORY_AND_DISK)
        counts[name] = frame.count()
        times[name] = round(perf_counter() - tick, 3)
        frame.createOrReplaceTempView(name)
        print(json.dumps({"stage": name, "rows": counts[name], "seconds": times[name]}))
        return frame

    frames = []
    lineage_paths = []
    for item in upstream:
        frame = spark.read.parquet(item["root"] + "prediction_history/").select(*HISTORY_COLUMNS)
        frame = frame.withColumn("upstream_run_id", F.lit(item["run_id"]))
        if frame.count() != item["report"]["counts"]["prediction_history"]:
            raise ValueError(f"Upstream Parquet/report count mismatch: {item['run_id']}")
        frames.append(frame)
        lineage_paths.append((item["root"] + "capture_lineage/", item["run_id"]))
    history = frames[0]
    for frame in frames[1:]:
        history = history.unionByName(frame)
    history = history.withColumn("effective_stop_sequence", F.coalesce("stop_sequence", "static_stop_sequence").cast("long"))
    required_key = ["source", "service_date", "trip_id", "stop_id", "event_type", "feed_version"]
    valid_key = F.lit(True)
    for column in required_key:
        valid_key = valid_key & F.col(column).isNotNull() & (F.length(F.trim(F.col(column))) > 0)
    valid_key = (valid_key & F.col("effective_stop_sequence").isNotNull()
                 & (F.col("effective_stop_sequence") >= 0)
                 & F.col("service_date").rlike(r"^\d{8}$")
                 & F.to_date("service_date", "yyyyMMdd").isNotNull())
    history = history.withColumn("event_key_valid", valid_key)
    json_options = {"ignoreNullFields": "false"}

    def fingerprint(columns):
        return F.sha2(F.to_json(F.struct(*[F.col(c) for c in columns]), json_options), 256)

    history = history.withColumn("event_key", fingerprint(EVENT_KEY_COLUMNS))
    history = history.withColumn("value_signature", fingerprint(VALUE_COLUMNS))
    history = history.withColumn("replay_content_signature", fingerprint(REPLAY_CONTENT_COLUMNS))
    history = materialize("history_input", history)
    if not counts["history_input"]:
        raise ValueError("No prediction history rows in the selected upstream runs")
    # Missing signatures cannot be safely used as replay keys.
    if history.filter("prediction_signature IS NULL OR trim(prediction_signature) = ''").limit(1).count():
        raise ValueError("Upstream history has missing prediction signatures")
    plan = sql_plan(config["start_epoch"], config["end_epoch"])
    spark.sql(plan["replay_conflicts"]).createOrReplaceTempView("replay_conflicts")
    if spark.table("replay_conflicts").limit(1).count():
        raise ValueError("Conflicting copies of an upstream prediction. Select one reference-consistent run per partition.")
    spark.sql(plan["replay_ranked"]).createOrReplaceTempView("replay_ranked")
    unique = materialize("history_unique", spark.sql(plan["history_unique"]))
    history.unpersist()
    observations = materialize("observations", spark.sql(plan["observations"]))
    unique.unpersist()
    eligible = materialize("eligible", spark.sql(plan["eligible"]))
    spark.sql(plan["point_conflicts"]).createOrReplaceTempView("point_conflicts")
    blocked = materialize("blocked_events", spark.sql(plan["blocked_events"]))
    for name in ["safe_eligible", "point_ranked"]:
        spark.sql(plan[name]).createOrReplaceTempView(name)
    timeline = materialize("timeline", spark.sql(plan["timeline"]))
    for name in ["changes", "revision_summary_base", "observed_summary"]:
        spark.sql(plan[name]).createOrReplaceTempView(name)
    candidates = materialize("event_candidates_base", spark.sql(plan["event_candidates_base"]))
    if candidates.select("event_key").distinct().count() != counts["event_candidates_base"]:
        raise RuntimeError("Event output is not unique by event_key")
    summarized_observations = candidates.agg(F.sum("prediction_observation_count")).first()[0] or 0
    if summarized_observations != counts["timeline"]:
        raise RuntimeError("Event summaries do not reconcile to the prediction timeline")

    # Load only the selected predictions' capture mappings. Overlapping job-1
    # runs are deduplicated so reruns do not inflate raw-capture counts.
    selected_signatures = candidates.select("prediction_signature").distinct()
    lineage_frames = []
    for path, label in lineage_paths:
        frame = spark.read.parquet(path).select(
            "prediction_signature", "lineage_id", "raw_object_key", "payload_sha256",
            "input_object_key", "ingested_at_utc", "source_row_count",
        ).join(selected_signatures, "prediction_signature", "left_semi")
        lineage_frames.append(frame.withColumn("upstream_run_id", F.lit(label)))
    lineage = lineage_frames[0]
    for frame in lineage_frames[1:]:
        lineage = lineage.unionByName(frame)
    lineage.createOrReplaceTempView("lineage_input")
    spark.sql(plan["lineage_unique"]).createOrReplaceTempView("lineage_unique")
    selected_lineage = materialize("selected_capture_lineage", spark.sql(plan["selected_capture_lineage"]))
    uncovered = selected_signatures.join(selected_lineage.select("prediction_signature").distinct(), "prediction_signature", "left_anti").count()
    if uncovered:
        raise RuntimeError("Selected events are missing upstream raw capture lineage")

    def annotate(frame):
        return (frame.withColumn("selection_rule_version", F.lit(RULE_VERSION))
                .withColumn("selection_window_start_utc", F.lit(args["WINDOW_START_UTC"]))
                .withColumn("selection_window_end_utc_exclusive", F.lit(args["WINDOW_END_UTC"]))
                .withColumn("selection_status", F.lit("latest_valid_in_selected_window"))
                .withColumn("is_final_published_prediction", F.lit(False)))

    events = annotate(candidates.drop("replay_rank", "point_rank", "timeline_number", "latest_rank",
                                     "previous_value_signature", "previous_delay_seconds"))
    summary = events.select(*EVENT_KEY_COLUMNS, "event_key", "prediction_signature",
                            "prediction_observation_count", "prediction_revision_count", "delay_revision_count",
                            "first_published_delay_seconds", "minimum_published_delay_seconds", "maximum_published_delay_seconds",
                            F.col("delay_seconds").alias("selected_published_delay_seconds"),
                            "first_valid_feed_timestamp", "last_valid_feed_timestamp", "last_observed_feed_timestamp",
                            "invalid_prediction_record_count", "distinct_input_prediction_count",
                            "observation_span_seconds", "published_delay_stddev_seconds", "has_newer_invalid_observation",
                            "selection_rule_version", "selection_window_start_utc", "selection_window_end_utc_exclusive",
                            "selection_status", "is_final_published_prediction")
    rejected = observations.filter("selection_reason <> 'eligible'")
    blocked_rows = eligible.join(blocked.select("event_key").distinct(), "event_key", "inner")
    quality = observations.groupBy("selection_reason").count()
    report = {
        "run_id": args["RUN_ID"], "input_run_ids": config["input_run_ids"],
        "selection_window": {"start_utc": args["WINDOW_START_UTC"], "end_utc_exclusive": args["WINDOW_END_UTC"]},
        "selection_rule_version": RULE_VERSION, "selection_status": "latest_valid_in_selected_window",
        "window_time_basis": "feed_timestamp; start inclusive, end exclusive",
        "final_published_prediction_selected": False,
        "counts": counts, "stage_materialization_seconds": times,
        "replayed_history_rows_removed": counts["history_input"] - counts["history_unique"],
        "valid_observation_rows_collapsed_to_events": counts["timeline"] - counts["event_candidates_base"],
        "selection_reasons": [r.asDict() for r in quality.collect()],
        "blocked_event_count": blocked.select("event_key").distinct().count(),
        "selected_zero_delay_events": events.filter("delay_seconds = 0").count(),
        "selected_events_with_newer_invalid_observations": events.filter("has_newer_invalid_observation").count(),
        "events_with_prediction_revisions": events.filter("prediction_revision_count > 0").count(),
        "source_scope": "only explicitly supplied successful job-1 runs",
        "silver_history_modified": False,
        "warnings": ["Provisional selection. Journey-wide finalisation and observation-window completeness are not certified."],
    }
    if sum(row["count"] for row in report["selection_reasons"]) != counts["history_unique"]:
        raise RuntimeError("Selection/exclusion counts do not reconcile")
    if counts["eligible"] < counts["timeline"] or counts["timeline"] < counts["event_candidates_base"]:
        raise RuntimeError("Unexpected event/timeline row expansion")
    for name, frame in [("event_candidates", events), ("revision_summary", summary),
                        ("selected_capture_lineage", annotate(selected_lineage)),
                        ("excluded_observations", rejected), ("blocked_event_observations", blocked_rows),
                        ("quality/selection_reasons", quality), ("quality/blocked_events", blocked)]:
        frame.coalesce(4 if name in {"event_candidates", "revision_summary", "selected_capture_lineage"} else 1).write.mode("errorifexists").parquet(config["output_root"] + name + "/")
    report["total_job_script_seconds"] = round(perf_counter() - started, 3)
    report["status"] = "success"
    s3.put_object(Bucket=bucket, Key=prefix + "report.json",
                  Body=(json.dumps(report, indent=2, default=str) + "\n").encode(), ContentType="application/json")
    job.commit()
    s3.put_object(Bucket=bucket, Key=prefix + "_EXPERIMENT_SUCCESS", Body=b"")
    print(json.dumps(report, default=str))


if __name__ == "__main__":
    main()
