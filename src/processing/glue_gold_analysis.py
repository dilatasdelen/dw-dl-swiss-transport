"""
Input:  s3://<bucket>/gold/stop_event_analysis/         
Output: s3://<bucket>/gold/analysis/weather_impact/      RQ1 normal vs adverse weather
        s3://<bucket>/gold/analysis/stop_differences/    RQ2 stops with the largest weather difference
        s3://<bucket>/gold/analysis/elevation_patterns/  RQ3 elevation groups x weather
        s3://<bucket>/gold/analysis/network_hotspots/    RQ4 stops, lines, hours, weekdays
        s3://<bucket>/gold/analysis/_definitions.json    rules used (also in every report)
Report: s3://<bucket>/quality/gold_analysis/report.json

Every summary row is split by source_type, time_status and event_type (arrival/departure),
so GTFS-RT predictions are never mixed with Ist-Daten measurements, and carries its
observation count and date range.

Job parameters:
  --DATA_BUCKET   datavault-v2
"""

import json
import sys
from datetime import datetime, timezone


COLUMNS = {
    "source_type": "source_type",
    "time_status": "time_status",          # Ist-Daten: REAL/GESCHAETZT/PROGNOSE/UNBEKANNT; GTFS-RT: selection status
    "event_type": "event_type",            # arrival or departure
    "service_date": "service_date",
    "event_time_utc": "scheduled_time_utc",
    "delay_s": "delay_seconds",
    "station_id": "didok",                 # Gold has no station name; stations are identified by didok
    "canton": "study_canton_code",
    "elevation_m": "elevation_m",
    "temperature_c": "temperature_2m_c",
    "precipitation_mm": "precipitation_mm",
    "snowfall_cm": "snowfall_cm",
    "wind_kmh": "wind_speed_10m_kmh",
    "missing_weather": "missing_weather",  
}
# Columns only present for Ist-Daten. Missing columns become NULL instead of failing the job.
OPTIONAL_COLUMNS = {
    "line": ("line_id", "string"),
    "is_cancelled": ("is_cancelled", "boolean"),
}

# Analysis rules (documented in _definitions.json and every report)
DEFINITIONS = {
    "delay": "delay_seconds of each stop event; cancelled events and events without a delay are excluded from delay metrics but counted",
    "severe_delay_threshold_seconds": 180,
    "severe_delay_rationale": "3 minutes, the threshold Swiss rail punctuality statistics use for 'late'",
    "weather_source": "gtfs_rt uses the forecast available at prediction time; istdaten uses ERA5 historical reanalysis",
    "weather_categories_in_priority_order": [
        {"category": "snow", "rule": "snowfall_cm >= 0.1 in the event hour"},
        {"category": "rain", "rule": "precipitation_mm >= 1.0 in the event hour"},
        {"category": "strong_wind", "rule": "wind_speed_10m_kmh >= 39 (Beaufort 6)"},
        {"category": "frost", "rule": "temperature_2m_c <= 0"},
        {"category": "normal", "rule": "none of the above"},
        {"category": "no_weather", "rule": "missing_weather is true or no weather value joined"},
    ],
    "adverse_weather": "any category except normal and no_weather",
    "elevation_groups_m": {"low": "< 500", "medium": "500 to < 1000", "high": ">= 1000", "unknown": "no elevation"},
    "minimum_observations": 30,
    "minimum_observations_rule": "rankings and differences use only groups with at least this many events on each compared side; smaller groups stay in the output with meets_minimum_observations = false",
    "time_zone_for_hour_and_weekday": "Europe/Zurich",
    "delay_meaning": {"gtfs_rt": "published_prediction", "REAL": "measured", "GESCHAETZT": "estimated",
                      "PROGNOSE": "forecast_in_source", "UNBEKANNT": "unknown"},
    "percentiles": "approximate (Spark percentile_approx, accuracy 10000)",
}

MANIFEST_KEY = "quality/stop_event_analysis_audit/run_id=e85ad74b72a34f148645542a73dd8cfe/input_manifest.json"
RULE_VERSION = "gold-events-v2"
OUTPUT_PREFIX = "gold/analysis"
REPORT_KEY = "quality/gold_analysis/report.json"


def prepare(events):
    """Rename to the logical names and add weather category, adverse flag, elevation group."""
    from pyspark.sql import functions as F

    rules = DEFINITIONS
    columns = [F.col(source).alias(name) for name, source in COLUMNS.items()]
    for name, (source, kind) in OPTIONAL_COLUMNS.items():
        columns.append((F.col(source) if source in events.columns else F.lit(None)).cast(kind).alias(name))
    frame = events.select(*columns)
    snow, rain, wind, frost = (F.col("snowfall_cm") >= 0.1, F.col("precipitation_mm") >= 1.0,
                               F.col("wind_kmh") >= 39, F.col("temperature_c") <= 0)

    has_weather = ~F.coalesce("missing_weather", F.lit(True)) & (
        F.col("temperature_c").isNotNull() | F.col("precipitation_mm").isNotNull())
    category = (F.when(~has_weather, "no_weather").when(snow, "snow").when(rain, "rain")
                 .when(wind, "strong_wind").when(frost, "frost").otherwise("normal"))
    elevation = (F.when(F.col("elevation_m").isNull(), "unknown")
                  .when(F.col("elevation_m") < 500, "low")
                  .when(F.col("elevation_m") < 1000, "medium").otherwise("high"))
    local = F.from_utc_timestamp("event_time_utc", rules["time_zone_for_hour_and_weekday"])
    meaning = F.when(F.col("source_type") == "gtfs_rt", "published_prediction")
    for status, label in rules["delay_meaning"].items():
        if status != "gtfs_rt":
            meaning = meaning.when(F.col("time_status") == status, label)
    return frame.select(
        "*",
        F.coalesce("time_status", F.lit("NONE")).alias("status_group"),
        meaning.otherwise("unknown").alias("delay_meaning"),
        category.alias("weather_category"),
        F.when(category.isin("normal", "no_weather"), F.lit(False))
         .otherwise(F.lit(True)).alias("is_adverse_weather"),
        elevation.alias("elevation_group"),
        F.hour(local).alias("hour_of_day_local"),
        F.date_format(local, "EEEE").alias("weekday_local"),
        (F.coalesce("is_cancelled", F.lit(False)) | F.col("delay_s").isNull()).alias("excluded_from_metrics"),
    )


def metrics(frame, group_by):
    """Observation counts and delay metrics per group (delay metrics only on usable events)."""
    from pyspark.sql import functions as F

    usable = ~F.col("excluded_from_metrics")
    delay = F.when(usable, F.col("delay_s"))
    threshold = DEFINITIONS["severe_delay_threshold_seconds"]
    return frame.groupBy(*group_by).agg(
        F.count(F.lit(1)).alias("events_total"),
        F.sum(usable.cast("long")).alias("observation_count"),
        F.sum(F.coalesce("is_cancelled", F.lit(False)).cast("long")).alias("cancelled_count"),
        F.percentile_approx(delay, 0.5, 10000).alias("median_delay_s"),
        F.percentile_approx(delay, 0.9, 10000).alias("p90_delay_s"),
        F.avg(delay).alias("mean_delay_s"),
        (F.sum(F.when(usable & (F.col("delay_s") >= threshold), 1).otherwise(0))
         / F.sum(usable.cast("long"))).alias("severe_delay_rate"),
        F.min("service_date").alias("date_range_start"),
        F.max("service_date").alias("date_range_end"),
    )


def with_min_obs(frame, *count_columns):
    from pyspark.sql import functions as F

    minimum = DEFINITIONS["minimum_observations"]
    condition = F.lit(True)
    for column in count_columns:
        condition = condition & (F.coalesce(F.col(column), F.lit(0)) >= minimum)
    return frame.withColumn("meets_minimum_observations", condition)


SOURCE = ["source_type", "status_group", "delay_meaning", "event_type"]


def weather_impact(events):
    """RQ1: per weather category, and normal vs any adverse weather."""
    from pyspark.sql import functions as F

    by_category = (metrics(events, SOURCE + ["weather_category"])
                   .withColumn("grouping", F.lit("weather_category"))
                   .withColumnRenamed("weather_category", "weather_group"))
    by_adverse = (metrics(events.where(F.col("weather_category") != "no_weather"),
                          SOURCE + ["is_adverse_weather"])
                  .withColumn("grouping", F.lit("adverse_vs_normal"))
                  .withColumn("weather_group", F.when(F.col("is_adverse_weather"), "adverse").otherwise("normal"))
                  .drop("is_adverse_weather"))
    return with_min_obs(by_category.unionByName(by_adverse), "observation_count")


def compare_normal_adverse(events, keys):
    """Per group: metrics under normal and adverse weather side by side, plus differences."""
    from pyspark.sql import functions as F

    base = events.where(F.col("weather_category") != "no_weather")
    normal = metrics(base.where(~F.col("is_adverse_weather")), keys)
    adverse = metrics(base.where(F.col("is_adverse_weather")), keys)
    value_columns = [c for c in normal.columns if c not in keys]
    joined = (normal.select(*keys, *[F.col(c).alias(f"normal_{c}") for c in value_columns])
              .join(adverse.select(*keys, *[F.col(c).alias(f"adverse_{c}") for c in value_columns]),
                    keys, "full"))
    return with_min_obs(
        joined.withColumn("median_delay_difference_s",
                          F.col("adverse_median_delay_s") - F.col("normal_median_delay_s"))
              .withColumn("severe_rate_difference",
                          F.col("adverse_severe_delay_rate") - F.col("normal_severe_delay_rate")),
        "normal_observation_count", "adverse_observation_count")


def stop_differences(events):
    """RQ2: stops ranked by the median-delay difference between adverse and normal weather."""
    from pyspark.sql import Window, functions as F

    keys = SOURCE + ["station_id", "canton", "elevation_group"]
    compared = compare_normal_adverse(events, keys)
    rank = Window.partitionBy(*SOURCE).orderBy(F.desc_nulls_last("median_delay_difference_s"),
                                                F.desc_nulls_last("severe_rate_difference"))
    ranked = compared.where("meets_minimum_observations").withColumn("difference_rank", F.row_number().over(rank))
    return compared.join(ranked.select(*keys, "difference_rank"), keys, "left")


def elevation_patterns(events):
    """RQ3: elevation groups x weather category (incl. frost and snow)."""
    return with_min_obs(metrics(events, SOURCE + ["elevation_group", "weather_category"]), "observation_count")


def network_hotspots(events):
    """RQ4: stops, lines, hours and weekdays, overall and under adverse vs normal weather."""
    from pyspark.sql import Window, functions as F

    parts = []
    for dimension, column in [("station", "station_id"), ("line", "line"),
                              ("hour_of_day", "hour_of_day_local"), ("weekday", "weekday_local")]:
        keyed = events.withColumn("dimension", F.lit(dimension)) \
                      .withColumn("dimension_value", F.col(column).cast("string"))
        keys = SOURCE + ["dimension", "dimension_value"]
        overall = with_min_obs(metrics(keyed, keys), "observation_count")
        compared = compare_normal_adverse(keyed, keys).select(
            *keys, "normal_observation_count", "adverse_observation_count",
            "normal_median_delay_s", "adverse_median_delay_s",
            "normal_severe_delay_rate", "adverse_severe_delay_rate",
            "median_delay_difference_s", "severe_rate_difference")
        parts.append(overall.join(compared, keys, "left"))
    combined = parts[0]
    for part in parts[1:]:
        combined = combined.unionByName(part)
    rank = Window.partitionBy(*SOURCE, "dimension").orderBy(F.desc_nulls_last("severe_delay_rate"),
                                                            F.desc_nulls_last("median_delay_s"))
    ranked = combined.where("meets_minimum_observations").withColumn("hotspot_rank", F.row_number().over(rank))
    keys = SOURCE + ["dimension", "dimension_value"]
    return combined.join(ranked.select(*keys, "hotspot_rank"), keys, "left")


OUTPUTS = {
    "weather_impact": ("RQ1: How do delays differ between normal and adverse weather?", weather_impact),
    "stop_differences": ("RQ2: Which stops show the largest weather-related differences?", stop_differences),
    "elevation_patterns": ("RQ3: Do cold and snow affect elevation groups differently?", elevation_patterns),
    "network_hotspots": ("RQ4: Which stops, lines and time periods are persistent hotspots?", network_hotspots),
}


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark import StorageLevel
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F

    names = ["JOB_NAME", "DATA_BUCKET"] + (["MANIFEST_KEY"] if "--MANIFEST_KEY" in sys.argv else [])
    args = getResolvedOptions(sys.argv, names)
    bucket = args["DATA_BUCKET"]
    manifest_key = args.get("MANIFEST_KEY", MANIFEST_KEY)
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    s3 = boto3.client("s3")
    created_at = datetime.now(timezone.utc).isoformat()

    # Read the approved manifest and refuse to run on an unapproved or outdated one.
    manifest = json.loads(s3.get_object(Bucket=bucket, Key=manifest_key)["Body"].read())
    if manifest.get("status") != "passed" or manifest.get("rule_version") != RULE_VERSION:
        raise ValueError(f"Manifest not approved for {RULE_VERSION}: {manifest.get('status')}")
    # One approved run folder per service date.
    paths = [item["output"] for item in manifest["inputs"]]
    if len(paths) != len({item["service_date"] for item in manifest["inputs"]}):
        raise ValueError("Manifest lists more than one output for a service date")
    raw = spark.read.parquet(*paths)
    missing = [source for source in COLUMNS.values() if source not in raw.columns]
    if missing:
        raise ValueError(f"Gold event dataset lacks columns {missing}; adjust COLUMNS. Found: {raw.columns}")
    # ~450M events: keep the prepared columns in memory, spilling to disk, for the many aggregations.
    events = prepare(raw).persist(StorageLevel.MEMORY_AND_DISK)

    # Run report: input counts per source/status/event type and the four outputs.
    report = {"status": "success", "created_at_utc": created_at, "definitions": DEFINITIONS,
              "input_manifest": f"s3://{bucket}/{manifest_key}",
              "input_dates": len(paths), "events_by_source": {}, "outputs": {}}
    for row in events.groupBy("source_type", "status_group", "event_type").agg(
            F.count(F.lit(1)).alias("events"),
            F.sum((~F.col("excluded_from_metrics")).cast("long")).alias("usable"),
            F.min("service_date").alias("start"), F.max("service_date").alias("end")).collect():
        report["events_by_source"][f"{row['source_type']}/{row['status_group']}/{row['event_type']}"] = {
            "events": row["events"], "usable_for_delay_metrics": row["usable"],
            "date_range": [str(row["start"]), str(row["end"])]}

    for name, (question, build) in OUTPUTS.items():
        table = build(events).withColumn("research_question", F.lit(question)) \
                             .withColumn("created_at_utc", F.lit(created_at))
        # Overwrite each summary table with this run's result (small, so one file each).
        path = f"s3://{bucket}/{OUTPUT_PREFIX}/{name}/"
        table.coalesce(1).write.mode("overwrite").parquet(path)
        report["outputs"][name] = {"research_question": question, "rows": spark.read.parquet(path).count(),
                                   "path": path}

    s3.put_object(Bucket=bucket, Key=f"{OUTPUT_PREFIX}/_definitions.json", ContentType="application/json",
                  ServerSideEncryption="AES256", Body=(json.dumps(DEFINITIONS, indent=2) + "\n").encode())
    s3.put_object(Bucket=bucket, Key=REPORT_KEY, ContentType="application/json",
                  ServerSideEncryption="AES256", Body=(json.dumps(report, indent=2, default=str) + "\n").encode())
    print(json.dumps(report, default=str))


if __name__ == "__main__":
    main()