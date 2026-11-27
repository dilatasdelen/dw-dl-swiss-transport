"""Build research summaries from one approved Gold snapshot.

Temperature update: research-summary-v2-temperature.
RQ3 uses an independent hourly temperature band, so rain/snow/wind cannot
hide cold or hot observations. Thresholds are analysis choices inspired by
MeteoSwiss daily climate indicators, NOT official hourly warning thresholds.
No AWS processing is started by uploading this file. Test in a separate output
prefix before replacing tables used by PostgreSQL or Tableau.

RQ4 still uses the original descriptive rankings. Persistence rules are a
separate change and are NOT implemented in this temperature-only update.
"""

import json
import sys
from datetime import datetime, timezone


COLUMNS = {
    "source_type": "source_type",
    "time_status": "time_status",
    "event_type": "event_type",
    "service_date": "service_date",
    "event_time_utc": "scheduled_time_utc",
    "delay_s": "delay_seconds",
    "station_id": "didok",
    "canton": "study_canton_code",
    "elevation_m": "elevation_m",
    "temperature_c": "temperature_2m_c",
    "precipitation_mm": "precipitation_mm",
    "snowfall_cm": "snowfall_cm",
    "wind_kmh": "wind_speed_10m_kmh",
    "missing_weather": "missing_weather",
}
OPTIONAL_COLUMNS = {
    "line": ("line_id", "string"),
    "is_cancelled": ("is_cancelled", "boolean"),
}


SUMMARY_RULE_VERSION = "research-summary-v2-temperature"


DEFINITIONS = {
    "summary_rule_version": SUMMARY_RULE_VERSION,
    "temperature_comparison": {
        "units": "degrees Celsius; event-hour temperature_2m_c",
        "bands": {"cold": "< 0", "normal": ">= 0 and < 25", "hot": ">= 25",
                  "unknown": "missing joined weather, null or NaN temperature"},
        "rationale": "Freezing and summer-day boundaries provide interpretable physical/climatological cutoffs; hourly adaptation is a project rule, not an official daily indicator or heat alert.",
        "reference": "https://www.meteoswiss.admin.ch/dam/jcr%3A50da2b8f-3a0b-4082-a3b9-a26cd9525670/CH2025_Uebersicht_Klimaindikatoren_en.pdf",
        "concurrent_conditions": "Report all events and strata none/rain/snow/strong_wind combinations/unknown; compare temperature bands only within the same stratum and elevation group.",
        "reference_group": "normal-temperature events in the same source, status, event type, elevation group and concurrent-weather stratum",
        "minimum": "30 usable events on each compared side, plus observed date coverage; descriptive association, not a causal weather effect",
    },
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
    "hotspot_scope": "Original descriptive rankings only; persistence is not implemented in this version",
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
    # Temperature is independent of the priority-based RQ1 weather category.
    # A snowy -5 C hour remains cold; a rainy 28 C hour remains hot.
    temperature_known = (~F.coalesce(F.col("missing_weather"), F.lit(True))
                         & F.col("temperature_c").isNotNull()
                         & ~F.isnan("temperature_c"))
    temperature_band = (F.when(~temperature_known, "unknown")
                        .when(F.col("temperature_c") < 0, "cold")
                        .when(F.col("temperature_c") >= 25, "hot")
                        .otherwise("normal"))

    # Use a separate combination label for concurrent conditions. Missing one
    # measurement prevents us from claiming that the hour had no other hazard.
    concurrent_known = ~F.coalesce(F.col("missing_weather"), F.lit(True))
    for field in ("precipitation_mm", "snowfall_cm", "wind_kmh"):
        concurrent_known = (concurrent_known & F.col(field).isNotNull() & ~F.isnan(field))
    hazards = F.concat_ws("+", F.when(rain, "rain"), F.when(snow, "snow"),
                         F.when(wind, "strong_wind"))
    concurrent = (F.when(~concurrent_known, "unknown")
                  .when(F.length(hazards) == 0, "none").otherwise(hazards))
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
        temperature_band.alias("temperature_band"),
        concurrent.alias("concurrent_weather"),
        F.when(category.isin("normal", "no_weather"), F.lit(False))
         .otherwise(F.lit(True)).alias("is_adverse_weather"),
        elevation.alias("elevation_group"),
        F.hour(local).alias("hour_of_day_local"),
        F.date_format(local, "EEEE").alias("weekday_local"),
        (F.coalesce("is_cancelled", F.lit(False)) | F.col("delay_s").isNull()).alias("excluded_from_metrics"),
    )


def metrics(frame, group_by):
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
         / F.when(F.sum(usable.cast("long")) > 0, F.sum(usable.cast("long")))).alias("severe_delay_rate"),
        F.countDistinct("service_date").alias("observed_dates"),
        F.countDistinct(F.when(usable, F.col("service_date"))).alias("usable_dates"),
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
    from pyspark.sql import Window, functions as F

    keys = SOURCE + ["station_id", "canton", "elevation_group"]
    compared = compare_normal_adverse(events, keys)
    rank = Window.partitionBy(*SOURCE).orderBy(F.desc_nulls_last("median_delay_difference_s"),
                                                F.desc_nulls_last("severe_rate_difference"))
    ranked = compared.where("meets_minimum_observations").withColumn("difference_rank", F.row_number().over(rank))
    return compared.join(ranked.select(*keys, "difference_rank"), keys, "left")


def elevation_patterns(events):
    """RQ3: counts/metrics and explicit hot/cold-minus-normal differences.

    Unknown temperatures stay visible in the band summary. Comparison rows
    retain sparse or absent normal references with a false sufficiency flag.
    Actual source statuses remain separate through SOURCE in every group key.
    """
    from pyspark.sql import functions as F

    # Two views: all concurrent conditions pooled, and combinations stratified.
    # Consumers must select one view, rather than summing both and double counting.
    pooled = events.withColumn("comparison_scope", F.lit("all_conditions")) \
                   .withColumn("concurrent_weather", F.lit("all"))
    stratified = events.withColumn("comparison_scope", F.lit("concurrent_weather_stratum"))
    expanded = pooled.unionByName(stratified)
    keys = SOURCE + ["elevation_group", "comparison_scope", "concurrent_weather"]
    bands = with_min_obs(metrics(expanded, keys + ["temperature_band"]), "observation_count")

    # Left join each band to its own normal-temperature baseline. All key
    # fields are non-null prepared group labels, so equality joins retain them.
    baseline_fields = [c for c in bands.columns if c not in keys + ["temperature_band", "meets_minimum_observations"]]
    normal = bands.where(F.col("temperature_band") == "normal").select(
        *keys, *[F.col(c).alias("normal_" + c) for c in baseline_fields])
    result = bands.join(normal, keys, "left")
    for output, metric in [("median_delay_difference_s", "median_delay_s"),
                           ("p90_delay_difference_s", "p90_delay_s"),
                           ("severe_rate_difference", "severe_delay_rate")]:
        result = result.withColumn(output, F.col(metric) - F.col("normal_" + metric))
    enough = ((F.col("observation_count") >= DEFINITIONS["minimum_observations"])
              & (F.coalesce(F.col("normal_observation_count"), F.lit(0))
                 >= DEFINITIONS["minimum_observations"])
              & F.col("temperature_band").isin("hot", "cold"))
    return (result.withColumn("comparison_meets_minimum_observations", enough)
            .withColumn("sparse_comparison", ~enough))


def network_hotspots(events):
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
    "elevation_patterns": ("RQ3: How do published delays under hot and cold temperatures differ from normal within elevation groups?", elevation_patterns),
    "network_hotspots": ("RQ4 descriptive precursor: Which stops, lines and time periods have high published delays? Persistence pending.", network_hotspots),
}


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark import StorageLevel
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F

    names = ["JOB_NAME", "DATA_BUCKET"] + [name for name in
        ("MANIFEST_KEY", "OUTPUT_PREFIX", "REPORT_KEY", "START_DATE", "END_DATE") if "--" + name in sys.argv]
    args = getResolvedOptions(sys.argv, names)
    bucket = args["DATA_BUCKET"]
    manifest_key = args.get("MANIFEST_KEY")
    output_prefix = args.get("OUTPUT_PREFIX", "gold/analysis_temperature_pilot").strip("/")
    report_key = args.get("REPORT_KEY", "quality/gold_analysis_temperature_pilot/report.json")
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
    s3 = boto3.client("s3")
    created_at = datetime.now(timezone.utc).isoformat()

    # Resolve the approved pointer once. Pin the immutable manifest in the
    # report so a later weekly refresh cannot change this run's source lineage.
    if manifest_key is None:
        pointer = json.loads(s3.get_object(
            Bucket=bucket, Key="quality/stop_event_analysis_audit/latest_approved.json")["Body"].read())
        uri = pointer["input_manifest"]
        prefix = f"s3://{bucket}/"
        if not uri.startswith(prefix):
            raise ValueError("Approved manifest must belong to the configured bucket")
        manifest_key = uri[len(prefix):]
    manifest = json.loads(s3.get_object(Bucket=bucket, Key=manifest_key)["Body"].read())
    if manifest.get("status") != "passed" or manifest.get("rule_version") != RULE_VERSION:
        raise ValueError(f"Manifest not approved for {RULE_VERSION}: {manifest.get('status')}")
    # Filter manifest entries BEFORE opening Parquet files. Filtering a DataFrame
    # afterwards can still scan the full year across separately stored run paths.
    # This summer week exercises temperature handling; hot-band availability is
    # checked in the results, never assumed or manufactured for the pilot.
    start_date = args.get("START_DATE", "2026-07-13")
    end_date = args.get("END_DATE", "2026-07-19")
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if end < start or (end - start).days > 6:
        raise ValueError("Temperature pilot accepts at most seven inclusive dates")
    selected = [item for item in manifest["inputs"]
                if start_date <= item["service_date"] <= end_date]
    paths = [item["output"] for item in selected]
    if len(paths) != len({item["service_date"] for item in selected}):
        raise ValueError("Manifest lists more than one output for a service date")
    if not paths:
        raise ValueError("Approved manifest contains no event paths")
    # Old historical outputs lack route_id; merge schemas rather than allowing
    # the first Parquet file to silently determine the shared schema.
    raw = spark.read.option("mergeSchema", "true").parquet(*paths)
    missing = [source for source in COLUMNS.values() if source not in raw.columns]
    if missing:
        raise ValueError(f"Gold event dataset lacks columns {missing}; adjust COLUMNS. Found: {raw.columns}")
    events = prepare(raw).persist(StorageLevel.MEMORY_AND_DISK)

    report = {"summary_rule_version": SUMMARY_RULE_VERSION, "status": "success", "created_at_utc": created_at, "definitions": DEFINITIONS,
              "input_manifest": f"s3://{bucket}/{manifest_key}",
              "input_dates": len(paths), "selected_service_dates": [item["service_date"] for item in selected],
              "requested_date_range": [start_date, end_date],
              "scope": "temperature-only pilot; does not validate all seasons or GTFS-RT coverage",
              "events_by_source": {}, "outputs": {}}
    for row in events.groupBy("source_type", "status_group", "event_type").agg(
            F.count(F.lit(1)).alias("events"),
            F.sum((~F.col("excluded_from_metrics")).cast("long")).alias("usable"),
            F.min("service_date").alias("start"), F.max("service_date").alias("end")).collect():
        report["events_by_source"][f"{row['source_type']}/{row['status_group']}/{row['event_type']}"] = {
            "events": row["events"], "usable_for_delay_metrics": row["usable"],
            "date_range": [str(row["start"]), str(row["end"])]}

    # Build only RQ3 here. The other three full summaries are intentionally not
    # refreshed by this isolated temperature validation job.
    for name in ("elevation_patterns",):
        question, build = OUTPUTS[name]
        table = build(events).withColumn("research_question", F.lit(question)) \
                             .withColumn("created_at_utc", F.lit(created_at))
        path = f"s3://{bucket}/{output_prefix}/{name}/"
        table.coalesce(1).write.mode("overwrite").parquet(path)
        report["outputs"][name] = {"research_question": question, "rows": spark.read.parquet(path).count(),
                                   "path": path}

    s3.put_object(Bucket=bucket, Key=f"{output_prefix}/_definitions.json", ContentType="application/json",
                  ServerSideEncryption="AES256", Body=(json.dumps(DEFINITIONS, indent=2) + "\n").encode())
    s3.put_object(Bucket=bucket, Key=report_key, ContentType="application/json",
                  ServerSideEncryption="AES256", Body=(json.dumps(report, indent=2, default=str) + "\n").encode())
    events.unpersist()
    print(json.dumps(report, default=str))


if __name__ == "__main__":
    main()
