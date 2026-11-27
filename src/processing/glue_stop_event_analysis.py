"""Build the Gold event-level analysis dataset in AWS Glue 5.0.

Choose either --SERVICE_DATE YYYY-MM-DD for one date, or --START_DATE
YYYY-MM-DD and --END_DATE YYYY-MM-DD for an inclusive batch. Both modes
require --DATA_BUCKET. Batch mode takes precedence if START_DATE is supplied.
Set --CHECK_ONLY true to report Silver coverage without building Gold events.
Coverage mode reads existing Silver data and publishes a small review report.

Weekly refresh configuration
- --WEEKLY true and --SOURCE_MODE both enable a moving date range.
- --LAG_DAYS defaults to 2, measured in Swiss calendar dates.
- --COLLECTION_END_DATE optionally caps future refreshes at a fixed date.
- --CHECK_ONLY must be false to build weekly outputs.
- New dates with processing-window, Ist-Daten, or forecast gaps are deferred.
- Weekly runs reuse only completed Silver selection outputs.
- The first route-aware refresh rebuilds the original GTFS-RT dates.
- Existing historical-only outputs remain reusable by the updated audit.
Set Glue maximum concurrent runs to 1 for this refresh job. After a successful
refresh, run the audit in WEEKLY mode to update the approved manifest pointer.
Deactivate the AWS scheduled trigger when recurring refreshes should stop.

Workflow
1. Find successful dates produced with the current rule version.
2. Process each remaining date sequentially.
3. Select the latest GTFS-RT prediction per event across Silver runs.
4. Convert each Ist-Daten stop visit into scheduled arrival/departure events.
5. Add versioned station, canton, elevation, and weather context.
6. Check event uniqueness and reconcile counts before writing.
7. Publish event Parquet first, then its successful report.
8. Release cached date data and continue with the next date.

Grain is one arrival or departure per source. The Gold key combines
source_type and event_key. The two transport sources are appended while
their source-specific status and lineage fields remain available.

Each run creates an immutable snapshot in its own run_id folder. Consumers
must select one successful snapshot per service date when reading Gold.
Missing context remains visible through null values and missing_* flags.
"""

from datetime import date, datetime, timedelta, timezone
import json
import sys
from uuid import uuid4
from zoneinfo import ZoneInfo

import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import Window, functions as F


# This label identifies the enrichment rules used to build an output.
# A successful date is skipped only when its report has this same version.
# Change this label when a logic change requires rebuilding previous dates.
RULE_VERSION = "gold-events-v3-route"

# stop_id can recur in several Static feeds. Include the feed version so
# platforms and stations resolve against the reference used by the event.
STOP_KEY = ["reference_feed_version", "stop_id"]
# source_type protects the combined table from collisions between sources.
EVENT_KEY = ["source_type", "event_key"]
# Weather has its own reference version because a verified cross-feed
# mapping can reuse a grid from an earlier feed. weather_time_utc is the
# scheduled event time rounded down to the start of its UTC hour.
WEATHER_KEY = ["weather_feed_version", "location_id", "weather_time_utc"]
# Silver stores both timings on one stop-visit row. Paired scheduled times
# help distinguish repeated calls at the same stop within a journey.
IST_VISIT_KEY = ["service_date", "trip_id", "bpuic", "sloid",
                 "scheduled_arrival_utc", "scheduled_departure_utc"]
# Pin the verified station-level production run. The broader S3 prefix also
# contains an older stop-level output with a different schema.
ELEVATION_PREFIX = (
    "silver/reference/stop_elevation/feed_version=20261104/"
    "run_id=20261110T191911578489Z/"
)
WEATHER_PREFIX = "silver/open_meteo_weather/"


def assert_unique(frame, key, label):
    # Check each key column before grouping. Null identifiers would make
    # later matches and deduplication ambiguous.
    missing = F.lit(False)
    for column in key:
        missing = missing | F.col(column).isNull()
    if frame.where(missing).take(1) or frame.groupBy(*key).count().where("count > 1").take(1):
        raise ValueError(f"Missing or duplicate {label} key: {key}")


def normalize_gtfs(rows):
    # Copies of the same event/feed-time/value are expected across runs.
    # Distinct values at the same feed time stop processing for investigation.
    assert_unique(
        rows.select("event_key", "feed_timestamp", "value_signature").distinct(),
        ["event_key", "feed_timestamp"], "GTFS feed instant",
    )
    # feed_timestamp orders published snapshots. The remaining columns break
    # equivalent ties deterministically, so the selected row is reproducible.
    ordering = Window.partitionBy("event_key").orderBy(
        F.desc("feed_timestamp"), F.desc_nulls_last("trip_update_timestamp"),
        F.desc_nulls_last("ingested_at_utc"), F.asc("prediction_signature"),
        F.asc("lineage_id"), F.asc("silver_input"),
    )
    # GTFS scheduled seconds may exceed 24 hours. Its service-day origin is
    # local noon minus 12 elapsed hours, which also handles DST transitions.
    # Convert the origin to UTC before adding elapsed scheduled seconds.
    scheduled = F.expr(
        "to_utc_timestamp(CAST(service_date AS TIMESTAMP) + INTERVAL 12 HOURS, "
        "'Europe/Zurich') - INTERVAL 12 HOURS + "
        "make_interval(0, 0, 0, 0, 0, 0, scheduled_time_seconds)"
    )
    return (
        rows.withColumn("rank", F.row_number().over(ordering)).where("rank = 1")
        .withColumn("service_date", F.to_date("service_date", "yyyyMMdd"))
        .select(
            F.lit("gtfs_rt").alias("source_type"), "event_key", "service_date",
            "source", "trip_id", "stop_id", "event_type", "didok",
            # GTFS route identifiers are scoped to a Static feed version.
            # Keep the value from the selected prediction without a crosswalk.
            F.col("route_id").cast("string").alias("route_id"),
            F.col("feed_version").alias("reference_feed_version"),
            F.col("study_canton_code").alias("source_canton"),
            "boundary_release", "delay_seconds", "silver_input", "raw_object_key",
            "upstream_run_id", "prediction_signature", "effective_stop_sequence",
            F.col("selection_status").alias("time_status"), "selection_rule_version",
            "selection_window_start_utc", "selection_window_end_utc_exclusive",
            "is_final_published_prediction",
            scheduled.alias("scheduled_time_utc"),
            F.timestamp_seconds("published_time_epoch").alias("reported_time_utc"),
            F.timestamp_seconds("feed_timestamp").alias("prediction_time_utc"),
        )
    )


def normalize_istdaten(rows):
    # A conflicting Silver visit should be resolved upstream before Gold.
    assert_unique(rows, ["service_date", "trip_id", "silver_input_key"], "Ist-Daten visit")
    # One visit can contribute two events. Each event keeps the delay and
    # status associated with its own arrival or departure.
    events = []
    for kind in ("arrival", "departure"):
        scheduled = f"scheduled_{kind}_utc"
        # A scheduled time defines whether that arrival/departure exists.
        # Events with a scheduled time and missing provided time are retained.
        # REAL, GESCHAETZT, PROGNOSE and UNBEKANNT remain in time_status.
        events.append(rows.where(F.col(scheduled).isNotNull()).select(
            "source_type", "service_date", "trip_id", "bpuic", "sloid", "didok",
            "reference_feed_version", "boundary_release", "silver_input",
            "bronze_object_key", "operator", "line_id", "transport_type",
            # Ist-Daten line_id remains source-specific. A null GTFS route_id
            # makes the shared schema explicit without guessing equivalence.
            F.lit(None).cast("string").alias("route_id"),
            "is_cancelled", "is_additional", "is_passing", "stop_mapping_method",
            F.col("matched_stop_id").alias("stop_id"),
            F.col("study_canton_code").alias("source_canton"),
            F.lit(kind).alias("event_type"),
            F.sha2(F.to_json(F.struct(F.lit(kind).alias("event_type"), *IST_VISIT_KEY),
                             {"ignoreNullFields": "false"}), 256).alias("event_key"),
            F.col(scheduled).alias("scheduled_time_utc"),
            F.col(f"provided_{kind}_utc").alias("reported_time_utc"),
            F.col(f"{kind}_delay_s").alias("delay_seconds"),
            F.col(f"{kind}_time_status").alias("time_status"),
        ))
    return events[0].unionByName(events[1])


def add_context(events, stops, elevation, locations):
    # Reference uniqueness is required before joining. Several reference
    # rows for one event key could multiply the transport rows.
    assert_unique(stops, STOP_KEY, "stop reference")
    assert_unique(elevation, ["reference_feed_version", "parent_stop_id"], "station elevation")
    assert_unique(locations, STOP_KEY, "weather location mapping")
    # Extend station elevations across feeds only after an exact spatial and
    # identifier check. Keep both reference versions in the Gold lineage.
    station_identity = ["parent_stop_id", "didok", "latitude", "longitude"]
    stations = stops.where(F.col("location_type").cast("int") == 1).select(
        "reference_feed_version", F.col("stop_id").alias("parent_stop_id"),
        "didok", F.col("stop_lat").alias("latitude"), F.col("stop_lon").alias("longitude"),
    ).dropna()
    elevation = elevation.withColumn(
        "elevation_feed_version", F.col("reference_feed_version")
    ).withColumn("elevation_mapping_method", F.lit("same_feed"))
    # Recover the original station coordinates from the matching Static feed.
    # Comparing both coordinates and identifiers supports traceable reuse of
    # the cached terrain elevation for an unchanged station in a newer feed.
    old_stations = stations.withColumnRenamed("reference_feed_version", "elevation_feed_version")
    original = elevation.drop("reference_feed_version").join(
        old_stations, ["elevation_feed_version", "parent_stop_id"], "inner"
    )
    reused = stations.join(original, station_identity, "inner").where(
        "reference_feed_version <> elevation_feed_version"
    )
    target_key = ["reference_feed_version", "parent_stop_id"]
    # Prefer an existing same-feed elevation. A left-anti join retains only
    # target stations that still need an elevation reference.
    reused = reused.join(elevation.select(*target_key), target_key, "left_anti")
    # Accept exactly one reuse candidate. Ambiguous stations retain a missing
    # elevation flag. The original elevation_feed_version stays in the output.
    reused = (
        reused.withColumn("candidate_count", F.count("*").over(Window.partitionBy(*target_key)))
        .where("candidate_count = 1")
        .withColumn("elevation_mapping_method", F.lit("verified_same_station_coordinates"))
        .select(*elevation.columns)
    )
    elevation = elevation.unionByName(reused)
    assert_unique(elevation, target_key, "extended station elevation")
    # Apply the same principle to the stop-to-weather grid mapping.
    # Require an exact stop ID, coordinates, didok and canton agreement.
    identity = ["stop_id", "stop_lat", "stop_lon", "didok", "study_canton_code"]
    positions = stops.select(*STOP_KEY, *identity[1:]).dropna()
    mapped = positions.join(locations, STOP_KEY).withColumnRenamed(
        "reference_feed_version", "fallback_weather_feed_version"
    )
    candidates = positions.join(mapped, identity).where(
        "reference_feed_version <> fallback_weather_feed_version"
    )
    # A stop with several eligible mapping versions is ambiguous.
    fallback = (
        candidates.withColumn("mapping_count", F.count("*").over(Window.partitionBy(*STOP_KEY)))
        .where("mapping_count = 1")
        .select(*STOP_KEY, "fallback_weather_feed_version",
                F.col("location_id").alias("fallback_location_id"),
                F.col("weather_mapping_input").alias("fallback_weather_mapping_input"))
    )
    # Parent stations use their own ID. Platforms use parent_station from
    # their versioned Static row to reach the station-level elevation table.
    stops = stops.select(
        *STOP_KEY, "study_canton_code", "stop_reference_input", "stop_boundary_release",
        F.when(F.col("location_type").cast("int") == 1, F.col("stop_id"))
        .when(F.trim("parent_station") != "", F.col("parent_station")).alias("parent_stop_id"),
    )
    return (
        # All enrichment joins retain unmatched transport events.
        # Broadcast distributes the small reference tables to Spark workers.
        events.join(F.broadcast(stops), STOP_KEY, "left")
        .join(F.broadcast(elevation), ["reference_feed_version", "parent_stop_id"], "left")
        .join(F.broadcast(locations), STOP_KEY, "left")
        .join(F.broadcast(fallback), STOP_KEY, "left")
        .withColumn("weather_mapping_method",
                    F.when(F.col("location_id").isNotNull(), "same_feed")
                    .when((F.col("source_type") == "gtfs_rt") &
                          F.col("fallback_location_id").isNotNull(), "verified_same_stop_coordinates"))
        .withColumn("weather_feed_version",
                    F.when(F.col("weather_mapping_method") == "same_feed", F.col("reference_feed_version"))
                    .when(F.col("weather_mapping_method") == "verified_same_stop_coordinates",
                          F.col("fallback_weather_feed_version")))
        .withColumn("location_id",
                    F.when(F.col("weather_mapping_method") == "verified_same_stop_coordinates",
                           F.col("fallback_location_id")).otherwise(F.col("location_id")))
        .withColumn("weather_mapping_input",
                    F.when(F.col("weather_mapping_method") == "verified_same_stop_coordinates",
                           F.col("fallback_weather_mapping_input")).otherwise(F.col("weather_mapping_input")))
        .drop("fallback_weather_feed_version", "fallback_location_id", "fallback_weather_mapping_input")
        .withColumn("weather_time_utc", F.date_trunc("hour", "scheduled_time_utc"))
    )


def add_weather(events, historical, forecast):
    assert_unique(historical, WEATHER_KEY, "historical weather")
    assert_unique(forecast, WEATHER_KEY + ["issued_hour_utc"], "forecast snapshot")
    # Historical reanalysis is assigned only to Ist-Daten. The matching row
    # must share the weather mapping version, grid and scheduled UTC hour.
    historic_events = (
        events.where("source_type = 'istdaten'")
        .join(F.broadcast(historical), WEATHER_KEY, "left")
    )
    # Each snapshot is eligible from retrieval until the next retrieval for
    # the same location and valid hour. Disjoint intervals prevent fan-out.
    # If two snapshots share a retrieval instant, prefer the later issued
    # snapshot. Then compute the next availability time for each valid hour.
    order = Window.partitionBy(*WEATHER_KEY, "retrieved_at_utc").orderBy(F.desc("issued_hour_utc"))
    timeline = Window.partitionBy(*WEATHER_KEY).orderBy("retrieved_at_utc")
    available = (
        forecast.where("retrieved_at_utc IS NOT NULL")
        .withColumn("rank", F.row_number().over(order)).where("rank = 1").drop("rank")
        .withColumn("next_retrieval", F.lead("retrieved_at_utc").over(timeline))
    )
    rt = events.where("source_type = 'gtfs_rt'").alias("event")
    weather = F.broadcast(available).alias("weather")
    # A forecast must have been available when the transport prediction was
    # published. The upper bound is exclusive, so a publication instant
    # belongs to a single availability interval.
    match = (
        (F.col("event.prediction_time_utc") >= F.col("weather.retrieved_at_utc")) &
        (F.col("weather.next_retrieval").isNull() |
         (F.col("event.prediction_time_utc") < F.col("weather.next_retrieval")))
    )
    for column in WEATHER_KEY:
        match = match & (F.col(f"event.{column}") == F.col(f"weather.{column}"))
    # Retain the weather model, product, issue/retrieval times and input
    # lineage alongside the matched variables.
    payload = [F.col(f"weather.{column}") for column in forecast.columns if column not in WEATHER_KEY]
    forecast_events = rt.join(weather, match, "left").select("event.*", *payload)
    return (
        historic_events.unionByName(forecast_events)
        .withColumn("missing_weather_location", F.col("location_id").isNull())
        .withColumn("missing_weather", F.col("data_product").isNull())
        .withColumn("missing_canton", F.col("study_canton_code").isNull())
        .withColumn("missing_elevation", F.col("elevation_m").isNull())
    )


def historical_window_keys(keys, first_epoch, last_epoch):
    """Select the 14-day ERA5 windows intersecting scheduled UTC hours."""
    selected = []
    for key in keys:
        # Silver historical collection uses 14-day inclusive windows.
        # UTC event bounds can include the previous or following calendar day.
        marker = "window_start="
        start_text = key.split(marker, 1)[1].split("/", 1)[0]
        start = datetime.combine(date.fromisoformat(start_text), datetime.min.time(),
                                 tzinfo=timezone.utc).timestamp()
        end_exclusive = start + 14 * 24 * 60 * 60
        if start <= last_epoch and end_exclusive > first_epoch:
            selected.append(key)
    return selected


def process_date(bucket, service_date, source_mode="both", batch_cache=None):
    # Each date gets an independent validated output. Dates run sequentially
    # so intermediate event data can be released between iterations.
    # Share listings and small, persisted reference/weather tables across dates.
    # Transport rows retain their per-date lifetime and are released below.
    if batch_cache is None:
        batch_cache = {"keys": {}, "frames": {}}
    day = service_date.isoformat()
    s3 = boto3.client("s3")
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    # Use UTC consistently for parsing, timestamp arithmetic and weather
    # hours. This avoids dependence on the Glue host timezone.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "32")

    def list_keys(prefix):
        # S3 listing is paginated. Include every Parquet part under the prefix.
        if prefix not in batch_cache["keys"]:
            pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
            batch_cache["keys"][prefix] = sorted(
                item["Key"] for page in pages for item in page.get("Contents", [])
                if item["Key"].endswith(".parquet"))
        return batch_cache["keys"][prefix]

    def read(keys, lineage="silver_input"):
        if not keys:
            raise ValueError(f"No inputs found for {lineage}")
        # Read explicit object paths and preserve each row's source file.
        # Recursive lookup avoids treating the surrounding S3 folder layout
        # as additional partition columns.
        paths = [f"s3://{bucket}/{key}" for key in keys]
        # Cache only reference and weather inputs. Transport can contain millions
        # of rows per date and is handled by the existing per-date disk cache.
        cacheable = lineage != "silver_input"
        cache_key = (tuple(keys), lineage)
        if cacheable and cache_key in batch_cache["frames"]:
            return batch_cache["frames"][cache_key]
        frame = (spark.read.option("recursiveFileLookup", "true").parquet(*paths)
                 .withColumn(lineage, F.input_file_name()))
        if cacheable:
            frame = frame.persist(StorageLevel.MEMORY_AND_DISK)
            batch_cache["frames"][cache_key] = frame
        return frame

    # A prediction window is distinct from the service date. Select on the
    # records themselves so predictions made on another day are considered.
    # Weekly coverage has already checked completion reports and markers.
    # Reuse exactly those candidate files to exclude unfinished Silver runs.
    if "validated_rt_keys" in batch_cache:
        rt_keys = batch_cache["validated_rt_keys"]
    else:
        rt_keys = [key for key in list_keys("silver/gtfs_rt_event_selection/run_id=")
                   if "/event_candidates/" in key]
    # Historical Ist-Daten mode reads one GTFS part only to obtain its schema.
    # An empty GTFS frame keeps the shared Gold columns available.
    # Normal combined mode continues to select candidates across all runs.
    rt_input = read(rt_keys[:1]).limit(0) if source_mode == "istdaten" else read(rt_keys)
    rt_rows = (rt_input.where(F.col("service_date") == service_date.strftime("%Y%m%d"))
               .persist(StorageLevel.DISK_ONLY))
    # Materialize the date-filtered input once on disk. Counts, selection
    # and validation can reuse it without retaining the full input in memory.
    rt_input_count = rt_rows.count()
    # The empty GTFS schema is constant throughout an Ist-Daten-only batch.
    # Reuse its analyzed frame to avoid repeated uniqueness Spark actions.
    if source_mode == "istdaten" and "empty_gtfs" in batch_cache:
        rt = batch_cache["empty_gtfs"]
    else:
        rt = normalize_gtfs(rt_rows)
        if source_mode == "istdaten":
            batch_cache["empty_gtfs"] = rt
    # Ist-Daten is already partitioned by service date. Read the entire
    # partition, including every part, and confirm its dates before unioning.
    ist_keys = list_keys(f"silver/stop_events/source=istdaten/date={day}/")
    ist_input_count = 0
    if source_mode == "istdaten" and not ist_keys:
        print(json.dumps({"service_date": day, "status": "missing_silver_istdaten"}))
        rt_rows.unpersist()
        return {"service_date": day, "status": "missing_silver_istdaten"}
    if ist_keys:
        ist_rows = (read(ist_keys)
                    .withColumn("silver_input_key", F.sha2(F.to_json(F.struct(*IST_VISIT_KEY),
                                {"ignoreNullFields": "false"}), 256))
                    .persist(StorageLevel.DISK_ONLY))
        if ist_rows.where(~F.col("service_date").eqNullSafe(F.lit(service_date))).take(1):
            raise ValueError(f"Ist-Daten partition contains another service date: {day}")
        ist_input_count = ist_rows.count()
        events = rt.unionByName(normalize_istdaten(ist_rows), allowMissingColumns=True)
    else:
        # Keep a stable schema for dates with just GTFS-RT.
        template = list_keys("silver/stop_events/source=istdaten/date=")[:1]
        empty_ist = read(template).limit(0).withColumn("silver_input_key", F.lit(None).cast("string"))
        events = rt.unionByName(normalize_istdaten(empty_ist), allowMissingColumns=True)
    events = events.persist(StorageLevel.DISK_ONLY)
    normalized_count = events.count()
    if not normalized_count:
        raise ValueError(f"No stop events found for {day}")

    # Load the available versioned Static references. They support same-feed
    # joins and explicit coordinate checks across reference versions.
    stop_keys = list_keys("silver/reference/gtfs_static_stops/feed_version=")
    stops = (read(stop_keys, "stop_reference_input")
             .withColumnRenamed("feed_version", "reference_feed_version")
             .withColumnRenamed("boundary_release", "stop_boundary_release"))
    elevation = (read(list_keys(ELEVATION_PREFIX), "elevation_input")
                 .withColumnRenamed("feed_version", "reference_feed_version")
                 .select("reference_feed_version", "parent_stop_id", "elevation_m",
                         "elevation_source", "elevation_input", "elevation_response_key"))
    locations = (read(list_keys(WEATHER_PREFIX + "stop_to_location/"), "weather_mapping_input")
                 .withColumnRenamed("feed_version", "reference_feed_version")
                 .select(*STOP_KEY, "location_id", "weather_mapping_input"))

    # Service dates can include after-midnight events. Derive weather bounds
    # from scheduled UTC timestamps instead of assuming a calendar-day window.
    # Epoch seconds keep these bounds independent of Python's local timezone.
    bounds = events.agg(
        F.min(F.unix_timestamp(F.date_trunc("hour", "scheduled_time_utc"))),
        F.max(F.unix_timestamp(F.date_trunc("hour", "scheduled_time_utc"))),
        F.max(F.unix_timestamp("prediction_time_utc")),
    ).first()
    if bounds[0] is None:
        raise ValueError("No scheduled UTC times")
    first_hour = F.timestamp_seconds(F.lit(bounds[0]))
    last_hour = F.timestamp_seconds(F.lit(bounds[1]))

    def weather_rows(product):
        # Keep relevant valid hours and rename weather lineage so transport
        # lineage survives the join. Product/model columns remain available.
        # Spark can use Parquet statistics to prune irrelevant row groups.
        keys = list_keys(WEATHER_PREFIX + f"data_product={product}/")
        if product == "historical_reanalysis":
            matching = historical_window_keys(keys, bounds[0], bounds[1])
            # An empty selection still needs the shared weather schema.
            # Read metadata for one file, then create a zero-row frame.
            frame = read(matching or keys[:1], "weather_silver_input")
            if not matching:
                frame = frame.limit(0)
        elif source_mode == "istdaten":
            # Historical-only processing requires no forecast records.
            frame = read(keys[:1], "weather_silver_input").limit(0)
        else:
            frame = read(keys, "weather_silver_input")
        return (
            frame
            .where((F.col("data_product") == product) &
                   F.col("weather_time_utc").between(first_hour, last_hour))
            .withColumnRenamed("feed_version", "weather_feed_version")
            .withColumnRenamed("bronze_object_key", "weather_bronze_object_key")
            .withColumnRenamed("processed_at_utc", "weather_processed_at_utc")
            .drop("latitude", "longitude")
        )

    # Exclude snapshots retrieved after every prediction in this date.
    # add_weather applies the stricter per-event availability test.
    forecast = weather_rows("forecast").where(
        F.col("retrieved_at_utc") <= F.timestamp_seconds(F.lit(bounds[2]).cast("long"))
    )
    gold = add_weather(add_context(events, stops, elevation, locations),
                       weather_rows("historical_reanalysis"), forecast).persist(StorageLevel.DISK_ONLY)
    # Validate the final result before publishing any Parquet output.
    assert_unique(gold, EVENT_KEY, "Gold event")
    # Reconcile the enriched route against the exact selected Silver candidate,
    # using the Gold event key. Null-safe equality also handles missing values.
    selected_routes = rt.select(
        *EVENT_KEY, F.col("route_id").alias("selected_route_id"),
        F.col("prediction_signature").alias("selected_prediction_signature"),
    )
    route_check = gold.where("source_type = 'gtfs_rt'").join(
        selected_routes, EVENT_KEY, "full"
    )
    route_mismatches = route_check.where(
        ~F.col("route_id").eqNullSafe(F.col("selected_route_id")) |
        ~F.col("prediction_signature").eqNullSafe(F.col("selected_prediction_signature"))
    ).count()
    if route_mismatches:
        raise RuntimeError(f"Selected Silver/Gold route mismatches: {route_mismatches}")

    # A match rate is the fraction of events with a context row/value.
    # A weather row can still contain null measurements for individual variables.
    # Report the two sources independently because their coverage differs.
    summaries = {
        row["source_type"]: row.asDict()
        for row in gold.groupBy("source_type").agg(
            F.count("*").alias("gold_events"),
            F.sum(F.when(F.length(F.trim("route_id")) > 0, 1).otherwise(0))
             .alias("route_id_present_events"),
            F.sum(F.when(F.length(F.trim("line_id")) > 0, 1).otherwise(0))
             .alias("line_id_present_events"),
            *[F.avg((~F.col(f"missing_{field}")).cast("double")).alias(f"{field}_match_rate")
              for field in ("weather_location", "weather", "canton", "elevation")],
        ).collect()
    }
    # Enrichment must preserve the normalized event count. Ist-Daten can
    # have more events than Silver visits because one visit has two timings.
    if sum(row["gold_events"] for row in summaries.values()) != normalized_count:
        raise RuntimeError("Enrichment changed the event count")
    for source, count in (("gtfs_rt", rt_input_count), ("istdaten", ist_input_count)):
        summary = summaries.setdefault(source, {"gold_events": 0})
        summary.update(silver_rows=count, duplicate_keys=0)
    summaries["gtfs_rt"]["repeated_candidates_removed"] = (
        rt_input_count - summaries["gtfs_rt"]["gold_events"]
    )

    # Give every attempt a unique output folder, including reruns. A failed
    # write can leave an incomplete folder, which has no successful report.
    root = f"gold/stop_event_analysis/service_date={day}/run_id={uuid4().hex}/"
    report = {
        "status": "success",
        "rule_version": RULE_VERSION,
        "source_mode": source_mode,
        "route_validation": {
            "silver_selected_prediction_mismatches": route_mismatches,
            "gtfs_route_key": ["reference_feed_version", "route_id"],
            "istdaten_line_key": "line_id; no GTFS crosswalk applied",
        },
        "weather_rule": "scheduled UTC hour; forecast available at prediction feed time",
        "elevation_rule": "same feed or exact parent station ID, didok and coordinates",
        "service_date": day,
        "output": f"s3://{bucket}/{root}events/",
        "by_source": summaries,
        "elevation_reference": f"s3://{bucket}/{ELEVATION_PREFIX}",
        "gtfs_candidate_files_considered": len(rt_keys),
        "istdaten_input_files": len(ist_keys),
    }
    gold.write.mode("errorifexists").parquet(report["output"])
    # Publish this last. Readers select one successful run per date.
    s3.put_object(Bucket=bucket, Key=root + "report.json",
                  Body=json.dumps(report, indent=2).encode("utf-8"),
                  ContentType="application/json")
    print(json.dumps(report, indent=2))
    # Release cached data before the next date to keep batch resource use
    # bounded. If an exception occurs, Glue ends the batch at that date.
    for frame in (rt_rows, events, gold):
        frame.unpersist()
    if ist_keys:
        ist_rows.unpersist()
    return report


def requested_dates(arguments):
    # Both bounds are inclusive. Single-date mode remains available for
    # targeted investigation or processing a newly arrived service date.
    if "START_DATE" in arguments:
        first = date.fromisoformat(arguments["START_DATE"])
        last = date.fromisoformat(arguments["END_DATE"])
        if last < first:
            raise ValueError("END_DATE must be on or after START_DATE")
        return [first + timedelta(days=offset) for offset in range((last - first).days + 1)]
    return [date.fromisoformat(arguments["SERVICE_DATE"])]


def completed_dates(s3, bucket, source_mode="both"):
    # A report is the completion marker. Earlier rule versions are rebuilt
    # to ensure that this batch uses the current enrichment rules.
    # This skip check does not inspect later changes in Silver input files.
    completed = set()
    pages = s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix="gold/stop_event_analysis/service_date=")
    for page in pages:
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.endswith("/report.json"):
                continue
            with s3.get_object(Bucket=bucket, Key=key)["Body"] as body:
                report = json.load(body)
            if report.get("status") == "success" and report.get("rule_version") == RULE_VERSION:
                # An Ist-Daten-only result cannot satisfy a combined run.
                # Combined output can satisfy the historical backfill when
                # that output already includes Ist-Daten events.
                report_mode = report.get("source_mode", "both")
                has_istdaten = report.get("by_source", {}).get("istdaten", {}).get("gold_events", 0) > 0
                if source_mode == "both" and report_mode == "both":
                    completed.add(date.fromisoformat(report["service_date"]))
                elif source_mode == "istdaten" and has_istdaten:
                    completed.add(date.fromisoformat(report["service_date"]))
    return completed


def local_day_bounds(day):
    """Return the UTC bounds of a Swiss calendar date, including DST."""
    swiss = ZoneInfo("Europe/Zurich")
    start = datetime.combine(day, datetime.min.time(), tzinfo=swiss)
    end = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=swiss)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)


def uncovered_intervals(start, end, windows):
    """Subtract completed selection windows from a half-open UTC interval."""
    cursor = start
    gaps = []
    for left, right in sorted(windows):
        if right <= cursor or left >= end:
            continue
        if left > cursor:
            gaps.append((cursor, min(left, end)))
        cursor = min(end, max(cursor, right))
        if cursor == end:
            break
    if cursor < end:
        gaps.append((cursor, end))
    return [[left.isoformat(), right.isoformat()] for left, right in gaps]


def check_source_coverage(bucket, days):
    """Produce a review report before choosing the final analysis cutoff.

    Selection windows describe completed Job 2 processing. Candidate rows
    describe published predictions, so zero candidates in an hour alone
    cannot establish that upstream collection was missing.
    """
    s3 = boto3.client("s3")
    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", "16")

    def list_objects(prefix):
        pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        return [item for page in pages for item in page.get("Contents", [])]

    def read_json(key):
        with s3.get_object(Bucket=bucket, Key=key)["Body"] as body:
            return json.load(body)

    def parquet(keys):
        # Explicit object paths preserve isolation between different datasets.
        # Select only the columns needed for this read-only coverage scan.
        return spark.read.option("recursiveFileLookup", "true").parquet(
            *[f"s3://{bucket}/{key}" for key in keys]
        )

    rt_objects = list_objects("silver/gtfs_rt_event_selection/run_id=")
    completed_roots = set()
    windows = []
    first_utc, _ = local_day_bounds(days[0])
    _, last_utc = local_day_bounds(days[-1])
    rt_object_keys = {item["Key"] for item in rt_objects}
    for item in rt_objects:
        key = item["Key"]
        if not key.endswith("/report.json") or "/quality/" in key:
            continue
        report = read_json(key)
        window = report.get("selection_window", {})
        root = key.rsplit("/", 1)[0] + "/"
        if not report.get("run_id") or not window:
            continue
        if report.get("status") != "success" or root + "_EXPERIMENT_SUCCESS" not in rt_object_keys:
            continue
        left = datetime.fromisoformat(window["start_utc"].replace("Z", "+00:00"))
        right = datetime.fromisoformat(window["end_utc_exclusive"].replace("Z", "+00:00"))
        if left < last_utc and right > first_utc:
            windows.append((left, right))
        # Reading all completed candidate roots retains cross-midnight
        # predictions for a service date. Silver file statistics may prune
        # irrelevant service dates without relying on run-folder names.
        completed_roots.add(key.rsplit("/", 1)[0] + "/")

    rt_keys = [item["Key"] for item in rt_objects
               if "/event_candidates/" in item["Key"] and item["Key"].endswith(".parquet")
               and item["Key"].split("event_candidates/", 1)[0] in completed_roots]
    candidate_counts = {}
    if rt_keys:
        rt = parquet(rt_keys).select(
            "service_date", "feed_version", "route_id", "prediction_signature"
        ).where(F.col("service_date").between(days[0].strftime("%Y%m%d"),
                                             days[-1].strftime("%Y%m%d")))
        rows = rt.groupBy("service_date").agg(
            F.count("*").alias("candidate_rows_across_runs"),
            F.countDistinct("prediction_signature").alias("distinct_prediction_signatures"),
            F.collect_set("feed_version").alias("reference_versions"),
            F.sum(F.when(F.length(F.trim("route_id")) > 0, 1).otherwise(0))
             .alias("route_id_present_rows"),
        ).collect()
        candidate_counts = {row["service_date"]: row.asDict() for row in rows}

    # List Ist-Daten once, then consult its per-date successful Silver report.
    ist_objects = list_objects("silver/stop_events/source=istdaten/date=")
    ist_keys = {item["Key"] for item in ist_objects if item["Key"].endswith(".parquet")}
    ist_reports = {item["Key"] for item in list_objects("quality/istdaten_stop_events/")
                   if item["Key"].endswith("/report.json")}

    # Weather coverage remains product-specific. In particular, a forecast
    # must never substitute for missing historical reanalysis in Ist-Daten.
    weather_objects = list_objects(WEATHER_PREFIX)
    weather_keys = [item["Key"] for item in weather_objects
                    if "/data_product=" in item["Key"] and item["Key"].endswith(".parquet")]
    weather_by_day = {}
    if weather_keys:
        weather = parquet(weather_keys).select(
            "data_product", "model", "location_id", "weather_time_utc", "retrieved_at_utc"
        ).where(F.col("weather_time_utc").between(
            F.timestamp_seconds(F.lit(int(first_utc.timestamp()))),
            F.timestamp_seconds(F.lit(int(last_utc.timestamp()) - 1)),
        ))
        local_date = F.to_date(F.from_utc_timestamp("weather_time_utc", "Europe/Zurich"))
        rows = weather.withColumn("local_date", local_date).groupBy(
            "local_date", "data_product"
        ).agg(
            F.countDistinct(F.struct("location_id", "weather_time_utc"))
             .alias("location_hours"),
            F.countDistinct("location_id").alias("locations"),
            F.collect_set("model").alias("models"),
            F.min("retrieved_at_utc").alias("first_retrieval_utc"),
            F.max("retrieved_at_utc").alias("last_retrieval_utc"),
        ).collect()
        for row in rows:
            record = row.asDict()
            weather_by_day.setdefault(str(row["local_date"]), {})[row["data_product"]] = record

    mapping_keys = [item["Key"] for item in weather_objects
                    if "/stop_to_location/" in item["Key"] and item["Key"].endswith(".parquet")]
    expected_locations = (
        parquet(mapping_keys).select("location_id").distinct().count() if mapping_keys else 0
    )
    dates = []
    for day in days:
        label = day.isoformat()
        start, end = local_day_bounds(day)
        silver_report_key = f"quality/istdaten_stop_events/date={label}/report.json"
        ist_report = read_json(silver_report_key) if silver_report_key in ist_reports else {}
        has_parts = any(key.startswith(f"silver/stop_events/source=istdaten/date={label}/")
                        for key in ist_keys)
        dates.append({
            "service_date": label,
            "swiss_calendar_day_hours": int((end - start).total_seconds() // 3600),
            "expected_weather_locations": expected_locations,
            "gtfs_rt": {
                **candidate_counts.get(day.strftime("%Y%m%d"), {}),
                "uncovered_job2_intervals_utc": uncovered_intervals(start, end, windows),
            },
            "istdaten": {
                "parquet_present": has_parts,
                "silver_report_status": ist_report.get("status", "missing"),
                "written_rows": ist_report.get("written_rows"),
            },
            "weather": weather_by_day.get(label, {}),
        })

    report = {
        "status": "coverage_review_required",
        "requested_date_range": [str(days[0]), str(days[-1])],
        "dates": dates,
        "validated_candidate_files": rt_keys,
        "limitations": [
            "Completed Job 2 windows do not prove complete upstream GTFS-RT collection.",
            "Weather totals do not prove every event has an eligible forecast snapshot.",
            "Zero candidate rows can reflect no published predictions or missing processing.",
            "Service-date journeys may extend beyond the Swiss calendar day.",
            "Choose the cutoff and document intentionally partial dates before Gold approval.",
        ],
    }
    key = f"quality/stop_event_analysis_coverage/run_id={uuid4().hex}/report.json"
    s3.put_object(Bucket=bucket, Key=key, ContentType="application/json",
                  Body=json.dumps(report, indent=2, default=str).encode("utf-8"))
    print(json.dumps({"status": report["status"], "dates_checked": len(days),
                      "coverage_report": f"s3://{bucket}/{key}"}))
    report["report_key"] = key
    return report


def weekly_eligibility(record):
    """Return reasons a new date should wait for a later weekly refresh."""
    reasons = []
    transport = record["gtfs_rt"]
    if not transport.get("candidate_rows_across_runs"):
        reasons.append("no_gtfs_rt_candidates")
    if transport["uncovered_job2_intervals_utc"]:
        reasons.append("incomplete_job2_processing_windows")
    observed = record["istdaten"]
    if not observed["parquet_present"] or observed["silver_report_status"] != "success":
        reasons.append("missing_successful_istdaten_input")
    # Forecast counts describe distinct grid/valid-hour pairs. Require all
    # mapped grid locations over this calendar day before processing new dates.
    # Historical weather may still be unavailable because of the ERA5 lag.
    # Gold retains and reports that historical coverage gap explicitly.
    forecast = record["weather"].get("forecast", {})
    expected = record["expected_weather_locations"] * record["swiss_calendar_day_hours"]
    if not expected or forecast.get("location_hours", 0) < expected:
        reasons.append("incomplete_forecast_location_hours")
    return reasons


def weekly_dates(arguments, today=None):
    """Use Swiss dates and a configurable lag to avoid today's partial inputs."""
    if today is None:
        today = datetime.now(ZoneInfo("Europe/Zurich")).date()
    lag = int(arguments.get("LAG_DAYS", "2"))
    if lag < 1:
        raise ValueError("LAG_DAYS must be at least 1")
    cutoff = today - timedelta(days=lag)
    if arguments.get("COLLECTION_END_DATE"):
        cutoff = min(cutoff, date.fromisoformat(arguments["COLLECTION_END_DATE"]))
    # Include the original RT period once so older predictions receive route_id.
    # Historical-only v2 outputs before November 5 remain reusable.
    first = date(2026, 11, 5)
    if cutoff < first:
        return []
    return requested_dates({"START_DATE": str(first), "END_DATE": str(cutoff)})


def main():
    # Resolve only the parameters required by the selected mode. Glue also
    # supplies unrelated system arguments, which getResolvedOptions ignores.
    parameters = ["JOB_NAME", "DATA_BUCKET"]
    weekly_requested = "--WEEKLY" in sys.argv
    if weekly_requested:
        parameters.append("WEEKLY")
        for name in ("LAG_DAYS", "COLLECTION_END_DATE"):
            if f"--{name}" in sys.argv:
                parameters.append(name)
    elif "--START_DATE" in sys.argv:
        parameters += ["START_DATE", "END_DATE"]
    else:
        parameters += ["SERVICE_DATE"]
    # SOURCE_MODE is optional. Existing jobs retain combined-source behavior.
    if "--SOURCE_MODE" in sys.argv:
        parameters.append("SOURCE_MODE")
    if "--CHECK_ONLY" in sys.argv:
        parameters.append("CHECK_ONLY")
    arguments = getResolvedOptions(sys.argv, parameters)
    source_mode = arguments.get("SOURCE_MODE", "both")
    if source_mode not in ("both", "istdaten"):
        raise ValueError("SOURCE_MODE must be both or istdaten")
    bucket = arguments["DATA_BUCKET"]
    if weekly_requested:
        if arguments["WEEKLY"].lower() != "true":
            raise ValueError("Omit WEEKLY for fixed-date mode, or set WEEKLY=true")
        if source_mode != "both":
            raise ValueError("Weekly mode requires SOURCE_MODE=both")
        days = weekly_dates(arguments)
        if not days:
            raise ValueError("Weekly cutoff precedes the analysis start date")
    else:
        days = requested_dates(arguments)
    check_only = arguments.get("CHECK_ONLY", "false").lower()
    if check_only not in ("true", "false"):
        raise ValueError("CHECK_ONLY must be true or false")
    if check_only == "true":
        check_source_coverage(bucket, days)
        return
    # Resume by skipping successful dates under the current rule version.
    # Completed dates remain published if a later date fails.
    done = completed_dates(boto3.client("s3"), bucket, source_mode)
    deferred = []
    if weekly_requested:
        coverage = check_source_coverage(bucket, days)
        eligible = []
        for record in coverage["dates"]:
            day = date.fromisoformat(record["service_date"])
            # Original November 5–15 dates were approved with documented
            # weather limitations. Rebuild those for route propagation.
            reasons = weekly_eligibility(record) if day > date(2026, 11, 15) else []
            if reasons:
                deferred.append({"service_date": str(day), "reasons": reasons})
            else:
                eligible.append(day)
        days = eligible
        print(json.dumps({"status": "weekly_plan", "eligible_dates": len(days),
                          "deferred_dates": deferred}))
    results = []
    # References and ERA5 windows are small enough to reuse across dates.
    # A 14-day window is loaded once while that group of dates is processed.
    batch_cache = {"keys": {}, "frames": {}}
    if weekly_requested:
        batch_cache["validated_rt_keys"] = coverage["validated_candidate_files"]
    for day in days:
        if day in done:
            print(json.dumps({"service_date": day.isoformat(), "status": "already_complete"}))
            continue
        results.append(process_date(bucket, day, source_mode, batch_cache))
        # Retain only the most recent weather file set. Old windows can be
        # released as the batch moves forward, keeping memory bounded.
        weather_frames = [key for key in batch_cache["frames"] if key[1] == "weather_silver_input"]
        for key in weather_frames[:-2]:
            batch_cache["frames"].pop(key).unpersist()
    for frame in batch_cache["frames"].values():
        frame.unpersist()
    if weekly_requested:
        # This completion record connects the weekly Gold run to its audit.
        # Deferred dates are explicit and remain eligible for a future run.
        # Publish only after every planned Gold date has completed or resumed.
        completion = {
            "status": "success",
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "rule_version": RULE_VERSION,
            "cutoff": str(weekly_dates(arguments)[-1]),
            "coverage_report_key": coverage["report_key"],
            "eligible_dates": [str(day) for day in days],
            "deferred_dates": deferred,
        }
        boto3.client("s3").put_object(
            Bucket=bucket, Key="quality/stop_event_analysis_coverage/weekly_refresh.json",
            ContentType="application/json",
            Body=json.dumps(completion, indent=2).encode("utf-8"),
        )
    print(json.dumps({
        "status": "batch_complete",
        "processed_dates": sum(item["status"] == "success" for item in results),
        "deferred_dates": deferred,
        "missing_silver_dates": [item["service_date"] for item in results
                                 if item["status"] == "missing_silver_istdaten"],
    }))


if __name__ == "__main__":
    main()
