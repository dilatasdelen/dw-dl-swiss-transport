import importlib.util
from pathlib import Path
import unittest

from src.processing import glue_event_selection as selection
from src.processing import glue_staging_silver as staging


FIXTURE = Path(__file__).resolve().parent / "fixtures" / "gtfs_rt_diagnostics.jsonl"
POINT_FIXTURE = FIXTURE.with_name("gtfs_rt_point_ranking.jsonl")
OBSERVATION_COLUMNS = [
    *selection.HISTORY_COLUMNS, "upstream_run_id", "effective_stop_sequence",
    "event_key", "event_key_valid", "value_signature", "replay_content_signature",
    "replay_rank", "selection_reason",
]


class DiagnosticProjectionTests(unittest.TestCase):
    def test_point_ranking_keeps_wide_default_and_rejects_unknown_mode(self):
        self.assertEqual(selection.sql_plan(100, 200), selection.sql_plan(100, 200, point_ranking="wide"))
        with self.assertRaisesRegex(ValueError, "POINT_RANKING"):
            selection.sql_plan(100, 200, point_ranking="unknown")

    def test_compact_selection_uses_observation_columns(self):
        self.assertEqual(len(selection.EXCLUDED_COLUMNS), len(set(selection.EXCLUDED_COLUMNS)))
        self.assertLess(set(selection.EXCLUDED_COLUMNS), set(OBSERVATION_COLUMNS))
        self.assertTrue({
            "raw_object_key", "lineage_id", "prediction_signature", "selection_reason",
            "static_match_reason", "feed_version", "delay_seconds", "is_delay_missing",
        }.issubset(selection.EXCLUDED_COLUMNS))


class ProductionPathTests(unittest.TestCase):
    def test_scheduled_paths_remain_unchanged(self):
        staging_input, history_output = staging.experiment_paths(
            "datavault-v2", "2026-11-25", "15", "history-slot",
        )
        config = selection.configuration(
            "datavault-v2", "history-slot,history-second", "events-slot",
            "2026-11-25T15:00:00Z", "2026-11-25T16:00:00Z", selection.RULE_VERSION,
        )
        self.assertEqual(staging_input, "silver/gtfs_rt_decoded/ingest_date=2026-11-25/hour=15/")
        self.assertEqual(history_output, "s3://datavault-v2/silver/gtfs_rt_processed/run_id=history-slot/")
        self.assertEqual(config["input_roots"], [
            history_output,
            "s3://datavault-v2/silver/gtfs_rt_processed/run_id=history-second/",
        ])
        self.assertEqual(config["output_root"], "s3://datavault-v2/silver/gtfs_rt_event_selection/run_id=events-slot/")


@unittest.skipUnless(importlib.util.find_spec("pyspark"), "requires a local PySpark runtime")
class SparkDiagnosticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        cls.F = F
        cls.spark = (SparkSession.builder.master("local[1]")
                     .appName("gtfs-rt-diagnostics-tests")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2")
                     .getOrCreate())
        cls.source = cls.spark.read.schema(
            staging.STAGING_DDL + ", input_object_key STRING, area_status STRING,"
            " study_canton_code STRING, boundary_release STRING, canton_join_status STRING,"
            " precheck_reason STRING"
        ).json(FIXTURE.as_uri())
        cls.prepared = (cls.source.withColumn("_source_ingested_at_utc", F.col("ingested_at_utc"))
                        .withColumn("ingested_at_utc", F.to_timestamp("ingested_at_utc")))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def legacy_identity(self, frame):
        F = self.F
        options = {"ignoreNullFields": "false"}
        return (frame.withColumn("lineage_id", F.sha2(F.to_json(
            F.struct(*staging.STAGING_COLUMNS, "input_object_key"), options,
        ), 256)).withColumn("prediction_signature", F.sha2(F.to_json(
            F.struct(*staging.PREDICTION_COLUMNS), options,
        ), 256)).withColumn("ingested_at_utc", F.to_timestamp("ingested_at_utc")))

    def test_delayed_identity_matches_legacy_hashes_and_schema(self):
        actual = staging.add_prediction_identity(self.prepared.where("area_status = 'eligible'"))
        legacy = self.legacy_identity(self.source).where("area_status = 'eligible'")
        column_order = [*staging.STAGING_COLUMNS, "input_object_key", "lineage_id",
                        "prediction_signature", "area_status", "study_canton_code",
                        "boundary_release", "canton_join_status", "precheck_reason"]
        self.assertEqual(actual.schema, legacy.select(*column_order).schema)
        rows = actual.orderBy("raw_object_key").collect()
        self.assertEqual(rows, legacy.select(*column_order).orderBy("raw_object_key").collect())
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].prediction_signature, rows[1].prediction_signature)
        self.assertNotEqual(rows[0].lineage_id, rows[1].lineage_id)
        self.assertEqual(rows[0].delay_seconds, 0)

    def test_staging_preserves_rejection_trace_and_per_capture_counts(self):
        area = self.prepared.groupBy("area_status", "study_canton_code", "raw_object_key").count()
        compact = dict(staging.diagnostic_outputs(self.prepared, area, area, "compact"))
        self.assertEqual(set(compact), {"rejected_rows", "quality/area_counts"})
        rows = compact["rejected_rows"].collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].area_status, "unknown_stop")
        self.assertEqual(rows[0].raw_object_key, "bronze/gtfs_rt/unknown.pb")
        self.assertIsNone(rows[0].delay_seconds)
        self.assertEqual(compact["quality/area_counts"].count(), 4)

        full = dict(staging.diagnostic_outputs(self.prepared, area, area, "full"))
        self.assertIn("quality/static_match", full)
        legacy = self.legacy_identity(self.source).where("area_status = 'unknown_stop'")
        self.assertEqual(full["rejected_rows"].select("lineage_id").first().lineage_id,
                         legacy.select("lineage_id").first().lineage_id)

    def test_selection_omits_empty_blocking_outputs_only_in_compact_mode(self):
        F = self.F
        source_columns = set(self.prepared.columns)
        observations = self.prepared.select(*[
            F.col(name) if name in source_columns else F.lit(None).alias(name)
            for name in OBSERVATION_COLUMNS
        ]).withColumn("event_key", F.col("trip_id")).withColumn(
            "selection_reason",
            F.when(F.col("trip_id") == "trip-1", F.lit("eligible"))
            .otherwise(F.lit("no_valid_static_match")),
        )
        eligible = observations.where("selection_reason = 'eligible'")
        blocked = self.spark.sql(
            "SELECT 'trip-1' AS event_key, 'conflicting_values_at_same_feed_time' AS blocking_reason"
        )
        quality = observations.groupBy("selection_reason").count()
        compact = dict(selection.diagnostic_outputs(observations, eligible, blocked.limit(0),
                                                     quality, 0, "compact"))
        self.assertEqual(set(compact), {"excluded_observations"})
        self.assertEqual(compact["excluded_observations"].count(), 2)

        with_blocks = dict(selection.diagnostic_outputs(
            observations, eligible, blocked, quality, 1, "compact",
        ))
        self.assertEqual(with_blocks["blocked_event_observations"].count(), 2)
        self.assertEqual(with_blocks["quality/blocked_events"].first().blocking_reason,
                         "conflicting_values_at_same_feed_time")
        full = dict(selection.diagnostic_outputs(observations, eligible, blocked.limit(0),
                                                  quality, 0, "full"))
        self.assertIn("quality/selection_reasons", full)
        self.assertEqual(full["blocked_event_observations"].count(), 0)
        self.assertIn("replay_content_signature", full["excluded_observations"].columns)

    def test_narrow_point_ranking_preserves_predictions_revisions_and_lineage(self):
        F = self.F
        history = self.spark.read.schema(
            "event_key STRING, prediction_signature STRING, feed_timestamp LONG, "
            "trip_update_timestamp LONG, ingested_at_utc TIMESTAMP, delay_seconds LONG, "
            "lineage_id STRING, upstream_run_id STRING"
        ).json(POINT_FIXTURE.as_uri()).select(
            "*",
            F.concat(F.lit("bronze/gtfs_rt/"), F.col("lineage_id"), F.lit(".pb")).alias("raw_object_key"),
            F.col("lineage_id").alias("payload_sha256"),
            F.lit(True).alias("event_key_valid"),
            F.lit(True).alias("payload_hash_valid"),
            F.lit(False).alias("entity_is_deleted"),
            F.lit("matched").alias("static_match_status"),
            F.lit(54000).alias("scheduled_time_seconds"),
            F.lit("arrival").alias("event_type"),
            F.lit("SCHEDULED").alias("trip_schedule_relationship"),
            F.lit("SCHEDULED").alias("stop_schedule_relationship"),
            F.lit("15:00:00").alias("start_time"),
            F.lit("Synthetic station").alias("stop_name"),
            F.col("delay_seconds").isNull().alias("is_delay_missing"),
            F.col("delay_seconds").cast("string").alias("value_signature"),
            F.col("prediction_signature").alias("replay_content_signature"),
        )
        history.createOrReplaceTempView("history_input")
        history.select(
            "prediction_signature", "lineage_id", "raw_object_key", "payload_sha256",
            "ingested_at_utc", "upstream_run_id",
            F.concat(F.lit("silver/gtfs_rt_decoded/"), F.col("lineage_id")).alias("input_object_key"),
            F.lit(1).alias("source_row_count"),
        ).createOrReplaceTempView("lineage_input")
        relations = {
            "event_candidates_base": ("event_key",),
            "revision_summary_base": ("event_key",),
            "selected_capture_lineage": ("event_key", "lineage_id"),
            "blocked_events": ("event_key", "blocking_reason"),
        }
        versions = {}
        for mode in ("wide", "narrow"):
            views = {}
            for name, query in selection.sql_plan(100, 200, point_ranking=mode).items():
                frame = self.spark.sql(query)
                if name in {"observations", "blocked_events", "timeline", "changes", "event_candidates_base"}:
                    frame = frame.cache()
                frame.createOrReplaceTempView(name)
                views[name] = frame
            if mode == "narrow":
                self.assertEqual(views["timeline"].columns, [
                    "event_key", "prediction_signature", "feed_timestamp", "value_signature", "delay_seconds",
                ])
                self.assertNotIn("stop_name", views["point_ranked"].columns)
            versions[mode] = {
                name: (views[name].schema, views[name].orderBy(*keys).collect())
                for name, keys in relations.items()
            }
            self.spark.catalog.clearCache()
        self.assertEqual(versions["wide"], versions["narrow"])
        results = versions["narrow"]
        candidates = {row.event_key: row for row in results["event_candidates_base"][1]}
        self.assertEqual(set(candidates), {"revising", "on-time"})
        self.assertEqual(candidates["revising"].prediction_signature, "tie-a")
        self.assertEqual(candidates["revising"].prediction_observation_count, 2)
        self.assertEqual(candidates["revising"].prediction_revision_count, 1)
        self.assertTrue(candidates["revising"].has_newer_invalid_observation)
        self.assertEqual(candidates["on-time"].delay_seconds, 0)
        self.assertEqual(
            {row.lineage_id: row.upstream_run_occurrences
             for row in results["selected_capture_lineage"][1] if row.event_key == "revising"},
            {"first-capture": 2, "second-capture": 1},
        )


if __name__ == "__main__":
    unittest.main()
