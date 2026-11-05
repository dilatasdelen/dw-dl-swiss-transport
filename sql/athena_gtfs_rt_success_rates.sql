-- GTFS-RT collection and decoding success rates from an existing Athena view.
-- Run this file as ONE SELECT statement in the Athena Query Editor.
-- It does not create or replace any database, table, or view.
--
-- Default scope: 2026-10-12, hours 15 through 22 inclusive, all in UTC.
-- This covers 15:00 inclusive to 23:00 exclusive. Edit the WHERE values
-- to inspect another completed collection period.
--
-- The FROM clause uses the view created by athena_gtfs_rt_monitoring.sql in
-- the datavault-v2 deployment. Change the example date/hour filter below to
-- the completed collection period being checked in the new bucket.
--
-- Interpretation:
-- * collection_success_percent = successful attempts / recorded attempts.
--   It does not measure successful runs / all scheduled runs: a collection
--   that never started or wrote no audit record is absent from that denominator.
-- * valid_decode_percent = valid decoded files / decoder quality records.
--   Valid means parse_status = success AND payload_hash_valid = true.
-- * Compare collected_files with valid_decoded_files, and inspect both error
--   counts. A high decode percentage alone does not prove all raw files were
--   decoded. Missing view metrics remain NULL rather than becoming zero.
-- * pipeline_status is the existing view's aggregate reconciliation flag.
--   OK does not certify schedule completeness, row-level analytical quality,
--   or individual raw-object-to-decoded-file correspondence.

SELECT
    ingest_date,
    hour,
    attempts,
    successful_attempts AS collected_files,
    failed_attempts,
    success_percent AS collection_success_percent,
    decoded_files,
    valid_decoded_files,
    valid_decode_percent,
    parse_failed_files,
    invalid_hash_files,
    pipeline_status
FROM swiss_transit.vw_gtfs_rt_monitoring_hourly
WHERE ingest_date = '2026-10-12'
  AND CAST(hour AS integer) BETWEEN 15 AND 22
ORDER BY CAST(hour AS integer) DESC;
