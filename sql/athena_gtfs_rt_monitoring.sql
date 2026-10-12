-- GTFS-RT collection monitoring for Amazon Athena.
--
-- Prerequisites
-- -------------
-- The collector must write one audit JSON object per collection attempt:
--   s3://<PROJECT_BUCKET>/audit/gtfs_rt/
--       ingest_date=YYYY-MM-DD/hour=HH/*.json
--
-- The decoder must write one quality JSON object per raw snapshot:
--   s3://<PROJECT_BUCKET>/quality/gtfs_rt_snapshot_decode/
--       ingest_date=YYYY-MM-DD/hour=HH/*.json
--
-- Required collector fields:
--   status, request_started_at, payload_byte_count, payload_sha256,
--   feed_timestamp, feed_version, entity_count, raw_object_key
--
-- Required decoder-quality fields:
--   ingested_at_utc, payload_hash_valid, parse_status, event_row_count,
--   trip_update_entity_count, stop_time_update_count, missing_delay_count,
--   zero_delay_count
--
-- Before running the external-table DDL, replace every <PROJECT_BUCKET>
-- placeholder with the actual S3 bucket name. Run each CREATE DATABASE,
-- CREATE TABLE, CREATE VIEW, SHOW, and SELECT statement separately in the
-- Athena Query Editor.
--
-- All partition hours and monitoring timestamps in this file represent UTC.


CREATE DATABASE IF NOT EXISTS swiss_transit;


CREATE EXTERNAL TABLE IF NOT EXISTS swiss_transit.gtfs_rt_audit (
    source string,
    status string,
    scheduled_time string,
    request_started_at string,
    response_received_at string,
    http_status int,
    network_byte_count bigint,
    payload_byte_count bigint,
    payload_sha256 string,
    feed_timestamp bigint,
    feed_version string,
    entity_count bigint,
    retry_count int,
    raw_object_key string,
    final_url string,
    error_stage string,
    error_class string,
    error_detail string,
    lambda_request_id string,
    audit_object_key string,
    duration_ms double
)
PARTITIONED BY (
    ingest_date string,
    hour string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<PROJECT_BUCKET>/audit/gtfs_rt/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.ingest_date.type'='date',
    'projection.ingest_date.range'='2026-09-01,NOW',
    'projection.ingest_date.format'='yyyy-MM-dd',
    'projection.hour.type'='integer',
    'projection.hour.range'='0,23',
    'projection.hour.digits'='2',
    'storage.location.template'=
        's3://<PROJECT_BUCKET>/audit/gtfs_rt/ingest_date=${ingest_date}/hour=${hour}/'
);


CREATE EXTERNAL TABLE IF NOT EXISTS swiss_transit.gtfs_rt_decode_quality (
    raw_object_key string,
    ingested_at_utc string,
    payload_sha256 string,
    expected_payload_sha256 string,
    payload_hash_valid boolean,
    parse_status string,
    error_class string,
    error_detail string,
    feed_timestamp bigint,
    feed_version string,
    entity_count bigint,
    trip_update_entity_count bigint,
    stop_time_update_count bigint,
    event_row_count bigint,
    missing_delay_count bigint,
    zero_delay_count bigint,
    ingest_date string,
    hour string
)
PARTITIONED BY (
    partition_date string,
    partition_hour string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<PROJECT_BUCKET>/quality/gtfs_rt_snapshot_decode/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.partition_date.type'='date',
    'projection.partition_date.range'='2026-09-01,NOW',
    'projection.partition_date.format'='yyyy-MM-dd',
    'projection.partition_hour.type'='integer',
    'projection.partition_hour.range'='0,23',
    'projection.partition_hour.digits'='2',
    'storage.location.template'=
        's3://<PROJECT_BUCKET>/quality/gtfs_rt_snapshot_decode/ingest_date=${partition_date}/hour=${partition_hour}/'
);


-- Generic hourly monitoring view. Date restrictions belong in dashboard or
-- example queries, not in this reusable view definition.

CREATE OR REPLACE VIEW swiss_transit.vw_gtfs_rt_monitoring_hourly AS

WITH audit_base AS (
    SELECT
        ingest_date,
        hour,
        status,
        payload_sha256,
        payload_byte_count,
        feed_timestamp,
        feed_version,
        entity_count,
        raw_object_key,
        try(from_iso8601_timestamp(request_started_at)) AS request_ts
    FROM swiss_transit.gtfs_rt_audit
),

successful_ordered AS (
    SELECT
        *,
        lag(payload_sha256) OVER (
            PARTITION BY ingest_date, hour
            ORDER BY request_ts, raw_object_key
        ) AS previous_payload_sha256
    FROM audit_base
    WHERE status = 'success'
),

audit_hourly AS (
    SELECT
        ingest_date,
        hour,
        count(*) AS attempts,
        count_if(status = 'success') AS successful_attempts,
        count_if(coalesce(status, '') <> 'success') AS failed_attempts,
        round(
            100.0 * count_if(status = 'success') / nullif(count(*), 0),
            2
        ) AS success_percent,
        count_if(
            status = 'success' AND feed_timestamp IS NULL
        ) AS missing_feed_timestamp,
        count_if(
            status = 'success' AND feed_version IS NULL
        ) AS missing_feed_version,
        count_if(
            status = 'success' AND entity_count IS NULL
        ) AS missing_entity_count
    FROM audit_base
    GROUP BY ingest_date, hour
),

dynamics_hourly AS (
    SELECT
        ingest_date,
        hour,
        count(*) AS successful_snapshots,
        count(DISTINCT payload_sha256) AS unique_payloads,
        round(
            100.0 * count(DISTINCT payload_sha256) / nullif(count(*), 0),
            2
        ) AS unique_payload_percent,
        count_if(
            previous_payload_sha256 IS NOT NULL
        ) AS comparable_payload_transitions,
        count_if(
            previous_payload_sha256 IS NOT NULL
            AND payload_sha256 <> previous_payload_sha256
        ) AS changed_payload_transitions,
        round(
            100.0 * count_if(
                previous_payload_sha256 IS NOT NULL
                AND payload_sha256 <> previous_payload_sha256
            ) / nullif(
                count_if(previous_payload_sha256 IS NOT NULL),
                0
            ),
            2
        ) AS changed_payload_percent,
        round(avg(payload_byte_count), 2) AS average_payload_bytes,
        round(avg(entity_count), 2) AS average_entity_count,
        cast(
            max(request_ts) AT TIME ZONE 'UTC' AS timestamp
        ) AS last_success_utc,
        max_by(feed_version, request_ts) AS latest_feed_version,
        max_by(feed_timestamp, request_ts) AS latest_feed_timestamp
    FROM successful_ordered
    GROUP BY ingest_date, hour
),

decode_hourly AS (
    SELECT
        partition_date AS ingest_date,
        partition_hour AS hour,
        count(*) AS decoded_files,
        count_if(
            parse_status = 'success' AND payload_hash_valid
        ) AS valid_decoded_files,
        count_if(
            parse_status <> 'success' OR parse_status IS NULL
        ) AS parse_failed_files,
        count_if(
            payload_hash_valid = false OR payload_hash_valid IS NULL
        ) AS invalid_hash_files,
        round(
            100.0 * count_if(
                parse_status = 'success' AND payload_hash_valid
            ) / nullif(count(*), 0),
            2
        ) AS valid_decode_percent,
        sum(event_row_count) AS event_rows,
        sum(trip_update_entity_count) AS trip_update_entities,
        sum(stop_time_update_count) AS stop_time_updates,
        sum(missing_delay_count) AS missing_delay_rows,
        sum(zero_delay_count) AS zero_delay_rows,
        round(
            100.0 * sum(missing_delay_count)
            / nullif(sum(event_row_count), 0),
            2
        ) AS missing_delay_percent,
        round(
            100.0 * sum(zero_delay_count)
            / nullif(sum(event_row_count), 0),
            2
        ) AS zero_delay_percent,
        cast(
            max(
                try(from_iso8601_timestamp(ingested_at_utc))
            ) AT TIME ZONE 'UTC' AS timestamp
        ) AS last_decoded_utc
    FROM swiss_transit.gtfs_rt_decode_quality
    GROUP BY partition_date, partition_hour
)

SELECT
    coalesce(a.ingest_date, d.ingest_date) AS ingest_date,
    coalesce(a.hour, d.hour) AS hour,
    a.attempts,
    a.successful_attempts,
    a.failed_attempts,
    a.success_percent,
    p.successful_snapshots,
    p.unique_payloads,
    p.unique_payload_percent,
    p.comparable_payload_transitions,
    p.changed_payload_transitions,
    p.changed_payload_percent,
    p.average_payload_bytes,
    p.average_entity_count,
    p.latest_feed_version,
    p.latest_feed_timestamp,
    p.last_success_utc,
    a.missing_feed_timestamp,
    a.missing_feed_version,
    a.missing_entity_count,
    d.decoded_files,
    d.valid_decoded_files,
    d.parse_failed_files,
    d.invalid_hash_files,
    d.valid_decode_percent,
    d.event_rows,
    d.trip_update_entities,
    d.stop_time_updates,
    d.missing_delay_rows,
    d.zero_delay_rows,
    d.missing_delay_percent,
    d.zero_delay_percent,
    d.last_decoded_utc,
    coalesce(a.successful_attempts, 0)
        - coalesce(d.decoded_files, 0)
        AS audit_decode_count_difference,
    CASE
        WHEN a.attempts IS NULL THEN 'NO_AUDIT'
        WHEN d.decoded_files IS NULL THEN 'NO_DECODE'
        WHEN a.successful_attempts = d.valid_decoded_files THEN 'OK'
        ELSE 'CHECK'
    END AS pipeline_status
FROM audit_hourly a
LEFT JOIN dynamics_hourly p
    ON a.ingest_date = p.ingest_date
   AND a.hour = p.hour
FULL OUTER JOIN decode_hourly d
    ON coalesce(a.ingest_date, p.ingest_date) = d.ingest_date
   AND coalesce(a.hour, p.hour) = d.hour;


-- Chart-ready view. hour_start_utc is a timezone-free Hive timestamp whose
-- values represent UTC.

CREATE OR REPLACE VIEW swiss_transit.vw_gtfs_rt_monitoring_chart AS
SELECT
    date_parse(
        concat(ingest_date, ' ', lpad(hour, 2, '0')),
        '%Y-%m-%d %H'
    ) AS hour_start_utc,
    hourly.*
FROM swiss_transit.vw_gtfs_rt_monitoring_hourly hourly;


-- Pilot example
-- -------------
-- On 2026-10-12, captures before hour=15 were produced by an earlier
-- collector version. This example uses 15:00 UTC as the monitoring baseline.
-- Earlier source objects remain in S3 and are excluded only from this query.

SELECT *
FROM swiss_transit.vw_gtfs_rt_monitoring_chart
WHERE ingest_date > '2026-10-12'
   OR (
        ingest_date = '2026-10-12'
        AND cast(hour AS integer) >= 15
   )
ORDER BY hour_start_utc;


-- Optional verification query. Run separately.

SHOW VIEWS IN swiss_transit;

