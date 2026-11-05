# AWS Glue job 1: Silver prediction history

[Code](glue_staging_silver.py) · [Execution report](reports/hour-20261012-20.report.json)

Tested study-area filtering and versioned GTFS Static matching on one hour of
decoded GTFS-RT data. The job retains prediction history and raw-capture lineage.

## Run parameters for datavault-v2

Replace the placeholders with an available decoded UTC partition, its actual
feed version, and a new run label.

```text
--DATA_BUCKET       datavault-v2
--INGEST_DATE       <UTC_DATE>
--INGEST_HOUR       <UTC_HOUR>
--FEED_VERSION      <FEED_VERSION>
--RUN_ID            <NEW_RUN_ID>
--MAX_INPUT_FILES   0
```

Input partition (UTC):
`s3://datavault-v2/silver/gtfs_rt_decoded/ingest_date=<UTC_DATE>/hour=<UTC_HOUR>/`

References: `silver/reference/gtfs_static_stops/feed_version=<FEED_VERSION>/` and
`silver/reference/gtfs_static_stop_times/feed_version=<FEED_VERSION>/` in the same bucket.
The stop reference includes polygon-based canton assignments. The job filters
ZH/ZG/LU/SZ before the larger timetable join.

## Recorded pilot results

The linked report records the previous `datavault-v1` run. Its original paths
and metrics remain unchanged; it is not evidence of a run in the new bucket.

| Metric | Result |
| --- | ---: |
| Input files | 60 |
| Decoded input rows | 9,659,781 |
| Study-area rows | 3,668,581 |
| Successful Static matches | 3,637,156 |
| Unmatched added-trip rows, retained with flags | 31,425 |
| Retained prediction-history rows | 3,668,581 |
| Exact prediction duplicates removed | 0 |
| Missing delay values | 27,269 |
| Explicit zero delays | 375,219 |
| Script runtime | 522.137 s (8 min 42 s) |

`counts.matched` in the report includes both matched and unmatched rows.
Different feed timestamps remain separate observations, so zero exact-duplicate
removals does not mean each trip-stop event appears only once. No event-level
final prediction is selected in this job.

Output:
`s3://datavault-v2/silver/gtfs_rt_processed/run_id=<NEW_RUN_ID>/`

Main datasets: `prediction_history/`, `capture_lineage/`, `rejected_rows/`,
and `quality/`. Runtime is measured inside the script, not end-to-end Glue time.

Output remains isolated by run ID. Input date/hour and the exact selected
objects are recorded in the input manifest and report. This path migration
does not add an hourly scheduler or automatically calculate the input hour.
