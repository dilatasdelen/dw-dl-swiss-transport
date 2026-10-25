# AWS Glue job 1: Silver prediction history

[Code](glue_staging_silver.py) · [Execution report](reports/hour-20261012-20.report.json)

Tested study-area filtering and versioned GTFS Static matching on one hour of
decoded GTFS-RT data. The job retains prediction history and raw-capture lineage.

## Run parameters

```text
--DATA_BUCKET       datavault-v1
--INGEST_DATE       2026-10-12
--INGEST_HOUR       20
--FEED_VERSION      20261010
--RUN_ID            hour-20261012-20
--MAX_INPUT_FILES   0
```

Input partition (UTC):
`s3://datavault-v1/staging/gtfs_rt_prediction_snapshot/ingest_date=2026-10-12/hour=20/`

References: `reference/gtfs_static_stops/feed_version=20261010/` and
`reference/gtfs_static_stop_times/feed_version=20261010/` in the same bucket.
The stop reference includes polygon-based canton assignments. The job filters
ZH/ZG/LU/SZ before the larger timetable join.

## Results

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
`s3://datavault-v1/experiments/staging_silver_pilot/run_id=hour-20261012-20/`

Main datasets: `prediction_history/`, `capture_lineage/`, `rejected_rows/`,
and `quality/`. Runtime is measured inside the script, not end-to-end Glue time.
