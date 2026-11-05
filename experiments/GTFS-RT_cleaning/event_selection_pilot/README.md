# AWS Glue job 2: event candidates and prediction revisions

[Code](glue_event_selection.py) · [Execution report](reports/events-20261012-20-v1-2.report.json)

We split the jobs to preserve job 1's detailed prediction history while
job 2 derives event-level candidates and revision summaries separately.
Static is not joined again. Arrival/departure events and feed versions remain
separate.

## Run parameters for datavault-v2

Use completed Glue 1 run labels from the new bucket and a new output run label.
Replace the UTC window placeholders before running.

```text
--DATA_BUCKET        datavault-v2
--INPUT_RUN_IDS      <JOB1_RUN_ID>
--RUN_ID             <NEW_RUN_ID>
--WINDOW_START_UTC   <UTC_START_ISO8601>
--WINDOW_END_UTC     <UTC_END_ISO8601>
--RULE_VERSION       pilot_latest_valid_feed_v1
```

Input:
`s3://datavault-v2/silver/gtfs_rt_processed/run_id=<JOB1_RUN_ID>/`

Multiple input run labels can be comma-separated. Inputs must contain
`prediction_history/`, `capture_lineage/`, `report.json` and `_EXPERIMENT_SUCCESS`.

The selection window uses feed timestamps, with an inclusive start and
exclusive end. The job selects the latest valid prediction per trip-stop
event and event type within that window.

## Recorded pilot results

The linked report describes the previous account's one-hour pilot. Its metrics
remain unchanged; the new deployment uses the Silver paths documented here.

| Metric | Result |
| --- | ---: |
| Input history records | 3,668,581 |
| Eligible prediction observations | 3,623,779 |
| Event candidates | 93,556 |
| Events with prediction revisions | 42,667 |
| Events blocked by conflicting values or multiple trip start times | 0 |
| Selected events with a later excluded observation | 121 |
| Selected explicit zero delays | 6,447 |
| Script runtime | 378.352 s (6 min 18 s) |

Excluded observations:

| Reason | Rows |
| --- | ---: |
| No valid Static match | 31,425 |
| SKIPPED / NO_DATA stop prediction | 12,172 |
| Missing published delay after the preceding checks | 1,205 |

Eligible and excluded counts sum to the input. Each excluded row receives
the first matching reason. Prediction revisions count changes in delay,
published event time or uncertainty, rather than new feed timestamps alone.

These are latest valid candidates in the selected window, not actual times
or journey-wide final predictions. Cross-hour finalisation remains open.

Output:
`s3://datavault-v2/silver/gtfs_rt_event_selection/run_id=<NEW_RUN_ID>/`

Main datasets: `event_candidates/`, `revision_summary/`,
`selected_capture_lineage/`, `excluded_observations/`,
`blocked_event_observations/`, and `quality/`.
Runtime is measured inside the script, not end-to-end Glue time.

These provisional event candidates remain in Silver. This job does not create
Gold facts or analytical aggregates.
