# AWS Glue job 2: event candidates and prediction revisions

[Code](glue_event_selection.py) · [Execution report](reports/events-20261012-20-v1-2.report.json)

We split the jobs to preserve job 1's detailed prediction history while
job 2 derives event-level candidates and revision summaries separately.
Static is not joined again. Arrival/departure events and feed versions remain
separate.

## Run parameters

```text
--DATA_BUCKET        datavault-v1
--INPUT_RUN_IDS      hour-20261012-20
--RUN_ID             events-20261012-20-v1-2
--WINDOW_START_UTC   2026-10-12T20:00:00Z
--WINDOW_END_UTC     2026-10-12T21:00:00Z
--RULE_VERSION       pilot_latest_valid_feed_v1
```

Input:
`s3://datavault-v1/experiments/staging_silver_pilot/run_id=hour-20261012-20/`

The selection window uses feed timestamps, with an inclusive start and
exclusive end. The job selects the latest valid prediction per trip-stop
event and event type within that window.

## Results

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
`s3://datavault-v1/experiments/event_selection_pilot/run_id=events-20261012-20-v1-2/`

Main datasets: `event_candidates/`, `revision_summary/`,
`selected_capture_lineage/`, `excluded_observations/`,
`blocked_event_observations/`, and `quality/`.
Runtime is measured inside the script, not end-to-end Glue time.
