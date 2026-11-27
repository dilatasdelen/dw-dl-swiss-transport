# Temperature comparison rules and pilot validation

Rule version: `research-summary-v2-temperature`

## Research question

How do published delay estimates under hot and cold conditions differ from
normal-temperature conditions within each elevation band?

These are descriptive comparisons of associations. They do not establish a
causal temperature effect; seasonal, route and service-composition differences
may influence the results.

## Temperature definition

Use the joined event-hour `temperature_2m_c` in degrees Celsius:

| Band | Rule |
| --- | --- |
| Cold | temperature < 0 °C |
| Normal temperature (reference) | 0 °C <= temperature < 25 °C |
| Hot | temperature >= 25 °C |
| Unknown | weather join missing, temperature null or NaN |

The freezing boundary provides an interpretable physical threshold. The 25 °C
boundary is inspired by the MeteoSwiss summer-day indicator. MeteoSwiss's
indicator uses a **daily maximum**, whereas this project applies the boundary
to **hourly temperature**. These bands are project comparison rules, not
official hourly heat/frost warnings. Normal temperature does not imply dry,
calm or climatologically typical weather.

Reference: [MeteoSwiss climate indicators](https://www.meteoswiss.admin.ch/dam/jcr%3A50da2b8f-3a0b-4082-a3b9-a26cd9525670/CH2025_Uebersicht_Klimaindikatoren_en.pdf).

## Concurrent conditions and missing measurements

Temperature bands are separate from the existing priority-based snow, rain,
strong-wind and frost categories. Those existing categories remain available
for the first two research questions; a snowy cold event remains cold here.

Two comparison scopes are produced:

- `all_conditions`: temperature groups pooling concurrent conditions.
- `concurrent_weather_stratum`: groups separated by combinations of rain
  (hourly precipitation >= 1 mm), snow (hourly snowfall >= 0.1 cm), and strong
  wind (wind speed >= 39 km/h). `none` means all three measurements are present
  and below those thresholds. Missing/NaN concurrent measurements give an
  `unknown` stratum rather than an assumed absence of adverse conditions.

Select one scope when reporting totals. Summing both scopes double-counts
events. Unknown temperature rows remain visible but cannot qualify as hot/cold
comparisons. Unknown concurrent weather is explicit and is not a clear-weather
control group.

## Grouping, metrics and sparse comparisons

Group by source, source time status, delay meaning, arrival/departure event
type, elevation group, comparison scope, concurrent-weather stratum and
temperature band. Elevation bands: low < 500 m; medium 500 to < 1000 m;
high >= 1000 m; unknown elevation remains explicit.

Compare each hot/cold group with normal temperature in the same remaining
group fields. Do not combine GTFS route IDs with Ist-Daten line IDs or combine
source time statuses.

Outputs include total events, usable observations, cancellations, observed and
usable date counts, observed date bounds, median delay, p90 delay, mean delay,
and severe-delay rate. Cancelled events and null delays are excluded from delay
metrics but retained in event/cancellation counts. Severe delay is the project's
existing threshold of delay >= 180 seconds. Percentiles use Spark
`percentile_approx` with accuracy 10000. Differences are hot/cold minus normal
for median, p90 and severe-delay rate (a fraction, not a percentage).

At least 30 usable observations are required on **each** compared side. This
is a pragmatic screening rule, not a guarantee of statistical significance or
independence. Smaller groups and absent baselines remain visible with
`comparison_meets_minimum_observations=false` and `sparse_comparison=true`.
Date coverage must accompany interpretation; event counts are not counts of
independent journeys or weather episodes.

GTFS-RT is labelled published prediction. Ist-Daten REAL, GESCHAETZT, PROGNOSE
and UNBEKANNT remain separate, labelled measured, estimated, forecast in source
and unknown respectively. Forecast weather is used for GTFS-RT and historical
reanalysis for Ist-Daten, according to the approved event dataset lineage.

## Executed pilot evidence

The summer and winter pilots used approved manifest:
`s3://datavault-v2/quality/stop_event_analysis_audit/run_id=9f686ddd3d264ad98b7222a06bdca3bd/input_manifest.json`.

| Check | Summer pilot | Winter pilot |
| --- | --- | --- |
| Inclusive dates | 2026-07-13 to 2026-07-19 | 2026-01-12 to 2026-01-18 |
| Summary rows | 128 | 147 |
| Usable extreme-temperature events, pooled scope | 3,071,686 hot | 1,405,254 cold |
| Usable normal-temperature events, pooled scope | 5,069,610 | 6,818,029 |
| Qualified hot/cold comparisons across both scopes | 45 | 48 |
| Duplicate summary grouping keys | 0 | 0 |
| Source present | Ist-Daten | Ist-Daten |

The winter output contains 21 sparse cold comparison rows. Both outputs
preserve source statuses and contain severe-delay rates within [0, 1]. Summer
has no cold groups; winter has no hot groups. These are observed coverage
limitations, not missing categories filled with invented values. Neither pilot
validates GTFS-RT temperature coverage or the full study period.

Local inspected artifacts:

- Summer: `part-00000-4b3aa9ff-85c5-4cba-a6bf-917d622ac4d7-c000.snappy.parquet`
- Winter: `part-00000-c533e3e5-bf0d-4853-8961-d76b472c27dc-c000.snappy.parquet`

Pilot output prefix:
`s3://datavault-v2/gold/analysis_temperature_pilot/elevation_patterns/`.
This prefix is overwritten by each pilot. The last inspected run there was
the November GTFS-RT pilot described below; retain downloaded summer and
winter artifacts as separate validation evidence.
Pilot report: `s3://datavault-v2/quality/gold_analysis_temperature_pilot/report.json`.

Implementation: `glue_gold_analysis_temperature.py`. The current script is
deliberately limited to seven inclusive dates and only the temperature summary.
It resolves the latest approved manifest once per run, merges historical
schemas, records the immutable input manifest and writes isolated pilot outputs.

## Completion scope and handoff

The temperature-definition task is complete: thresholds, reference group,
concurrent/missing-weather handling, metrics, status separation and sparse-data
rules are documented and exercised on summer/winter Ist-Daten and November
GTFS-RT output artifacts.

The final full-period summary refresh is downstream work. The standalone pilot
retains its seven-day limit. The prepared integrated script supports an explicit
full-period mode, but must first pass integration testing and use the final
approved snapshot, reviewed all-period coverage and compatible consumer tables.
Recurring hotspot persistence is a separate task and is not implemented here.

## Additional GTFS-RT pilot

An additional pilot covered 2026-11-10 through 2026-11-15. Its inspected file
was `part-00000-b8e49fe3-88ed-49d6-8e65-c98da474b2b3-c000.snappy.parquet`.
It contains 100 summary rows and zero duplicate grouping keys. All 56 GTFS-RT
summary rows are labelled `published_prediction`; Ist-Daten retains measured,
estimated, source-forecast and unknown statuses separately.

In the pooled scope, GTFS-RT has 557,119 usable cold observations, 6,460,927
normal observations and 2,081 unknown-temperature observations. Twenty cold
comparison rows across both scopes meet the minimum-count rule. No hot
GTFS-RT observations are present in this November pilot.

All 6,961,035 usable Ist-Daten observations in this pilot have unknown
temperature, consistent with the documented absence of historical weather in
the approved inputs for these dates. They remain visible and do not qualify
for temperature comparisons. This verifies missing-context handling, not
historical weather completeness. All reported severe-delay rates are in
[0, 1]. The pilot output prefix now contains this November run, replacing the
winter pilot there; downloaded artifacts preserve the earlier evidence.

Together, the summer/winter Ist-Daten pilots and November GTFS-RT pilot exercise
hot/cold comparisons, source/status separation and missing weather handling.
Hot GTFS-RT coverage remains unobserved and must not be implied in final results.

## Latest approved Gold snapshot and integration status

As of the successful refresh on 27 November 2026, Gold is approved through
**25 November 2026**: 381 selected dates, including 364 historical dates, with
no unexpected missing dates, no missing required sources and no deferred dates.
The 60 missing Silver processing windows have been repaired; all six formerly
deferred dates are now eligible. This does not remove the previously documented
missing historical source date (19 October 2026) or create absent weather.

Latest approved manifest:
`s3://datavault-v2/quality/stop_event_analysis_audit/run_id=4efac5fc48a24a97ab2554d33f2e44c5/input_manifest.json`.
Audit: `report.json` in the same folder.

This newer manifest does not retroactively replace the manifests used by the
pilots above. Their results remain evidence for their recorded snapshots.
Weekly refresh is enabled for Sunday 03:00 UTC, with a two-day lag; it defers
ineligible dates and updates approval only after a successful audit. Deactivate
the schedule at the agreed final collection cutoff.

`glue_gold_analysis_integrated.py` has been prepared and syntax/import checked,
but has not yet passed an integration run. It preserves the original four
summary layouts and adds `temperature_comparisons` and `network_persistence`,
plus weekly supporting evidence. Its isolated output root is
`gold/analysis_integration/run_id=RUN_ID/`; production summaries are unchanged.
New PostgreSQL definitions and loader mappings are still needed for the two
additional summaries. Keep detailed events and weekly evidence in S3.
