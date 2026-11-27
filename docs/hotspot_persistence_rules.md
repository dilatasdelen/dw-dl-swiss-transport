# RQ4 persistence and minimum coverage

Implemented rule version: `hotspot-persistence-v2-weekly-pilot`.
Status: rules documented, synthetic scenarios passed and five-week Ist-Daten
outcome pilot validated. The user confirmed the final Glue run Succeeded.
Production integration, full-period refresh and GTFS-RT outcome validation
remain separate downstream work. The dependency-free scenario evaluator uses
the corresponding draft identifier `hotspot-persistence-v2-weekly-draft`.

## Group and observation period

Each group contains source type, original time-status group, delay meaning,
arrival/departure, stop ID, route identifier namespace, route identifier,
reference feed version, local scheduled hour (0–23), and weekday/weekend class.

For GTFS-RT, use the Gold `stop_id` and string `route_id`, versioned by
`reference_feed_version`. For Ist-Daten, use its versioned stop ID and native
`line_id`, labelled as an Ist-Daten identifier. Do not pool these namespaces or
join them without an independently validated crosswalk. Missing stop, route,
line or version values go to explicit excluded-identity diagnostics; do not
merge all unidentified routes into one hotspot. The pilot groups by native stop_id, not a station-level didok roll-up. A station
label/roll-up would require a versioned lookup; it is not implemented here.

One period is one **Monday–Sunday service week** for a fixed group. Derive
week start from service_date. Derive hour (0–23) and weekday/weekend class from
scheduled_time_utc converted to Europe/Zurich. After-midnight events retain
their original service week. Repeated autumn DST hours share the local bucket;
the spring skipped hour has no observations. Include complete approved weeks
only; exclude and report incomplete weeks. Weekday and weekend groups remain
separate, so a weekly weekday group can span five scheduled dates, a weekend
group two.

Identifiers are not assumed stable between feed versions. A version boundary
starts another group. This can reduce valid date coverage; do not claim
cross-version persistence without validated identifier continuity.

## Selected and pilot-validated rule

| Parameter | Value |
| --- | --- |
| Severe event | delay >= 180 seconds |
| Valid weekly period | >= 10 usable events |
| Severe weekly period | >= 20% of usable events severe |
| Minimum valid weeks | 5 |
| Minimum valid-week coverage | 50% of eligible weeks |
| Persistent hotspot | severe on >= 50% of valid weeks, after both coverage checks pass |

Usable events exclude cancelled events and null delays. Count each approved
Gold `(source_type, event_key)` once, not each prediction revision. Keep total,
cancelled, null-delay, usable and severe counts separately. GTFS-RT severity
describes published predictions, not measured outcomes; Ist-Daten time statuses
remain separate in every group.

The 180-second event threshold retains the existing summary setting. The
weekly 10-event minimum is informed by coverage checks, not delay outcomes:
on 9–15 November, measured Ist-Daten weekly groups meeting 10 observations
retained about 70% of usable events (34–35% of observed groups). A threshold of
30 retained only about 25% of usable events and 6% of groups. Daily groups had
a median of two usable events, supporting longer observation periods.

Five valid weeks prevents one disrupted week from establishing persistence.
Require severe periods on at least 50% of valid weeks and valid coverage on at
least 50% of eligible weeks. A severe week has at least 20% events delayed by
180 seconds or more. These remain project operational choices, not scientific
significance thresholds. Ten observations give coarse rates and favor busier
services; sparse groups stay visible. Test sensitivity to 5/10/30 observations,
10%/20% severe rates and 50%/75% recurrence; freeze the agreed version before
final outcome comparisons.

Use the longer Ist-Daten series for recurrence where versioned stop/line
identity is consistent. Do not treat a shared reference version as proof that
native line identifiers never changed meaning. GTFS-RT remains strictly grouped
by route ID plus feed version. Short-lived versions with fewer than five
eligible weeks receive insufficient coverage, not a persistence claim. No
cross-version route crosswalk is created by this work.

## Coverage denominator

List eligible weeks from the approved manifest where the required
source and feed version have validated processing coverage and the local time
bucket/day class can occur. Do not derive eligibility only from observed rows.
An absent group on an eligible week is missing/possibly non-operated, not
evidence of punctual service. Without a timetable-based operation denominator,
coverage is an availability screen and must not be reported as scheduled
service reliability. If exact route-operation eligibility cannot be derived,
use source-covered complete weeks for that version and explicitly label this limitation.
Never include the present date or incomplete inputs as complete periods.

## Weather comparisons and output

Keep the existing normal/adverse category rules for RQ4. Missing weather is a
third explicit class; it never enters normal. The temperature bands defined
for RQ3 do not replace these categories. Retain weather product/model provenance
and source-specific weather meaning (forecast for GTFS-RT, historical for
Ist-Daten). Do not pool weather models silently.

Produce overall and normal/adverse per-week aggregates for the same group.
Retain unknown-weather counts too. For each class publish observation count,
median, p90, severe-delay rate, observed and usable dates/date bounds, valid
periods, periods above threshold, valid-period share and severe-period share.
Keep eligible/valid week sets and observed service-date sets/counts so coverage
gaps are visible; the implementation does not enumerate all eligible route-operation dates.

Evaluate recurrence separately overall, under normal weather and under adverse
weather using the same weekly rules. Weather-specific periods need 10 usable
events **of that class**. Compare severe-delay rates and persistence only when
both normal/adverse sides meet the valid-week and count rules. Otherwise keep
the values/counts visible and set a sparse-comparison flag. Use null persistence
for insufficient evidence rather than falsely declaring a reliable group.
Normal/adverse observed date arrays and bounds are retained separately. Their
overlap can be calculated from those arrays; no dedicated overlap-count column
is currently emitted. Contrasts across different dates are descriptive
associations, not matched causal effects.

The final summary must record its rule version, immutable approved input
manifest, included dates and exclusions. Integrate only after the route-enriched
Gold snapshot is approved; existing separate station/line/hour rankings do not
implement this persistence definition.

## Synthetic validation

`hotspot_persistence_rules.py` is a dependency-free reference evaluator. It
expects already grouped weekly counts and deliberately performs no AWS writes.

| Example | Expected result |
| --- | --- |
| One severe week among 10 valid weeks | Not persistent (1/10) |
| Exactly 20% severe on 5 of 10 valid weeks | Persistent (5/10) |
| Four severe valid weeks, six missing | Sparse; persistence null |
| Ten weeks with only 9 usable events each | No valid weeks; sparse |
| No observed weeks | Sparse; persistence null |
| Five severe valid weeks among 20 eligible weeks | Sparse (25% coverage) |

Run `python3 hotspot_persistence_rules.py` to check all cases. These checks
do not alone establish real-data suitability. The frequency checks and successful
Ist-Daten outcome run below provide additional evidence for the selected rules.
Full-period and GTFS-RT outcome checks, integration testing and consumer-schema
alignment remain before updating production summary tables.

## Executed real-data coverage check

The executed diagnostic checked Ist-Daten for 2026-01-05 through 2026-02-08
(five complete weeks). It reported weekly count distributions and the number
of groups with five valid weeks at 10 observations, without classifying delay
outcomes. This checks feasibility, not recurrence itself. Existing November
GTFS coverage evidence shows feed-version fragmentation; final GTFS output
must explicitly retain insufficient-coverage flags.

The five-week coverage diagnostic succeeded on 41,779,362 Ist-Daten events.
All five weeks met the 10-observation minimum for 126,221 measured arrival
groups (27.22% of observed groups) and 120,604 measured departure groups
(26.11%). Estimated/source-forecast groups had very limited support; unknown
status had no qualifying groups. This supports a screened analysis of busier
services, not a whole-network reliability claim.

## Outcome pilot implementation

`glue_hotspot_persistence_pilot.py` applies the weekly rules on the same five
weeks. Its runtime rule version is `hotspot-persistence-v2-weekly-pilot`.
It retains normal/adverse/unknown counts, per-week and pooled metrics, exact
observed date lists, version eligibility and null sparse classifications.
Pooled medians/p90 are recalculated from event delays, not averaged from
weekly percentiles. Normal/adverse comparisons are flagged sparse unless both
classes have enough valid weeks. It rejects duplicate event/summary keys and
inconsistent counts/rates before publishing success.

Outputs are immutable attempts under
`gold/analysis_hotspot_pilot/run_id=RUN_ID/network_hotspots/` and
`gold/analysis_hotspot_pilot/run_id=RUN_ID/weekly_periods/`, with a report under
`quality/hotspot_persistence/run_id=RUN_ID/report.json`. These do not replace
production summaries. Local syntax/scenario checks and the final Glue outcome
run passed. Short GTFS history is not validated by this Ist-Daten-only pilot.

## Outcome evidence from the executed pilot

Final successful run ID: `8063f3033a374e6095c8ca84c8f161d4`.

Report:
`s3://datavault-v2/quality/hotspot_persistence/run_id=8063f3033a374e6095c8ca84c8f161d4/report.json`.

Summary:
`s3://datavault-v2/gold/analysis_hotspot_pilot/run_id=8063f3033a374e6095c8ca84c8f161d4/network_hotspots/`.

Weekly evidence:
`s3://datavault-v2/gold/analysis_hotspot_pilot/run_id=8063f3033a374e6095c8ca84c8f161d4/weekly_periods/`.

Implementation version: `combined-validation-v1`. Key uniqueness and
count/rate/sparse checks run in the same summary aggregation. The user confirmed
the Glue orchestration status is Succeeded, in addition to the script report.

The report records zero duplicate input/summary keys and zero invalid
count/rate/sparse-classification checks. Overall measured-event scopes contain
21,898 persistent arrival groups and 22,359 persistent departure groups. These
are versioned stop/line/local-hour/day-class groups, not unique stops.
Only 130 measured arrival groups and 129 measured departure groups support
both normal and adverse weather comparisons under the strict five-week rules.

The preceding attempt (run `598b091f5d8c4e5ea5823ab028f5b503`) produced the same
reported group counts but reached the orchestration timeout. Its downloaded summary part
`part-00000-01e8dcf3-c306-467d-86a2-700bb1ce08f2-c000.snappy.parquet`
contains 943,532 rows. Inspection found no mismatches in sufficient-coverage
or persistence classification, no non-null classifications on sparse groups,
and no severe-delay rates outside [0,1]. Source statuses remain separate and
all inspected weather lineage arrays contain ERA5. This is one output part,
not an independent rescan of the complete distributed output.

The earlier timeout attempt is superseded by the final successful run above.
Its part inspection is supporting evidence, not an independent rescan of the
new run's complete output. The final report's full-summary checks found zero
duplicates and invalid rows, and reproduced the previous pilot group counts.

## Handoff and completion scope

The definition task has documented keys/time buckets, weekly severity and
minimum-coverage rules, threshold justification, normal/adverse counts and date
coverage, explicit sparse flags, and isolated/recurring/sparse example checks.
Use the final successful pilot paths above as validation evidence. No production
summary table or approved event manifest was replaced by this pilot.

Before final loading, integrate the agreed rules into the main summary job and
refresh using the final approved event manifest. Retain short-version GTFS
groups as insufficient coverage; do not merge route versions without a validated
crosswalk. Report the limited normal/adverse comparison support explicitly.

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
