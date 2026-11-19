# Analyzing Weather-Related Delay Patterns in Swiss Public Transport

Data lake and warehouse pipeline analyzing weather, elevation, and public transport delays in Zürich, Zug, Luzern, and Schwyz. HSLU DW&DL course project.

## Project status

Updated November 19, 2026.

- Bronze ingestion and Silver preparation are complete for the collected inputs.
- Gold event-level processing and coverage validation are complete.
- The audit verified 364 historical service dates and 371 dates overall.
- October 19, 2026 is the known missing upstream Ist-Daten date.
- Gold research summaries for KAN-38 are the next stage.

## AWS deployment

- Account ID `144911976943`
- Region `us-east-1`
- Data bucket `datavault-v2`
- Glue execution role `gtfs-glue-job-role`
- Glue scripts location `s3://aws-glue-assets-144911976943-us-east-1/scripts/`

All dataset prefixes below are relative to `s3://datavault-v2/`.

| Dataset | S3 prefix |
| --- | --- |
| Bronze GTFS-RT protobuf | `bronze/gtfs_rt/ingest_date=YYYY-MM-DD/hour=HH/` |
| Bronze Static ZIPs | `bronze/gtfs_static/` |
| Bronze boundary releases | `bronze/swissboundaries3d/` |
| Bronze Ist-Daten | `bronze/ist_daten/version=v2/service_date=YYYY-MM-DD/` |
| Bronze elevation | `bronze/open_meteo/elevation/feed_version=VERSION/` |
| Bronze forecasts | `bronze/open_meteo/weather/feed_version=VERSION/` |
| Bronze historical ERA5 weather | `bronze/open_meteo/weather_historical/model=era5/feed_version=VERSION/` |
| Silver decoded snapshots | `silver/gtfs_rt_decoded/ingest_date=YYYY-MM-DD/hour=HH/` |
| Silver stop/canton reference | `silver/reference/gtfs_static_stops/feed_version=VERSION/` |
| Silver timetable reference | `silver/reference/gtfs_static_stop_times/feed_version=VERSION/` |
| Silver station elevation | `silver/reference/stop_elevation/feed_version=VERSION/run_id=RUN_ID/` |
| Glue 1 prediction history | `silver/gtfs_rt_processed/run_id=RUN_ID/` |
| Glue 2 event candidates and revision summaries | `silver/gtfs_rt_event_selection/run_id=RUN_ID/` |
| Silver Ist-Daten stop visits | `silver/stop_events/source=istdaten/date=YYYY-MM-DD/` |
| Silver hourly weather | `silver/open_meteo_weather/` |
| Silver stop-to-weather mapping | `silver/open_meteo_weather/stop_to_location/feed_version=VERSION/` |
| Collection audit and decode quality | `audit/gtfs_rt/`, `quality/gtfs_rt_snapshot_decode/` |
| Gold stop-event analysis | `gold/stop_event_analysis/service_date=DATE/run_id=RUN_ID/` |
| Gold coverage audit and input manifest | `quality/stop_event_analysis_audit/run_id=RUN_ID/` |

## Bronze ingestion

### Ist-Daten

The archive backfill collected 364 available service dates from
November 9, 2025 through November 8, 2026.

Each date preserves the original `IstDaten.csv` and an `ingestion.json`
manifest containing source and ingestion metadata.

October 19, 2026 was absent from the official archive and fallback source.
Daily ingestion is scheduled at 16:00 UTC.

### Weather

- Forecast snapshots are collected hourly at minute 10 UTC for 19 locations.
- Forecast collection begins November 9, 2026.
- The ERA5 historical backfill completed 513 windows across 19 grid cells.
- Historical coverage is November 8, 2025 through November 8, 2026 UTC.
- The extra initial UTC day covers Swiss midnight at the start of the study.
- Temperature, precipitation, rain, snowfall, wind speed, and weather code
  are retained.

### Elevation

Open-Meteo terrain elevation collection covered 13,207 unique valid
coordinate pairs for feed `20261104`, reusing cached responses where available.

## Silver preparation

### GTFS-RT

- The decoder converts protobuf snapshots into structured records.
- Glue Job 1 filters to the study area, matches Static references,
  and preserves prediction history.
- Glue Job 2 selects the latest valid prediction in its processing window
  and writes event candidates and revision summaries.
- An hourly Lambda launcher orchestrates processing.
- The reported backfill covered 241 hours spanning November 5–15, 2026.

Delay values represent published predictions in seconds. Events can recur
across processing windows, so Gold performs cross-run deduplication.

### Stop, canton, and elevation references

The stop-to-canton reference for feed `20261104` contains 105,021 stop rows,
including 16,062 study-area rows. It uses swissBOUNDARIES3D release `2026-01`
and coordinate system `EPSG:2056`.

The verified elevation lookup contains 4,441 parent stations.

Use this exact station-level production path:

```text
s3://datavault-v2/silver/reference/stop_elevation/feed_version=20261104/run_id=20261110T191911578489Z/
```

The station key is `(feed_version, parent_stop_id)`. Platforms resolve through
`parent_station` in the matching Static feed. The broader elevation prefix
also contains an older stop-level output with a different schema.

### Ist-Daten and weather

Silver Ist-Daten retains scheduled and provided arrival/departure times,
delays, cancellation flags, and timing-quality statuses. Source times are
converted from Swiss local time to UTC.

Silver weather contains hourly grid-level records. Forecast snapshots retain
their issue and retrieval times. Historical reanalysis and forecast products
retain separate `data_product` and `model` values.

## Gold event-level analysis

Script `src/processing/glue_stop_event_analysis.py` builds the Gold dataset
in AWS Glue 5.0.

### Grain and processing rules

- One event is one source-specific stop arrival or departure.
- The Gold key is `(source_type, event_key)`.
- Ist-Daten visits become separate scheduled arrival/departure events.
- GTFS-RT candidates are deduplicated across runs using publication timestamps
  and deterministic tie-breaking.
- Both sources retain their timing statuses, delays, event times,
  source paths, and reference lineage.
- Weather joins use the mapped location and scheduled event's UTC hour.
- GTFS-RT uses forecasts available at prediction publication time.
- Ist-Daten uses historical reanalysis.
- Canton and parent-station joins use versioned stop references.
- Elevation reuse across feeds requires identical parent station ID,
  didok, and coordinates.
- Weather mapping reuse across feeds requires identical stop ID,
  didok, coordinates, and canton.
- Unmatched context remains flagged in the output.

Each successful date report contains source row counts, Gold event counts,
duplicate-key checks, and weather, elevation, and canton match rates.

### Parameters

| Parameter | Purpose |
| --- | --- |
| `--DATA_BUCKET` | `datavault-v2` |
| `--SERVICE_DATE` | Process one date |
| `--START_DATE` and `--END_DATE` | Process an inclusive date range |
| `--SOURCE_MODE` | `both` by default, or `istdaten` for historical processing |

Batch mode skips completed dates under the same rule version and required
source coverage. This completion check does not detect subsequent changes
to Silver inputs.

Reference tables and S3 listings are reused across dates. Historical
processing reads relevant ERA5 windows.

Each attempt writes to a separate run folder. Consumers must select one
approved output per date to avoid counting reruns twice.

## Gold validation and KAN-38 handoff

Script `src/processing/audit_stop_event_analysis.py` checks successful
reports, required source coverage, duplicate-check results, and nonempty
Parquet presence. It relies on each processing run's row-level validation.

The completed audit verified:

- 364 historical dates from November 9, 2025 through November 8, 2026.
- 371 dates overall, including November 9–15, 2026.
- No unexpected missing dates.
- No dates missing their required source outputs.

The handoff files are stored together under:

```text
s3://datavault-v2/quality/stop_event_analysis_audit/run_id=e85ad74b72a34f148645542a73dd8cfe/
```

- `input_manifest.json` lists the approved event output paths for KAN-38.
- `report.json` contains validation results, match rates, and coverage notes.

Use the manifest's exact paths when reading Gold.

### Coverage limitations

- October 19, 2026 is the known missing upstream Ist-Daten date.
- Forecast collection starts partway through November 9, 2026.
- Historical weather ends November 8, 2026 UTC.
- Some events retain missing weather or elevation context.
- Weather row matches can contain missing individual measurements.
- Historical events retain the reference versions assigned during Silver preparation.

KAN-38 will produce weather-impact, stop-difference, elevation-pattern,
and network-hotspot summaries. These must retain source distinctions,
Ist-Daten status, date ranges, and observation counts.

## Main scripts

| Stage | Scripts |
| --- | --- |
| GTFS-RT collection | `src/ingestion/collect_gtfs_rt.py` |
| Ist-Daten collection | `src/ingestion/ist_daten_archive_backfill.py`, `src/ingestion/ist_daten_daily_ingest.py` |
| Weather collection | `src/ingestion/open_meteo_forecast_collector.py`, `src/ingestion/open_meteo_historical_backfill.py` |
| Elevation collection | `src/ingestion/open_meteo_elevation.py` |
| GTFS-RT decoding | `src/processing/gtfs_rt_decoder_lambda.py` |
| GTFS-RT Silver processing | `src/processing/glue_staging_silver.py`, `src/processing/glue_event_selection.py` |
| Hourly orchestration | `src/orchestration/gtfs_rt_batch_launcher.py` |
| Stop/canton reference | `src/processing/glue_static_stops_canton.py` |
| Timetable reference | `src/processing/glue_static_stop_times.py` |
| Station elevation | `src/processing/glue_stop_elevation.py` |
| Ist-Daten Silver processing | `src/processing/glue_istdaten_stop_events.py` |
| Weather Silver processing | `src/processing/glue_open_meteo_weather.py` |
| Gold event processing | `src/processing/glue_stop_event_analysis.py` |
| Gold audit | `src/processing/audit_stop_event_analysis.py` |

## Setup and deployment notes

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For the local GTFS-RT collector, create a gitignored `.env` file:

```dotenv
GTFS_RT_API_KEY=your_key_here
```

Run the local collector:

```bash
python3 src/ingestion/gtfs_rt_ingest.py
```

Local snapshots are saved under `data/raw/gtfs_rt/`.

AWS Glue scripts require the Glue runtime and execution role. Local
dependency installation alone does not provide that runtime.

For the deployed collector, set `RAW_BUCKET=datavault-v2`.

Package the decoder with `src/models/gtfs_rt_snapshot.py` and
`requirements-lambda.txt` dependencies. Configure:

```text
DATA_BUCKET=datavault-v2
RAW_PREFIX=bronze/gtfs_rt/
STAGING_PREFIX=silver/gtfs_rt_decoded
QUALITY_PREFIX=quality/gtfs_rt_snapshot_decode
```

Restrict the decoder S3 trigger to `bronze/gtfs_rt/` and suffix `.pb`.
Check deployed Lambda environment values when updating collectors.

Use `sql/athena_gtfs_rt_monitoring.sql` for collection/decode monitoring
and `s3://datavault-v2/athena-results/` for Athena results.

Gold jobs used two G.1X workers with zero retries. Job metrics and
Job observability metrics were disabled. Output/error logging can still
incur CloudWatch charges.

Raw data, credentials, and generated deployment packages should remain
excluded from commits.

## Authors

Dila Tasdelen, Ji Hyeon Choung, Sediqullah Kismat Khan