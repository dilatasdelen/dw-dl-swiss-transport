# Analyzing Weather-Related Delay Patterns in Swiss Public Transport
Data lake and warehouse pipeline analyzing weather, elevation, and real-time delay patterns in Swiss public transport (Zürich, Zug, Luzern, Schwyz). HSLU DW&DL course project.

## AWS deployment: datavault-v2

Storage paths updated on 2026-11-05 for the new account. The existing local
scripts below keep their local directories.

| Dataset | S3 prefix in `datavault-v2` |
| --- | --- |
| Bronze GTFS-RT protobuf | `bronze/gtfs_rt/ingest_date=YYYY-MM-DD/hour=HH/` |
| Bronze Static ZIPs and boundary releases | `bronze/gtfs_static/`, `bronze/swissboundaries3d/` |
| Silver decoded snapshots | `silver/gtfs_rt_decoded/ingest_date=YYYY-MM-DD/hour=HH/` |
| Silver stop/canton reference | `silver/reference/gtfs_static_stops/feed_version=VERSION/` |
| Silver timetable reference | `silver/reference/gtfs_static_stop_times/feed_version=VERSION/` |
| Glue 1 Silver history and quality outputs | `silver/gtfs_rt_processed/run_id=RUN_ID/` |
| Glue 2 Silver candidates and revision summaries | `silver/gtfs_rt_event_selection/run_id=RUN_ID/` |
| Collection audit and decode quality | `audit/gtfs_rt/`, `quality/gtfs_rt_snapshot_decode/` |
| Future Gold facts and aggregates | `gold/` (not written by these jobs) |

- Collector: `src/ingestion/collect_gtfs_rt.py`, with `RAW_BUCKET=datavault-v2`.
- Decoder: `src/processing/gtfs_rt_decoder_lambda.py`, packaged with
  `src/models/gtfs_rt_snapshot.py` and `requirements-lambda.txt` dependencies.
  Set `DATA_BUCKET=datavault-v2`, `RAW_PREFIX=bronze/gtfs_rt/`,
  `STAGING_PREFIX=silver/gtfs_rt_decoded` and
  `QUALITY_PREFIX=quality/gtfs_rt_snapshot_decode`.
- Restrict the decoder S3 trigger to `bronze/gtfs_rt/` and suffix `.pb`.
- Both Glue scripts under `experiments/GTFS-RT_cleaning/` require
  `--DATA_BUCKET=datavault-v2`. Their READMEs list the remaining run arguments.
- Use `sql/athena_gtfs_rt_monitoring.sql` for the new audit/quality tables.
  Set the Athena query-result location to `s3://datavault-v2/athena-results/`.

Existing Lambda environment values override code defaults, so update them
when deploying. Upload matching reference Parquet to the Silver reference
paths before running Glue. Bronze Static/boundary paths are source-storage
destinations; this change does not add new source collectors or a timetable
reference builder.

Run IDs, no-overwrite behaviour and processing rules are unchanged. Glue still
requires explicit input dates/hours and versions; an hourly launcher is not
included in this storage migration. Historical reports retain their original
account paths and results.

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
Create a `.env` file with the API key: 
    GTFS_RT_API_KEY = api_key_here

## Scripts
```
dw-dl-swiss-transport/
├── src/
│   ├── ingestion/
│   │   ├── gtfs_rt_ingest.py       # polls GTFS-RT feed, saves raw snapshots
│   │   └── open_meteo_ingest.py    # polls current weather per canton
│   └── processing/
│       └── canton_join.py          # assigns canton to lat/lon points (needs swissBOUNDARIES3D)
├── data/
│   └── raw/            (gitignored — raw pulled data is excluded from git)
│        ├── stops.txt
├── notebooks/          (optional, for exploration/testing)
├── requirements.txt
├── .env                (gitignored — contains the API key and is excluded from git for security)
├── README.md
└── .gitignore
```

## Running the ingestion scripts

**GTFS-RT** (needs an API key):
1. Add the GTFS-RT API key to `.env`: `GTFS_RT_API_KEY=api_key_here`
2. Run: `python3 src/ingestion/gtfs_rt_ingest.py`
3. Saves a `.pb` snapshot to `data/raw/gtfs_rt/`

**Open-Meteo** (no key needed — free, open API):
1. Run: `python3 src/ingestion/open_meteo_ingest.py`
2. Saves a `.json` with current weather for the 4 cantons to `data/raw/weather/`

Each run creates a new timestamped file, so running them repeatedly over time builds up the raw data collection (not a one-off pull).

## Authors
Dila Tasdelen, Ji Hyeon Choung, Sediqullah Kismat Khan
