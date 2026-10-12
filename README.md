# Analyzing Weather-Related Delay Patterns in Swiss Public Transport
Data lake and warehouse pipeline analyzing weather, elevation, and real-time delay patterns in Swiss public transport (Zürich, Zug, Luzern, Schwyz). HSLU DW&DL course project.

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

## Building the versioned stop-to-canton reference

The canton reference job downloads and preserves the official
swissBOUNDARIES3D GeoPackage, reads `stops.txt` directly from a versioned GTFS
Static ZIP, transforms WGS84 stop coordinates to the polygon CRS, and assigns
each stop with an auditable point-in-polygon join.

```bash
python -m src.processing.canton_join \
  --gtfs-zip data/raw/gtfs_static/feed_version=20261010/GTFS_FP2026_20261010.zip \
  --feed-version 20261010 \
  --boundary-release 2026-01
```

Generated outputs are gitignored and written to:

```text
data/raw/swissboundaries3d/release=2026-01/
data/reference/gtfs_static_stops/feed_version=20261010/part-00000.parquet
data/quality/canton_assignment/feed_version=20261010/summary.json
```

Join this reference to version-matched GTFS-RT rows with `feed_version` and
`static_stop_id = stop_id`. Retain all join statuses for quality reporting and
filter on `in_study_area` only in the analytical stage.

The verified `20261010` reference-run results are documented in
`docs/canton_join_validation_20261010.md`.

## Authors
Dila Tasdelen, Ji Hyeon Choung, Sediqullah Kismat Khan
