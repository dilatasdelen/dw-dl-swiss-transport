# Analyzing Weather-Related Delay Patterns in Swiss Public Transport
Data lake and warehouse pipeline analyzing weather, elevation, and real-time delay patterns in Swiss public transport (Zürich, Zug, Luzern, Schwyz). HSLU DW&DL course project.

## Setup
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
Create a `.env` file with your own API key: 
    GTFS_RT_API_KEY = ##

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
│   └── raw/            (gitignored — don't commit pulled data)
├── notebooks/          (optional, for exploration/testing)
├── requirements.txt
├── .env                (gitignored — your own API key, never commit this)
├── README.md
└── .gitignore
```

## Running the ingestion scripts

**GTFS-RT** (needs an API key):
1. Add the GTFS-RT API key to `.env`: `GTFS_RT_API_KEY=your_key_here`
2. Run: `python3 src/ingestion/gtfs_rt_ingest.py`
3. Saves a `.pb` snapshot to `data/raw/gtfs_rt/`

**Open-Meteo** (no key needed — free, open API):
1. Run: `python3 src/ingestion/open_meteo_ingest.py`
2. Saves a `.json` with current weather for the 4 cantons to `data/raw/weather/`

Each run creates a new timestamped file, so running them repeatedly over time builds up the raw data collection (not a one-off pull).

## Authors
Dila Tasdelen, Ji Hyeon Choung, Sediqullah Kismat Khan