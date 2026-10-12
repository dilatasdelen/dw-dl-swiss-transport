# Canton Join Validation: GTFS Static 20261010

## Scope

This validation assigns every stop in the GTFS Static `20261010` release to
the canton polygons in swissBOUNDARIES3D `2026-01`. The spatial join is run
once per versioned stop record; GTFS-RT event rows later use a relational join
on `feed_version` and `stop_id`.

## Source provenance

- swissBOUNDARIES3D release: `2026-01`
- Official source URL:
  `https://data.geo.admin.ch/ch.swisstopo.swissboundaries3d/swissboundaries3d_2026-01/swissboundaries3d_2026-01_2056_5728.gpkg.zip`
- Archive SHA-256:
  `68e922353c76fa5db3cef06a32f9711c0198faa6fbd2b5bcde9edc88b0f8999f`
- Extracted GeoPackage SHA-256:
  `1f122cb7a06f2d312a84b7c0a91116348ba907054d487f0a70b9d2302984e6fc`
- Layer: `tlm_kantonsgebiet`
- Polygon CRS: `EPSG:2056`
- GTFS point CRS before transformation: `EPSG:4326`
- Spatial predicate: `covered_by`

## Reconciliation results

| Check | Result |
|---|---:|
| Input and output stop rows | 104,479 |
| Unique stop identifiers | 104,479 |
| Duplicate stop identifiers | 0 |
| Matched to one Swiss canton | 83,079 |
| Outside all Swiss canton polygons | 21,400 |
| Ambiguous boundary matches | 0 |
| Invalid coordinates | 0 |
| Invalid boundary geometries before repair | 0 |

The output row count equals the input row count. No records were silently
dropped. Inspection of unmatched samples found international stops such as
Praha, Barcelona, German stations, and Liechtenstein stops. The national GTFS
feed is therefore broader than the Swiss canton polygons, as expected.

## Study-area results

| Canton | Code | Assigned stop rows |
|---|---:|---:|
| Zürich | ZH | 9,571 |
| Luzern | LU | 3,242 |
| Schwyz | SZ | 1,843 |
| Zug | ZG | 944 |
| **Total** | | **15,600** |

Known-location smoke checks passed for Zürich HB, Luzern, Zug, and Schwyz.
Each location, including its available platform records, was assigned to the
expected canton.

## Reproduction

Install `requirements.txt`, then run:

```bash
python -m src.processing.canton_join \
  --gtfs-zip data/raw/gtfs_static/feed_version=20261010/GTFS_FP2026_20261010.zip \
  --feed-version 20261010 \
  --boundary-release 2026-01
```

Local outputs:

```text
data/raw/swissboundaries3d/release=2026-01/
data/reference/gtfs_static_stops/feed_version=20261010/part-00000.parquet
data/quality/canton_assignment/feed_version=20261010/summary.json
```

Recommended S3 destinations:

```text
s3://datavault-v1/raw/swissboundaries3d/release=2026-01/
s3://datavault-v1/reference/gtfs_static_stops/feed_version=20261010/
s3://datavault-v1/quality/canton_assignment/feed_version=20261010/
```

The Parquet output contains all stops and retains `canton_join_status`,
`canton_candidate_count`, the boundary release and CRS, and the Boolean
`in_study_area`. Filtering to the four cantons must happen after this reference
is retained so unmatched and out-of-area coverage remains auditable.
