import json
import re
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter


BRONZE_GTFS_PREFIX = "bronze/gtfs_static"
BRONZE_BOUNDARY_PREFIX = "bronze/swissboundaries3d"
SILVER_PREFIX = "silver/reference/gtfs_static_stops"
QUALITY_PREFIX = "quality/gtfs_static_stops"
SUCCESS_MARKER = "_PREPARED_SUCCESS"
OUTPUT_FILE = "part-00000.parquet"

# Swiss canton numbers (BFS) of the study area
TARGET_CANTONS = {1: "ZH", 3: "LU", 5: "SZ", 9: "ZG"}

# Required by KAN-19 / glue_staging_silver.py, followed by useful extras
# (location_type and parent_station let the elevation lookup pick parent stations)
OUTPUT_COLUMNS = [
    ("feed_version", "string"), ("stop_id", "string"), ("stop_name", "string"),
    ("didok", "string"), ("stop_lat", "double"), ("stop_lon", "double"),
    ("study_canton_code", "string"), ("canton_join_status", "string"),
    ("in_study_area", "bool"), ("boundary_release", "string"), ("boundary_crs", "string"),
    ("location_type", "string"), ("parent_station", "string"),
    ("canton_number", "int64"), ("canton_name", "string"),
    ("canton_candidate_count", "int64"), ("boundary_layer", "string"),
    ("canton_joined_at_utc", "string"),
]


def validate_arguments(bucket, version, release):
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket):
        raise ValueError("Invalid DATA_BUCKET")
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", version):
        raise ValueError("Unsupported FEED_VERSION label")
    if not re.fullmatch(r"\d{4}-\d{2}", release):
        raise ValueError("BOUNDARY_RELEASE must look like 2026-01")


def load_gtfs_stops(gtfs_zip):
    """Read stops.txt from the GTFS ZIP; identifiers stay strings, coordinates become numbers."""
    import pandas as pd

    with zipfile.ZipFile(gtfs_zip) as archive:
        if "stops.txt" not in archive.namelist():
            raise ValueError("GTFS ZIP does not contain stops.txt")
        with archive.open("stops.txt") as source:
            stops = pd.read_csv(source, dtype=str, encoding="utf-8-sig", keep_default_na=False)
    stops.columns = [c.strip() for c in stops.columns]
    missing = {"stop_id", "stop_name", "stop_lat", "stop_lon"} - set(stops.columns)
    if missing:
        raise ValueError(f"Missing GTFS stop columns: {sorted(missing)}")
    for column in ["didok", "location_type", "parent_station"]:
        if column not in stops.columns:
            stops[column] = None  # nullable when the feed does not provide it
    for column in stops.columns:
        if stops[column].dtype == object:
            stops[column] = stops[column].str.strip().replace("", None)
    stops["stop_lat"] = pd.to_numeric(stops["stop_lat"], errors="coerce")
    stops["stop_lon"] = pd.to_numeric(stops["stop_lon"], errors="coerce")
    stops["_stop_row_id"] = range(len(stops))
    return stops


def extract_geopackage(boundary_zip, target_dir):
    with zipfile.ZipFile(boundary_zip) as archive:
        members = [m for m in archive.namelist() if m.lower().endswith(".gpkg")]
        if len(members) != 1:
            raise ValueError(f"Expected one GeoPackage in the boundary ZIP, found {members}")
        target = Path(target_dir) / Path(members[0]).name
        with archive.open(members[0]) as source, open(target, "wb") as output:
            while chunk := source.read(1024 * 1024):
                output.write(chunk)
    return target


def find_canton_layer(gpkg_path):
    import pyogrio

    names = [str(row[0]) for row in pyogrio.list_layers(gpkg_path)]
    matches = [name for name in names if name.upper().endswith("TLM_KANTONSGEBIET")]
    if len(matches) != 1:
        raise ValueError(f"Could not identify one TLM_KANTONSGEBIET layer: {names}")
    return matches[0]


def assign_cantons(stops, cantons, feed_version, boundary_release, boundary_layer):
    """Point-in-polygon canton assignment, as in canton_join.py. Returns (rows, counts)."""
    import geopandas as gpd
    import pandas as pd

    if cantons.crs is None:
        raise ValueError("swissBOUNDARIES3D has no declared CRS")
    lookup = {c.upper(): c for c in cantons.columns}
    for required in ["KANTONSNUMMER", "NAME"]:
        if required not in lookup:
            raise ValueError(f"Canton layer has no {required} column")
    if "ICC" in lookup:
        cantons = cantons[cantons[lookup["ICC"]] == "CH"].copy()  # Swiss cantons only

    invalid_geometries = int((~cantons.geometry.is_valid).sum())
    if invalid_geometries:
        cantons = cantons.copy()
        cantons["geometry"] = cantons.geometry.make_valid()
    cantons = cantons[[lookup["KANTONSNUMMER"], lookup["NAME"], "geometry"]].rename(
        columns={lookup["KANTONSNUMMER"]: "canton_number", lookup["NAME"]: "canton_name"})
    cantons["canton_number"] = pd.to_numeric(cantons["canton_number"], errors="raise").astype("int64")

    valid = stops["stop_lat"].between(-90, 90) & stops["stop_lon"].between(-180, 180)
    points = gpd.GeoDataFrame(
        stops.loc[valid, ["_stop_row_id"]],
        geometry=gpd.points_from_xy(stops.loc[valid, "stop_lon"], stops.loc[valid, "stop_lat"]),
        crs="EPSG:4326",
    ).to_crs(cantons.crs)
    predicate = "covered_by" if "covered_by" in points.sindex.valid_query_predicates else "intersects"
    joined = points.sjoin(cantons, how="left", predicate=predicate)

    pairs = joined.loc[joined["index_right"].notna(),
                       ["_stop_row_id", "canton_number", "canton_name"]].drop_duplicates()
    candidates = pairs.groupby("_stop_row_id").size().rename("canton_candidate_count")
    single = pairs[pairs["_stop_row_id"].map(candidates) == 1]

    result = stops.merge(candidates, left_on="_stop_row_id", right_index=True, how="left") \
                  .merge(single, on="_stop_row_id", how="left")
    result["canton_candidate_count"] = result["canton_candidate_count"].fillna(0).astype("int64")
    result["canton_number"] = pd.to_numeric(result["canton_number"], errors="coerce").astype("Int64")

    status = pd.Series("invalid_coordinate", index=result.index)
    status[valid & result["canton_candidate_count"].eq(0)] = "unmatched"
    status[valid & result["canton_candidate_count"].eq(1)] = "matched"
    status[valid & result["canton_candidate_count"].gt(1)] = "ambiguous_boundary"
    result["canton_join_status"] = status
    result["study_canton_code"] = result["canton_number"].map(TARGET_CANTONS)
    result["in_study_area"] = result["canton_join_status"].eq("matched") & \
        result["canton_number"].isin(list(TARGET_CANTONS))

    result["feed_version"] = feed_version
    result["boundary_release"] = boundary_release
    result["boundary_layer"] = boundary_layer
    result["boundary_crs"] = cantons.crs.to_string()
    result["canton_joined_at_utc"] = datetime.now(timezone.utc).isoformat()

    counts = {
        "stop_rows": len(result),
        "canton_join_status_counts": {str(k): int(v) for k, v in
                                      result["canton_join_status"].value_counts().items()},
        "study_canton_counts": {str(k): int(v) for k, v in
                                result.loc[result["in_study_area"], "study_canton_code"]
                                .value_counts().sort_index().items()},
        "study_area_stop_count": int(result["in_study_area"].sum()),
        "study_area_parent_stations": int((result["in_study_area"] &
                                           result["location_type"].eq("1")).sum()),
        "spatial_predicate": predicate,
        "invalid_boundary_geometries_repaired": invalid_geometries,
        "swiss_canton_polygons": len(cantons),
    }
    return result, counts


def to_output_table(result):
    """Fixed column order and types, so the Parquet schema is stable across versions."""
    import pyarrow as pa

    types = {"string": pa.string(), "double": pa.float64(), "bool": pa.bool_(), "int64": pa.int64()}
    schema = pa.schema([(name, types[kind]) for name, kind in OUTPUT_COLUMNS])
    frame = result[[name for name, _ in OUTPUT_COLUMNS]].copy()
    frame["canton_number"] = frame["canton_number"].astype("Int64")
    return pa.Table.from_pandas(frame, schema=schema, preserve_index=False)


def verify_table(table, version):
    """Acceptance checks on the table as written: columns, version, key uniqueness."""
    import pyarrow.compute as pc

    expected = [name for name, _ in OUTPUT_COLUMNS]
    if table.column_names != expected:
        raise RuntimeError(f"Output columns differ: {table.column_names}")
    versions = pc.unique(table["feed_version"]).to_pylist()
    if versions != [version]:
        raise RuntimeError(f"Output feed_version mismatch: {versions}")
    if pc.count_distinct(table["stop_id"]).as_py() != table.num_rows:
        raise RuntimeError("stop_id is not unique within the feed_version")
    if table["stop_id"].null_count:
        raise RuntimeError("Output contains rows without stop_id")
    return {"rows": table.num_rows, "columns": [f"{f.name}:{f.type}" for f in table.schema],
            "feed_versions": versions, "duplicate_stop_ids": 0}


def main():
    import boto3
    import geopandas as gpd
    import pyarrow.parquet as pq
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "DATA_BUCKET", "FEED_VERSION", "BOUNDARY_RELEASE", "REPLACE_INCOMPLETE"])
    bucket, version, release = args["DATA_BUCKET"], args["FEED_VERSION"], args["BOUNDARY_RELEASE"]
    validate_arguments(bucket, version, release)
    s3 = boto3.client("s3")
    started = perf_counter()
    silver = f"{SILVER_PREFIX}/feed_version={version}/"
    quality_key = f"{QUALITY_PREFIX}/feed_version={version}/report.json"

    def keys(prefix):
        return [o["Key"] for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix) for o in page.get("Contents", [])]

    def single_zip(prefix, hint):
        zips = [k for k in keys(prefix) if k.endswith(".zip")]
        if len(zips) != 1:
            raise ValueError(f"Expected one ZIP in s3://{bucket}/{prefix} ({hint}), found {zips}")
        return zips[0]

    # 1. Already prepared versions are never rebuilt
    existing = keys(silver)
    if silver + SUCCESS_MARKER in existing:
        print(json.dumps({"status": "already_prepared", "feed_version": version}))
        return
    if existing:
        if args["REPLACE_INCOMPLETE"].lower() != "true":
            raise ValueError("Incomplete Silver output exists without _PREPARED_SUCCESS. "
                             "Inspect it, then rerun with --REPLACE_INCOMPLETE true.")
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in existing]})

    # 2. Inputs from Bronze (the same files KAN-14 and KAN-18 preserved)
    gtfs_key = single_zip(f"{BRONZE_GTFS_PREFIX}/feed_version={version}/", "run KAN-14 first")
    boundary_key = single_zip(f"{BRONZE_BOUNDARY_PREFIX}/release={release}/", "run KAN-18 first")
    work = Path("/tmp/stops_canton")
    work.mkdir(parents=True, exist_ok=True)
    s3.download_file(bucket, gtfs_key, str(work / "gtfs.zip"))
    s3.download_file(bucket, boundary_key, str(work / "boundaries.zip"))
    gpkg = extract_geopackage(work / "boundaries.zip", work)

    # 3. Canton assignment
    stops = load_gtfs_stops(work / "gtfs.zip")
    layer = find_canton_layer(gpkg)
    cantons = gpd.read_file(gpkg, layer=layer, engine="pyogrio")
    result, counts = assign_cantons(stops, cantons, version, release, layer)
    table = to_output_table(result)
    verification = verify_table(table, version)

    # 4. Write, read back, verify again, then mark complete
    local_file = work / OUTPUT_FILE
    pq.write_table(table, local_file, compression="snappy")
    written = pq.read_table(local_file)
    verify_table(written, version)
    s3.upload_file(str(local_file), bucket, silver + OUTPUT_FILE,
                   ExtraArgs={"ServerSideEncryption": "AES256"})

    report = {
        "status": "success", "feed_version": version, "boundary_release": release,
        "boundary_layer": layer, "boundary_crs": str(result["boundary_crs"].iloc[0]),
        "gtfs_zip_key": gtfs_key, "boundary_zip_key": boundary_key,
        "output_path": f"s3://{bucket}/{silver}", **counts,
        "output_verification": verification,
        "total_job_script_seconds": round(perf_counter() - started, 3),
    }
    s3.put_object(Bucket=bucket, Key=quality_key, ContentType="application/json",
                  ServerSideEncryption="AES256",
                  Body=(json.dumps(report, indent=2, ensure_ascii=False) + "\n").encode())
    # Written last: Glue job 1 and the version-check Lambda trust only marked versions
    s3.put_object(Bucket=bucket, Key=silver + SUCCESS_MARKER, Body=b"")
    print(json.dumps(report, ensure_ascii=False))


if __name__ == "__main__":
    main()
