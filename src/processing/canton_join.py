"""Create a versioned GTFS Static stop-to-canton reference dataset."""

from __future__ import annotations

import argparse
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd

from src.ingestion.swissboundaries_download import (
    DEFAULT_RELEASE,
    prepare_swissboundaries,
)


TARGET_CANTONS = {1: "ZH", 3: "LU", 5: "SZ", 9: "ZG"}


def resolve_column(frame: pd.DataFrame, expected: str) -> str:
    lookup = {column.upper(): column for column in frame.columns}
    try:
        return lookup[expected.upper()]
    except KeyError as error:
        raise ValueError(
            f"Required column {expected!r} not found. "
            f"Available columns: {list(frame.columns)}"
        ) from error


def load_gtfs_stops(gtfs_zip: Path) -> pd.DataFrame:
    with zipfile.ZipFile(gtfs_zip) as archive:
        if "stops.txt" not in archive.namelist():
            raise ValueError("GTFS archive does not contain stops.txt")
        with archive.open("stops.txt") as source:
            stops = pd.read_csv(
                source,
                dtype={
                    "stop_id": "string",
                    "parent_station": "string",
                    "original_stop_id": "string",
                    "didok": "string",
                },
            )

    required = {"stop_id", "stop_name", "stop_lat", "stop_lon"}
    missing = required.difference(stops.columns)
    if missing:
        raise ValueError(f"Missing GTFS stop columns: {sorted(missing)}")

    stops["stop_lat"] = pd.to_numeric(stops["stop_lat"], errors="coerce")
    stops["stop_lon"] = pd.to_numeric(stops["stop_lon"], errors="coerce")
    stops["_stop_row_id"] = range(len(stops))
    return stops


def find_canton_layer(boundary_gpkg: Path) -> str:
    layers = gpd.list_layers(boundary_gpkg)
    matches = [
        str(name)
        for name in layers["name"]
        if str(name).upper().endswith("TLM_KANTONSGEBIET")
    ]
    if len(matches) != 1:
        raise ValueError(
            "Could not identify exactly one TLM_KANTONSGEBIET layer. "
            f"Available layers: {layers['name'].tolist()}"
        )
    return matches[0]


def assign_cantons(
    stops: pd.DataFrame,
    cantons: gpd.GeoDataFrame,
    feed_version: str,
    boundary_release: str,
    boundary_layer: str,
) -> tuple[pd.DataFrame, dict[str, object]]:
    if cantons.crs is None:
        raise ValueError("swissBOUNDARIES3D has no declared CRS")

    number_column = resolve_column(cantons, "KANTONSNUMMER")
    name_column = resolve_column(cantons, "NAME")
    column_lookup = {column.upper(): column for column in cantons.columns}
    icc_column = column_lookup.get("ICC")
    if icc_column:
        cantons = cantons[cantons[icc_column] == "CH"].copy()

    invalid_geometry_count = int((~cantons.geometry.is_valid).sum())
    if invalid_geometry_count:
        cantons = cantons.copy()
        cantons["geometry"] = cantons.geometry.make_valid()

    cantons = cantons[[number_column, name_column, "geometry"]].rename(
        columns={
            number_column: "canton_number",
            name_column: "canton_name",
        }
    )
    cantons["canton_number"] = pd.to_numeric(
        cantons["canton_number"], errors="raise"
    ).astype("int64")

    valid_coordinate = (
        stops["stop_lon"].between(-180, 180)
        & stops["stop_lat"].between(-90, 90)
    )
    points = gpd.GeoDataFrame(
        stops.loc[valid_coordinate].copy(),
        geometry=gpd.points_from_xy(
            stops.loc[valid_coordinate, "stop_lon"],
            stops.loc[valid_coordinate, "stop_lat"],
        ),
        crs="EPSG:4326",
    ).to_crs(cantons.crs)

    predicates = points.sindex.valid_query_predicates
    predicate = "covered_by" if "covered_by" in predicates else "intersects"
    joined = points.sjoin(cantons, how="left", predicate=predicate)

    pairs = joined.loc[
        joined["index_right"].notna(),
        ["_stop_row_id", "canton_number", "canton_name"],
    ].drop_duplicates()
    candidate_summary = pairs.groupby("_stop_row_id", as_index=False).agg(
        canton_candidate_count=("canton_number", "size"),
        canton_candidate_numbers=(
            "canton_number",
            lambda values: "|".join(str(value) for value in sorted(values)),
        ),
        canton_candidate_names=(
            "canton_name",
            lambda values: "|".join(sorted(set(values))),
        ),
    )
    single_matches = pairs.merge(
        candidate_summary[["_stop_row_id", "canton_candidate_count"]],
        on="_stop_row_id",
        how="left",
    )
    single_matches = single_matches.loc[
        single_matches["canton_candidate_count"] == 1,
        ["_stop_row_id", "canton_number", "canton_name"],
    ]

    result = stops.merge(candidate_summary, on="_stop_row_id", how="left").merge(
        single_matches, on="_stop_row_id", how="left"
    )
    result["canton_candidate_count"] = (
        result["canton_candidate_count"].fillna(0).astype("int64")
    )
    result["canton_number"] = pd.to_numeric(
        result["canton_number"], errors="coerce"
    ).astype("Int64")

    result["canton_join_status"] = "invalid_coordinate"
    result.loc[
        valid_coordinate & result["canton_candidate_count"].eq(0),
        "canton_join_status",
    ] = "unmatched"
    result.loc[
        valid_coordinate & result["canton_candidate_count"].eq(1),
        "canton_join_status",
    ] = "matched"
    result.loc[
        valid_coordinate & result["canton_candidate_count"].gt(1),
        "canton_join_status",
    ] = "ambiguous_boundary"

    result["study_canton_code"] = result["canton_number"].map(TARGET_CANTONS)
    result["in_study_area"] = (
        result["canton_join_status"].eq("matched")
        & result["canton_number"].isin(TARGET_CANTONS)
    )
    joined_at = datetime.now(timezone.utc).isoformat()
    result["feed_version"] = feed_version
    result["boundary_release"] = boundary_release
    result["boundary_layer"] = boundary_layer
    result["boundary_crs"] = cantons.crs.to_string()
    result["canton_joined_at_utc"] = joined_at

    quality = {
        "dataset": "gtfs_static_stops_with_canton",
        "feed_version": feed_version,
        "boundary_release": boundary_release,
        "boundary_layer": boundary_layer,
        "boundary_crs": cantons.crs.to_string(),
        "spatial_predicate": predicate,
        "total_stop_rows": len(result),
        "unique_stop_ids": int(result["stop_id"].nunique()),
        "duplicate_stop_id_rows": int(result["stop_id"].duplicated().sum()),
        "invalid_boundary_geometry_count_before_repair": invalid_geometry_count,
        "join_status_counts": {
            str(key): int(value)
            for key, value in result["canton_join_status"]
            .value_counts(dropna=False)
            .items()
        },
        "study_canton_counts": {
            str(key): int(value)
            for key, value in result.loc[
                result["in_study_area"], "study_canton_code"
            ].value_counts().items()
        },
        "study_area_stop_count": int(result["in_study_area"].sum()),
        "created_at_utc": joined_at,
    }
    return result.drop(columns=["_stop_row_id"]), quality


def prepare_canton_reference(
    gtfs_zip: Path,
    boundary_gpkg: Path,
    output_root: Path,
    quality_root: Path,
    feed_version: str,
    boundary_release: str,
) -> dict[str, object]:
    stops = load_gtfs_stops(gtfs_zip)
    boundary_layer = find_canton_layer(boundary_gpkg)
    cantons = gpd.read_file(boundary_gpkg, layer=boundary_layer, engine="pyogrio")
    result, quality = assign_cantons(
        stops=stops,
        cantons=cantons,
        feed_version=feed_version,
        boundary_release=boundary_release,
        boundary_layer=boundary_layer,
    )

    output_partition = output_root / f"feed_version={feed_version}"
    quality_partition = quality_root / f"feed_version={feed_version}"
    output_partition.mkdir(parents=True, exist_ok=True)
    quality_partition.mkdir(parents=True, exist_ok=True)
    output_path = output_partition / "part-00000.parquet"
    quality_path = quality_partition / "summary.json"
    result.to_parquet(output_path, index=False)
    quality.update(
        {
            "output_path": str(output_path),
            "output_bytes": output_path.stat().st_size,
        }
    )
    quality_path.write_text(
        json.dumps(quality, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return quality


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download swissBOUNDARIES3D and assign GTFS stops to cantons."
    )
    parser.add_argument("--gtfs-zip", type=Path, required=True)
    parser.add_argument("--feed-version", required=True)
    parser.add_argument("--boundary-release", default=DEFAULT_RELEASE)
    parser.add_argument("--boundary-source-url")
    parser.add_argument(
        "--boundary-raw-root",
        type=Path,
        default=Path("data/raw/swissboundaries3d"),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/reference/gtfs_static_stops"),
    )
    parser.add_argument(
        "--quality-root",
        type=Path,
        default=Path("data/quality/canton_assignment"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    boundary = prepare_swissboundaries(
        release=args.boundary_release,
        raw_root=args.boundary_raw_root,
        source_url=args.boundary_source_url,
    )
    quality = prepare_canton_reference(
        gtfs_zip=args.gtfs_zip,
        boundary_gpkg=Path(str(boundary["geopackage_path"])),
        output_root=args.output_root,
        quality_root=args.quality_root,
        feed_version=args.feed_version,
        boundary_release=args.boundary_release,
    )
    print(json.dumps({"boundary": boundary, "quality": quality}, indent=2))


if __name__ == "__main__":
    main()

