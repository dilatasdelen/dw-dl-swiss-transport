import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from src.processing.canton_join import assign_cantons


def test_assign_cantons_preserves_rows_and_flags_edge_cases():
    stops = pd.DataFrame(
        {
            "_stop_row_id": [0, 1, 2, 3, 4],
            "stop_id": ["zh", "zg", "border", "outside", "invalid"],
            "stop_name": ["ZH", "ZG", "Border", "Outside", "Invalid"],
            "stop_lon": [8.0, 9.0, 8.5, 10.0, None],
            "stop_lat": [47.0, 47.0, 47.0, 47.0, 47.0],
        }
    )
    cantons = gpd.GeoDataFrame(
        {
            "KANTONSNUMMER": [1, 9],
            "NAME": ["Zürich", "Zug"],
            "ICC": ["CH", "CH"],
        },
        geometry=[
            Polygon([(7.5, 46.5), (8.5, 46.5), (8.5, 47.5), (7.5, 47.5)]),
            Polygon([(8.5, 46.5), (9.5, 46.5), (9.5, 47.5), (8.5, 47.5)]),
        ],
        crs="EPSG:4326",
    )

    result, quality = assign_cantons(
        stops=stops,
        cantons=cantons,
        feed_version="test-version",
        boundary_release="test-release",
        boundary_layer="TLM_KANTONSGEBIET",
    )
    by_stop = result.set_index("stop_id")

    assert len(result) == len(stops)
    assert by_stop.loc["zh", "study_canton_code"] == "ZH"
    assert by_stop.loc["zg", "study_canton_code"] == "ZG"
    assert by_stop.loc["border", "canton_join_status"] == "ambiguous_boundary"
    assert by_stop.loc["outside", "canton_join_status"] == "unmatched"
    assert by_stop.loc["invalid", "canton_join_status"] == "invalid_coordinate"
    assert quality["join_status_counts"] == {
        "matched": 2,
        "ambiguous_boundary": 1,
        "unmatched": 1,
        "invalid_coordinate": 1,
    }
