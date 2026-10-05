"""
Assigns a Swiss canton to each point (e.g. GTFS stop lat/lon) using a
point-in-polygon spatial join against the swissBOUNDARIES3D canton layer.

Before running:
1. Download the canton boundaries from opendata.swiss / swisstopo
   (swissBOUNDARIES3D, "Hoheitsgrenzen" product), e.g. as GeoJSON.
2. Place the file at: data/raw/boundaries/swissboundaries3d_kanton.geojson
3. pip install geopandas shapely (already in requirements.txt)

"""

