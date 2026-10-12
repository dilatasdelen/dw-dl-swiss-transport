import zipfile

import pytest

from src.ingestion.swissboundaries_download import (
    sha256_file,
    validate_and_extract_archive,
)


def test_validate_and_extract_archive(tmp_path):
    archive_path = tmp_path / "boundaries.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("nested/boundaries.gpkg", b"gpkg fixture")

    extracted = validate_and_extract_archive(archive_path, tmp_path / "raw")

    assert extracted.name == "boundaries.gpkg"
    assert extracted.read_bytes() == b"gpkg fixture"
    assert sha256_file(extracted) == (
        "344d9dc90b8d7b45799e7d99458d66d6d949e46309156cd93bdffe64a3cae5ec"
    )


def test_validate_archive_requires_one_geopackage(tmp_path):
    archive_path = tmp_path / "boundaries.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("readme.txt", b"no geodata")

    with pytest.raises(ValueError, match="exactly one GeoPackage"):
        validate_and_extract_archive(archive_path, tmp_path / "raw")
