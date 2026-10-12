"""Download and preserve a versioned swissBOUNDARIES3D GeoPackage archive."""

from __future__ import annotations

import hashlib
import json
import shutil
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_RELEASE = "2026-01"
DEFAULT_FILENAME_TEMPLATE = "swissboundaries3d_{release}_2056_5728.gpkg.zip"
DEFAULT_URL_TEMPLATE = (
    "https://data.geo.admin.ch/ch.swisstopo.swissboundaries3d/"
    "swissboundaries3d_{release}/swissboundaries3d_{release}_2056_5728.gpkg.zip"
)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def download_file(url: str, destination: Path, user_agent: str) -> bool:
    """Download atomically, or reuse an existing non-empty archive."""
    if destination.exists() and destination.stat().st_size > 0:
        return False

    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".part")
    request = urllib.request.Request(url, headers={"User-Agent": user_agent})
    try:
        with urllib.request.urlopen(request, timeout=180) as response:
            with temporary.open("wb") as output:
                shutil.copyfileobj(response, output, length=1024 * 1024)
        temporary.replace(destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return True


def validate_and_extract_archive(zip_path: Path, extract_dir: Path) -> Path:
    """Validate the ZIP and return the single extracted GeoPackage path."""
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Not a valid ZIP archive: {zip_path}")

    with zipfile.ZipFile(zip_path) as archive:
        bad_member = archive.testzip()
        if bad_member:
            raise ValueError(f"Corrupt member in swissBOUNDARIES3D ZIP: {bad_member}")

        gpkg_members = [
            member for member in archive.namelist() if member.lower().endswith(".gpkg")
        ]
        if len(gpkg_members) != 1:
            raise ValueError(
                "Expected exactly one GeoPackage in swissBOUNDARIES3D ZIP, "
                f"found {gpkg_members}"
            )

        member = gpkg_members[0]
        gpkg_path = extract_dir / Path(member).name
        if not gpkg_path.exists() or gpkg_path.stat().st_size == 0:
            extract_dir.mkdir(parents=True, exist_ok=True)
            temporary = gpkg_path.with_suffix(gpkg_path.suffix + ".part")
            with archive.open(member) as source, temporary.open("wb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
            temporary.replace(gpkg_path)

    return gpkg_path


def prepare_swissboundaries(
    release: str,
    raw_root: Path,
    source_url: str | None = None,
    user_agent: str = "hslu-data-vault-swissboundaries-preparer/1.0",
) -> dict[str, object]:
    url = source_url or DEFAULT_URL_TEMPLATE.format(release=release)
    filename = DEFAULT_FILENAME_TEMPLATE.format(release=release)
    release_dir = raw_root / f"release={release}"
    zip_path = release_dir / filename

    downloaded = download_file(url, zip_path, user_agent)
    gpkg_path = validate_and_extract_archive(zip_path, release_dir)
    manifest = {
        "dataset": "swissBOUNDARIES3D",
        "release": release,
        "source_url": url,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "archive_path": str(zip_path),
        "archive_bytes": zip_path.stat().st_size,
        "archive_sha256": sha256_file(zip_path),
        "geopackage_path": str(gpkg_path),
        "geopackage_bytes": gpkg_path.stat().st_size,
        "geopackage_sha256": sha256_file(gpkg_path),
        "downloaded": downloaded,
    }
    manifest_path = release_dir / "_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest
