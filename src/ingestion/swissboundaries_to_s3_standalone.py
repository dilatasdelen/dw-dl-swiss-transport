import argparse
import hashlib
import json
import shutil
import sqlite3
import tempfile
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_RELEASE = "2026-01"
DEFAULT_BUCKET = "datavault-v2"
BRONZE_PREFIX = "bronze/swissboundaries3d"
FILENAME_TEMPLATE = "swissboundaries3d_{release}_2056_5728.gpkg.zip"
URL_TEMPLATE = (
    "https://data.geo.admin.ch/ch.swisstopo.swissboundaries3d/"
    "swissboundaries3d_{release}/swissboundaries3d_{release}_2056_5728.gpkg.zip"
)
USER_AGENT = "hslu-data-vault-swissboundaries-preparer/1.0"
# Swiss canton numbers (BFS) of the study area: ZH, LU, SZ, ZG
STUDY_CANTONS = {1: "ZH", 3: "LU", 5: "SZ", 9: "ZG"}


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def download(url, destination):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=180) as response, open(destination, "wb") as out:
        shutil.copyfileobj(response, out, length=1024 * 1024)


def extract_geopackage(zip_path, target_dir):
    """Validate the ZIP and extract its single GeoPackage."""
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Not a valid ZIP archive: {zip_path}")
    with zipfile.ZipFile(zip_path) as archive:
        bad = archive.testzip()
        if bad:
            raise ValueError(f"Corrupt member in swissBOUNDARIES3D ZIP: {bad}")
        members = [m for m in archive.namelist() if m.lower().endswith(".gpkg")]
        if len(members) != 1:
            raise ValueError(f"Expected exactly one GeoPackage in the ZIP, found {members}")
        target = Path(target_dir) / Path(members[0]).name
        with archive.open(members[0]) as source, open(target, "wb") as out:
            shutil.copyfileobj(source, out, length=1024 * 1024)
    return target


def describe_geopackage(gpkg_path):
    """Read layers, the canton layer and its CRS from the GeoPackage's own tables."""
    with sqlite3.connect(f"file:{gpkg_path}?mode=ro", uri=True) as db:
        layers = db.execute(
            "SELECT table_name, data_type, srs_id FROM gpkg_contents ORDER BY table_name"
        ).fetchall()
        cantons = [row for row in layers
                   if row[0].upper().endswith("TLM_KANTONSGEBIET") and row[1] == "features"]
        if len(cantons) != 1:
            raise ValueError("Expected exactly one TLM_KANTONSGEBIET feature layer, "
                             f"found layers: {[row[0] for row in layers]}")
        layer, _, srs_id = cantons[0]

        srs = db.execute(
            "SELECT srs_name, organization, organization_coordsys_id "
            "FROM gpkg_spatial_ref_sys WHERE srs_id = ?", (srs_id,)
        ).fetchone()
        if srs is None:
            raise ValueError(f"Canton layer declares unknown srs_id {srs_id}")
        srs_name, organization, code = srs

        columns = {row[1].upper() for row in db.execute(f'PRAGMA table_info("{layer}")')}
        if "KANTONSNUMMER" not in columns:
            raise ValueError(f"Canton layer has no KANTONSNUMMER column: {sorted(columns)}")
        # ICC separates Swiss cantons from Liechtenstein, if the column exists
        swiss_only = " WHERE ICC = 'CH'" if "ICC" in columns else ""
        polygon_rows, canton_numbers = db.execute(
            f'SELECT COUNT(*), COUNT(DISTINCT KANTONSNUMMER) FROM "{layer}"{swiss_only}'
        ).fetchone()
        present = {int(row[0]) for row in db.execute(
            f'SELECT DISTINCT KANTONSNUMMER FROM "{layer}"{swiss_only}')}

    missing = sorted(c for number, c in STUDY_CANTONS.items() if number not in present)
    if canton_numbers != 26 or missing:
        raise ValueError(f"Canton layer is incomplete: {canton_numbers} Swiss cantons, "
                         f"missing study cantons {missing}")
    return {
        "layers": [row[0] for row in layers],
        "canton_layer": layer,
        "canton_polygon_rows": polygon_rows,
        "swiss_canton_count": canton_numbers,
        "study_cantons_present": sorted(STUDY_CANTONS.values()),
        "crs": f"{organization.upper()}:{code}",
        "crs_name": srs_name,
    }


def upload(zip_path, record, bucket):
    """Upload the original ZIP and the manifest. An existing different ZIP is never replaced."""
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3")
    prefix = f"{BRONZE_PREFIX}/release={record['release']}/"
    zip_key = prefix + zip_path.name
    try:
        existing = s3.head_object(Bucket=bucket, Key=zip_key)
        if existing.get("Metadata", {}).get("sha256") != record["archive_sha256"]:
            raise ValueError(f"s3://{bucket}/{zip_key} already exists with different content")
        action = "already_present"
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"404", "NoSuchKey", "NotFound"}:
            raise
        s3.upload_file(str(zip_path), bucket, zip_key, ExtraArgs={
            "ContentType": "application/zip", "ServerSideEncryption": "AES256",
            "Metadata": {"sha256": record["archive_sha256"], "release": record["release"]},
        })
        action = "uploaded"

    record.update({"archive_key": zip_key, "upload_action": action,
                   "recorded_at_utc": datetime.now(timezone.utc).isoformat()})
    s3.put_object(Bucket=bucket, Key=prefix + "_manifest.json", ContentType="application/json",
                  ServerSideEncryption="AES256",
                  Body=(json.dumps(record, indent=2, ensure_ascii=False) + "\n").encode())
    return record


def main():
    parser = argparse.ArgumentParser(description="Upload swissBOUNDARIES3D to S3 Bronze")
    parser.add_argument("--release", default=DEFAULT_RELEASE)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    args = parser.parse_args()

    url = URL_TEMPLATE.format(release=args.release)
    with tempfile.TemporaryDirectory() as work:
        zip_path = Path(work) / FILENAME_TEMPLATE.format(release=args.release)
        retrieved_at = datetime.now(timezone.utc).isoformat()
        download(url, zip_path)
        gpkg = extract_geopackage(zip_path, work)
        record = {
            "dataset": "swissBOUNDARIES3D",
            "publisher": "Federal Office of Topography swisstopo",
            "release": args.release,
            "source_url": url,
            "retrieved_at_utc": retrieved_at,
            "archive_bytes": zip_path.stat().st_size,
            "archive_sha256": sha256_file(zip_path),
            "geopackage_member": gpkg.name,
            "geopackage_sha256": sha256_file(gpkg),
            **describe_geopackage(gpkg),
        }
        record = upload(zip_path, record, args.bucket)
    print(json.dumps(record, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
