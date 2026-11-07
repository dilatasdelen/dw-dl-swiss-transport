import json
import os
import re
import urllib.error
import urllib.parse
import urllib.request

import boto3


s3 = boto3.client("s3")
glue = boto3.client("glue")

DATA_BUCKET = os.environ.get("DATA_BUCKET", "datavault-v2")
GLUE_JOB_NAME = os.environ.get("GLUE_JOB_NAME", "glue_static_stop_times")
STOPS_JOB_NAME = os.environ.get("STOPS_JOB_NAME", "glue_static_stops_canton")
BOUNDARY_RELEASE = os.environ.get("BOUNDARY_RELEASE", "2026-01")
# Comma-separated: each timetable year has its own dataset and permalink
# (timetable change mid-December), e.g. add .../timetable-2027-gtfs2020/permalink when published
PERMALINK_URLS = [u.strip() for u in os.environ.get(
    "PERMALINK_URLS",
    "https://data.opentransportdata.swiss/dataset/timetable-2026-gtfs2020/permalink",
).split(",") if u.strip()]
USER_AGENT = os.environ.get("USER_AGENT", "HSLU-DWDL-DataVault/1.0")
SILVER_PREFIX = "silver/reference/gtfs_static_stop_times"
STOPS_PREFIX = "silver/reference/gtfs_static_stops"
SUCCESS_MARKER = "_PREPARED_SUCCESS"
ACTIVE_STATES = {"STARTING", "RUNNING", "STOPPING", "WAITING"}

VERSION_LABEL = re.compile(r"[A-Za-z0-9_.-]{1,80}")
DATE_IN_NAME = re.compile(r"(20\d{2})-?(\d{2})-?(\d{2})")


def lambda_handler(event, context):
    event = event or {}
    if event.get("source") == "aws.glue":
        # Job 1 just finished: continue with job 2 for exactly that version
        run_id = event["detail"]["jobRunId"]
        run = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)["JobRun"]
        candidate = {"feed_version": run["Arguments"]["--FEED_VERSION"],
                     "source_url": "", "origin": f"glue_success:{run_id}"}
    elif event.get("feed_version"):
        # Fallback / manual request for one specific version
        candidate = {"feed_version": str(event["feed_version"]),
                     "source_url": event.get("source_url", ""), "origin": "direct_request"}
    else:
        candidate = newest_release()
        candidate["origin"] = "permalink"

    version = candidate["feed_version"]
    if not VERSION_LABEL.fullmatch(version):
        raise ValueError(f"Unsupported feed_version label: {version!r}")

    if not is_prepared(SILVER_PREFIX, version):
        action = start_once(GLUE_JOB_NAME, version, {
            "--DATA_BUCKET": DATA_BUCKET,
            "--FEED_VERSION": version,
            "--SOURCE_URL": candidate.get("source_url") or "none",
            "--USER_AGENT": USER_AGENT,
            "--REPLACE_INCOMPLETE": "false",
        })
        result = {"step": "timetable", **action, **candidate}
    elif not is_prepared(STOPS_PREFIX, version):
        action = start_once(STOPS_JOB_NAME, version, {
            "--DATA_BUCKET": DATA_BUCKET,
            "--FEED_VERSION": version,
            "--BOUNDARY_RELEASE": BOUNDARY_RELEASE,
            "--REPLACE_INCOMPLETE": "false",
        })
        result = {"step": "stop_to_canton", **action, **candidate}
    else:
        result = {"step": "none", "action": "skipped_already_prepared", **candidate}

    print(json.dumps(result))
    return result


def start_once(job_name, version, arguments):
    """Start the Glue job unless a run for this version is already active."""
    active_run = running_run(job_name, version)
    if active_run:
        return {"action": "skipped_already_running", "job_name": job_name, "job_run_id": active_run}
    run = glue.start_job_run(JobName=job_name, Arguments=arguments)
    return {"action": "started_preparation", "job_name": job_name, "job_run_id": run["JobRunId"]}


def newest_release():
    """Return the newest GTFS ZIP that the configured permalinks currently point to."""
    candidates = []
    for permalink in PERMALINK_URLS:
        try:
            file_url, file_name = resolve_permalink(permalink)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                # e.g. next year's dataset is configured but not published yet
                print(json.dumps({"permalink_not_found": permalink}))
                continue
            raise
        version = version_from_name(file_name) or version_from_name(file_url)
        # The permalink redirects to a signed link that expires after 60 seconds, too short
        # for a Glue job to start. Glue gets the permalink and follows it when downloading.
        candidates.append({"feed_version": version, "source_url": permalink,
                           "file_name": file_name,
                           "resolved_file": file_url.split("?", 1)[0]})

    # Logged every time, so the resolved files can be checked in CloudWatch
    print(json.dumps({"permalink_candidates": candidates}))
    usable = [c for c in candidates if c["feed_version"]]
    if not usable:
        raise RuntimeError(f"No permalink led to a GTFS ZIP with a release date: {candidates}")
    return max(usable, key=lambda c: c["feed_version"])


def resolve_permalink(permalink):
    """Follow the permalink's redirects and return (final file URL, file name).

    Only the response headers are read; the ZIP itself is downloaded later by Glue.
    """
    request = urllib.request.Request(permalink, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=30) as response:
        file_url = response.geturl()
        disposition = response.headers.get("Content-Disposition", "")
    match = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', disposition)
    file_name = match.group(1) if match else \
        os.path.basename(urllib.parse.urlparse(file_url).path)
    return file_url, file_name


def version_from_name(name):
    """gtfs_fp2026_20261104.zip -> 20261104 (the value GTFS-RT reports as feed_version)."""
    match = DATE_IN_NAME.search(os.path.basename(name or ""))
    return "".join(match.groups()) if match else None


def is_prepared(prefix, version):
    key = f"{prefix}/feed_version={version}/{SUCCESS_MARKER}"
    try:
        s3.head_object(Bucket=DATA_BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError as error:
        if error.response["Error"]["Code"] in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def running_run(job_name, version):
    """Return the id of an active Glue run for this version, so it is not started twice."""
    runs = glue.get_job_runs(JobName=job_name, MaxResults=50).get("JobRuns", [])
    for run in runs:
        if run["JobRunState"] in ACTIVE_STATES and \
                run.get("Arguments", {}).get("--FEED_VERSION") == version:
            return run["Id"]
    return None
