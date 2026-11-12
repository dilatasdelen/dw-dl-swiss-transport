"""AWS Lambda launcher and completion verifier for hourly GTFS-RT Glue jobs.

Scheduler input: {"time": "<aws.scheduler.scheduled-time>"}.
Step Functions calls the same function with mode=verify after both Glue stages.
Source data and existing Glue scripts are never modified by this function.
"""

import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError


s3 = boto3.client("s3")
states = boto3.client("stepfunctions")
RULE_VERSION = "pilot_latest_valid_feed_v1"
PLAN_VERSION = "hourly_orchestration_v1"


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def read_json(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response["Error"]["Code"] in {"NoSuchKey", "404", "NotFound"}:
            return None
        raise
    return json.loads(response["Body"].read())


def exists(bucket, key):
    # Listing an exact prefix works with prefix-scoped ListBucket permission,
    # including when a missing object's HEAD would otherwise return HTTP 403.
    response = s3.list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
    return any(item["Key"] == key for item in response.get("Contents", []))


def objects(bucket, prefix, suffix):
    return sorted(
        [{"key": item["Key"], "bytes": item["Size"], "etag": item["ETag"]}
         for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
         for item in page.get("Contents", []) if item["Key"].endswith(suffix)],
        key=lambda item: item["key"],
    )


def previous_hour(value):
    if not isinstance(value, str) or not value:
        raise ValueError("Provide time as the Scheduler's scheduled-time, including its timezone")
    scheduled = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if scheduled.tzinfo is None:
        raise ValueError("time must include a timezone")
    upper = scheduled.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    lower = upper - timedelta(hours=1)
    return lower, upper


def snapshot(bucket, date, hour):
    partition = f"ingest_date={date}/hour={hour}/"
    raw = objects(bucket, "bronze/gtfs_rt/" + partition, ".pb")
    decoded = objects(bucket, "silver/gtfs_rt_decoded/" + partition, ".jsonl.gz")
    quality = objects(bucket, "quality/gtfs_rt_snapshot_decode/" + partition, ".json")
    if not raw:
        raise RuntimeError(f"No Bronze snapshots for {partition}; investigate collection before replaying")
    expected_decoded, expected_quality, versions = set(), set(), set()
    quality_by_key = {item["key"]: item for item in quality}
    for item in raw:
        filename = item["key"].rsplit("/", 1)[-1]
        match = re.fullmatch(r"(\d{8}T\d{12}Z)_([0-9a-f]{64})\.pb", filename)
        if not match:
            raise ValueError(f"Unexpected Bronze filename: {item['key']}")
        base = filename[:-3]
        expected_decoded.add("silver/gtfs_rt_decoded/" + partition + base + ".jsonl.gz")
        quality_key = "quality/gtfs_rt_snapshot_decode/" + partition + base + ".json"
        expected_quality.add(quality_key)
        if quality_key not in quality_by_key:
            raise RuntimeError(f"Decoder quality record not ready: {quality_key}")
        record = read_json(bucket, quality_key)
        if (not record or record.get("parse_status") != "success"
                or record.get("payload_hash_valid") is not True
                or record.get("raw_object_key") != item["key"]
                or record.get("payload_sha256") != match.group(2)):
            raise RuntimeError(f"Invalid decoder quality or source lineage: {quality_key}")
        version = record.get("feed_version")
        if not isinstance(version, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", version):
            raise RuntimeError(f"Missing or unsupported feed_version: {quality_key}")
        versions.add(version)
    if expected_decoded != {item["key"] for item in decoded}:
        raise RuntimeError("Bronze/decoded object sets differ; finish decoding before replaying")
    if expected_quality != set(quality_by_key):
        raise RuntimeError("Bronze/quality object sets differ; investigate source lineage")
    value = {"raw": raw, "decoded": decoded, "quality": quality, "feed_versions": sorted(versions)}
    value["fingerprint"] = hashlib.sha256(encode(value)).hexdigest()
    return value


def prepare(event, bucket):
    lower, upper = previous_hour(event.get("time"))
    tag = event.get("run_tag", "v1")
    if not isinstance(tag, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,15}", tag):
        raise ValueError("run_tag must be a simple label of 1 to 16 characters")
    date, hour = lower.strftime("%Y-%m-%d"), lower.strftime("%H")
    captured = snapshot(bucket, date, hour)
    for version in captured["feed_versions"]:
        for reference in ("gtfs_static_stops", "gtfs_static_stop_times"):
            prefix = f"silver/reference/{reference}/feed_version={version}/"
            if not objects(bucket, prefix, ".parquet"):
                raise RuntimeError(f"Reference not ready: s3://{bucket}/{prefix}")
    slot = lower.strftime("%Y%m%d-%H")
    jobs = []
    for version in captured["feed_versions"]:
        version_label = hashlib.sha256(version.encode()).hexdigest()[:12]
        jobs.append({
            "JobName": os.environ["GLUE1_JOB_NAME"],
            "Arguments": {
                "--DATA_BUCKET": bucket, "--INGEST_DATE": date, "--INGEST_HOUR": hour,
                "--FEED_VERSION": version, "--RUN_ID": f"history-{slot}-{version_label}-{tag}",
                "--MAX_INPUT_FILES": "0",
            },
        })
    run_id = f"events-{slot}-{tag}"
    prefix = f"audit/gtfs_rt_batch/ingest_date={date}/hour={hour}/"
    workflow = {
        "plan_key": prefix + tag + ".plan.json",
        "input_fingerprint": captured["fingerprint"],
        "glue1_jobs": jobs,
        "glue2_job": {
            "JobName": os.environ["GLUE2_JOB_NAME"],
            "Arguments": {
                "--DATA_BUCKET": bucket,
                "--INPUT_RUN_IDS": ",".join(job["Arguments"]["--RUN_ID"] for job in jobs),
                "--RUN_ID": run_id,
                "--WINDOW_START_UTC": lower.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "--WINDOW_END_UTC": upper.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "--RULE_VERSION": RULE_VERSION,
            },
        },
    }
    return {
        "plan_version": PLAN_VERSION, "bucket": bucket, "ingest_date": date, "ingest_hour": hour,
        "execution_name": f"batch-{slot}-{tag}", "workflow_input": workflow,
        "checkpoint_key": prefix + tag + ".complete.json", "snapshot": captured,
    }


def claim_plan(plan):
    bucket, key = plan["bucket"], plan["workflow_input"]["plan_key"]
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=encode(plan), ContentType="application/json",
                      ServerSideEncryption="AES256", IfNoneMatch="*")
    except ClientError as error:
        if error.response["Error"]["Code"] not in {"PreconditionFailed", "412"}:
            raise
        if read_json(bucket, key) != plan:
            raise RuntimeError("This hour/run_tag already has a different plan. Review late data or settings and use a new run_tag") from error


def verify(event, bucket):
    key = event.get("plan_key", "")
    if not isinstance(key, str) or not key.startswith("audit/gtfs_rt_batch/") or not key.endswith(".plan.json"):
        raise ValueError("Provide a batch plan_key under audit/gtfs_rt_batch/")
    plan = read_json(bucket, key)
    if not plan or plan.get("bucket") != bucket or plan.get("plan_version") != PLAN_VERSION:
        raise RuntimeError("Missing or unsupported batch plan")
    current = snapshot(bucket, plan["ingest_date"], plan["ingest_hour"])
    if current["fingerprint"] != plan["snapshot"]["fingerprint"]:
        raise RuntimeError("Inputs changed during processing. Review late data and replay with a new run_tag")
    workflow = plan["workflow_input"]
    for job in workflow["glue1_jobs"]:
        args = job["Arguments"]
        root = f"silver/gtfs_rt_processed/run_id={args['--RUN_ID']}/"
        if not exists(bucket, root + "_EXPERIMENT_SUCCESS"):
            raise RuntimeError("Glue 1 completion marker missing: " + root)
        report = read_json(bucket, root + "report.json")
        if (not report or report.get("status") != "success"
                or report.get("run_id") != args["--RUN_ID"]
                or report.get("feed_version") != args["--FEED_VERSION"]
                or report.get("input_scope") != "whole_hour"
                or report.get("input_files") != len(current["decoded"])
                or report.get("final_published_prediction_selected") is not False):
            raise RuntimeError("Glue 1 report does not match the batch plan: " + root)
        manifest = read_json(bucket, root + "input_manifest.json")
        actual = [{name: item[name] for name in ("key", "bytes", "etag")}
                  for item in (manifest or {}).get("objects", [])]
        if sorted(actual, key=lambda item: item["key"]) != current["decoded"]:
            raise RuntimeError("Glue 1 selected inputs differ from the batch plan: " + root)
    args = workflow["glue2_job"]["Arguments"]
    root = f"silver/gtfs_rt_event_selection/run_id={args['--RUN_ID']}/"
    report = read_json(bucket, root + "report.json")
    if (not exists(bucket, root + "_EXPERIMENT_SUCCESS") or not report
            or report.get("status") != "success" or report.get("run_id") != args["--RUN_ID"]
            or report.get("input_run_ids") != args["--INPUT_RUN_IDS"].split(",")
            or report.get("selection_rule_version") != args["--RULE_VERSION"]
            or report.get("selection_window") != {"start_utc": args["--WINDOW_START_UTC"],
                                                  "end_utc_exclusive": args["--WINDOW_END_UTC"]}
            or report.get("final_published_prediction_selected") is not False):
        raise RuntimeError("Glue 2 completion/report does not match the batch plan: " + root)
    result = {"status": "completed", "plan_key": key, "input_fingerprint": current["fingerprint"],
              "event_run_id": args["--RUN_ID"], "completed_at_utc": datetime.now(timezone.utc).isoformat()}
    s3.put_object(Bucket=bucket, Key=plan["checkpoint_key"], Body=encode(result),
                  ContentType="application/json", ServerSideEncryption="AES256")
    return result


def lambda_handler(event, context):
    if not isinstance(event, dict):
        raise ValueError("Expected a JSON object")
    bucket = os.environ.get("DATA_BUCKET", "datavault-v2")
    if event.get("mode", "launch") == "verify":
        result = verify(event, bucket)
    else:
        if event.get("mode", "launch") != "launch":
            raise ValueError("mode must be launch or verify")
        plan = prepare(event, bucket)
        if "dry_run" in event and not isinstance(event["dry_run"], bool):
            raise ValueError("dry_run must be a JSON boolean")
        if event.get("dry_run", False):
            return {"status": "ready", "plan": plan}
        claim_plan(plan)
        checkpoint = (read_json(bucket, plan["checkpoint_key"])
                      if exists(bucket, plan["checkpoint_key"]) else None)
        if checkpoint:
            if checkpoint.get("input_fingerprint") != plan["snapshot"]["fingerprint"]:
                raise RuntimeError("Completed checkpoint differs from the input plan")
            return {"status": "already_completed", "checkpoint": checkpoint}
        machine = os.environ["STATE_MACHINE_ARN"]
        if not re.fullmatch(r"arn:aws:states:[a-z0-9-]+:\d{12}:stateMachine:[A-Za-z0-9_-]+", machine):
            raise ValueError("STATE_MACHINE_ARN must be an unqualified Standard state machine ARN")
        try:
            execution = states.start_execution(stateMachineArn=machine, name=plan["execution_name"],
                                               input=encode(plan["workflow_input"]).decode())
            result = {"status": "accepted", "execution_arn": execution["executionArn"]}
        except ClientError as error:
            if error.response["Error"]["Code"] != "ExecutionAlreadyExists":
                raise
            arn = machine.replace(":stateMachine:", ":execution:") + ":" + plan["execution_name"]
            execution = states.describe_execution(executionArn=arn)
            if json.loads(execution["input"]) != plan["workflow_input"]:
                raise RuntimeError("Existing execution has different inputs; review and use a new run_tag") from error
            if execution["status"] != "RUNNING":
                raise RuntimeError(f"Existing execution is {execution['status']} without a completed checkpoint; investigate and use a new run_tag") from error
            result = {"status": "already_running", "execution_arn": arn}
    print(json.dumps(result))
    return result
