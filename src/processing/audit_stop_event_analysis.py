"""Audit Gold reports and publish a KAN-38 input manifest.

Run as a Glue Spark script with --DATA_BUCKET datavault-v2, or locally
with Python and boto3 using --DATA_BUCKET datavault-v2.
This script reads small JSON reports and lists event objects. It does not
start ETL jobs or scan transport Parquet rows.
"""
import argparse
from collections import defaultdict
from datetime import date, timedelta
import json
import sys
from uuid import uuid4

import boto3

ROOT = "gold/stop_event_analysis/"
RULE_VERSION = "gold-events-v2"
FIRST_DATE = date(2025, 11, 9)
LAST_DATE = date(2026, 11, 15)
HISTORICAL_END = date(2026, 11, 8)
KNOWN_MISSING = date(2026, 10, 19)


def expected_sources(day):
    """Historical dates require Ist-Daten; the RT study starts November 5."""
    sources = {"istdaten"}
    if day >= date(2026, 11, 5):
        sources.add("gtfs_rt")
    return sources


def report_problem(report, key, bucket):
    """Validate completion, source counts, output location and quality fields."""
    if report.get("status") != "success" or report.get("rule_version") != RULE_VERSION:
        return "earlier_rules_or_incomplete"
    try:
        day = date.fromisoformat(report["service_date"])
    except (KeyError, ValueError, TypeError):
        return "invalid_service_date"
    prefix = f"{ROOT}service_date={day}/"
    if not key.startswith(prefix):
        return "report_date_differs_from_folder"
    expected_output = f"s3://{bucket}/{key[:-len('report.json')]}events/"
    if report.get("output") != expected_output:
        return "unexpected_output_path"
    for source, summary in report.get("by_source", {}).items():
        count = summary.get("gold_events")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            return f"invalid_count_{source}"
        if summary.get("duplicate_keys") != 0:
            return f"duplicate_keys_{source}"
        if count:
            for field in ("weather_location", "weather", "canton", "elevation"):
                rate = summary.get(f"{field}_match_rate")
                if not isinstance(rate, (int, float)) or not 0 <= rate <= 1:
                    return f"invalid_match_rate_{source}_{field}"
    return None


def audit(s3, bucket):
    # List every Parquet and report once. Their keys also let us confirm that
    # each selected successful report points to at least one nonempty object.
    objects = {}
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=ROOT)
    for page in pages:
        for item in page.get("Contents", []):
            objects[item["Key"]] = item
    by_day = defaultdict(list)
    ignored = []
    for key, item in sorted(objects.items()):
        if not key.endswith("/report.json"):
            continue
        with s3.get_object(Bucket=bucket, Key=key)["Body"] as body:
            report = json.load(body)
        problem = report_problem(report, key, bucket)
        if problem:
            ignored.append({"report": key, "reason": problem})
            continue
        output_prefix = report["output"].split(f"s3://{bucket}/", 1)[1]
        parts = [name for name, entry in objects.items()
                 if name.startswith(output_prefix) and name.endswith(".parquet")
                 and entry["Size"] > 0]
        if not parts:
            ignored.append({"report": key, "reason": "missing_event_parquet"})
            continue
        by_day[date.fromisoformat(report["service_date"])].append(
            (item["LastModified"], key, report, len(parts)))

    selected, missing, incomplete = [], [], []
    totals = defaultdict(lambda: {"gold_events": 0, "weighted_matches": defaultdict(float)})
    for offset in range((LAST_DATE - FIRST_DATE).days + 1):
        day = FIRST_DATE + timedelta(days=offset)
        if day == KNOWN_MISSING:
            continue
        required = expected_sources(day)
        # Prefer the latest report containing all sources required for the date.
        # This prevents a newer Ist-Daten-only rerun from hiding RT events.
        candidates = [
            entry for entry in by_day.get(day, [])
            if required.issubset({source for source, summary in entry[2]["by_source"].items()
                                 if summary["gold_events"] > 0})
        ]
        if not candidates:
            (incomplete if by_day.get(day) else missing).append(day.isoformat())
            continue
        _, key, report, part_count = max(candidates, key=lambda entry: (entry[0], entry[1]))
        selected.append({
            "service_date": day.isoformat(),
            "output": report["output"],
            "report": f"s3://{bucket}/{key}",
            "parquet_parts": part_count,
            "by_source": report["by_source"],
        })
        for source, summary in report["by_source"].items():
            count = summary["gold_events"]
            totals[source]["gold_events"] += count
            if count:
                for field in ("weather_location", "weather", "canton", "elevation"):
                    totals[source]["weighted_matches"][field] += count * summary[f"{field}_match_rate"]

    # Match rates below describe context-row availability. Report rates are
    # rounded floating-point values, so weighted matches are approximate.
    aggregate = {}
    for source, summary in totals.items():
        count = summary["gold_events"]
        aggregate[source] = {
            "gold_events": count,
            "event_weighted_match_rates": {
                field: value / count for field, value in summary["weighted_matches"].items()
            } if count else {},
        }
    historical_dates = sum(date.fromisoformat(row["service_date"]) <= HISTORICAL_END
                           for row in selected)
    result = {
        "status": "passed" if not missing and not incomplete and historical_dates == 364 else "needs_review",
        "rule_version": RULE_VERSION,
        "historical_date_range": [str(FIRST_DATE), str(HISTORICAL_END)],
        "expected_historical_dates": 364,
        "verified_historical_dates": historical_dates,
        "selected_dates_total": len(selected),
        "expected_missing_source_date": str(KNOWN_MISSING),
        "unexpected_missing_dates": missing,
        "dates_missing_required_source": incomplete,
        "aggregate_by_source": aggregate,
        "ignored_reports": ignored,
        "verification_scope": "report checks and nonempty Parquet presence; no row-level Parquet rescan",
        "coverage_notes": [
            "Forecast collection begins November 9, 2026, partway through the day.",
            "Historical ERA5 collection ends November 8, 2026 UTC.",
            "Missing context is retained and flagged in Gold.",
            "Read only manifest-selected paths to avoid counting duplicate run snapshots.",
            "Static reference versions and coordinate-verified reuse are retained in event lineage.",
        ],
    }
    manifest = {
        "status": result["status"],
        "rule_version": RULE_VERSION,
        "inputs": selected,
        "coverage_notes": result["coverage_notes"],
    }
    return result, manifest


def main():
    # Glue passes unrelated system options. Resolve just DATA_BUCKET there.
    if "--JOB_NAME" in sys.argv:
        from awsglue.utils import getResolvedOptions
        bucket = getResolvedOptions(sys.argv, ["DATA_BUCKET"])["DATA_BUCKET"]
    else:
        parser = argparse.ArgumentParser(description=__doc__)
        parser.add_argument("--DATA_BUCKET", required=True)
        bucket = parser.parse_args().DATA_BUCKET
    s3 = boto3.client("s3")
    result, manifest = audit(s3, bucket)
    # Every audit gets its own folder. A needs_review manifest is clearly
    # labeled, allowing unresolved coverage to be reviewed before handoff.
    prefix = f"quality/stop_event_analysis_audit/run_id={uuid4().hex}/"
    for filename, payload in (("report.json", result), ("input_manifest.json", manifest)):
        s3.put_object(Bucket=bucket, Key=prefix + filename,
                      Body=json.dumps(payload, indent=2).encode(),
                      ContentType="application/json")
    print(json.dumps({
        **{key: result[key] for key in (
            "status", "verified_historical_dates", "selected_dates_total",
            "unexpected_missing_dates", "dates_missing_required_source")},
        "audit_report": f"s3://{bucket}/{prefix}report.json",
        "input_manifest": f"s3://{bucket}/{prefix}input_manifest.json",
    }, indent=2))


if __name__ == "__main__":
    main()
