"""Read-only Gold frequency diagnostic for proposed RQ4 minimum counts.

Default input: approved Ist-Daten weeks 2026-01-05 through 2026-02-08. Use the immutable approved
manifest; never glob all run folders, which would double-count reruns.
This compares whether 5/10/30 events per daily or weekly group are feasible.
It does NOT decide persistence, inspect delay severity, or rewrite summaries.
Only a small immutable JSON report is written under quality/hotspot_coverage/.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from uuid import uuid4


def main():
    import boto3
    from awsglue.context import GlueContext
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    from pyspark import StorageLevel

    optional = [n for n in ('SERVICE_DATE', 'START_DATE', 'END_DATE', 'MANIFEST_KEY', 'SOURCE_MODE') if '--' + n in sys.argv]
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'DATA_BUCKET'] + optional)
    bucket = args['DATA_BUCKET']
    if 'SERVICE_DATE' in args and ('START_DATE' in args or 'END_DATE' in args):
        raise ValueError('Use SERVICE_DATE or a date range, not both')
    start = args.get('SERVICE_DATE', args.get('START_DATE', '2026-01-05'))
    end = args.get('SERVICE_DATE', args.get('END_DATE', '2026-02-08'))
    source_mode = args.get('SOURCE_MODE', 'istdaten')
    if source_mode not in ('istdaten', 'gtfs_rt', 'both'):
        raise ValueError('SOURCE_MODE must be istdaten, gtfs_rt or both')
    lower, upper = [datetime.strptime(d, '%Y-%m-%d').date() for d in (start, end)]
    if upper < lower or (upper - lower).days > 41:
        raise ValueError('Coverage pilot accepts at most 42 inclusive dates')
    expected_dates = {(lower + timedelta(days=n)).isoformat()
                      for n in range((upper - lower).days + 1)}
    s3 = boto3.client('s3')

    def read(key):
        return json.loads(s3.get_object(Bucket=bucket, Key=key)['Body'].read())

    manifest_key = args.get('MANIFEST_KEY')
    if not manifest_key:
        pointer = read('quality/stop_event_analysis_audit/latest_approved.json')
        uri = pointer['input_manifest']
        prefix = f's3://{bucket}/'
        if pointer.get('status') != 'passed' or not uri.startswith(prefix):
            raise ValueError('Invalid approved manifest pointer')
        manifest_key = uri[len(prefix):]
    manifest = read(manifest_key)
    if manifest.get('status') != 'passed':
        raise ValueError('Manifest is not approved')
    entries = [r for r in manifest['inputs'] if r['service_date'] in expected_dates]
    if len(entries) != len(expected_dates) or {r['service_date'] for r in entries} != expected_dates:
        raise ValueError('Need exactly one approved output for EVERY requested date')

    spark = GlueContext(SparkContext.getOrCreate()).spark_session
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    raw = spark.read.option('mergeSchema', 'true').parquet(*[r['output'] for r in entries])
    # Historical v2 Gold has no GTFS route_id. Its native line_id is sufficient
    # for an Ist-Daten-only coverage diagnostic; never invent a GTFS identifier.
    if 'route_id' not in raw.columns:
        raw = raw.withColumn('route_id', F.lit(None).cast('string'))
    if source_mode != 'both':
        raw = raw.where(F.col('source_type') == source_mode)
    required = ['source_type', 'time_status', 'event_type', 'stop_id',
                'reference_feed_version', 'route_id', 'line_id', 'event_key',
                'service_date', 'scheduled_time_utc', 'delay_seconds', 'is_cancelled']
    absent = set(required) - set(raw.columns)
    if absent:
        raise ValueError(f'Missing Gold fields: {sorted(absent)}')
    base = raw.select(*required).persist(StorageLevel.MEMORY_AND_DISK)
    if base.groupBy('source_type', 'event_key').count().where('count > 1').take(1):
        raise ValueError('Duplicate event keys; stop before evaluating coverage')

    # Use the route namespace actually supplied by each source. Unknown identity
    # rows stay in diagnostics; they never become a pooled anonymous hotspot.
    route = F.when(F.col('source_type') == 'gtfs_rt', F.col('route_id')).otherwise(F.col('line_id'))
    identified = base.withColumn('route_key', route.cast('string'))
    identity_ok = F.lit(True)
    for field in ('stop_id', 'reference_feed_version', 'route_key'):
        identity_ok = identity_ok & (F.length(F.trim(F.coalesce(F.col(field).cast('string'), F.lit('')))) > 0)
    local = F.from_utc_timestamp('scheduled_time_utc', 'Europe/Zurich')
    eligible = identity_ok & F.col('scheduled_time_utc').isNotNull()
    diagnostics = identified.groupBy('source_type').agg(
        F.count('*').alias('events_total'),
        F.sum((~eligible).cast('long')).alias('excluded_identity_or_time_events'),
    ).collect()
    keyed = (identified.where(eligible)
             .withColumn('status_group', F.coalesce('time_status', F.lit('NONE')))
             .withColumn('local_hour', F.hour(local))
             .withColumn('day_class', F.when(F.dayofweek(local).isin(1, 7), 'weekend').otherwise('weekday')))
    usable = (~F.coalesce(F.col('is_cancelled'), F.lit(False))
              & F.col('delay_seconds').isNotNull() & ~F.isnan('delay_seconds'))
    keys = ['source_type', 'status_group', 'event_type', 'stop_id',
            'reference_feed_version', 'route_key', 'service_date', 'local_hour', 'day_class']
    # No delay values or severe-delay outcomes are used to choose minimum counts.
    periods = keyed.groupBy(*keys).agg(
        F.count('*').alias('events_total'),
        F.sum(usable.cast('long')).alias('usable_observations'),
    ).persist(StorageLevel.MEMORY_AND_DISK)
    partitions = ['source_type', 'status_group', 'event_type']
    # Reaggregate the small daily count table rather than rescanning raw events.
    # Week starts Monday, using the service date; local hour/day class remain
    # unchanged. A feed-version change creates separate, possibly partial groups.
    week_keys = [k for k in keys if k != 'service_date'] + ['week_start']
    weekly = (periods.withColumn('week_start', F.to_date(F.date_trunc('week', F.to_timestamp('service_date'))))
              .groupBy(*week_keys).agg(
                  F.sum('events_total').alias('events_total'),
                  F.sum('usable_observations').alias('usable_observations'),
                  F.countDistinct('service_date').alias('observed_dates'),
                  F.countDistinct(F.when(F.col('usable_observations') > 0, F.col('service_date')))
                   .alias('usable_dates'),
              )).persist(StorageLevel.MEMORY_AND_DISK)

    def summarize(frame):
        return [r.asDict() for r in frame.groupBy(*partitions).agg(
        F.count('*').alias('observed_groups'),
        F.sum('usable_observations').alias('usable_events'),
        F.percentile_approx('usable_observations', [0.1, 0.5, 0.9], 10000).alias('count_p10_p50_p90'),
        *[F.sum((F.col('usable_observations') >= n).cast('long')).alias(f'groups_at_least_{n}')
          for n in (5, 10, 30)],
        *[F.sum(F.when(F.col('usable_observations') >= n, F.col('usable_observations')).otherwise(0))
          .alias(f'usable_events_in_groups_at_least_{n}') for n in (5, 10, 30)],
        ).collect()]

    # Report date support and version spans explicitly. One week tests frequency
    # feasibility only; it cannot establish recurrence across several weeks.
    date_support = weekly.groupBy(*partitions, 'day_class', 'reference_feed_version').agg(
        F.count('*').alias('weekly_groups'),
        F.percentile_approx('usable_dates', [0.1, 0.5, 0.9], 10000).alias('usable_dates_p10_p50_p90'),
        F.max('observed_dates').alias('max_observed_dates'),
    ).collect()
    version_dates = keyed.groupBy('source_type', 'reference_feed_version').agg(
        F.sort_array(F.collect_set('service_date')).alias('observed_service_dates')).collect()
    # A complete requested calendar week is necessary but not sufficient for
    # route operation coverage. This is explicitly an observed-group screen.
    complete_weeks = sum(1 for n in range((upper - lower).days + 1)
                         if (lower + timedelta(days=n)).weekday() == 0
                         and lower + timedelta(days=n + 6) <= upper)
    recurrence_keys = [k for k in week_keys if k != 'week_start']
    supported = weekly.groupBy(*recurrence_keys).agg(
        F.count('*').alias('observed_weeks'),
        F.sum((F.col('usable_observations') >= 10).cast('long')).alias('valid_weeks_at_10'),
    )
    repeated_support = supported.groupBy(*partitions).agg(
        F.count('*').alias('observed_groups'),
        F.percentile_approx('valid_weeks_at_10', [0.1, 0.5, 0.9], 10000).alias('valid_weeks_p10_p50_p90'),
        F.sum(((F.col('valid_weeks_at_10') >= 5)
               & (F.col('valid_weeks_at_10') >= complete_weeks * 0.5)).cast('long'))
         .alias('groups_with_at_least_5_valid_weeks_and_half_requested_weeks'),
    ).collect()
    report = {
        'status': 'success', 'diagnostic_version': 'hotspot-count-coverage-v3-multiweek',
        'source_mode': source_mode, 'requested_complete_weeks': complete_weeks,
        'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'date_range': [start, end], 'selected_dates': sorted(expected_dates),
        'input_manifest': f's3://{bucket}/{manifest_key}',
        'input_events': [r['output'] for r in entries],
        'identity_diagnostics': [r.asDict() for r in diagnostics],
        'daily_group_counts': summarize(periods),
        'weekly_group_counts': summarize(weekly),
        'weekly_date_support': [r.asDict() for r in date_support],
        'source_version_dates': [r.asDict() for r in version_dates],
        'repeated_week_support': [r.asDict() for r in repeated_support],
        'limitations': [
            'Frequency and repeated-week support only; no delay-based hotspot classifications.',
            'Requested-week denominator is a conservative coverage screen, not scheduled-route operation eligibility.',
            'Weekly groups retain weekday/weekend and local hour; feed versions are not combined.',
            'Feed-version changes or requested partial weeks reduce period coverage.',
            'Denominators are observed groups only, not all scheduled services.',
            'Retain source and time-status separation when evaluating thresholds.',
            'Repeat representative dates before agreeing final minimum-count rules.',
        ],
    }
    key = f'quality/hotspot_coverage/run_id={uuid4().hex}/report.json'
    s3.put_object(Bucket=bucket, Key=key, ContentType='application/json',
                  Body=json.dumps(report, indent=2, default=str).encode())
    periods.unpersist()
    weekly.unpersist()
    base.unpersist()
    print(json.dumps({'status': 'success', 'report': f's3://{bucket}/{key}'}))


if __name__ == '__main__':
    main()
