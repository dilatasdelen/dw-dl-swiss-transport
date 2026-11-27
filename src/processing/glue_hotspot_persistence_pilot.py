"""RQ4 weekly persistence pilot: isolated outputs, immutable approved inputs.

Defaults: Ist-Daten, 2026-01-05 through 2026-02-08 (five complete weeks).
Optional SOURCE_MODE: istdaten, gtfs_rt, both. Maximum six weeks in this pilot.
GTFS route identifiers are NEVER combined across reference feed versions.

Group: source/status/meaning/event type/versioned stop/native route or line/
local scheduled hour/weekday-weekend. Period: Monday-Sunday SERVICE week.
Rules: >=10 usable observations per valid week; >=20% at delay >=180 seconds
makes a severe week. Need >=5 valid weeks, >=50% eligible-week coverage,
and >=50% severe weeks among valid weeks to classify a persistent hotspot.

Coverage counts are observation screens, not scheduled-service reliability.
Insufficient coverage yields a NULL classification, not a reliable-service claim.
Weather scopes are all/normal/adverse/unknown. Do not sum across scopes.
Pure reference scenario checks are in hotspot_persistence_rules.py; this job
must still be validated in Glue before its outputs are used downstream.
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from uuid import uuid4


RULE_VERSION = "hotspot-persistence-v2-weekly-pilot"
MIN_OBSERVATIONS = 10
MIN_WEEKS = 5
SEVERE_DELAY_SECONDS = 180
SEVERE_WEEK_RATE = 0.20
MIN_VALID_SHARE = 0.50
MIN_SEVERE_SHARE = 0.50


def log(stage):
    # Lightweight progress messages use existing output logs; no custom metrics.
    print(json.dumps({'stage': stage, 'at_utc': datetime.now(timezone.utc).isoformat()}), flush=True)


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
    if lower.weekday() != 0 or upper.weekday() != 6:
        raise ValueError('Use complete Monday-to-Sunday weeks for the persistence pilot')
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
                'service_date', 'scheduled_time_utc', 'delay_seconds', 'is_cancelled',
                'missing_weather', 'temperature_2m_c', 'precipitation_mm',
                'snowfall_cm', 'wind_speed_10m_kmh', 'data_product', 'model']
    absent = set(required) - set(raw.columns)
    if absent:
        raise ValueError(f'Missing Gold fields: {sorted(absent)}')
    base = raw.select(*required).persist(StorageLevel.MEMORY_AND_DISK)
    log('checking_input_event_keys')
    if base.groupBy('source_type', 'event_key').count().where('count > 1').take(1):
        raise ValueError('Duplicate event keys; stop before evaluating coverage')

    log('input_key_check_complete')
    # Use source-native identifiers; missing identities are diagnostics rather
    # than a single anonymous route that would create a misleading hotspot.
    route = F.when(F.col('source_type') == 'gtfs_rt', F.col('route_id')).otherwise(F.col('line_id'))
    identified = base.withColumn('route_key', route.cast('string'))
    identity_ok = F.lit(True)
    for field in ('stop_id', 'reference_feed_version', 'route_key'):
        identity_ok = identity_ok & (F.length(F.trim(F.coalesce(F.col(field).cast('string'), F.lit('')))) > 0)
    identity_ok = identity_ok & F.col('scheduled_time_utc').isNotNull()
    diagnostics = [r.asDict() for r in identified.groupBy('source_type').agg(
        F.count('*').alias('events_total'),
        F.sum((~identity_ok).cast('long')).alias('excluded_identity_or_time_events'),
    ).collect()]
    local = F.from_utc_timestamp('scheduled_time_utc', 'Europe/Zurich')
    meaning = F.when(F.col('source_type') == 'gtfs_rt', 'published_prediction')
    for code, label in [('REAL', 'measured'), ('GESCHAETZT', 'estimated'),
                        ('PROGNOSE', 'forecast_in_source'), ('UNBEKANNT', 'unknown')]:
        meaning = meaning.when(F.col('time_status') == code, label)

    # Keep the EXISTING RQ1 category priority, rather than silently substituting
    # RQ3's independent hot/cold temperature bands. Report unknowns explicitly.
    has_weather = ~F.coalesce(F.col('missing_weather'), F.lit(True)) & (
        F.col('temperature_2m_c').isNotNull() | F.col('precipitation_mm').isNotNull())
    category = (F.when(~has_weather, 'no_weather')
                .when(F.col('snowfall_cm') >= 0.1, 'snow')
                .when(F.col('precipitation_mm') >= 1.0, 'rain')
                .when(F.col('wind_speed_10m_kmh') >= 39, 'strong_wind')
                .when(F.col('temperature_2m_c') <= 0, 'frost').otherwise('normal'))
    usable = (~F.coalesce(F.col('is_cancelled'), F.lit(False))
              & F.col('delay_seconds').isNotNull() & ~F.isnan('delay_seconds'))
    prepared = (identified.where(identity_ok)
        .withColumn('status_group', F.coalesce('time_status', F.lit('NONE')))
        .withColumn('delay_meaning', meaning.otherwise('unknown'))
        .withColumn('route_namespace', F.when(F.col('source_type') == 'gtfs_rt', 'gtfs_route').otherwise('istdaten_line'))
        .withColumn('local_hour', F.hour(local))
        .withColumn('day_class', F.when(F.dayofweek(local).isin(1, 7), 'weekend').otherwise('weekday'))
        .withColumn('week_start', F.to_date(F.date_trunc('week', F.to_timestamp('service_date'))))
        .withColumn('weather_category', category)
        .withColumn('weather_scope_value', F.when(category == 'no_weather', 'unknown')
                    .when(category == 'normal', 'normal').otherwise('adverse'))
        .withColumn('usable', usable))

    # Determine source/version availability independently from a particular
    # route/status/hour's observed dates. Seven observed service dates are a
    # conservative eligibility screen. They do not prove timetable operation.
    version_week = ['source_type', 'reference_feed_version', 'week_start']
    eligibility = (prepared.groupBy(*version_week)
        .agg(F.countDistinct('service_date').alias('source_version_dates'))
        .withColumn('eligible_week', F.col('source_version_dates') == 7))
    eligible_lists = eligibility.where('eligible_week').groupBy(
        'source_type', 'reference_feed_version').agg(
        F.sort_array(F.collect_set('week_start')).alias('eligible_week_starts'),
        F.count('*').alias('eligible_weeks'))
    log('checking_version_week_coverage')
    eligible_rows = [r.asDict() for r in eligibility.collect()]

    log('version_week_coverage_complete')
    # Each event contributes to an overall row AND exactly one weather class.
    # These are alternative views, not additive datasets.
    expanded = prepared.withColumn('weather_scope', F.explode(
        F.array(F.lit('all'), F.col('weather_scope_value'))))
    group = ['source_type', 'status_group', 'delay_meaning', 'event_type',
             'stop_id', 'reference_feed_version', 'route_namespace', 'route_key',
             'local_hour', 'day_class']
    scope_keys = group + ['weather_scope']

    def aggregate(frame, keys):
        # Null conditional delay excludes cancelled/missing-delay observations
        # from percentiles without dropping their diagnostic event counts.
        delay = F.when(F.col('usable'), F.col('delay_seconds'))
        severe = F.col('usable') & (F.col('delay_seconds') >= SEVERE_DELAY_SECONDS)
        return frame.groupBy(*keys).agg(
            F.count('*').alias('events_total'),
            F.sum(F.col('usable').cast('long')).alias('observation_count'),
            F.sum(severe.cast('long')).alias('severe_observation_count'),
            F.sum(F.coalesce(F.col('is_cancelled'), F.lit(False)).cast('long')).alias('cancelled_count'),
            F.sum((F.col('delay_seconds').isNull() | F.isnan('delay_seconds')).cast('long')).alias('missing_delay_count'),
            F.percentile_approx(delay, 0.5, 10000).alias('median_delay_s'),
            F.percentile_approx(delay, 0.9, 10000).alias('p90_delay_s'),
            F.countDistinct('service_date').alias('observed_dates_count'),
            F.countDistinct(F.when(F.col('usable'), F.col('service_date'))).alias('usable_dates_count'),
            F.sort_array(F.collect_set('service_date')).alias('observed_service_dates'),
            F.min('service_date').alias('date_range_start'),
            F.max('service_date').alias('date_range_end'),
            F.sort_array(F.collect_set('data_product')).alias('weather_products'),
            F.sort_array(F.collect_set('model')).alias('weather_models'),
        ).withColumn('severe_delay_rate', F.col('severe_observation_count') /
                     F.when(F.col('observation_count') > 0, F.col('observation_count')))

    weekly = (aggregate(expanded, scope_keys + ['week_start'])
        .join(eligibility, version_week, 'left')
        .withColumn('valid_period', F.col('eligible_week') & (F.col('observation_count') >= MIN_OBSERVATIONS))
        .withColumn('period_above_threshold', F.col('valid_period') & (F.col('severe_delay_rate') >= SEVERE_WEEK_RATE))
        .persist(StorageLevel.MEMORY_AND_DISK))

    recurrence = weekly.groupBy(*scope_keys).agg(
        F.count('*').alias('observed_weeks'),
        F.sum(F.col('valid_period').cast('long')).alias('valid_weeks'),
        F.sum(F.col('period_above_threshold').cast('long')).alias('weeks_above_threshold'),
        F.sort_array(F.collect_set(F.when(F.col('valid_period'), F.col('week_start')))).alias('valid_week_starts'),
        F.sum((~F.col('eligible_week')).cast('long')).alias('incomplete_version_weeks_observed'),
    ).join(eligible_lists, ['source_type', 'reference_feed_version'], 'left')
    recurrence = (recurrence.fillna({'eligible_weeks': 0})
        .withColumn('valid_week_share', F.col('valid_weeks') /
                    F.when(F.col('eligible_weeks') > 0, F.col('eligible_weeks')))
        .withColumn('severe_week_share', F.col('weeks_above_threshold') /
                    F.when(F.col('valid_weeks') > 0, F.col('valid_weeks')))
        .withColumn('sufficient_coverage', (F.col('valid_weeks') >= MIN_WEEKS)
                    & (F.coalesce(F.col('valid_week_share'), F.lit(0)) >= MIN_VALID_SHARE))
        .withColumn('persistent_hotspot', F.when(F.col('sufficient_coverage'),
                    F.col('severe_week_share') >= MIN_SEVERE_SHARE).otherwise(F.lit(None).cast('boolean'))))

    # Recompute pooled percentiles from event delays. Never average weekly
    # medians or p90s: that would not give the period-wide median or p90.
    summary = aggregate(expanded, scope_keys).join(recurrence, scope_keys, 'inner')
    from pyspark.sql import Window
    comparison_window = Window.partitionBy(*group)
    for scope in ('normal', 'adverse', 'unknown'):
        summary = summary.withColumn(scope + '_observation_count', F.coalesce(
            F.max(F.when(F.col('weather_scope') == scope, F.col('observation_count')))
             .over(comparison_window), F.lit(0)))
    normal_enough = F.max(F.when(F.col('weather_scope') == 'normal', F.col('sufficient_coverage').cast('int'))).over(comparison_window)
    adverse_enough = F.max(F.when(F.col('weather_scope') == 'adverse', F.col('sufficient_coverage').cast('int'))).over(comparison_window)
    summary = (summary.withColumn('sparse_weather_comparison',
                 ~((F.coalesce(normal_enough, F.lit(0)) == 1) & (F.coalesce(adverse_enough, F.lit(0)) == 1)))
               .withColumn('rule_version', F.lit(RULE_VERSION))
               .persist(StorageLevel.MEMORY_AND_DISK))
    # Combine result counts, key uniqueness and consistency validation in one
    # aggregation instead of launching three separate scans of the summary.
    invalid = ((F.col('valid_weeks') > F.col('eligible_weeks'))
               | (F.col('weeks_above_threshold') > F.col('valid_weeks'))
               | (F.col('severe_observation_count') > F.col('observation_count'))
               | (F.col('severe_delay_rate') < 0) | (F.col('severe_delay_rate') > 1)
               | ((~F.col('sufficient_coverage')) & F.col('persistent_hotspot').isNotNull()))
    log('calculating_and_validating_summary')
    checks = [r.asDict() for r in summary.groupBy('source_type', 'status_group', 'event_type', 'weather_scope').agg(
        F.count('*').alias('groups'),
        F.countDistinct(F.struct(*[F.col(k) for k in scope_keys])).alias('distinct_summary_keys'),
        F.sum(F.coalesce(invalid, F.lit(False)).cast('long')).alias('invalid_rows'),
        F.sum(F.col('sufficient_coverage').cast('long')).alias('groups_with_sufficient_coverage'),
        F.sum(F.coalesce(F.col('persistent_hotspot'), F.lit(False)).cast('long')).alias('persistent_groups'),
        F.sum(F.col('persistent_hotspot').isNull().cast('long')).alias('unclassified_sparse_groups'),
        F.sum(F.col('sparse_weather_comparison').cast('long')).alias('sparse_weather_comparisons'),
    ).collect()]
    if any(r['groups'] != r['distinct_summary_keys'] for r in checks):
        raise ValueError('Duplicate persistence summary key')
    if any(r['invalid_rows'] for r in checks):
        raise ValueError('Inconsistent counts, rates or sparse classification')
    log('summary_validation_complete')

    run_id = uuid4().hex
    root = f'gold/analysis_hotspot_pilot/run_id={run_id}/'
    summary_path = f's3://{bucket}/{root}network_hotspots/'
    weekly_path = f's3://{bucket}/{root}weekly_periods/'
    # Immutable per-run outputs: reruns do not erase prior pilot evidence.
    log('writing_summary')
    summary.write.mode('errorifexists').parquet(summary_path)
    log('writing_weekly_periods')
    weekly.withColumn('rule_version', F.lit(RULE_VERSION)).write.mode('errorifexists').parquet(weekly_path)
    report = {
        'status': 'success', 'implementation_version': 'combined-validation-v1', 'rule_version': RULE_VERSION,
        'created_at_utc': datetime.now(timezone.utc).isoformat(),
        'source_mode': source_mode, 'requested_date_range': [start, end],
        'input_manifest': f's3://{bucket}/{manifest_key}',
        'selected_dates': sorted(expected_dates),
        'input_events': [r['output'] for r in entries],
        'rules': {'period': 'Monday-Sunday service week', 'timezone': 'Europe/Zurich',
                  'minimum_observations_per_week': MIN_OBSERVATIONS, 'minimum_valid_weeks': MIN_WEEKS,
                  'severe_event_seconds': SEVERE_DELAY_SECONDS, 'severe_week_rate': SEVERE_WEEK_RATE,
                  'minimum_valid_week_share': MIN_VALID_SHARE, 'minimum_severe_week_share': MIN_SEVERE_SHARE,
                  'weather_rule': 'existing snow > rain > wind > frost > normal priority; missing weather separate'},
        'identity_diagnostics': diagnostics, 'source_version_week_coverage': eligible_rows,
        'validation': {'duplicate_input_event_keys': 0, 'duplicate_summary_keys': 0,
                       'invalid_counts_rates_or_sparse_classifications': 0},
        'groups_by_source_status_event_weather': checks,
        'outputs': {'summary': summary_path, 'weekly_periods': weekly_path},
        'limitations': [
            'Pilot, not a production summary refresh.',
            'Coverage denominator uses complete observed source/version weeks, not validated route-operation schedules.',
            'Native line continuity across the selected period is assumed, not validated by a GTFS crosswalk.',
            'Short GTFS feed versions remain separate and may have insufficient weeks.',
            'Normal/adverse scopes can cover different dates; differences are descriptive, not causal.',
            'Weather lineage arrays disclose pooled products/models; multiple models require review before downstream use.',
        ],
    }
    log('outputs_written')
    report_key = f'quality/hotspot_persistence/run_id={run_id}/report.json'
    s3.put_object(Bucket=bucket, Key=report_key, ContentType='application/json',
                  Body=json.dumps(report, indent=2, default=str).encode())
    summary.unpersist()
    weekly.unpersist()
    base.unpersist()
    print(json.dumps({'status': 'success', 'report': f's3://{bucket}/{report_key}',
                      'summary': summary_path, 'weekly_periods': weekly_path}))


if __name__ == '__main__':
    main()
