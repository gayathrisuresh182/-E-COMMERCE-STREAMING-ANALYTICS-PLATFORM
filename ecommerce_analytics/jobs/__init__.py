"""
Dagster jobs and schedules for batch, stream, and reconciliation orchestration.

Batch layer (Phase 3J):
  - refresh_dimensions: Source → staging → dimensions. Non-partitioned.
  - daily_partitioned_facts: fct_orders, fct_reviews, fct_daily_metrics.
    Requires --partition flag.
  - refresh_agg_facts: Non-partitioned aggregate facts.
  - daily_batch_processing: Scheduled at 02:00 (dims + agg facts).

Stream layer (Phase 3K):
  - stream_sync: Hourly snapshot of MongoDB realtime collections +
    consumer health metrics. Also triggered by sensors.
  - realtime_snapshot: Sensor-triggered only (same realtime group,
    no health metrics).
  - batch_stream_reconciliation_job: Nightly at 03:00, compares stream
    layer (MongoDB) against batch layer (staging), reports drift.

Schedules:
  daily_batch_schedule   – 02:00 São Paulo
  hourly_stream_schedule – Every hour (*:00)
  nightly_reconciliation_schedule – 03:00 São Paulo (after batch)
"""
from __future__ import annotations

from dagster import (
    AssetSelection,
    ScheduleDefinition,
    define_asset_job,
    job,
)
from ecommerce_analytics.ops.report_ops import send_experiment_report_email

# ── Asset Selections ─────────────────────────────────────────────────

NON_PARTITIONED_SELECTION = (
    AssetSelection.key_substring("raw_")
    | AssetSelection.key_substring("stg_")
    | AssetSelection.key_substring("dim_")
    | AssetSelection.keys("phase1_verification_asset")
)

PARTITIONED_FACT_SELECTION = AssetSelection.keys(
    "fct_orders", "fct_reviews", "fct_daily_metrics"
)

AGG_FACT_SELECTION = AssetSelection.keys(
    "raw_experiment_assignments",
    "fct_order_items",
    "fct_experiment_results",
    "fct_product_performance",
    "fct_seller_performance",
)

REALTIME_SELECTION = AssetSelection.groups("realtime")

STREAM_OPS_SELECTION = AssetSelection.groups("stream_ops")

UNIFIED_SELECTION = AssetSelection.groups("unified")

STREAM_SYNC_SELECTION = REALTIME_SELECTION | AssetSelection.keys(
    "consumer_health_metrics"
)

RECONCILIATION_SELECTION = AssetSelection.keys("batch_stream_reconciliation")

ANALYSIS_SELECTION = AssetSelection.keys(
    "experiment_exp_001_analysis",
    "experiment_exp_002_analysis",
    "experiment_exp_003_analysis",
    "experiment_exp_004_analysis",
    "experiment_exp_005_analysis",
    "experiment_exp_006_analysis",
    "experiment_exp_007_analysis",
    "experiment_exp_008_analysis",
    "experiment_exp_009_analysis",
    "experiment_exp_010_analysis",
    "all_experiments_summary",
)

WEEKLY_REPORT_SELECTION = AssetSelection.keys("weekly_experiment_report")

# ── Batch Jobs ────────────────────────────────────────────────────────

refresh_dimensions = define_asset_job(
    name="refresh_dimensions",
    selection=NON_PARTITIONED_SELECTION,
    description=(
        "Refresh source → staging → dimension assets. "
        "Non-partitioned, full table refresh. ~2 min."
    ),
)

daily_partitioned_facts = define_asset_job(
    name="daily_partitioned_facts",
    selection=PARTITIONED_FACT_SELECTION,
    description=(
        "Materialize partitioned fact assets for a single date. "
        "Requires --partition flag (e.g. --partition 2017-06-15). "
        "Assets: fct_orders, fct_reviews, fct_daily_metrics."
    ),
)

refresh_agg_facts = define_asset_job(
    name="refresh_agg_facts",
    selection=AGG_FACT_SELECTION,
    description=(
        "Refresh non-partitioned aggregate facts: fct_order_items, "
        "fct_experiment_results, fct_product_performance, fct_seller_performance. "
        "Reads from staging tables (not partitioned facts). ~3 min."
    ),
)

daily_batch_processing = define_asset_job(
    name="daily_batch_processing",
    selection=NON_PARTITIONED_SELECTION | AGG_FACT_SELECTION,
    description=(
        "Full daily batch pipeline: source → staging → dimensions → "
        "aggregate facts. Non-partitioned assets only. "
        "Partitioned facts are backfilled separately via CLI or Dagit."
    ),
)

# ── Stream Jobs ───────────────────────────────────────────────────────

realtime_snapshot = define_asset_job(
    name="realtime_snapshot",
    selection=REALTIME_SELECTION,
    description=(
        "Snapshot MongoDB realtime collections into Dagster assets. "
        "Triggered by sensors, not scheduled."
    ),
)

stream_sync = define_asset_job(
    name="stream_sync",
    selection=STREAM_SYNC_SELECTION,
    description=(
        "Hourly stream sync: snapshot realtime MongoDB collections "
        "AND refresh consumer health metrics. Scheduled every hour "
        "and also triggered by sensors on high lag."
    ),
)

batch_stream_reconciliation_job = define_asset_job(
    name="batch_stream_reconciliation_job",
    selection=RECONCILIATION_SELECTION,
    description=(
        "Nightly reconciliation: compare stream layer (MongoDB) with "
        "batch layer (staging). Reports overlap, drift, and candidates "
        "for merge. Scheduled at 03:00, after daily batch completes."
    ),
)

refresh_unified = define_asset_job(
    name="refresh_unified",
    selection=UNIFIED_SELECTION,
    description=(
        "Refresh Lambda serving layer: unified_orders, "
        "unified_daily_metrics, unified_experiment_results. "
        "Combines batch + stream data with deduplication."
    ),
)

weekly_statistical_analysis = define_asset_job(
    name="weekly_statistical_analysis",
    selection=ANALYSIS_SELECTION,
    description=(
        "Run statistical analysis on all 10 experiments (frequentist + Bayesian "
        "+ multi-metric). Produces JSON results and executive summary."
    ),
)

weekly_report_job = define_asset_job(
    name="weekly_report_job",
    selection=WEEKLY_REPORT_SELECTION,
    description=(
        "Generate weekly experiment report (HTML, PDF, charts). "
        "Depends on all experiment analyses. Triggers email sensor on completion."
    ),
)

@job(
    name="weekly_report_email_job",
    description="Send weekly report via email. Triggered by sensor when report is materialized.",
)
def weekly_report_email_job():
    send_experiment_report_email()

daily_batch_schedule = ScheduleDefinition(
    name="daily_batch_schedule",
    job=daily_batch_processing,
    cron_schedule="0 2 * * *",
    description=(
        "Daily at 02:00 UTC-3 (São Paulo): refresh all non-partitioned "
        "batch assets (source → staging → dims → agg facts)."
    ),
    execution_timezone="America/Sao_Paulo",
)

hourly_stream_schedule = ScheduleDefinition(
    name="hourly_stream_schedule",
    job=stream_sync,
    cron_schedule="0 * * * *",
    description=(
        "Every hour: snapshot realtime MongoDB collections and "
        "capture consumer health metrics."
    ),
    execution_timezone="America/Sao_Paulo",
)

nightly_reconciliation_schedule = ScheduleDefinition(
    name="nightly_reconciliation_schedule",
    job=batch_stream_reconciliation_job,
    cron_schedule="0 3 * * *",
    description=(
        "Daily at 03:00 UTC-3 (São Paulo): reconcile stream vs batch "
        "layers. Runs after daily_batch_processing completes at 02:00."
    ),
    execution_timezone="America/Sao_Paulo",
)

weekly_analysis_schedule = ScheduleDefinition(
    name="weekly_analysis_schedule",
    job=weekly_statistical_analysis,
    cron_schedule="0 6 * * 0",
    description=(
        "Sundays at 06:00 UTC-3 (São Paulo): run statistical analysis "
        "on all 10 experiments and produce executive summary."
    ),
    execution_timezone="America/Sao_Paulo",
)

weekly_report_schedule = ScheduleDefinition(
    name="weekly_report_schedule",
    job=weekly_report_job,
    cron_schedule="0 8 * * 1",
    description=(
        "Mondays at 08:00 UTC-3 (São Paulo): generate weekly experiment report "
        "(HTML, PDF, charts). Sensor triggers email delivery on completion."
    ),
    execution_timezone="America/Sao_Paulo",
)

# ── Exports ───────────────────────────────────────────────────────────

core_jobs = [
    refresh_dimensions,
    daily_partitioned_facts,
    refresh_agg_facts,
    daily_batch_processing,
    realtime_snapshot,
    stream_sync,
    batch_stream_reconciliation_job,
    refresh_unified,
    weekly_statistical_analysis,
    weekly_report_job,
    weekly_report_email_job,
]

core_schedules = [
    daily_batch_schedule,
    hourly_stream_schedule,
    nightly_reconciliation_schedule,
    weekly_analysis_schedule,
    weekly_report_schedule,
]
