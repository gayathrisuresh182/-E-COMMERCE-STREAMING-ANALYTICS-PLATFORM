"""
Phase 4I: Sensor to trigger email delivery when weekly report is materialized.
"""
from __future__ import annotations

from dagster import AssetKey, RunRequest, asset_sensor


@asset_sensor(
    asset_key=AssetKey("weekly_experiment_report"),
    job_name="weekly_report_email_job",
)
def weekly_report_email_sensor(context, asset_event):
    """
    When weekly_experiment_report is materialized, trigger the email job.
    """
    return RunRequest()
