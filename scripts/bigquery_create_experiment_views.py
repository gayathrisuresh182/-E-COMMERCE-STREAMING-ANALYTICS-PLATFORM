#!/usr/bin/env python3
"""
Create BigQuery views for Phase 5C A/B Testing dashboard.

Creates marts.experiment_comparison_view: pivoted fct_experiment_results
joined with experiment_summary for easy Looker consumption.

Prerequisite: marts.fct_experiment_results and marts.experiment_summary must exist and have data.

Usage: python scripts/bigquery_create_experiment_views.py [--project YOUR_PROJECT]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
    load_dotenv(PROJECT_ROOT / "docs" / ".env")
except ImportError:
    pass

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=PROJECT, help="GCP project ID")
    args = ap.parse_args()
    if not args.project:
        print("Set GOOGLE_CLOUD_PROJECT or pass --project")
        sys.exit(1)
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_APPLICATION_CREDENTIALS")
        sys.exit(1)

    from google.cloud import bigquery
    client = bigquery.Client(project=args.project)
    project = args.project

    sql = f"""
    CREATE OR REPLACE VIEW `{project}.marts.experiment_comparison_view` AS
    WITH pivoted AS (
      SELECT
        experiment_id,
        MAX(experiment_name) AS experiment_name,
        SUM(CASE WHEN variant = 'Control' THEN unique_users ELSE 0 END) AS control_users,
        SUM(CASE WHEN variant = 'Treatment' THEN unique_users ELSE 0 END) AS treatment_users,
        MAX(CASE WHEN variant = 'Control' THEN conversion_rate END) AS control_conversion_rate,
        MAX(CASE WHEN variant = 'Treatment' THEN conversion_rate END) AS treatment_conversion_rate,
        MAX(CASE WHEN variant = 'Control' THEN total_revenue END) AS control_revenue,
        MAX(CASE WHEN variant = 'Treatment' THEN total_revenue END) AS treatment_revenue,
        MAX(CASE WHEN variant = 'Control' THEN avg_order_value END) AS control_aov,
        MAX(CASE WHEN variant = 'Treatment' THEN avg_order_value END) AS treatment_aov
      FROM `{project}.marts.fct_experiment_results`
      GROUP BY experiment_id
    )
    SELECT
      p.*,
      s.p_value,
      s.significant,
      s.recommendation,
      s.revenue_impact_monthly,
      SAFE_DIVIDE(p.treatment_conversion_rate - p.control_conversion_rate, NULLIF(p.control_conversion_rate, 0)) * 100 AS lift_percent
    FROM pivoted p
    LEFT JOIN `{project}.marts.experiment_summary` s ON p.experiment_id = s.experiment_id
    """
    try:
        client.query(sql).result()
        print("Created: marts.experiment_comparison_view")
    except Exception as e:
        print(f"Failed: {e}")
        sys.exit(1)
    print("Done.")


if __name__ == "__main__":
    main()
