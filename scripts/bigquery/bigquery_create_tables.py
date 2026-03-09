#!/usr/bin/env python3
"""
Phase 1D: Create BigQuery tables (staging, marts, realtime, experiments).
Run after: python scripts/bigquery_create_datasets.py

Usage: python scripts/bigquery_create_tables.py [--project my-gcp-project]
Requires: GOOGLE_APPLICATION_CREDENTIALS set
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
load_dotenv(Path(__file__).resolve().parent.parent.parent / "docs" / ".env")

from google.cloud import bigquery


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    args = ap.parse_args()

    if not args.project:
        print("Set GOOGLE_CLOUD_PROJECT or pass --project")
        sys.exit(1)

    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_APPLICATION_CREDENTIALS")
        sys.exit(1)

    client = bigquery.Client(project=args.project)
    project = args.project

    # Staging
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.staging.stg_orders` (
            order_id STRING NOT NULL,
            customer_id STRING NOT NULL,
            order_status STRING,
            order_purchase_timestamp TIMESTAMP,
            order_approved_at TIMESTAMP,
            order_delivered_carrier_date TIMESTAMP,
            order_delivered_customer_date TIMESTAMP,
            order_estimated_delivery_date TIMESTAMP,
            order_total FLOAT64,
            created_at TIMESTAMP
        )
    """).result()

    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.staging.stg_customers` (
            customer_id STRING NOT NULL,
            customer_unique_id STRING,
            customer_zip_code_prefix STRING,
            customer_city STRING,
            customer_state STRING,
            created_at TIMESTAMP
        )
    """).result()

    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.staging.stg_products` (
            product_id STRING NOT NULL,
            product_category_name STRING,
            product_category_name_english STRING,
            product_name_length INT64,
            product_description_length INT64,
            product_photos_qty INT64,
            product_weight_g FLOAT64,
            product_length_cm FLOAT64,
            product_height_cm FLOAT64,
            product_width_cm FLOAT64,
            created_at TIMESTAMP
        )
    """).result()

    # Marts - partitioned + clustered (PHASE_5A: fct_orders, fct_daily_metrics, fct_experiment_results)
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.marts.fct_orders` (
            order_id STRING NOT NULL,
            customer_id STRING NOT NULL,
            order_status STRING,
            customer_state STRING,
            order_purchase_timestamp TIMESTAMP,
            order_total FLOAT64
        )
        PARTITION BY DATE(order_purchase_timestamp)
        CLUSTER BY order_status, customer_state
        OPTIONS (
            partition_expiration_days = 730,
            description = 'Order fact - partitioned by date'
        )
    """).result()

    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.marts.fct_daily_metrics` (
            metric_date DATE NOT NULL,
            total_orders INT64,
            total_gmv FLOAT64,
            total_items INT64,
            avg_order_value FLOAT64,
            unique_customers INT64,
            orders_delivered INT64,
            orders_canceled INT64,
            avg_delivery_time_days FLOAT64,
            late_delivery_rate FLOAT64,
            created_at TIMESTAMP
        )
        PARTITION BY metric_date
        OPTIONS (
            partition_expiration_days = 730,
            description = 'Daily aggregated metrics - one row per date'
        )
    """).result()

    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.marts.fct_experiment_results` (
            experiment_id STRING NOT NULL,
            experiment_name STRING,
            variant STRING NOT NULL,
            unique_users INT64,
            total_orders INT64,
            converting_users INT64,
            conversion_rate FLOAT64,
            total_revenue FLOAT64,
            avg_order_value FLOAT64,
            revenue_per_user FLOAT64,
            created_at TIMESTAMP
        )
        OPTIONS (description = 'A/B test results - one row per experiment x variant')
    """).result()

    # experiment_summary: p_value, recommendation, revenue impact (PHASE_5C A/B dashboard)
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.marts.experiment_summary` (
            experiment_id STRING NOT NULL,
            experiment_name STRING,
            p_value FLOAT64,
            significant STRING,
            recommendation STRING,
            revenue_impact_monthly FLOAT64,
            created_at TIMESTAMP
        )
        OPTIONS (description = 'Experiment-level stats: p_value, recommendation, revenue impact')
    """).result()

    # clickstream_events: funnel stages (PHASE_5D Customer Journey dashboard)
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.marts.clickstream_events` (
            event_id STRING NOT NULL,
            event_type STRING,
            event_timestamp TIMESTAMP,
            session_id STRING,
            customer_id STRING,
            experiment_id STRING,
            variant STRING,
            device STRING,
            referrer STRING,
            page_url STRING,
            order_id STRING,
            _loaded_at TIMESTAMP
        )
        PARTITION BY DATE(event_timestamp)
        OPTIONS (
            partition_expiration_days = 90,
            description = 'Clickstream events for funnel analysis - from events.ndjson'
        )
    """).result()

    # Realtime - short retention (PHASE_5B dashboard)
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.realtime.realtime_orders` (
            event_id STRING NOT NULL,
            order_id STRING,
            customer_id STRING,
            customer_state STRING,
            event_timestamp TIMESTAMP,
            order_total FLOAT64,
            event_type STRING,
            created_at TIMESTAMP
        )
        PARTITION BY DATE(event_timestamp)
        OPTIONS (
            partition_expiration_days = 7,
            description = 'Kafka stream orders - 7 day retention'
        )
    """).result()

    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.realtime.realtime_metrics` (
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            metric_name STRING NOT NULL,
            metric_value FLOAT64,
            _ingested_at TIMESTAMP
        )
        PARTITION BY DATE(window_start)
        OPTIONS (
            partition_expiration_days = 30,
            description = '5-minute window metrics from Kafka MetricsAggregator'
        )
    """).result()

    # Monitoring (PHASE_5F Data Quality dashboard)
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.monitoring.consumer_health_metrics` (
            consumer_group STRING,
            topics STRING,
            total_lag INT64,
            partitions INT64,
            status STRING,
            measured_at TIMESTAMP,
            source STRING,
            collection STRING,
            label STRING,
            document_count INT64,
            newest_timestamp STRING,
            error STRING
        )
        OPTIONS (description = 'Kafka consumer lag + MongoDB collection stats')
    """).result()

    # Experiments
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{project}.experiments.experiment_results` (
            experiment_id STRING NOT NULL,
            variant STRING NOT NULL,
            n_users INT64,
            conversions INT64,
            conversion_rate FLOAT64,
            revenue FLOAT64,
            p_value FLOAT64,
            created_at TIMESTAMP
        )
    """).result()

    print("Done. Tables created.")


if __name__ == "__main__":
    main()
