#!/usr/bin/env python3
"""
Create unified Lambda views that combine batch and stream tables.

Actual tables in BigQuery:
  Batch:    marts.fct_orders           (99K rows, 2016-2018 historical)
  Stream:   realtime.realtime_orders   (500 rows, recent stream events)
  Batch:    marts.fct_daily_metrics    (634 rows, daily KPIs)
  Stream:   realtime.realtime_metrics  (66 rows, 5-min windowed metrics)

Creates:
  marts.fct_orders_unified       — all batch + stream orders, deduplicated
  marts.daily_metrics_unified    — batch daily KPIs + stream windowed metrics

Usage: python scripts/bigquery/bigquery_create_unified_views.py [--project YOUR_PROJECT]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    args = ap.parse_args()
    if not args.project:
        print("Set GOOGLE_CLOUD_PROJECT or pass --project")
        sys.exit(1)

    from google.cloud import bigquery
    client = bigquery.Client(project=args.project)
    p = args.project

    # ── 1. fct_orders_unified ────────────────────────────────────────────
    # Batch columns: order_id, customer_id, order_status, customer_state,
    #                order_purchase_timestamp, order_total
    # Stream columns: event_id, order_id, customer_id, customer_state,
    #                 event_timestamp, order_total, event_type, created_at
    #
    # Plain UNION ALL — batch holds the historical record-of-truth,
    # stream holds live event-level data. Both are queryable through
    # the _source_layer discriminator column.
    fct_unified = f"""
    CREATE OR REPLACE VIEW `{p}.marts.fct_orders_unified` AS

    SELECT
        order_id,
        DATE(order_purchase_timestamp)  AS order_date,
        order_status,
        order_total,
        customer_id,
        customer_state,
        'batch'                         AS _source_layer,
        order_purchase_timestamp        AS _timestamp
    FROM `{p}.marts.fct_orders`

    UNION ALL

    SELECT
        order_id,
        DATE(event_timestamp)                           AS order_date,
        COALESCE(event_type, 'order_placed')            AS order_status,
        order_total,
        customer_id,
        customer_state,
        'stream'                                        AS _source_layer,
        event_timestamp                                 AS _timestamp
    FROM `{p}.realtime.realtime_orders`
    """
    client.query(fct_unified).result()
    print("Created: marts.fct_orders_unified")

    # ── 2. daily_metrics_unified ─────────────────────────────────────────
    # Batch: metric_date, total_orders, total_gmv, total_items, avg_order_value,
    #        unique_customers, orders_delivered, orders_canceled,
    #        avg_delivery_time_days, late_delivery_rate, created_at
    # Stream: window_start, window_end, metric_name, metric_value, _ingested_at
    #         (pivoted from name/value rows into columns)
    daily_unified = f"""
    CREATE OR REPLACE VIEW `{p}.marts.daily_metrics_unified` AS

    SELECT
        metric_date,
        total_orders,
        total_gmv,
        avg_order_value,
        unique_customers,
        late_delivery_rate,
        'batch' AS _source_layer
    FROM `{p}.marts.fct_daily_metrics`

    UNION ALL

    SELECT
        DATE(window_start)  AS metric_date,
        CAST(SUM(CASE WHEN metric_name = 'total_orders'
                      THEN metric_value ELSE 0 END) AS INT64)        AS total_orders,
        SUM(CASE WHEN metric_name = 'total_gmv'
                 THEN metric_value ELSE 0 END)                       AS total_gmv,
        SAFE_DIVIDE(
            SUM(CASE WHEN metric_name = 'avg_order_value'
                     THEN metric_value END),
            NULLIF(COUNT(CASE WHEN metric_name = 'avg_order_value'
                              THEN 1 END), 0))                       AS avg_order_value,
        CAST(SUM(CASE WHEN metric_name = 'unique_sessions'
                      THEN metric_value ELSE 0 END) AS INT64)        AS unique_customers,
        CAST(NULL AS FLOAT64)                                        AS late_delivery_rate,
        'stream' AS _source_layer
    FROM `{p}.realtime.realtime_metrics`
    GROUP BY DATE(window_start)
    """
    client.query(daily_unified).result()
    print("Created: marts.daily_metrics_unified")

    # ── 3. Verify ────────────────────────────────────────────────────────
    rows = list(client.query(f"""
        SELECT _source_layer, COUNT(*) AS cnt,
               ROUND(SUM(order_total), 2) AS gmv
        FROM `{p}.marts.fct_orders_unified`
        GROUP BY _source_layer
    """).result())
    print("\n  fct_orders_unified:")
    total_cnt, total_gmv = 0, 0.0
    for r in rows:
        d = dict(r)
        print(f"    {d['_source_layer']}: {d['cnt']:,} rows, ${d['gmv']:,.2f}")
        total_cnt += d["cnt"]
        total_gmv += d["gmv"]
    print(f"    TOTAL: {total_cnt:,} rows, ${total_gmv:,.2f}")

    rows = list(client.query(f"""
        SELECT _source_layer, COUNT(*) AS cnt,
               ROUND(SUM(total_gmv), 2) AS gmv
        FROM `{p}.marts.daily_metrics_unified`
        GROUP BY _source_layer
    """).result())
    print("\n  daily_metrics_unified:")
    for r in rows:
        d = dict(r)
        print(f"    {d['_source_layer']}: {d['cnt']:,} rows, ${d['gmv']:,.2f}")

    print("\nDone.")


if __name__ == "__main__":
    main()
