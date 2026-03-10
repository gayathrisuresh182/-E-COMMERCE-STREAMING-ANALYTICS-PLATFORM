#!/usr/bin/env python3
"""
Create and populate dimension tables in BigQuery marts dataset.

Source tables:
  staging.stg_customers  -> marts.dim_customers  (with order aggregates)
  staging.stg_products   -> marts.dim_products   (with computed fields)
  staging.stg_order_items -> marts.dim_sellers   (with performance metrics)

Usage: python scripts/bigquery/bigquery_create_dimension_tables.py [--project YOUR_PROJECT]
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

    def run(sql, label):
        print(f"  Creating {label}...", end=" ", flush=True)
        client.query(sql).result()
        count = list(client.query(f"SELECT COUNT(*) AS n FROM `{p}.marts.{label}`").result())
        n = dict(count[0])["n"]
        print(f"{n:,} rows")

    # ── dim_customers ────────────────────────────────────────────────────
    run(f"""
    CREATE OR REPLACE TABLE `{p}.marts.dim_customers` AS

    WITH order_agg AS (
        SELECT
            customer_id,
            COUNT(*)                                          AS total_orders,
            COALESCE(SUM(order_total), 0)                     AS total_spent,
            ROUND(SAFE_DIVIDE(SUM(order_total), COUNT(*)), 2) AS avg_order_value,
            MIN(order_purchase_timestamp)                     AS first_order_date,
            MAX(order_purchase_timestamp)                     AS last_order_date
        FROM `{p}.staging.stg_orders`
        WHERE order_status = 'delivered'
        GROUP BY customer_id
    )
    SELECT
        CONCAT('cust_', SUBSTR(TO_HEX(SHA256(CAST(c.customer_id AS BYTES))), 1, 16))
            AS customer_key,
        c.customer_id,
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        CASE
            WHEN c.customer_state IN ('AC','AM','AP','PA','RO','RR','TO') THEN 'North'
            WHEN c.customer_state IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE')
                THEN 'Northeast'
            WHEN c.customer_state IN ('ES','MG','RJ','SP') THEN 'Southeast'
            WHEN c.customer_state IN ('PR','RS','SC') THEN 'South'
            WHEN c.customer_state IN ('DF','GO','MS','MT') THEN 'Center-West'
            ELSE 'Other'
        END AS customer_region,
        COALESCE(o.total_orders, 0)     AS total_orders,
        COALESCE(o.total_spent, 0)      AS total_spent,
        COALESCE(o.avg_order_value, 0)  AS avg_order_value,
        o.first_order_date,
        o.last_order_date,
        CASE
            WHEN COALESCE(o.total_orders, 0) > 10 THEN 'vip'
            WHEN COALESCE(o.total_orders, 0) >= 2  THEN 'regular'
            ELSE 'new'
        END AS customer_tier,
        CASE
            WHEN COALESCE(o.total_spent, 0) > 1000 THEN 'high'
            WHEN COALESCE(o.total_spent, 0) >= 100  THEN 'medium'
            ELSE 'low'
        END AS customer_value,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM `{p}.staging.stg_customers` c
    LEFT JOIN order_agg o ON c.customer_id = o.customer_id
    """, "dim_customers")

    # ── dim_products ─────────────────────────────────────────────────────
    run(f"""
    CREATE OR REPLACE TABLE `{p}.marts.dim_products` AS

    SELECT
        CONCAT('prd_', SUBSTR(TO_HEX(SHA256(CAST(product_id AS BYTES))), 1, 16))
            AS product_key,
        product_id,
        product_category_name,
        COALESCE(product_category_name_english, product_category_name)
            AS product_category_name_english,
        CASE COALESCE(product_category_name_english, product_category_name)
            WHEN 'health_beauty'          THEN 'lifestyle'
            WHEN 'computers_accessories'  THEN 'electronics'
            WHEN 'auto'                   THEN 'auto'
            WHEN 'bed_bath_table'         THEN 'home'
            WHEN 'furniture_decor'        THEN 'home'
            WHEN 'sports_leisure'         THEN 'lifestyle'
            WHEN 'perfumery'              THEN 'lifestyle'
            WHEN 'housewares'             THEN 'home'
            WHEN 'telephony'              THEN 'electronics'
            WHEN 'watches_gifts'          THEN 'lifestyle'
            WHEN 'food_drink'             THEN 'food'
            WHEN 'baby'                   THEN 'lifestyle'
            WHEN 'stationery'             THEN 'office'
            WHEN 'toys'                   THEN 'lifestyle'
            ELSE 'other'
        END AS product_category_group,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        COALESCE(product_length_cm * product_height_cm * product_width_cm, 0)
            AS product_volume_cm3,
        CASE
            WHEN COALESCE(product_length_cm * product_height_cm * product_width_cm, 0)
                < 5000  THEN 'small'
            WHEN COALESCE(product_length_cm * product_height_cm * product_width_cm, 0)
                < 50000 THEN 'medium'
            ELSE 'large'
        END AS product_size_category,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM `{p}.staging.stg_products`
    """, "dim_products")

    # ── dim_sellers ──────────────────────────────────────────────────────
    run(f"""
    CREATE OR REPLACE TABLE `{p}.marts.dim_sellers` AS

    WITH seller_base AS (
        SELECT DISTINCT seller_id
        FROM `{p}.staging.stg_order_items`
    ),
    seller_orders AS (
        SELECT
            oi.seller_id,
            COUNT(DISTINCT oi.order_id) AS total_orders_fulfilled,
            SUM(oi.price)               AS total_revenue
        FROM `{p}.staging.stg_order_items` oi
        GROUP BY oi.seller_id
    ),
    seller_reviews AS (
        SELECT
            oi.seller_id,
            ROUND(AVG(r.review_score), 2) AS avg_review_score
        FROM `{p}.staging.stg_order_items` oi
        JOIN `{p}.staging.stg_reviews` r ON oi.order_id = r.order_id
        GROUP BY oi.seller_id
    ),
    seller_delivery AS (
        SELECT
            oi.seller_id,
            ROUND(AVG(DATE_DIFF(
                DATE(o.order_delivered_customer_date),
                DATE(o.order_purchase_timestamp),
                DAY
            )), 1) AS avg_delivery_time_days
        FROM `{p}.staging.stg_order_items` oi
        JOIN `{p}.staging.stg_orders` o ON oi.order_id = o.order_id
        WHERE o.order_delivered_customer_date IS NOT NULL
        GROUP BY oi.seller_id
    )
    SELECT
        CONCAT('sell_', SUBSTR(TO_HEX(SHA256(CAST(sb.seller_id AS BYTES))), 1, 16))
            AS seller_key,
        sb.seller_id,
        COALESCE(so.total_orders_fulfilled, 0)  AS total_orders_fulfilled,
        COALESCE(so.total_revenue, 0)           AS total_revenue,
        COALESCE(sr.avg_review_score, 0)        AS avg_review_score,
        COALESCE(sd.avg_delivery_time_days, 0)  AS avg_delivery_time_days,
        CASE
            WHEN COALESCE(sr.avg_review_score, 0) >= 4.5 THEN 'top'
            WHEN COALESCE(sr.avg_review_score, 0) >= 4   THEN 'good'
            WHEN COALESCE(sr.avg_review_score, 0) >= 3   THEN 'average'
            ELSE 'poor'
        END AS seller_tier,
        CURRENT_TIMESTAMP() AS _loaded_at
    FROM seller_base sb
    LEFT JOIN seller_orders so ON sb.seller_id = so.seller_id
    LEFT JOIN seller_reviews sr ON sb.seller_id = sr.seller_id
    LEFT JOIN seller_delivery sd ON sb.seller_id = sd.seller_id
    """, "dim_sellers")

    print("\nDone. All dimension tables created.")


if __name__ == "__main__":
    main()
