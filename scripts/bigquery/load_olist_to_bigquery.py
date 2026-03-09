#!/usr/bin/env python3
"""
Load Olist e-commerce data into BigQuery for Looker Studio dashboards.

Populates: marts.fct_orders, marts.fct_daily_metrics, marts.fct_experiment_results,
          staging.stg_orders (per PHASE_5A / docs/BIGQUERY_ARCHITECTURE.md)

Usage:
  python scripts/load_olist_to_bigquery.py [--project ecommerce-analytics-prod]

Requires: GOOGLE_APPLICATION_CREDENTIALS, raw/olist/*.csv
Optional: output/analysis/experiment_results.csv for fct_experiment_results
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pandas as pd

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")
    load_dotenv(Path(__file__).resolve().parent.parent.parent / "docs" / ".env")
except ImportError:
    pass

try:
    from google.cloud import bigquery
except ImportError:
    print("pip install google-cloud-bigquery pandas")
    sys.exit(1)

RAW_DIR = Path(__file__).resolve().parent.parent.parent / "raw" / "olist"
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "output"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project", default=os.environ.get("GOOGLE_CLOUD_PROJECT"))
    ap.add_argument("--limit", type=int, default=None, help="Limit rows (for testing)")
    args = ap.parse_args()

    project = args.project
    if not project:
        print("Set GOOGLE_CLOUD_PROJECT or pass --project")
        sys.exit(1)
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_APPLICATION_CREDENTIALS")
        sys.exit(1)

    # Load Olist CSVs
    orders_path = RAW_DIR / "olist_orders_dataset.csv"
    items_path = RAW_DIR / "olist_order_items_dataset.csv"
    customers_path = RAW_DIR / "olist_customers_dataset.csv"

    if not orders_path.exists():
        print(f"Missing {orders_path}. Run download script first.")
        sys.exit(1)

    print("Loading Olist data...")
    orders = pd.read_csv(orders_path)
    items = pd.read_csv(items_path)
    customers = pd.read_csv(customers_path)

    if args.limit:
        orders = orders.head(args.limit)

    # Order total = sum(price + freight_value) per order; item count per order
    items["item_total"] = items["price"] + items["freight_value"]
    order_totals = items.groupby("order_id").agg(
        order_total=("item_total", "sum"),
        item_count=("order_item_id", "count"),
    ).reset_index()
    order_totals.columns = ["order_id", "order_total", "item_count"]

    # Join orders + totals + customer_state
    orders = orders.merge(order_totals[["order_id", "order_total", "item_count"]], on="order_id", how="left")
    orders = orders.merge(
        customers[["customer_id", "customer_state"]],
        on="customer_id",
        how="left",
    )
    orders["order_total"] = orders["order_total"].fillna(0)
    orders["item_count"] = orders["item_count"].fillna(0).astype(int)

    # Build fct_orders (marts schema)
    fct = orders[
        [
            "order_id",
            "customer_id",
            "order_status",
            "customer_state",
            "order_purchase_timestamp",
            "order_total",
        ]
    ].copy()
    fct["order_purchase_timestamp"] = pd.to_datetime(fct["order_purchase_timestamp"])
    fct["customer_state"] = fct["customer_state"].fillna("").astype(str)
    fct["order_status"] = fct["order_status"].fillna("").astype(str)
    # Drop rows with null partition key (BigQuery can drop them silently)
    fct = fct.dropna(subset=["order_purchase_timestamp"])
    if len(fct) == 0:
        print("WARNING: No rows with valid order_purchase_timestamp")
        return

    # Load via CSV (reliable for partitioned tables; load_table_from_dataframe can fail)
    import tempfile
    client = bigquery.Client(project=project)
    table_id = f"{project}.marts.fct_orders"
    fct["order_total"] = fct["order_total"].fillna(0).round(2)
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w", encoding="utf-8", newline="") as f:
        fct.to_csv(f, index=False, date_format="%Y-%m-%d %H:%M:%S", float_format="%.2f")
        csv_path = f.name
    try:
        # Use non-partitioned table (CSV load to partitioned tables can fail; ~99K rows is fine unpartitioned)
        client.query(f"DROP TABLE IF EXISTS `{table_id}`").result()
        client.query(f"""
            CREATE TABLE `{table_id}` (
                order_id STRING NOT NULL,
                customer_id STRING NOT NULL,
                order_status STRING,
                customer_state STRING,
                order_purchase_timestamp TIMESTAMP,
                order_total FLOAT64
            )
            OPTIONS (description='Order fact - one row per order')
        """).result()
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            skip_leading_rows=1,
            autodetect=True,
        )
        with open(csv_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
        job.result()
        if job.errors:
            print(f"Load errors: {job.errors}")
    finally:
        os.unlink(csv_path)
    final_rows = list(client.query(f"SELECT COUNT(*) AS n FROM `{table_id}`").result())
    n = final_rows[0].n if final_rows else 0
    print(f"Loaded {n:,} rows into {table_id}")

    # fct_daily_metrics: aggregate from orders (per PHASE_5A)
    orders["order_date"] = pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce").dt.date
    orders["delivery_time_days"] = (
        pd.to_datetime(orders["order_delivered_customer_date"], errors="coerce")
        - pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce")
    ).dt.total_seconds() / 86400
    orders["is_late"] = (
        pd.to_datetime(orders["order_delivered_customer_date"], errors="coerce")
        > pd.to_datetime(orders["order_estimated_delivery_date"], errors="coerce")
    )
    daily = orders.groupby("order_date").agg(
        total_orders=("order_id", "count"),
        total_gmv=("order_total", "sum"),
        total_items=("item_count", "sum"),
        unique_customers=("customer_id", "nunique"),
        orders_delivered=("order_status", lambda s: (s.str.lower() == "delivered").sum()),
        orders_canceled=("order_status", lambda s: (s.str.lower() == "canceled").sum()),
        avg_delivery_time_days=("delivery_time_days", "mean"),
        late_count=("is_late", "sum"),
    ).reset_index()
    daily["metric_date"] = daily["order_date"]
    daily["avg_order_value"] = (daily["total_gmv"] / daily["total_orders"].replace(0, 1)).round(2)
    daily["late_delivery_rate"] = (daily["late_count"] / daily["total_orders"].replace(0, 1) * 100).round(2)
    daily["created_at"] = pd.Timestamp.now()
    dm_cols = ["metric_date", "total_orders", "total_gmv", "total_items", "avg_order_value",
               "unique_customers", "orders_delivered", "orders_canceled",
               "avg_delivery_time_days", "late_delivery_rate", "created_at"]
    dm = daily[dm_cols].copy()
    dm["avg_delivery_time_days"] = dm["avg_delivery_time_days"].round(2)
    dm = dm.dropna(subset=["metric_date"])
    dm["created_at"] = pd.to_datetime(dm["created_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    dm_table = f"{project}.marts.fct_daily_metrics"
    # Use CSV load (reliable for partitioned tables, same as fct_orders)
    dm_csv = tempfile.mktemp(suffix=".csv")
    dm.to_csv(dm_csv, index=False, header=False, date_format="%Y-%m-%d")
    try:
        client.query(f"DROP TABLE IF EXISTS `{dm_table}`").result()
        client.query(f"""
            CREATE TABLE `{dm_table}` (
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
            OPTIONS (description='Daily aggregated metrics')
        """).result()
        with open(dm_csv, "rb") as f:
            job_dm = client.load_table_from_file(
                f, dm_table,
                bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.CSV,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    skip_leading_rows=0,
                    autodetect=False,
                    schema=[
                        bigquery.SchemaField("metric_date", "DATE"),
                        bigquery.SchemaField("total_orders", "INT64"),
                        bigquery.SchemaField("total_gmv", "FLOAT64"),
                        bigquery.SchemaField("total_items", "INT64"),
                        bigquery.SchemaField("avg_order_value", "FLOAT64"),
                        bigquery.SchemaField("unique_customers", "INT64"),
                        bigquery.SchemaField("orders_delivered", "INT64"),
                        bigquery.SchemaField("orders_canceled", "INT64"),
                        bigquery.SchemaField("avg_delivery_time_days", "FLOAT64"),
                        bigquery.SchemaField("late_delivery_rate", "FLOAT64"),
                        bigquery.SchemaField("created_at", "TIMESTAMP"),
                    ],
                ),
            )
        job_dm.result()
        dm_count = list(client.query(f"SELECT COUNT(*) AS n FROM `{dm_table}`").result())
        dm_n = dm_count[0].n if dm_count else 0
        print(f"Loaded {dm_n:,} rows into {dm_table}")
    finally:
        os.unlink(dm_csv)

    # fct_experiment_results: from output/analysis/experiment_results.csv if present
    exp_path = OUTPUT_DIR / "analysis" / "experiment_results.csv"
    if exp_path.exists():
        exp_df = pd.read_csv(exp_path)
        exp_df = exp_df.rename(columns={
            "Experiment_ID": "experiment_id",
            "Experiment": "experiment_name",
            "Variant": "variant",
            "Users": "unique_users",
            "Conversions": "converting_users",
            "Conv_Rate": "conversion_rate",
            "Revenue": "total_revenue",
            "AOV": "avg_order_value",
        })
        exp_df["revenue_per_user"] = (exp_df["total_revenue"] / exp_df["unique_users"].replace(0, 1)).round(2)
        exp_df["total_orders"] = exp_df["converting_users"]  # approximate
        exp_df["created_at"] = pd.Timestamp.now()
        exp_cols = ["experiment_id", "experiment_name", "variant", "unique_users", "total_orders",
                    "converting_users", "conversion_rate", "total_revenue", "avg_order_value",
                    "revenue_per_user", "created_at"]
        exp_df = exp_df[[c for c in exp_cols if c in exp_df.columns]]
        exp_table = f"{project}.marts.fct_experiment_results"
        job_exp = client.load_table_from_dataframe(
            exp_df,
            exp_table,
            bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
        )
        job_exp.result()
        print(f"Loaded {len(exp_df):,} rows into {exp_table}")
    else:
        print(f"Skipped fct_experiment_results (missing {exp_path})")

    # experiment_summary: p_value, recommendation, revenue impact (PHASE_5C A/B dashboard)
    summary_path = OUTPUT_DIR / "analysis" / "experiment_summary.csv"
    if summary_path.exists():
        sum_df = pd.read_csv(summary_path)
        sum_df = sum_df.rename(columns={
            "Experiment_ID": "experiment_id",
            "Experiment": "experiment_name",
            "P_Value": "p_value",
            "Significant": "significant",
            "Recommendation": "recommendation",
            "Revenue_Impact_Monthly": "revenue_impact_monthly",
        })
        sum_df["created_at"] = pd.Timestamp.now()
        sum_cols = ["experiment_id", "experiment_name", "p_value", "significant", "recommendation", "revenue_impact_monthly", "created_at"]
        sum_df = sum_df[[c for c in sum_cols if c in sum_df.columns]]
        sum_table = f"{project}.marts.experiment_summary"
        job_sum = client.load_table_from_dataframe(
            sum_df,
            sum_table,
            bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
        )
        job_sum.result()
        print(f"Loaded {len(sum_df):,} rows into {sum_table}")
    else:
        print(f"Skipped experiment_summary (missing {summary_path})")

    # Load staging (Olist orders has order_approved_at, order_delivered_*, order_estimated_delivery_date)
    stg_cols = [
        "order_id", "customer_id", "order_status", "order_purchase_timestamp",
        "order_approved_at", "order_delivered_carrier_date",
        "order_delivered_customer_date", "order_estimated_delivery_date",
        "order_total",
    ]
    stg = orders[[c for c in stg_cols if c in orders.columns]].copy()
    for c in stg_cols:
        if c not in stg.columns:
            stg[c] = None
    stg = stg[stg_cols]
    for c in ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]:
        stg[c] = pd.to_datetime(stg[c], errors="coerce")
    stg["created_at"] = pd.Timestamp.now()
    stg_table = f"{project}.staging.stg_orders"
    job2 = client.load_table_from_dataframe(
        stg,
        stg_table,
        bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
    )
    job2.result()
    print(f"Loaded {len(stg):,} rows into {stg_table}")
    print("Done. Refresh Looker Studio to see data.")


if __name__ == "__main__":
    main()
