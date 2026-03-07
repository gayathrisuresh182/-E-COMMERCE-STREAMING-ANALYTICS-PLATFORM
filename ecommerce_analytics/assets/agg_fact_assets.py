"""
Phase 3E: Aggregated fact assets built from detail facts.

Multi-grain summaries: daily metrics, experiment results, product performance,
seller performance. Demonstrates aggregation from detail facts and Dagster
partition mapping (daily → daily identity).

Dependency strategy:
  - fct_daily_metrics: same DailyPartitionsDefinition as fct_orders → identity
    partition mapping (partition "2016-09-01" reads fct_orders "2016-09-01").
  - Non-partitioned aggregates (fct_product_performance, fct_seller_performance,
    fct_experiment_results): depend on non-partitioned upstream assets
    (fct_order_items, stg_orders, stg_reviews) because Dagster's
    FilesystemIOManager cannot load all partitions of a partitioned asset
    into a non-partitioned downstream.  In BigQuery production the equivalent
    queries would read directly from the partitioned fact tables.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
from dagster import (
    AssetCheckResult,
    asset,
    asset_check,
    get_dagster_logger,
)

from ecommerce_analytics.assets.fact_assets import ORDER_DATE_PARTITIONS

logger = get_dagster_logger()

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Source: experiment assignment data (Phase 2D output)
# ---------------------------------------------------------------------------

@asset(
    description="Experiment assignment data from Phase 2D A/B test simulation.",
    compute_kind="pandas",
)
def raw_experiment_assignments(context) -> pd.DataFrame:
    path = PROJECT_ROOT / "output" / "experiment_assignments.csv"
    if not path.exists():
        raise FileNotFoundError(f"Experiment assignments not found: {path}")
    df = pd.read_csv(path)
    context.add_output_metadata({
        "rows": len(df),
        "columns": len(df.columns),
        "experiments": int(df["experiment_id"].nunique()),
        "source": str(path),
    })
    return df


# ---------------------------------------------------------------------------
# fct_daily_metrics – grain: one row per date
# Partition: daily (identity-mapped to fct_orders partitions)
# ---------------------------------------------------------------------------

@asset(
    partitions_def=ORDER_DATE_PARTITIONS,
    description="Daily aggregated business metrics (from fct_orders). One row per date.",
    compute_kind="pandas",
    metadata={"schema": "marts", "table": "fct_daily_metrics"},
)
def fct_daily_metrics(context, fct_orders: pd.DataFrame) -> pd.DataFrame:
    partition_key = context.partition_key
    df = fct_orders

    if df.empty:
        return _empty_daily_metrics(partition_key)

    total_orders = len(df)
    total_gmv = float(df["order_total"].sum())
    total_items = int(df["item_count"].sum())
    avg_order_value = float(df["order_total"].mean())

    unique_customers = int(df["customer_key"].nunique()) if "customer_key" in df.columns else 0
    orders_delivered = int((df["order_status"] == "delivered").sum()) if "order_status" in df.columns else 0
    orders_canceled = int((df["order_status"] == "canceled").sum()) if "order_status" in df.columns else 0

    delivery_times = df["delivery_time_days"].dropna() if "delivery_time_days" in df.columns else pd.Series(dtype=float)
    avg_delivery_time_days = round(float(delivery_times.mean()), 2) if len(delivery_times) > 0 else None

    late_count = int(df["is_late_delivery"].sum()) if "is_late_delivery" in df.columns else 0
    late_delivery_rate = round(late_count / total_orders * 100, 2) if total_orders > 0 else 0.0

    total_revenue = float(df["payment_value"].sum()) if "payment_value" in df.columns else 0.0
    total_freight = float(df["freight_value"].sum()) if "freight_value" in df.columns else 0.0
    avg_items_per_order = round(float(df["item_count"].mean()), 2) if "item_count" in df.columns else 0.0

    result = pd.DataFrame([{
        "date": partition_key,
        "total_orders": total_orders,
        "total_gmv": round(total_gmv, 2),
        "total_items": total_items,
        "avg_order_value": round(avg_order_value, 2),
        "unique_customers": unique_customers,
        "orders_delivered": orders_delivered,
        "orders_canceled": orders_canceled,
        "avg_delivery_time_days": avg_delivery_time_days,
        "late_delivery_rate": late_delivery_rate,
        "total_revenue": round(total_revenue, 2),
        "total_freight": round(total_freight, 2),
        "avg_items_per_order": avg_items_per_order,
    }])

    context.add_output_metadata({
        "row_count": 1,
        "total_orders": total_orders,
        "total_gmv": round(total_gmv, 2),
        "partition": partition_key,
    })
    return result


def _empty_daily_metrics(partition_key: str) -> pd.DataFrame:
    return pd.DataFrame([{
        "date": partition_key,
        "total_orders": 0, "total_gmv": 0.0, "total_items": 0,
        "avg_order_value": 0.0, "unique_customers": 0,
        "orders_delivered": 0, "orders_canceled": 0,
        "avg_delivery_time_days": None, "late_delivery_rate": 0.0,
        "total_revenue": 0.0, "total_freight": 0.0, "avg_items_per_order": 0.0,
    }])


# ---------------------------------------------------------------------------
# fct_experiment_results – grain: one row per experiment × variant
# Not partitioned (experiments span multiple dates)
# ---------------------------------------------------------------------------

@asset(
    description="A/B test results aggregated by experiment and variant.",
    compute_kind="pandas",
    metadata={"schema": "marts", "table": "fct_experiment_results"},
)
def fct_experiment_results(
    raw_experiment_assignments: pd.DataFrame,
    stg_orders: pd.DataFrame,
) -> pd.DataFrame:
    assignments = raw_experiment_assignments.copy()
    orders = stg_orders.copy()

    assignments["customer_id"] = assignments["customer_id"].astype(str).str.strip()
    orders["customer_id"] = orders["customer_id"].astype(str).str.strip()

    order_cols = ["order_id", "customer_id", "order_total", "order_status"]
    order_cols = [c for c in order_cols if c in orders.columns]
    merged = assignments.merge(orders[order_cols], on="customer_id", how="left")

    results = []
    for (exp_id, exp_name, variant), grp in merged.groupby(
        ["experiment_id", "experiment_name", "variant"]
    ):
        unique_users = int(grp["customer_id"].nunique())
        has_order = grp["order_id"].notna()
        total_orders = int(has_order.sum())
        converting_users = int(grp.loc[has_order, "customer_id"].nunique())
        conversion_rate = round(converting_users / unique_users * 100, 4) if unique_users else 0.0
        total_revenue = round(float(grp["order_total"].fillna(0).sum()), 2)
        avg_order_value = round(float(grp.loc[has_order, "order_total"].mean()), 2) if total_orders else 0.0
        revenue_per_user = round(total_revenue / unique_users, 2) if unique_users else 0.0

        results.append({
            "experiment_id": exp_id,
            "experiment_name": exp_name,
            "variant": variant,
            "unique_users": unique_users,
            "total_orders": total_orders,
            "converting_users": converting_users,
            "conversion_rate": conversion_rate,
            "total_revenue": total_revenue,
            "avg_order_value": avg_order_value,
            "revenue_per_user": revenue_per_user,
        })

    if not results:
        return _empty_experiment_results()
    return pd.DataFrame(results)


def _empty_experiment_results() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "experiment_id", "experiment_name", "variant", "unique_users",
        "total_orders", "converting_users", "conversion_rate",
        "total_revenue", "avg_order_value", "revenue_per_user",
    ])


# ---------------------------------------------------------------------------
# fct_product_performance – grain: one row per product
# Not partitioned (full product catalog snapshot)
# ---------------------------------------------------------------------------

@asset(
    description="Product performance summary (units sold, revenue, reviews).",
    compute_kind="pandas",
    metadata={"schema": "marts", "table": "fct_product_performance"},
)
def fct_product_performance(
    fct_order_items: pd.DataFrame,
    stg_reviews: pd.DataFrame,
    stg_order_items: pd.DataFrame,
    dim_products: pd.DataFrame,
) -> pd.DataFrame:
    items = fct_order_items.copy()

    prod_agg = items.groupby("product_key").agg(
        units_sold=("quantity", "sum"),
        total_revenue=("price", "sum"),
        total_freight=("freight_value", "sum"),
        avg_price=("price", "mean"),
        order_count=("order_id", "nunique"),
    ).reset_index()

    # Reviews per product: stg_order_items maps order_id→product_id
    item_products = stg_order_items[["order_id", "product_id"]].drop_duplicates()
    reviews = stg_reviews[["order_id", "review_score"]].drop_duplicates(subset=["order_id"])
    product_reviews = item_products.merge(reviews, on="order_id", how="inner")
    review_agg = product_reviews.groupby("product_id").agg(
        num_reviews=("review_score", "count"),
        avg_review_score=("review_score", "mean"),
    ).reset_index()

    prd_map = dim_products[["product_id", "product_key",
                            "product_category_name_english",
                            "product_category_group"]].drop_duplicates()

    review_agg = review_agg.merge(prd_map[["product_id", "product_key"]], on="product_id", how="left")

    result = prod_agg.merge(
        review_agg[["product_key", "num_reviews", "avg_review_score"]],
        on="product_key", how="left",
    )
    result = result.merge(
        prd_map[["product_key", "product_category_name_english",
                 "product_category_group"]].drop_duplicates(),
        on="product_key", how="left",
    )

    result["num_reviews"] = result["num_reviews"].fillna(0).astype(int)
    result["avg_review_score"] = result["avg_review_score"].fillna(0).round(2)
    result["review_rate"] = (result["num_reviews"] / result["units_sold"].replace(0, 1) * 100).round(2)
    result["total_revenue"] = result["total_revenue"].round(2)
    result["avg_price"] = result["avg_price"].round(2)

    out_cols = [
        "product_key", "product_category_name_english", "product_category_group",
        "units_sold", "total_revenue", "total_freight", "avg_price", "order_count",
        "num_reviews", "avg_review_score", "review_rate",
    ]
    return result[[c for c in out_cols if c in result.columns]]


# ---------------------------------------------------------------------------
# fct_seller_performance – grain: one row per seller
# Not partitioned (full seller roster snapshot)
# ---------------------------------------------------------------------------

@asset(
    description="Seller performance summary (orders, revenue, delivery, reviews).",
    compute_kind="pandas",
    metadata={"schema": "marts", "table": "fct_seller_performance"},
)
def fct_seller_performance(
    fct_order_items: pd.DataFrame,
    stg_orders: pd.DataFrame,
    stg_reviews: pd.DataFrame,
    dim_sellers: pd.DataFrame,
) -> pd.DataFrame:
    items = fct_order_items.copy()
    orders = stg_orders.copy()

    seller_agg = items.groupby("seller_key").agg(
        total_orders_fulfilled=("order_id", "nunique"),
        total_revenue=("price", "sum"),
        total_freight=("freight_value", "sum"),
        total_items_sold=("quantity", "sum"),
    ).reset_index()

    # Delivery metrics: join fct_order_items → stg_orders via order_id
    seller_orders = items[["order_id", "seller_key"]].drop_duplicates()
    order_delivery = orders[["order_id", "delivery_time_days", "is_late", "order_status"]].copy()
    order_delivery["order_id"] = order_delivery["order_id"].astype(str).str.strip()
    seller_delivery = seller_orders.merge(order_delivery, on="order_id", how="left")

    delivery_agg = seller_delivery.groupby("seller_key").agg(
        avg_delivery_time_days=("delivery_time_days", "mean"),
        late_deliveries=("is_late", "sum"),
        total_deliveries=("is_late", "count"),
    ).reset_index()
    delivery_agg["late_delivery_rate"] = (
        delivery_agg["late_deliveries"]
        / delivery_agg["total_deliveries"].replace(0, 1) * 100
    ).round(2)

    # Reviews per seller via order_id
    reviews = stg_reviews[["order_id", "review_score"]].drop_duplicates(subset=["order_id"])
    reviews["order_id"] = reviews["order_id"].astype(str).str.strip()
    seller_reviews = seller_orders.merge(reviews, on="order_id", how="inner")
    review_agg = seller_reviews.groupby("seller_key").agg(
        avg_review_score=("review_score", "mean"),
        num_reviews=("review_score", "count"),
    ).reset_index()

    result = seller_agg.merge(
        delivery_agg[["seller_key", "avg_delivery_time_days", "late_delivery_rate"]],
        on="seller_key", how="left",
    )
    result = result.merge(review_agg, on="seller_key", how="left")

    seller_attrs = dim_sellers[
        ["seller_key", "seller_city", "seller_state", "seller_region", "seller_tier"]
    ].drop_duplicates()
    result = result.merge(seller_attrs, on="seller_key", how="left")

    result["total_revenue"] = result["total_revenue"].round(2)
    result["avg_delivery_time_days"] = result["avg_delivery_time_days"].round(2)
    result["avg_review_score"] = result["avg_review_score"].fillna(0).round(2)
    result["num_reviews"] = result["num_reviews"].fillna(0).astype(int)
    result["late_delivery_rate"] = result["late_delivery_rate"].fillna(0)

    out_cols = [
        "seller_key", "seller_city", "seller_state", "seller_region", "seller_tier",
        "total_orders_fulfilled", "total_revenue", "total_freight", "total_items_sold",
        "avg_delivery_time_days", "late_delivery_rate",
        "avg_review_score", "num_reviews",
    ]
    return result[[c for c in out_cols if c in result.columns]]


# ---------------------------------------------------------------------------
# Asset checks (non-partitioned aggregates only; fct_daily_metrics uses
# inline validation via output metadata)
# ---------------------------------------------------------------------------

@asset_check(asset=fct_product_performance, description="At least one product row")
def check_fct_product_performance_row_count(fct_product_performance: pd.DataFrame) -> AssetCheckResult:
    n = len(fct_product_performance)
    return AssetCheckResult(passed=n > 0, metadata={"row_count": n})


@asset_check(asset=fct_product_performance, description="No negative revenue")
def check_fct_product_performance_no_neg_revenue(fct_product_performance: pd.DataFrame) -> AssetCheckResult:
    if fct_product_performance.empty:
        return AssetCheckResult(passed=True, metadata={"row_count": 0})
    neg = int((fct_product_performance["total_revenue"] < 0).sum())
    return AssetCheckResult(passed=neg == 0, metadata={"negative_revenue_products": neg})


@asset_check(asset=fct_seller_performance, description="At least one seller row")
def check_fct_seller_performance_row_count(fct_seller_performance: pd.DataFrame) -> AssetCheckResult:
    n = len(fct_seller_performance)
    return AssetCheckResult(passed=n > 0, metadata={"row_count": n})


@asset_check(asset=fct_seller_performance, description="Late delivery rate between 0-100%")
def check_fct_seller_performance_late_rate(fct_seller_performance: pd.DataFrame) -> AssetCheckResult:
    if fct_seller_performance.empty:
        return AssetCheckResult(passed=True, metadata={"row_count": 0})
    violations = int((
        (fct_seller_performance["late_delivery_rate"] < 0)
        | (fct_seller_performance["late_delivery_rate"] > 100)
    ).sum())
    return AssetCheckResult(passed=violations == 0, metadata={"rate_violations": violations})


@asset_check(asset=fct_experiment_results, description="At least one experiment result row")
def check_fct_experiment_results_row_count(fct_experiment_results: pd.DataFrame) -> AssetCheckResult:
    n = len(fct_experiment_results)
    return AssetCheckResult(passed=n > 0, metadata={"row_count": n})


# ---------------------------------------------------------------------------
# Export lists for Definitions
# ---------------------------------------------------------------------------

AGG_FACT_ASSETS = [
    raw_experiment_assignments,
    fct_daily_metrics,
    fct_experiment_results,
    fct_product_performance,
    fct_seller_performance,
]

AGG_FACT_ASSET_CHECKS = [
    check_fct_product_performance_row_count,
    check_fct_product_performance_no_neg_revenue,
    check_fct_seller_performance_row_count,
    check_fct_seller_performance_late_rate,
    check_fct_experiment_results_row_count,
]
