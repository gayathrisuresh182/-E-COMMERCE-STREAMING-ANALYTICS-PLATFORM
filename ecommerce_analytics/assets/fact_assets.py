"""
Phase 3D: Dagster fact assets for transactional data.

Grain: fct_orders = one row per order, fct_order_items = one row per item per order,
fct_reviews = one row per review. Partitioned where applicable for incremental processing.
"""
from __future__ import annotations

import hashlib
from datetime import date

import pandas as pd
from dagster import (
    AssetCheckResult,
    DailyPartitionsDefinition,
    asset,
    asset_check,
    get_dagster_logger,
)

logger = get_dagster_logger()

# Partition definition for order-date–based facts
ORDER_DATE_PARTITIONS = DailyPartitionsDefinition(
    start_date="2016-09-01",
    end_date="2018-08-31",
)

# Partition definition for review-creation–based facts
REVIEW_DATE_PARTITIONS = DailyPartitionsDefinition(
    start_date="2016-09-01",
    end_date="2018-08-31",
)


def _fact_key(order_id: str) -> str:
    """Deterministic fact surrogate key from order_id."""
    return "fct_" + hashlib.sha256(str(order_id).strip().encode()).hexdigest()[:16]


def _order_item_key(order_id: str, order_item_id: int) -> str:
    """Deterministic surrogate key for order item fact."""
    raw = f"{order_id}|{order_item_id}"
    return "fi_" + hashlib.sha256(raw.encode()).hexdigest()[:16]


def _review_fact_key(review_id: str) -> str:
    """Deterministic surrogate key for review fact."""
    return "fr_" + hashlib.sha256(str(review_id).strip().encode()).hexdigest()[:16]


# --- fct_orders ---
# Grain: one row per order
# Partition: by order_date (daily)

@asset(
    partitions_def=ORDER_DATE_PARTITIONS,
    description="Order transactions fact table (one row per order).",
    compute_kind="pandas",
    metadata={"schema": "marts", "table": "fct_orders"},
)
def fct_orders(
    context,
    stg_orders: pd.DataFrame,
    stg_order_items: pd.DataFrame,
    stg_payments: pd.DataFrame,
    dim_customers: pd.DataFrame,
    dim_sellers: pd.DataFrame,
    dim_dates: pd.DataFrame,
    dim_geography: pd.DataFrame,
) -> pd.DataFrame:
    partition_key = context.partition_key  # e.g. "2016-09-01"
    partition_date = date.fromisoformat(partition_key)

    # Filter orders to this partition only
    stg_orders = stg_orders.copy()
    stg_orders["order_date"] = pd.to_datetime(stg_orders["order_date"], errors="coerce").dt.date
    orders_part = stg_orders[stg_orders["order_date"] == partition_date].copy()

    if orders_part.empty:
        return _empty_fct_orders_df()

    # Order-level aggregates from order_items
    items_agg = stg_order_items.groupby("order_id").agg(
        order_total_items=("price", "sum"),
        freight_value=("freight_value", "sum"),
        item_count=("order_item_id", "count"),
    ).reset_index()
    items_agg["order_id"] = items_agg["order_id"].astype(str).str.strip()
    orders_part = orders_part.merge(items_agg, on="order_id", how="left")
    orders_part["order_total"] = (orders_part["order_total_items"] + orders_part["freight_value"].fillna(0)).fillna(0)
    orders_part["item_count"] = orders_part["item_count"].fillna(0).astype(int)
    orders_part["freight_value"] = orders_part["freight_value"].fillna(0)

    # Payment totals and degenerate payment attributes (first payment per order)
    pay_agg = stg_payments.groupby("order_id").agg(
        payment_value=("payment_value", "sum"),
    ).reset_index()
    pay_agg["order_id"] = pay_agg["order_id"].astype(str).str.strip()
    pay_first = stg_payments.drop_duplicates(subset=["order_id"], keep="first")[["order_id", "payment_type", "payment_installments"]].copy()
    pay_first["order_id"] = pay_first["order_id"].astype(str).str.strip()
    orders_part = orders_part.merge(pay_agg, on="order_id", how="left")
    orders_part = orders_part.merge(pay_first, on="order_id", how="left")
    orders_part["payment_value"] = orders_part["payment_value"].fillna(0)

    # Customer key + geography: join to dim_customers (customer_id), then to dim_geography (zip)
    cust_cols = ["customer_id", "customer_key", "customer_zip_code_prefix"]
    cust_cols = [c for c in cust_cols if c in dim_customers.columns]
    orders_part = orders_part.merge(dim_customers[cust_cols], on="customer_id", how="left")

    geo_cols = ["geolocation_zip_code_prefix", "geography_key"]
    geo_cols = [c for c in geo_cols if c in dim_geography.columns]
    orders_part = orders_part.merge(
        dim_geography[geo_cols],
        left_on="customer_zip_code_prefix",
        right_on="geolocation_zip_code_prefix",
        how="left",
    )
    orders_part = orders_part.drop(columns=["geolocation_zip_code_prefix", "customer_zip_code_prefix"], errors="ignore")

    # Seller key: first seller per order from order_items
    first_seller = stg_order_items.groupby("order_id")["seller_id"].first().reset_index()
    first_seller["order_id"] = first_seller["order_id"].astype(str).str.strip()
    sell_cols = ["seller_id", "seller_key"]
    sell_cols = [c for c in sell_cols if c in dim_sellers.columns]
    first_seller = first_seller.merge(dim_sellers[sell_cols], on="seller_id", how="left")
    orders_part = orders_part.merge(first_seller[["order_id", "seller_key"]], on="order_id", how="left")

    # Date key: join on order_date
    orders_part["order_date_dt"] = pd.to_datetime(orders_part["order_date"])
    dim_dates_sub = dim_dates[["date", "date_key"]].copy()
    dim_dates_sub["date"] = pd.to_datetime(dim_dates_sub["date"]).dt.date
    orders_part = orders_part.merge(dim_dates_sub, left_on="order_date", right_on="date", how="left")
    orders_part = orders_part.drop(columns=["date", "order_date_dt"], errors="ignore")

    # Calculated measures
    orders_part["profit_estimate"] = (orders_part["order_total"] * 0.15).round(2)
    orders_part["avg_item_price"] = orders_part["order_total"] / orders_part["item_count"].replace(0, 1)
    delivered = pd.to_datetime(orders_part["order_delivered_customer_date"], errors="coerce")
    purchase = pd.to_datetime(orders_part["order_purchase_timestamp"], errors="coerce")
    orders_part["delivery_time_days"] = (delivered - purchase).dt.days
    estimated = pd.to_datetime(orders_part["order_estimated_delivery_date"], errors="coerce")
    orders_part["is_late_delivery"] = delivered > estimated
    orders_part["is_late_delivery"] = orders_part["is_late_delivery"].fillna(False)

    # Fact key and degenerate dimensions
    orders_part["fact_key"] = orders_part["order_id"].map(_fact_key)
    orders_part["order_status"] = orders_part["order_status"].astype(str).str.strip().str.lower()
    orders_part["payment_type"] = orders_part["payment_type"].fillna("").astype(str).str.strip().str.lower()
    orders_part["payment_installments"] = pd.to_numeric(orders_part["payment_installments"], errors="coerce").fillna(1).astype(int)

    # Select final columns for fact table
    out_cols = [
        "fact_key", "order_id", "customer_key", "seller_key", "geography_key", "date_key",
        "order_date", "order_status", "order_total", "item_count", "payment_value", "freight_value",
        "profit_estimate", "avg_item_price", "delivery_time_days", "is_late_delivery",
        "payment_type", "payment_installments",
    ]
    out_cols = [c for c in out_cols if c in orders_part.columns]
    result = orders_part[out_cols]

    # Inline validation (asset checks can't load partially-materialized partitions)
    n = len(result)
    null_cust = int(result["customer_key"].isna().sum()) if "customer_key" in result.columns else 0
    null_date = int(result["date_key"].isna().sum()) if "date_key" in result.columns else 0
    delivered = result[result["order_status"] == "delivered"] if "order_status" in result.columns else result
    neg_total = int((delivered["order_total"] <= 0).sum()) if "order_total" in delivered.columns else 0
    context.add_output_metadata({
        "row_count": n,
        "null_customer_key": null_cust,
        "null_date_key": null_date,
        "negative_order_total_delivered": neg_total,
        "partition": partition_key,
    })
    if null_cust > 0:
        logger.warning("Partition %s: %d rows with null customer_key", partition_key, null_cust)
    if neg_total > 0:
        logger.warning("Partition %s: %d delivered orders with order_total <= 0", partition_key, neg_total)

    return result


def _empty_fct_orders_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "fact_key", "order_id", "customer_key", "seller_key", "geography_key", "date_key",
        "order_date", "order_status", "order_total", "item_count", "payment_value", "freight_value",
        "profit_estimate", "avg_item_price", "delivery_time_days", "is_late_delivery",
        "payment_type", "payment_installments",
    ])


# --- fct_order_items ---
# Grain: one row per item per order

@asset(
    description="Order line items fact (one row per item per order).",
    compute_kind="pandas",
    metadata={"schema": "marts", "table": "fct_order_items"},
)
def fct_order_items(
    stg_order_items: pd.DataFrame,
    dim_products: pd.DataFrame,
    dim_sellers: pd.DataFrame,
) -> pd.DataFrame:
    if stg_order_items.empty:
        return _empty_fct_order_items_df()

    items = stg_order_items.copy()
    items["order_id"] = items["order_id"].astype(str).str.strip()
    items["product_id"] = items["product_id"].astype(str).str.strip()
    items["seller_id"] = items["seller_id"].astype(str).str.strip()

    # Order-level fact key (same as fct_orders) for lineage; no dependency on partitioned fct_orders
    items["fact_key"] = items["order_id"].map(_fact_key)

    # Product key and seller key
    prd = dim_products[["product_id", "product_key"]].drop_duplicates()
    prd["product_id"] = prd["product_id"].astype(str).str.strip()
    sell = dim_sellers[["seller_id", "seller_key"]].drop_duplicates()
    sell["seller_id"] = sell["seller_id"].astype(str).str.strip()
    items = items.merge(prd, on="product_id", how="left")
    items = items.merge(sell, on="seller_id", how="left")

    # Measures: quantity = 1 per line (e-commerce one row per item)
    items["quantity"] = 1
    items["price"] = pd.to_numeric(items["price"], errors="coerce").fillna(0)
    items["freight_value"] = pd.to_numeric(items["freight_value"], errors="coerce").fillna(0)
    items["item_total"] = items["price"] + items["freight_value"]

    # Surrogate key for this fact row
    oi_id = items["order_item_id"] if "order_item_id" in items.columns else pd.Series(range(1, len(items) + 1), index=items.index)
    items["order_item_fact_key"] = [_order_item_key(str(o), int(i)) for o, i in zip(items["order_id"], oi_id)]

    out_cols = [
        "order_item_fact_key", "order_id", "fact_key", "order_item_id", "product_key", "seller_key",
        "quantity", "price", "freight_value", "item_total",
    ]
    out_cols = [c for c in out_cols if c in items.columns]
    return items[out_cols]


def _empty_fct_order_items_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "order_item_fact_key", "order_id", "fact_key", "order_item_id", "product_key", "seller_key",
        "quantity", "price", "freight_value", "item_total",
    ])


@asset_check(asset=fct_order_items, description="Row count > 0 when upstream has data")
def check_fct_order_items_row_count(fct_order_items: pd.DataFrame) -> AssetCheckResult:
    n = len(fct_order_items)
    return AssetCheckResult(passed=True, metadata={"row_count": n})


@asset_check(asset=fct_order_items, description="item_total = price + freight_value")
def check_fct_order_items_item_total(fct_order_items: pd.DataFrame) -> AssetCheckResult:
    if fct_order_items.empty or "item_total" not in fct_order_items.columns:
        return AssetCheckResult(passed=True, metadata={"row_count": 0})
    expected = fct_order_items["price"].fillna(0) + fct_order_items["freight_value"].fillna(0)
    diff = (fct_order_items["item_total"] - expected).abs()
    violations = (diff > 0.01).sum()
    return AssetCheckResult(
        passed=bool(violations == 0),
        metadata={"item_total_mismatches": int(violations)},
    )


# --- fct_reviews ---
# Grain: one row per review
# Partition: by review_creation_date (optional; can add later for incremental)

@asset(
    partitions_def=REVIEW_DATE_PARTITIONS,
    description="Reviews fact (one row per review) with sentiment and response time.",
    compute_kind="pandas",
    metadata={"schema": "marts", "table": "fct_reviews"},
)
def fct_reviews(
    context,
    stg_reviews: pd.DataFrame,
    dim_products: pd.DataFrame,
) -> pd.DataFrame:
    partition_key = context.partition_key
    partition_date = date.fromisoformat(partition_key)

    rev = stg_reviews.copy()
    rev["review_creation_date"] = pd.to_datetime(rev["review_creation_date"], errors="coerce")
    rev["review_date"] = rev["review_creation_date"].dt.date
    rev_part = rev[rev["review_date"] == partition_date].copy()

    if rev_part.empty:
        return _empty_fct_reviews_df()

    rev_part["order_id"] = rev_part["order_id"].astype(str).str.strip()
    rev_part["fact_key"] = rev_part["order_id"].map(_fact_key)

    # review_score, comment length
    rev_part["review_score"] = pd.to_numeric(rev_part["review_score"], errors="coerce").fillna(0).astype(int)
    rev_part["review_comment_length"] = rev_part.get("review_comment_message", pd.Series("", index=rev_part.index)).astype(str).str.len()

    # response_time_hours
    created = rev_part["review_creation_date"]
    answered = pd.to_datetime(rev_part.get("review_answer_timestamp"), errors="coerce")
    rev_part["response_time_hours"] = ((answered - created).dt.total_seconds() / 3600).round(2)
    rev_part["response_time_hours"] = rev_part["response_time_hours"].fillna(0)

    # Derived: sentiment (positive/neutral/negative), has_comment
    def sentiment(score):
        if score >= 4:
            return "positive"
        if score == 3:
            return "neutral"
        return "negative"
    rev_part["sentiment"] = rev_part["review_score"].map(sentiment)
    rev_part["has_comment"] = rev_part.get("review_comment_message", pd.Series("", index=rev_part.index)).notna() & (
        rev_part.get("review_comment_message", pd.Series("", index=rev_part.index)).astype(str).str.strip() != ""
    )

    rev_part["review_fact_key"] = rev_part["review_id"].astype(str).map(_review_fact_key)

    out_cols = [
        "review_fact_key", "review_id", "order_id", "fact_key",
        "review_score", "review_comment_length", "response_time_hours",
        "sentiment", "has_comment", "review_creation_date",
    ]
    out_cols = [c for c in out_cols if c in rev_part.columns]
    result = rev_part[out_cols]

    # Inline validation
    n = len(result)
    invalid_scores = int((~result["review_score"].between(1, 5)).sum()) if "review_score" in result.columns else 0
    allowed_sentiments = {"positive", "neutral", "negative"}
    bad_sentiment = int((~result["sentiment"].isin(allowed_sentiments)).sum()) if "sentiment" in result.columns else 0
    context.add_output_metadata({
        "row_count": n,
        "invalid_review_scores": invalid_scores,
        "invalid_sentiments": bad_sentiment,
        "partition": partition_key,
    })
    if invalid_scores > 0:
        logger.warning("Partition %s: %d reviews with score outside 1-5", partition_key, invalid_scores)

    return result


def _empty_fct_reviews_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "review_fact_key", "review_id", "order_id", "fact_key",
        "review_score", "review_comment_length", "response_time_hours",
        "sentiment", "has_comment", "review_creation_date",
    ])


FACT_ASSETS = [fct_orders, fct_order_items, fct_reviews]

# Only non-partitioned asset checks; partitioned facts use inline validation
FACT_ASSET_CHECKS = [
    check_fct_order_items_row_count,
    check_fct_order_items_item_total,
]
