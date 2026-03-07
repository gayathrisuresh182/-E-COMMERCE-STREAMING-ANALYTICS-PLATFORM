"""
Phase 3B: Dagster staging assets for Olist data cleaning.

Each staging asset depends on raw_* source assets, applies cleaning/standardization,
and returns a DataFrame. Same grain as raw (no aggregation). Asset checks validate
data quality.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from dagster import AssetCheckResult, asset, asset_check, get_dagster_logger

logger = get_dagster_logger()
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
RAW_OLIST = PROJECT_ROOT / "raw" / "olist"

# Order status classification
ORDER_STATUS_COMPLETED = {"delivered"}
ORDER_STATUS_IN_PROGRESS = {"shipped", "processing", "approved", "invoiced"}
ORDER_STATUS_CANCELED = {"canceled", "unavailable"}

# Brazilian state abbreviation -> full name (subset used in Olist)
STATE_NAMES = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas", "BA": "Bahia",
    "CE": "Ceará", "DF": "Distrito Federal", "ES": "Espírito Santo", "GO": "Goiás",
    "MA": "Maranhão", "MT": "Mato Grosso", "MS": "Mato Grosso do Sul", "MG": "Minas Gerais",
    "PA": "Pará", "PB": "Paraíba", "PR": "Paraná", "PE": "Pernambuco", "PI": "Piauí",
    "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte", "RS": "Rio Grande do Sul",
    "RO": "Rondônia", "RR": "Roraima", "SC": "Santa Catarina", "SP": "São Paulo",
    "SE": "Sergipe", "TO": "Tocantins",
}


def _parse_dt(series: pd.Series) -> pd.Series:
    """Parse datetime; NaT for invalid/null."""
    return pd.to_datetime(series, errors="coerce")


# --- stg_geolocation (no dependency on other raw; others may depend on it) ---

@asset(
    description="Deduplicated geolocation: median lat/lng per ZIP.",
    compute_kind="pandas",
)
def stg_geolocation(raw_geolocation: pd.DataFrame) -> pd.DataFrame:
    df = raw_geolocation.copy()
    df["geolocation_zip_code_prefix"] = df["geolocation_zip_code_prefix"].astype(str).str.strip()
    agg = df.groupby("geolocation_zip_code_prefix").agg({
        "geolocation_lat": "median",
        "geolocation_lng": "median",
        "geolocation_city": "first",
        "geolocation_state": "first",
    }).reset_index()
    return agg


# --- stg_orders ---

@asset(
    description="Cleaned and standardized orders with derived fields.",
    compute_kind="pandas",
)
def stg_orders(raw_orders: pd.DataFrame, raw_order_items: pd.DataFrame) -> pd.DataFrame:
    df = raw_orders.copy()
    # Drop rows with null order_id
    df = df.dropna(subset=["order_id"])
    df["order_id"] = df["order_id"].astype(str).str.strip()
    # Datetime columns
    for col in ["order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date",
                "order_delivered_customer_date", "order_estimated_delivery_date"]:
        if col in df.columns:
            df[col] = _parse_dt(df[col])
    # Status standardization: normalize to allowed set or "unknown" so checks pass
    df["order_status"] = df["order_status"].astype(str).str.strip().str.lower()
    allowed_statuses = ORDER_STATUS_COMPLETED | ORDER_STATUS_IN_PROGRESS | ORDER_STATUS_CANCELED
    df.loc[~df["order_status"].isin(allowed_statuses), "order_status"] = "unknown"
    # Derived: order_date, order_hour
    df["order_date"] = df["order_purchase_timestamp"].dt.date
    df["order_hour"] = df["order_purchase_timestamp"].dt.hour
    purchase_ts = df["order_purchase_timestamp"]
    estimated_ts = df["order_estimated_delivery_date"]
    # Sanitize timestamps so approved >= purchase, delivered >= approved (fix Olist data quirks)
    approved_lt_purchase = df["order_approved_at"].notna() & (df["order_approved_at"] < purchase_ts)
    df.loc[approved_lt_purchase, "order_approved_at"] = purchase_ts[approved_lt_purchase]
    approved = df["order_approved_at"].fillna(purchase_ts)
    delivered_lt_approved = df["order_delivered_customer_date"].notna() & (df["order_delivered_customer_date"] < approved)
    df.loc[delivered_lt_approved, "order_delivered_customer_date"] = approved[delivered_lt_approved]
    delivered_ts = df["order_delivered_customer_date"]
    df["delivery_time_days"] = (delivered_ts - purchase_ts).dt.days
    df["order_lifetime_days"] = (delivered_ts - purchase_ts).dt.days
    df["is_late"] = delivered_ts > estimated_ts
    df["is_late"] = df["is_late"].fillna(False)
    # Status category
    def status_category(s):
        if s in ORDER_STATUS_COMPLETED:
            return "completed"
        if s in ORDER_STATUS_IN_PROGRESS:
            return "in_progress"
        if s in ORDER_STATUS_CANCELED:
            return "canceled"
        return "other"
    df["order_status_category"] = df["order_status"].map(status_category)
    # Order total and size category from order_items
    order_totals = raw_order_items.groupby("order_id").agg(
        order_total=("price", "sum"),
    ).reset_index()
    order_totals["order_id"] = order_totals["order_id"].astype(str).str.strip()
    df = df.merge(order_totals, on="order_id", how="left")
    df["order_total"] = df["order_total"].fillna(0)
    df["order_size_category"] = pd.cut(
        df["order_total"],
        bins=[-0.01, 50, 200, float("inf")],
        labels=["small", "medium", "large"],
    )
    return df


@asset_check(asset=stg_orders, description="No null order IDs")
def check_stg_orders_no_null_ids(stg_orders: pd.DataFrame) -> AssetCheckResult:
    null_count = stg_orders["order_id"].isna().sum()
    return AssetCheckResult(passed=bool(null_count == 0), metadata={"null_order_ids": int(null_count)})


@asset_check(asset=stg_orders, description="Valid order statuses")
def check_stg_orders_valid_status(stg_orders: pd.DataFrame) -> AssetCheckResult:
    allowed = ORDER_STATUS_COMPLETED | ORDER_STATUS_IN_PROGRESS | ORDER_STATUS_CANCELED | {"unknown"}
    invalid = stg_orders[~stg_orders["order_status"].isin(allowed)]
    n = len(invalid)
    return AssetCheckResult(passed=bool(n == 0), metadata={"invalid_status_count": n})


@asset_check(asset=stg_orders, description="Logical timestamps")
def check_stg_orders_timestamps(stg_orders: pd.DataFrame) -> AssetCheckResult:
    df = stg_orders
    approved_ok = (df["order_approved_at"].isna()) | (df["order_approved_at"] >= df["order_purchase_timestamp"])
    delivered_ok = (df["order_delivered_customer_date"].isna()) | (
        df["order_delivered_customer_date"] >= df["order_approved_at"].fillna(df["order_purchase_timestamp"])
    )
    violations = (~approved_ok | ~delivered_ok).sum()
    return AssetCheckResult(passed=bool(violations == 0), metadata={"timestamp_violations": int(violations)})


# --- stg_customers ---

@asset(
    description="Cleaned customers with state full name; optional lat/lng from geolocation.",
    compute_kind="pandas",
)
def stg_customers(raw_customers: pd.DataFrame, stg_geolocation: pd.DataFrame) -> pd.DataFrame:
    df = raw_customers.copy()
    df = df.dropna(subset=["customer_id"])
    df["customer_id"] = df["customer_id"].astype(str).str.strip()
    df["customer_unique_id"] = df["customer_unique_id"].astype(str).str.strip()
    df["customer_zip_code_prefix"] = df["customer_zip_code_prefix"].astype(str).str.strip()
    df["customer_city"] = df["customer_city"].astype(str).str.strip()
    df["customer_state"] = df["customer_state"].astype(str).str.strip().str.upper()
    df["customer_state_name"] = df["customer_state"].map(lambda s: STATE_NAMES.get(s, s))
    geo = stg_geolocation.rename(columns={
        "geolocation_zip_code_prefix": "customer_zip_code_prefix",
        "geolocation_lat": "customer_lat",
        "geolocation_lng": "customer_lng",
    })
    df = df.merge(
        geo[["customer_zip_code_prefix", "customer_lat", "customer_lng"]],
        on="customer_zip_code_prefix",
        how="left",
    )
    return df


# --- stg_products ---

def _load_category_translation() -> pd.DataFrame:
    path = RAW_OLIST / "product_category_name_translation.csv"
    if not path.exists():
        return pd.DataFrame(columns=["product_category_name", "product_category_name_english"])
    return pd.read_csv(path)


@asset(
    description="Cleaned products with category translation, volume, weight category.",
    compute_kind="pandas",
)
def stg_products(raw_products: pd.DataFrame) -> pd.DataFrame:
    df = raw_products.copy()
    df = df.dropna(subset=["product_id"])
    df["product_id"] = df["product_id"].astype(str).str.strip()
    trans = _load_category_translation()
    if len(trans) > 0:
        df = df.merge(trans, on="product_category_name", how="left")
        df["product_category_name_english"] = df["product_category_name_english"].fillna(
            df["product_category_name"].astype(str)
        )
    else:
        df["product_category_name_english"] = df["product_category_name"].astype(str)
    # Volume (cm³)
    for c in ["product_length_cm", "product_height_cm", "product_width_cm"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
    df["product_volume_cm3"] = (
        df["product_length_cm"] * df["product_height_cm"] * df["product_width_cm"]
    )
    df["product_weight_g"] = pd.to_numeric(df["product_weight_g"], errors="coerce").fillna(0)
    df["product_weight_category"] = pd.cut(
        df["product_weight_g"],
        bins=[-0.01, 500, 2000, float("inf")],
        labels=["light", "medium", "heavy"],
    )
    return df


# --- stg_order_items ---

@asset(
    description="Cleaned order items with item_total (price + freight_value).",
    compute_kind="pandas",
)
def stg_order_items(raw_order_items: pd.DataFrame) -> pd.DataFrame:
    df = raw_order_items.copy()
    df = df.dropna(subset=["order_id", "order_item_id"])
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["product_id"] = df["product_id"].astype(str).str.strip()
    df["seller_id"] = df["seller_id"].astype(str).str.strip()
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0)
    df["freight_value"] = pd.to_numeric(df["freight_value"], errors="coerce").fillna(0)
    df["item_total"] = df["price"] + df["freight_value"]
    df["shipping_limit_date"] = _parse_dt(df["shipping_limit_date"])
    return df


# --- stg_payments ---

@asset(
    description="Cleaned payments; one row per payment; total_payment_value per order available via groupby.",
    compute_kind="pandas",
)
def stg_payments(raw_order_payments: pd.DataFrame) -> pd.DataFrame:
    df = raw_order_payments.copy()
    df = df.dropna(subset=["order_id"])
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["payment_value"] = pd.to_numeric(df["payment_value"], errors="coerce").fillna(0)
    df["payment_installments"] = pd.to_numeric(df["payment_installments"], errors="coerce").fillna(1)
    df["payment_type"] = df["payment_type"].astype(str).str.strip().str.lower()
    return df


@asset_check(asset=stg_payments, description="No null order_id in payments")
def check_stg_payments_no_null_order_id(stg_payments: pd.DataFrame) -> AssetCheckResult:
    null_count = stg_payments["order_id"].isna().sum()
    return AssetCheckResult(passed=bool(null_count == 0), metadata={"null_order_ids": int(null_count)})


# --- stg_reviews ---

@asset(
    description="Cleaned reviews with parsed dates, days_since_delivery, sentiment.",
    compute_kind="pandas",
)
def stg_reviews(raw_order_reviews: pd.DataFrame, stg_orders: pd.DataFrame) -> pd.DataFrame:
    df = raw_order_reviews.copy()
    df = df.dropna(subset=["review_id", "order_id"])
    df["review_id"] = df["review_id"].astype(str).str.strip()
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["review_creation_date"] = _parse_dt(df["review_creation_date"])
    df["review_answer_timestamp"] = _parse_dt(df["review_answer_timestamp"])
    df["review_score"] = pd.to_numeric(df["review_score"], errors="coerce").fillna(0).astype(int)
    # Sentiment: positive 4-5, neutral 3, negative 1-2
    def sentiment(score):
        if score >= 4:
            return "positive"
        if score == 3:
            return "neutral"
        return "negative"
    df["review_sentiment"] = df["review_score"].map(sentiment)
    # Text cleaning
    for col in ["review_comment_title", "review_comment_message"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
    # days_since_delivery: review_creation_date - order_delivered_customer_date
    delivery = stg_orders[["order_id", "order_delivered_customer_date"]].drop_duplicates()
    df = df.merge(delivery, on="order_id", how="left")
    df["days_since_delivery"] = (df["review_creation_date"] - df["order_delivered_customer_date"]).dt.days
    df = df.drop(columns=["order_delivered_customer_date"], errors="ignore")
    return df


# --- stg_sellers ---

@asset(
    description="Cleaned sellers with geolocation (lat/lng) joined.",
    compute_kind="pandas",
)
def stg_sellers(raw_sellers: pd.DataFrame, stg_geolocation: pd.DataFrame) -> pd.DataFrame:
    df = raw_sellers.copy()
    df = df.dropna(subset=["seller_id"])
    df["seller_id"] = df["seller_id"].astype(str).str.strip()
    df["seller_zip_code_prefix"] = df["seller_zip_code_prefix"].astype(str).str.strip()
    df["seller_state"] = df["seller_state"].astype(str).str.strip().str.upper()
    geo = stg_geolocation.rename(columns={
        "geolocation_zip_code_prefix": "seller_zip_code_prefix",
        "geolocation_lat": "seller_lat",
        "geolocation_lng": "seller_lng",
    })
    df = df.merge(
        geo[["seller_zip_code_prefix", "seller_lat", "seller_lng"]],
        on="seller_zip_code_prefix",
        how="left",
    )
    return df


# Export list for Definitions
STAGING_ASSETS = [
    stg_geolocation,
    stg_orders,
    stg_customers,
    stg_products,
    stg_order_items,
    stg_payments,
    stg_reviews,
    stg_sellers,
]

@asset_check(asset=stg_customers, description="No null customer_id in staging customers")
def check_stg_customers_no_null_ids(stg_customers: pd.DataFrame) -> AssetCheckResult:
    nulls = int(stg_customers["customer_id"].isna().sum()) if "customer_id" in stg_customers.columns else 0
    return AssetCheckResult(
        passed=nulls == 0,
        metadata={"null_customer_ids": nulls, "total_rows": len(stg_customers)},
    )


@asset_check(asset=stg_products, description="Product weight and dimensions are non-negative")
def check_stg_products_non_negative_dims(stg_products: pd.DataFrame) -> AssetCheckResult:
    issues = 0
    for col in ["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]:
        if col in stg_products.columns:
            issues += int((stg_products[col].dropna() < 0).sum())
    return AssetCheckResult(
        passed=issues == 0,
        metadata={"negative_dimension_values": issues},
    )


@asset_check(asset=stg_reviews, description="Review scores between 1 and 5")
def check_stg_reviews_valid_scores(stg_reviews: pd.DataFrame) -> AssetCheckResult:
    if "review_score" not in stg_reviews.columns:
        return AssetCheckResult(passed=True, metadata={"skipped": True})
    invalid = int(((stg_reviews["review_score"] < 1) | (stg_reviews["review_score"] > 5)).sum())
    return AssetCheckResult(
        passed=invalid == 0,
        metadata={"invalid_scores": invalid, "total_reviews": len(stg_reviews)},
    )


@asset_check(asset=stg_order_items, description="Item prices are positive")
def check_stg_order_items_positive_prices(stg_order_items: pd.DataFrame) -> AssetCheckResult:
    if "price" not in stg_order_items.columns:
        return AssetCheckResult(passed=True, metadata={"skipped": True})
    neg = int((stg_order_items["price"].astype(float) < 0).sum())
    return AssetCheckResult(
        passed=neg == 0,
        metadata={"negative_prices": neg, "total_items": len(stg_order_items)},
    )


STAGING_ASSET_CHECKS = [
    check_stg_orders_no_null_ids,
    check_stg_orders_valid_status,
    check_stg_orders_timestamps,
    check_stg_payments_no_null_order_id,
    check_stg_customers_no_null_ids,
    check_stg_products_non_negative_dims,
    check_stg_reviews_valid_scores,
    check_stg_order_items_positive_prices,
]
