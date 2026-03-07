"""
Phase 3C: Dagster dimension assets for star schema.

Grain: one row per entity (customer, product, seller, geography, date).
Surrogate keys: hash-based for reproducibility. Type 1 (overwrite) for simplicity.
"""
from __future__ import annotations

import hashlib
from datetime import date, timedelta

import pandas as pd
from dagster import AssetCheckResult, asset, asset_check, get_dagster_logger

logger = get_dagster_logger()

# Brazilian state -> region (IBGE regions)
STATE_TO_REGION = {
    "AC": "North", "AM": "North", "AP": "North", "PA": "North", "RO": "North", "RR": "North", "TO": "North",
    "AL": "Northeast", "BA": "Northeast", "CE": "Northeast", "MA": "Northeast", "PB": "Northeast",
    "PE": "Northeast", "PI": "Northeast", "RN": "Northeast", "SE": "Northeast",
    "ES": "Southeast", "MG": "Southeast", "RJ": "Southeast", "SP": "Southeast",
    "PR": "South", "RS": "South", "SC": "South",
    "DF": "Center-West", "GO": "Center-West", "MS": "Center-West", "MT": "Center-West",
}

# High-level category group (subset for portfolio)
CATEGORY_TO_GROUP = {
    "health_beauty": "lifestyle",
    "computers_accessories": "electronics",
    "auto": "auto",
    "bed_bath_table": "home",
    "furniture_decor": "home",
    "sports_leisure": "lifestyle",
    "perfumery": "lifestyle",
    "housewares": "home",
    "telephony": "electronics",
    "watches_gifts": "lifestyle",
    "food_drink": "food",
    "baby": "lifestyle",
    "stationery": "office",
    "toys": "lifestyle",
}


def _surrogate_key(series: pd.Series, prefix: str = "") -> pd.Series:
    """Deterministic surrogate key from string column (hash)."""
    def _hash_id(x):
        s = str(x).strip()
        return prefix + hashlib.sha256(s.encode()).hexdigest()[:16]
    return series.map(_hash_id)


# --- dim_dates (no dependencies) ---

@asset(
    description="Date dimension: 2016-09-01 to 2018-08-31 for Olist analysis.",
    compute_kind="pandas",
)
def dim_dates() -> pd.DataFrame:
    start = date(2016, 9, 1)
    end = date(2018, 8, 31)
    dates = pd.date_range(start=start, end=end, freq="D")
    df = pd.DataFrame({"date": dates.date})
    df["date_key"] = _surrogate_key(df["date"].astype(str), "dt_")
    df["year"] = df["date"].apply(lambda d: d.year)
    df["quarter"] = df["date"].apply(lambda d: (d.month - 1) // 3 + 1)
    df["month"] = df["date"].apply(lambda d: d.month)
    df["month_name"] = df["date"].apply(lambda d: d.strftime("%B"))
    df["week_of_year"] = df["date"].apply(lambda d: d.isocalendar()[1])
    df["day_of_week"] = df["date"].apply(lambda d: d.weekday() + 1)  # 1=Mon
    df["day_name"] = df["date"].apply(lambda d: d.strftime("%A"))
    df["is_weekend"] = df["day_of_week"].isin([6, 7])
    # Brazilian holidays: simplified set (optional expansion)
    br_holidays = {date(2016, 1, 1), date(2016, 12, 25), date(2017, 1, 1), date(2017, 12, 25), date(2018, 1, 1), date(2018, 12, 25)}
    df["is_holiday"] = df["date"].isin(br_holidays)
    df["fiscal_year"] = df["year"]  # Brazil fiscal = calendar
    df["fiscal_quarter"] = df["quarter"]
    return df


# --- dim_geography ---

@asset(
    description="Geography dimension: one row per ZIP prefix with region.",
    compute_kind="pandas",
)
def dim_geography(stg_geolocation: pd.DataFrame) -> pd.DataFrame:
    df = stg_geolocation.copy()
    df["geography_key"] = _surrogate_key(df["geolocation_zip_code_prefix"], "geo_")
    df["region"] = df["geolocation_state"].map(lambda s: STATE_TO_REGION.get(str(s).upper(), "Other"))
    return df


# --- dim_products ---

@asset(
    description="Product dimension: one row per product with category group and size.",
    compute_kind="pandas",
)
def dim_products(stg_products: pd.DataFrame) -> pd.DataFrame:
    df = stg_products.copy()
    df["product_key"] = _surrogate_key(df["product_id"], "prd_")
    # Size category by volume (fixed bins: small < 5k, medium 5k–50k, large > 50k cm³)
    vol = df["product_volume_cm3"].fillna(0)
    df["product_size_category"] = pd.cut(
        vol,
        bins=[-0.01, 5000, 50000, float("inf")],
        labels=["small", "medium", "large"],
    ).astype(object).fillna("medium")
    df["product_category_group"] = df["product_category_name_english"].map(
        lambda x: CATEGORY_TO_GROUP.get(str(x), "other")
    )
    return df


# --- dim_customers ---

@asset(
    description="Customer dimension: one row per customer with lifetime metrics and tier.",
    compute_kind="pandas",
)
def dim_customers(stg_customers: pd.DataFrame, stg_orders: pd.DataFrame) -> pd.DataFrame:
    # Aggregate orders by customer_id (completed/delivered only for spend metrics)
    orders_completed = stg_orders[stg_orders["order_status"] == "delivered"]
    cust_agg = orders_completed.groupby("customer_id").agg(
        total_orders=("order_id", "count"),
        total_spent=("order_total", "sum"),
    ).reset_index()
    cust_agg["avg_order_value"] = cust_agg["total_spent"] / cust_agg["total_orders"].replace(0, 1)

    df = stg_customers.copy()
    df = df.merge(cust_agg, on="customer_id", how="left")
    df["total_orders"] = df["total_orders"].fillna(0).astype(int)
    df["total_spent"] = df["total_spent"].fillna(0)
    df["avg_order_value"] = df["avg_order_value"].fillna(0)

    # Customer tier: VIP >10, Regular 2-10, New 1
    def tier(n):
        if n > 10:
            return "vip"
        if n >= 2:
            return "regular"
        return "new"
    df["customer_tier"] = df["total_orders"].map(tier)

    # Customer value: High >1000, Medium 100-1000, Low <100
    def value_class(s):
        if s > 1000:
            return "high"
        if s >= 100:
            return "medium"
        return "low"
    df["customer_value"] = df["total_spent"].map(value_class)

    df["customer_key"] = _surrogate_key(df["customer_id"], "cust_")
    df["customer_region"] = df["customer_state"].map(lambda s: STATE_TO_REGION.get(str(s).upper(), "Other"))
    return df


@asset_check(asset=dim_customers, description="One row per customer_id")
def check_dim_customers_unique_customer_id(dim_customers: pd.DataFrame) -> AssetCheckResult:
    dupes = dim_customers["customer_id"].duplicated().sum()
    return AssetCheckResult(passed=bool(dupes == 0), metadata={"duplicate_customer_ids": int(dupes)})


# --- dim_sellers ---

@asset(
    description="Seller dimension: one row per seller with performance metrics.",
    compute_kind="pandas",
)
def dim_sellers(
    stg_sellers: pd.DataFrame,
    stg_order_items: pd.DataFrame,
    stg_orders: pd.DataFrame,
    stg_reviews: pd.DataFrame,
) -> pd.DataFrame:
    # Orders per seller (via order_items)
    seller_orders = stg_order_items.groupby("seller_id").agg(
        total_orders_fulfilled=("order_id", "nunique"),
    ).reset_index()

    # Avg review score: order_items (seller_id, order_id) -> reviews (order_id, review_score)
    order_review = stg_reviews[["order_id", "review_score"]].drop_duplicates()
    seller_order = stg_order_items[["seller_id", "order_id"]].drop_duplicates()
    seller_reviews = seller_order.merge(order_review, on="order_id")
    seller_avg_review = seller_reviews.groupby("seller_id")["review_score"].mean().reset_index()
    seller_avg_review = seller_avg_review.rename(columns={"review_score": "avg_review_score"})

    # Avg delivery time: order_items (seller_id, order_id) -> orders (order_id, delivery_time_days)
    order_delivery = stg_orders[["order_id", "delivery_time_days"]].dropna(subset=["delivery_time_days"])
    seller_delivery = seller_order.merge(order_delivery, on="order_id")
    seller_avg_delivery = seller_delivery.groupby("seller_id")["delivery_time_days"].mean().reset_index()
    seller_avg_delivery = seller_avg_delivery.rename(columns={"delivery_time_days": "avg_delivery_time_days"})

    df = stg_sellers.copy()
    df = df.merge(seller_orders, on="seller_id", how="left")
    df = df.merge(seller_avg_review, on="seller_id", how="left")
    df = df.merge(seller_avg_delivery, on="seller_id", how="left")
    df["total_orders_fulfilled"] = df["total_orders_fulfilled"].fillna(0).astype(int)
    df["avg_review_score"] = df["avg_review_score"].fillna(0)
    df["avg_delivery_time_days"] = df["avg_delivery_time_days"].fillna(0)

    # Seller tier: Top (avg>=4.5), Good (4-4.5), Average (3-4), Poor (<3)
    def seller_tier(score):
        if score >= 4.5:
            return "top"
        if score >= 4:
            return "good"
        if score >= 3:
            return "average"
        return "poor"
    df["seller_tier"] = df["avg_review_score"].map(seller_tier)

    df["seller_key"] = _surrogate_key(df["seller_id"], "sell_")
    df["seller_region"] = df["seller_state"].map(lambda s: STATE_TO_REGION.get(str(s).upper(), "Other"))
    return df


@asset_check(asset=dim_sellers, description="One row per seller_id")
def check_dim_sellers_unique_seller_id(dim_sellers: pd.DataFrame) -> AssetCheckResult:
    dupes = dim_sellers["seller_id"].duplicated().sum()
    return AssetCheckResult(passed=bool(dupes == 0), metadata={"duplicate_seller_ids": int(dupes)})


# Export for Definitions
DIMENSION_ASSETS = [
    dim_dates,
    dim_geography,
    dim_products,
    dim_customers,
    dim_sellers,
]

DIMENSION_ASSET_CHECKS = [
    check_dim_customers_unique_customer_id,
    check_dim_sellers_unique_seller_id,
]
