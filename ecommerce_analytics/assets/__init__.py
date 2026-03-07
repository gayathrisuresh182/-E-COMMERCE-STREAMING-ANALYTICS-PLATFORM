"""Dagster assets: source → staging → dims → facts → aggs → realtime → stream_ops → unified."""
from ecommerce_analytics.assets.analysis_assets import (
    EXPERIMENT_ANALYSIS_ASSETS,
    all_experiments_summary,
    experiment_exp_001_notebook,
)
from ecommerce_analytics.assets.report_assets import weekly_experiment_report
from ecommerce_analytics.assets.agg_fact_assets import (
    AGG_FACT_ASSETS,
    AGG_FACT_ASSET_CHECKS,
    fct_daily_metrics,
    fct_experiment_results,
    fct_product_performance,
    fct_seller_performance,
    raw_experiment_assignments,
)
from ecommerce_analytics.assets.dimension_assets import (
    DIMENSION_ASSETS,
    DIMENSION_ASSET_CHECKS,
    dim_customers,
    dim_dates,
    dim_geography,
    dim_products,
    dim_sellers,
)
from ecommerce_analytics.assets.fact_assets import (
    FACT_ASSETS,
    FACT_ASSET_CHECKS,
    fct_orders,
    fct_order_items,
    fct_reviews,
)
from ecommerce_analytics.assets.realtime_assets import (
    REALTIME_ASSETS,
    REALTIME_ASSET_CHECKS,
    realtime_experiment_results,
    realtime_metrics_5min,
    realtime_orders,
)
from ecommerce_analytics.assets.source_assets import (
    SOURCE_ASSETS,
    raw_customers,
    raw_geolocation,
    raw_order_items,
    raw_order_payments,
    raw_order_reviews,
    raw_orders,
    raw_products,
    raw_sellers,
)
from ecommerce_analytics.assets.staging_assets import (
    STAGING_ASSETS,
    STAGING_ASSET_CHECKS,
    stg_orders,
)
from ecommerce_analytics.assets.stream_ops_assets import (
    STREAM_OPS_ASSETS,
    STREAM_OPS_ASSET_CHECKS,
    batch_stream_reconciliation,
    consumer_health_metrics,
)
from ecommerce_analytics.assets.unified_assets import (
    UNIFIED_ASSETS,
    UNIFIED_ASSET_CHECKS,
    unified_daily_metrics,
    unified_experiment_results,
    unified_orders,
)
from ecommerce_analytics.assets.verification_asset import phase1_verification_asset

__all__ = [
    "raw_orders",
    "raw_customers",
    "raw_products",
    "raw_order_items",
    "raw_order_payments",
    "raw_order_reviews",
    "raw_sellers",
    "raw_geolocation",
    "raw_experiment_assignments",
    "stg_orders",
    "stg_geolocation",
    "stg_customers",
    "stg_products",
    "stg_order_items",
    "stg_payments",
    "stg_reviews",
    "stg_sellers",
    "dim_dates",
    "dim_geography",
    "dim_products",
    "dim_customers",
    "dim_sellers",
    "fct_orders",
    "fct_order_items",
    "fct_reviews",
    "fct_daily_metrics",
    "fct_experiment_results",
    "fct_product_performance",
    "fct_seller_performance",
    "realtime_orders",
    "realtime_metrics_5min",
    "realtime_experiment_results",
    "consumer_health_metrics",
    "batch_stream_reconciliation",
    "unified_orders",
    "unified_daily_metrics",
    "unified_experiment_results",
    "phase1_verification_asset",
    "core_assets",
    "core_asset_checks",
]

core_assets = [
    *SOURCE_ASSETS,
    *STAGING_ASSETS,
    *DIMENSION_ASSETS,
    *FACT_ASSETS,
    *AGG_FACT_ASSETS,
    *REALTIME_ASSETS,
    *STREAM_OPS_ASSETS,
    *UNIFIED_ASSETS,
    *EXPERIMENT_ANALYSIS_ASSETS,
    all_experiments_summary,
    weekly_experiment_report,
    experiment_exp_001_notebook,
    phase1_verification_asset,
]
core_asset_checks = [
    *STAGING_ASSET_CHECKS,
    *DIMENSION_ASSET_CHECKS,
    *FACT_ASSET_CHECKS,
    *AGG_FACT_ASSET_CHECKS,
    *REALTIME_ASSET_CHECKS,
    *STREAM_OPS_ASSET_CHECKS,
    *UNIFIED_ASSET_CHECKS,
]
