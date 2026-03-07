"""
BigQuery I/O manager configuration for Lambda architecture.

Defines how Dagster assets map to BigQuery tables:
  - Batch assets → WRITE_TRUNCATE per partition (daily overwrite)
  - Stream assets → observational only (consumers write directly)
  - Unified views → created by SQL DDL, not managed by Dagster

In local dev, FilesystemIOManager is used. In production, swap to
dagster-bigquery's BigQueryIOManager with this configuration.

Usage in Definitions (production):
    from dagster_bigquery_pandas import BigQueryPandasIOManager

    definitions = Definitions(
        resources={
            "io_manager": BigQueryPandasIOManager(
                project="your-gcp-project",
                dataset="marts",
                timeout=30,
            ),
        },
    )
"""
from __future__ import annotations

# Table mapping: asset_key → BigQuery {dataset}.{table}
# Used by I/O managers and for documentation.
BIGQUERY_TABLE_MAP: dict[str, dict[str, str]] = {
    # Batch layer (Dagster writes via WRITE_TRUNCATE per partition)
    "fct_orders": {
        "dataset": "marts",
        "table": "fct_orders_batch",
        "partition_field": "order_date",
        "partition_type": "DAY",
        "clustering_fields": ["order_status", "payment_type"],
        "write_disposition": "WRITE_TRUNCATE",
    },
    "fct_order_items": {
        "dataset": "marts",
        "table": "fct_order_items",
        "clustering_fields": ["product_key", "seller_key"],
        "write_disposition": "WRITE_TRUNCATE",
    },
    "fct_reviews": {
        "dataset": "marts",
        "table": "fct_reviews",
        "partition_field": "review_creation_date",
        "partition_type": "DAY",
        "clustering_fields": ["sentiment", "review_score"],
        "write_disposition": "WRITE_TRUNCATE",
    },
    "fct_daily_metrics": {
        "dataset": "marts",
        "table": "fct_daily_metrics",
        "partition_field": "metric_date",
        "partition_type": "DAY",
        "write_disposition": "WRITE_TRUNCATE",
    },
    "fct_experiment_results": {
        "dataset": "marts",
        "table": "fct_experiment_results",
        "write_disposition": "WRITE_TRUNCATE",
    },
    "fct_product_performance": {
        "dataset": "marts",
        "table": "fct_product_performance",
        "write_disposition": "WRITE_TRUNCATE",
    },
    "fct_seller_performance": {
        "dataset": "marts",
        "table": "fct_seller_performance",
        "write_disposition": "WRITE_TRUNCATE",
    },

    # Dimensions (full refresh, WRITE_TRUNCATE)
    "dim_customers": {
        "dataset": "marts",
        "table": "dim_customers",
        "clustering_fields": ["customer_state"],
        "write_disposition": "WRITE_TRUNCATE",
    },
    "dim_products": {
        "dataset": "marts",
        "table": "dim_products",
        "clustering_fields": ["product_category_name"],
        "write_disposition": "WRITE_TRUNCATE",
    },
    "dim_sellers": {
        "dataset": "marts",
        "table": "dim_sellers",
        "clustering_fields": ["seller_state"],
        "write_disposition": "WRITE_TRUNCATE",
    },

    # Stream layer (consumers write directly; Dagster observes via sensors)
    "realtime_orders": {
        "dataset": "realtime",
        "table": "fct_orders_stream",
        "partition_field": "processing_timestamp",
        "partition_type": "DAY",
        "clustering_fields": ["order_status", "event_type"],
        "write_disposition": "WRITE_APPEND",
        "managed_by": "kafka_consumer",
    },
    "realtime_metrics_5min": {
        "dataset": "realtime",
        "table": "realtime_metrics",
        "partition_field": "window_start",
        "partition_type": "DAY",
        "clustering_fields": ["metric_name"],
        "write_disposition": "WRITE_APPEND",
        "managed_by": "kafka_consumer",
    },
    "realtime_experiment_results": {
        "dataset": "realtime",
        "table": "experiments_realtime",
        "clustering_fields": ["experiment_id"],
        "write_disposition": "WRITE_APPEND",
        "managed_by": "kafka_consumer",
    },
}


# Streaming insert configuration for Kafka consumers
STREAMING_CONFIG = {
    "buffer_size": 100,
    "flush_interval_seconds": 5,
    "max_retries": 3,
    "retry_backoff_seconds": 2,
    "use_storage_write_api": True,
    "api_method": "STORAGE_WRITE_API",
}


# Cost guardrails
COST_CONTROLS = {
    "max_bytes_billed_per_query": 10 * 1024**3,
    "partition_expiration_days": {
        "batch": 730,
        "stream": 7,
        "metrics": 30,
        "alerts": 90,
    },
    "require_partition_filter": True,
}
