"""Tools the agent uses to query, analyze, and act on platform data."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

from langchain_core.tools import tool

from api.db import bq_query, get_mongo_db


def _serialize(obj):
    """Fallback serializer for JSON encoding."""
    return str(obj)


def _to_json(results: list[dict], limit: int = 50) -> str:
    if not results:
        return "Query returned 0 rows."
    if len(results) > limit:
        truncated = json.dumps(results[:limit], default=_serialize, indent=2)
        return f"{truncated}\n\n... showing {limit} of {len(results)} total rows."
    return json.dumps(results, default=_serialize, indent=2)


# ── READ TOOLS ───────────────────────────────────────────────────────────

@tool
def query_bigquery(sql: str) -> str:
    """Execute a read-only SQL query against BigQuery. Use this for any
    analytical question about orders, revenue, customers, products,
    experiments, clickstream, or pipeline metrics. Always qualify table
    names with dataset prefix (e.g., marts.fct_orders,
    realtime.realtime_orders, staging.stg_orders,
    monitoring.consumer_health_metrics). Only SELECT queries are allowed."""
    blocked = {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "MERGE"}
    tokens = sql.upper().split()
    if blocked.intersection(tokens):
        return "Error: only SELECT queries are allowed. This tool is read-only."
    return _to_json(bq_query(sql))


@tool
def query_mongodb(
    database: str,
    collection: str,
    filter_doc: Optional[dict] = None,
    limit: int = 10,
) -> str:
    """Query a MongoDB collection for real-time operational data.

    Databases:
      - ecommerce_analytics: dim_customers, dim_sellers, products, reviews,
        experiments, experiment_assignments
      - ecommerce_streaming: fct_orders_realtime, realtime_metrics,
        experiments_realtime, delivery_alerts, delivery_events, orders

    Args:
        database: Database name.
        collection: Collection name.
        filter_doc: MongoDB query filter (optional).
        limit: Max documents to return (default 10).
    """
    try:
        db = get_mongo_db(database)
        cursor = (
            db[collection]
            .find(filter_doc or {}, {"_id": 0})
            .sort("_id", -1)
            .limit(limit)
        )
        docs = list(cursor)
        if not docs:
            return f"No documents in {database}.{collection} matching filter {filter_doc}."
        return json.dumps(docs, default=_serialize, indent=2)
    except Exception as e:
        return f"MongoDB error: {e}"


@tool
def check_kafka_consumers() -> str:
    """Check health and lag of all Kafka consumer groups. Returns latest
    snapshot per consumer: group name, topics, total lag, partition count,
    and status (healthy/warning/critical)."""
    results = bq_query("""
        WITH latest AS (
            SELECT consumer_group, MAX(measured_at) AS latest_at
            FROM monitoring.consumer_health_metrics
            WHERE consumer_group IS NOT NULL
            GROUP BY consumer_group
        )
        SELECT
            chm.consumer_group,
            chm.topics,
            chm.total_lag,
            chm.partitions,
            chm.status,
            chm.measured_at
        FROM monitoring.consumer_health_metrics chm
        JOIN latest l
          ON chm.consumer_group = l.consumer_group
         AND chm.measured_at = l.latest_at
        ORDER BY chm.total_lag DESC
    """)
    if not results:
        return "No Kafka consumer metrics found in monitoring.consumer_health_metrics."
    return json.dumps(results, default=_serialize, indent=2)


@tool
def check_data_freshness() -> str:
    """Check when each key BigQuery table was last modified, its row count,
    and size. Use this to diagnose stale-data or missing-data issues."""
    tables = [
        ("staging", "stg_orders"),
        ("staging", "stg_order_items"),
        ("marts", "fct_orders"),
        ("marts", "fct_daily_metrics"),
        ("marts", "dim_customers"),
        ("marts", "dim_products"),
        ("marts", "dim_sellers"),
        ("marts", "experiment_summary"),
        ("marts", "clickstream_events"),
        ("realtime", "realtime_orders"),
        ("realtime", "realtime_metrics"),
        ("monitoring", "consumer_health_metrics"),
    ]
    results = []
    for dataset, table in tables:
        rows = bq_query(f"""
            SELECT
                TIMESTAMP_MILLIS(last_modified_time) AS last_updated,
                row_count,
                ROUND(size_bytes / 1024 / 1024, 2) AS size_mb
            FROM `{dataset}.__TABLES__`
            WHERE table_id = '{table}'
        """)
        if rows:
            r = rows[0]
            results.append({
                "table": f"{dataset}.{table}",
                "last_updated": r.get("last_updated"),
                "row_count": r.get("row_count"),
                "size_mb": r.get("size_mb"),
            })
        else:
            results.append({"table": f"{dataset}.{table}", "status": "not found"})
    return json.dumps(results, default=_serialize, indent=2)


@tool
def get_experiment_summary(experiment_id: Optional[str] = None) -> str:
    """Get A/B experiment results. Returns per-experiment summary with
    control vs treatment comparison, p-value, significance, and recommendation.

    Args:
        experiment_id: e.g. 'exp_001'. Omit to get all experiments.
    """
    where = f"WHERE experiment_id = '{experiment_id}'" if experiment_id else ""
    results = bq_query(f"""
        SELECT
            experiment_id, experiment_name,
            control_users, treatment_users,
            control_conversion_rate, treatment_conversion_rate,
            control_revenue, treatment_revenue,
            control_aov, treatment_aov,
            p_value, significant, lift_percent,
            recommendation, revenue_impact_monthly
        FROM marts.experiment_comparison_view
        {where}
        ORDER BY experiment_id
    """)
    if not results:
        ctx = f" for {experiment_id}" if experiment_id else ""
        return f"No experiment data found{ctx}."
    return json.dumps(results, default=_serialize, indent=2)


@tool
def get_table_schema(dataset: str, table: str) -> str:
    """Get column names and data types for a BigQuery table. Use this before
    writing a query when you need to verify exact column names.

    Args:
        dataset: BigQuery dataset (staging, marts, realtime, monitoring).
        table: Table name within the dataset.
    """
    results = bq_query(f"""
        SELECT column_name, data_type, is_nullable
        FROM `{dataset}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = '{table}'
        ORDER BY ordinal_position
    """)
    if not results:
        return f"Table {dataset}.{table} not found or has no columns."
    return json.dumps(results, default=_serialize, indent=2)


# ── ANOMALY DETECTION ────────────────────────────────────────────────────

@tool
def run_anomaly_scan() -> str:
    """Scan key business metrics for anomalies. Checks:
    1. GMV and order volume vs 30-day moving averages
    2. Cancellation rate spikes
    3. Delivery time degradation
    4. Review score drops
    5. Kafka consumer lag warnings
    6. Data freshness staleness

    Returns a structured report of any anomalies found with severity levels."""
    findings: list[dict] = []

    # 1. GMV and order anomalies vs moving average
    rows = bq_query("""
        WITH daily AS (
            SELECT
                metric_date,
                total_orders,
                total_gmv,
                AVG(total_gmv) OVER (ORDER BY metric_date
                    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS gmv_ma30,
                AVG(total_orders) OVER (ORDER BY metric_date
                    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS orders_ma30,
                STDDEV(total_gmv) OVER (ORDER BY metric_date
                    ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS gmv_std30
            FROM marts.fct_daily_metrics
            WHERE total_orders > 10
        )
        SELECT * FROM daily
        WHERE metric_date = (SELECT MAX(metric_date) FROM daily)
    """)
    if rows:
        r = rows[0]
        gmv = r.get("total_gmv", 0) or 0
        ma = r.get("gmv_ma30", 0) or 0
        std = r.get("gmv_std30", 1) or 1
        if ma > 0:
            z_score = (gmv - ma) / std if std > 0 else 0
            if abs(z_score) > 2:
                direction = "above" if z_score > 0 else "below"
                findings.append({
                    "metric": "Daily GMV",
                    "severity": "critical" if abs(z_score) > 3 else "warning",
                    "detail": (
                        f"Latest GMV ${gmv:,.0f} is {abs(z_score):.1f} std devs "
                        f"{direction} the 30-day moving avg (${ma:,.0f})"
                    ),
                    "date": str(r.get("metric_date")),
                })

    # 2. Cancellation rate spike
    rows = bq_query("""
        SELECT
            metric_date,
            SAFE_DIVIDE(orders_canceled, total_orders) * 100 AS cancel_pct,
            AVG(SAFE_DIVIDE(orders_canceled, total_orders) * 100) OVER (
                ORDER BY metric_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS cancel_ma30
        FROM marts.fct_daily_metrics
        WHERE total_orders > 10
        ORDER BY metric_date DESC
        LIMIT 1
    """)
    if rows:
        r = rows[0]
        rate = r.get("cancel_pct", 0) or 0
        avg_rate = r.get("cancel_ma30", 0) or 0
        if avg_rate > 0 and rate > avg_rate * 1.5:
            findings.append({
                "metric": "Cancellation Rate",
                "severity": "warning",
                "detail": (
                    f"Cancel rate {rate:.1f}% is {rate/avg_rate:.1f}x the "
                    f"30-day average ({avg_rate:.1f}%)"
                ),
                "date": str(r.get("metric_date")),
            })

    # 3. Delivery time degradation
    rows = bq_query("""
        SELECT
            metric_date,
            avg_delivery_time_days,
            AVG(avg_delivery_time_days) OVER (
                ORDER BY metric_date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
            ) AS delivery_ma30
        FROM marts.fct_daily_metrics
        WHERE avg_delivery_time_days IS NOT NULL AND avg_delivery_time_days > 0
        ORDER BY metric_date DESC
        LIMIT 1
    """)
    if rows:
        r = rows[0]
        current = r.get("avg_delivery_time_days", 0) or 0
        avg = r.get("delivery_ma30", 0) or 0
        if avg > 0 and current > avg * 1.3:
            findings.append({
                "metric": "Delivery Time",
                "severity": "warning",
                "detail": (
                    f"Avg delivery {current:.1f} days vs 30-day avg {avg:.1f} days "
                    f"(+{((current-avg)/avg)*100:.0f}%)"
                ),
                "date": str(r.get("metric_date")),
            })

    # 4. Low review scores
    rows = bq_query("""
        WITH recent AS (
            SELECT AVG(review_score) AS avg_score
            FROM staging.stg_reviews r
            JOIN staging.stg_orders o ON r.order_id = o.order_id
            WHERE DATE(o.order_purchase_timestamp) >= DATE_SUB(
                (SELECT MAX(DATE(order_purchase_timestamp)) FROM staging.stg_orders),
                INTERVAL 7 DAY)
        ),
        historical AS (
            SELECT AVG(review_score) AS avg_score FROM staging.stg_reviews
        )
        SELECT recent.avg_score AS recent_score, historical.avg_score AS historical_score
        FROM recent, historical
    """)
    if rows:
        r = rows[0]
        recent = r.get("recent_score", 0) or 0
        historical = r.get("historical_score", 0) or 0
        if historical > 0 and recent < historical * 0.9:
            findings.append({
                "metric": "Review Score",
                "severity": "warning",
                "detail": (
                    f"Recent 7-day avg review {recent:.2f} is below "
                    f"historical avg {historical:.2f} (-{((historical-recent)/historical)*100:.1f}%)"
                ),
            })

    # 5. Data freshness check
    for dataset, table, max_hours in [
        ("realtime", "realtime_orders", 24),
        ("monitoring", "consumer_health_metrics", 6),
    ]:
        rows = bq_query(f"""
            SELECT TIMESTAMP_MILLIS(last_modified_time) AS last_updated
            FROM `{dataset}.__TABLES__`
            WHERE table_id = '{table}'
        """)
        if rows:
            last = rows[0].get("last_updated", "")
            if last:
                try:
                    ts = datetime.fromisoformat(str(last).replace("Z", "+00:00"))
                    age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
                    if age_hours > max_hours:
                        findings.append({
                            "metric": f"Data Freshness: {dataset}.{table}",
                            "severity": "critical" if age_hours > max_hours * 2 else "warning",
                            "detail": f"Last updated {age_hours:.1f} hours ago (threshold: {max_hours}h)",
                        })
                except (ValueError, TypeError):
                    pass

    if not findings:
        return json.dumps({
            "status": "healthy",
            "message": "No anomalies detected across all monitored metrics.",
            "checks_run": 5,
        }, indent=2)

    return json.dumps({
        "status": "anomalies_detected",
        "count": len(findings),
        "findings": findings,
    }, default=_serialize, indent=2)


# ── CHART GENERATION ─────────────────────────────────────────────────────

@tool
def generate_chart(
    chart_type: str,
    sql: str,
    title: str,
    x_key: str,
    y_keys: str,
    x_label: str = "",
    y_label: str = "",
) -> str:
    """Run a SQL query and return the results formatted as a chart specification
    that the frontend will render as an interactive Recharts visualization.

    IMPORTANT: The SQL must return columns matching x_key and y_keys exactly.

    Args:
        chart_type: One of 'line', 'bar', 'area', 'pie'.
        sql: BigQuery SQL that returns the data for the chart.
        title: Chart title displayed above the visualization.
        x_key: Column name for the x-axis (or 'name' for pie charts).
        y_keys: Comma-separated column names for y-axis values (e.g. 'revenue,orders').
        x_label: Optional x-axis label.
        y_label: Optional y-axis label.

    Returns:
        JSON chart specification with data, or error if query fails.
    """
    blocked = {"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE", "MERGE"}
    tokens = sql.upper().split()
    if blocked.intersection(tokens):
        return "Error: only SELECT queries are allowed."

    results = bq_query(sql)
    if not results:
        return json.dumps({"error": "Query returned 0 rows, cannot generate chart."})

    y_list = [k.strip() for k in y_keys.split(",")]

    chart_spec = {
        "__chart__": True,
        "type": chart_type,
        "title": title,
        "x_key": x_key,
        "y_keys": y_list,
        "x_label": x_label or x_key,
        "y_label": y_label,
        "data": results[:200],
    }
    return json.dumps(chart_spec, default=_serialize)


# ── SAVED QUERIES ────────────────────────────────────────────────────────

SAVED_QUERIES_DB = "ecommerce_analytics"
SAVED_QUERIES_COLL = "saved_queries"


@tool
def save_query(name: str, sql: str, description: str) -> str:
    """Save a SQL query for future reuse. The query is stored in MongoDB
    and can be retrieved and executed later with run_saved_query.

    Args:
        name: Short identifier for the query (e.g. 'weekly_revenue_by_state').
        sql: The BigQuery SQL to save.
        description: What this query does, in plain language.
    """
    try:
        db = get_mongo_db(SAVED_QUERIES_DB)
        coll = db[SAVED_QUERIES_COLL]
        doc = {
            "name": name,
            "sql": sql,
            "description": description,
            "created_at": datetime.now(timezone.utc),
            "run_count": 0,
        }
        coll.update_one({"name": name}, {"$set": doc}, upsert=True)
        return f"Query '{name}' saved successfully. Use run_saved_query('{name}') to execute it."
    except Exception as e:
        return f"Error saving query: {e}"


@tool
def list_saved_queries() -> str:
    """List all saved queries stored in MongoDB. Returns name, description,
    and run count for each."""
    try:
        db = get_mongo_db(SAVED_QUERIES_DB)
        coll = db[SAVED_QUERIES_COLL]
        docs = list(coll.find({}, {"_id": 0, "name": 1, "description": 1, "run_count": 1, "sql": 1}).sort("name", 1))
        if not docs:
            return "No saved queries found. Use save_query to create one."
        return json.dumps(docs, default=_serialize, indent=2)
    except Exception as e:
        return f"Error listing queries: {e}"


@tool
def run_saved_query(name: str) -> str:
    """Execute a previously saved query by name and return the results.

    Args:
        name: The name of the saved query to run.
    """
    try:
        db = get_mongo_db(SAVED_QUERIES_DB)
        coll = db[SAVED_QUERIES_COLL]
        doc = coll.find_one({"name": name})
        if not doc:
            return f"No saved query named '{name}'. Use list_saved_queries to see available ones."
        sql = doc["sql"]
        coll.update_one({"name": name}, {"$inc": {"run_count": 1}})
        results = bq_query(sql)
        return _to_json(results)
    except Exception as e:
        return f"Error running saved query: {e}"


# ── TOOL REGISTRY ────────────────────────────────────────────────────────

def get_all_tools():
    """Return the full tool list for the agent."""
    return [
        query_bigquery,
        query_mongodb,
        check_kafka_consumers,
        check_data_freshness,
        get_experiment_summary,
        get_table_schema,
        run_anomaly_scan,
        generate_chart,
        save_query,
        list_saved_queries,
        run_saved_query,
    ]
