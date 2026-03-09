"""Real-Time Operations — queries realtime.realtime_orders + MongoDB."""
from __future__ import annotations

from fastapi import APIRouter, Query

from api.db import bq_query, get_mongo_db

router = APIRouter()


def _latest_date() -> str:
    """Find the most recent date with streaming data."""
    rows = bq_query("""
        SELECT MAX(DATE(event_timestamp)) AS d
        FROM realtime.realtime_orders
    """)
    return rows[0]["d"] if rows and rows[0].get("d") else "1970-01-01"


@router.get("/kpis")
def kpis():
    latest = _latest_date()

    today = bq_query(f"""
        SELECT
            COALESCE(SUM(order_total), 0)                       AS total_gmv,
            COUNT(DISTINCT order_id)                             AS total_orders,
            COALESCE(AVG(order_total), 0)                       AS avg_order_value,
            MAX(event_timestamp)                                 AS last_event,
            COUNTIF(event_type = 'order_canceled')               AS canceled
        FROM realtime.realtime_orders
        WHERE DATE(event_timestamp) = '{latest}'
    """)
    t = today[0] if today else {}

    yesterday = bq_query(f"""
        SELECT
            COALESCE(SUM(order_total), 0) AS total_gmv,
            COUNT(DISTINCT order_id)      AS total_orders
        FROM realtime.realtime_orders
        WHERE DATE(event_timestamp) = DATE_SUB(DATE '{latest}', INTERVAL 1 DAY)
    """)
    y = yesterday[0] if yesterday else {}

    total_orders = t.get("total_orders", 0) or 0
    total_gmv = t.get("total_gmv", 0) or 0
    y_gmv = y.get("total_gmv", 0) or 0
    y_orders = y.get("total_orders", 0) or 0
    canceled = t.get("canceled", 0) or 0

    gmv_delta = ((total_gmv - y_gmv) / y_gmv * 100) if y_gmv else 0
    orders_delta = ((total_orders - y_orders) / y_orders * 100) if y_orders else 0
    cancel_rate = (canceled / total_orders * 100) if total_orders else 0

    sparklines = bq_query(f"""
        SELECT
            DATE(event_timestamp) AS d,
            COALESCE(SUM(order_total), 0) AS gmv,
            COUNT(DISTINCT order_id) AS orders,
            COALESCE(AVG(order_total), 0) AS aov
        FROM realtime.realtime_orders
        WHERE DATE(event_timestamp)
              BETWEEN DATE_SUB(DATE '{latest}', INTERVAL 23 DAY) AND DATE '{latest}'
        GROUP BY d ORDER BY d
    """)

    return {
        "total_gmv": total_gmv,
        "total_orders": total_orders,
        "avg_order_value": t.get("avg_order_value", 0) or 0,
        "last_event": t.get("last_event", ""),
        "orders_per_minute": round(total_orders / 1440, 2) if total_orders else 0,
        "active_sessions": 0,
        "revenue_per_session": 0,
        "cancel_rate": round(cancel_rate, 2),
        "gmv_vs_yesterday": round(gmv_delta, 1),
        "orders_vs_yesterday": round(orders_delta, 1),
        "sparklines": {
            "gmv": [r["gmv"] for r in sparklines],
            "orders": [r["orders"] for r in sparklines],
            "aov": [round(r["aov"], 2) for r in sparklines],
            "cancel_rate": [],
        },
    }


@router.get("/timeseries")
def timeseries(hours: int = Query(24, ge=1, le=72)):
    latest = _latest_date()
    rows = bq_query(f"""
        WITH today AS (
            SELECT
                TIMESTAMP_TRUNC(event_timestamp, HOUR) AS ts,
                COUNT(DISTINCT order_id)      AS orders,
                COALESCE(SUM(order_total), 0) AS gmv
            FROM realtime.realtime_orders
            WHERE DATE(event_timestamp) = '{latest}'
            GROUP BY ts
        ),
        yesterday AS (
            SELECT
                TIMESTAMP_TRUNC(event_timestamp, HOUR) AS ts,
                COUNT(DISTINCT order_id)      AS orders,
                COALESCE(SUM(order_total), 0) AS gmv
            FROM realtime.realtime_orders
            WHERE DATE(event_timestamp) = DATE_SUB(DATE '{latest}', INTERVAL 1 DAY)
            GROUP BY ts
        )
        SELECT
            t.ts                            AS timestamp,
            t.orders                        AS orders,
            t.gmv                           AS gmv,
            COALESCE(y.orders, 0)           AS orders_yesterday,
            COALESCE(y.gmv, 0)              AS gmv_yesterday
        FROM today t
        LEFT JOIN yesterday y
          ON EXTRACT(HOUR FROM t.ts) = EXTRACT(HOUR FROM y.ts)
        ORDER BY t.ts
    """)
    return rows


@router.get("/orders-by-state")
def orders_by_state():
    latest = _latest_date()
    return bq_query(f"""
        SELECT
            customer_state                 AS state,
            COUNT(DISTINCT order_id)       AS orders,
            COALESCE(SUM(order_total), 0)  AS revenue
        FROM realtime.realtime_orders
        WHERE DATE(event_timestamp) = '{latest}'
          AND customer_state IS NOT NULL
        GROUP BY customer_state
        ORDER BY revenue DESC
        LIMIT 15
    """)


@router.get("/orders-by-status")
def orders_by_status():
    latest = _latest_date()
    color_map = {
        "order_placed": "#3b82f6",
        "order_shipped": "#10b981",
        "order_canceled": "#ef4444",
        "order_delivered": "#a855f7",
    }
    rows = bq_query(f"""
        SELECT
            event_type AS status,
            COUNT(*)   AS count
        FROM realtime.realtime_orders
        WHERE DATE(event_timestamp) = '{latest}'
        GROUP BY event_type
        ORDER BY count DESC
    """)
    for r in rows:
        r["color"] = color_map.get(r["status"], "#64748b")
    return rows


@router.get("/recent-orders")
def recent_orders(limit: int = Query(15, ge=1, le=50)):
    return bq_query(f"""
        SELECT
            order_id,
            customer_id,
            customer_state,
            order_total,
            event_type,
            event_timestamp
        FROM realtime.realtime_orders
        ORDER BY event_timestamp DESC
        LIMIT {limit}
    """)


@router.get("/stream-health")
def stream_health():
    for db_name in ["ecommerce_analytics", "ecommerce_streaming"]:
        try:
            db = get_mongo_db(db_name)
            coll_name = "fct_orders_realtime" if db_name == "ecommerce_analytics" else "dim_customers"
            coll = db[coll_name]
            total = coll.estimated_document_count()
            if total > 0:
                latest = coll.find_one(sort=[("_id", -1)])
                last_ts = ""
                if latest:
                    for f in ("processed_at", "updated_at", "created_at"):
                        if f in latest:
                            last_ts = str(latest[f])
                            break
                return {
                    "last_event_age_minutes": 0,
                    "status": "healthy",
                    "events_last_hour": total,
                    "events_last_24h": total,
                    "last_updated": last_ts,
                }
        except Exception:
            continue
    return {
        "last_event_age_minutes": -1,
        "status": "no_data",
        "events_last_hour": 0,
        "events_last_24h": 0,
        "last_updated": "",
    }


@router.get("/heatmap")
def heatmap():
    day_names = {1: "Sun", 2: "Mon", 3: "Tue", 4: "Wed", 5: "Thu", 6: "Fri", 7: "Sat"}
    rows = bq_query("""
        SELECT
            EXTRACT(DAYOFWEEK FROM order_purchase_timestamp) AS dow,
            EXTRACT(HOUR FROM order_purchase_timestamp)      AS hour,
            COUNT(*) AS orders
        FROM marts.fct_orders
        GROUP BY dow, hour
        ORDER BY dow, hour
    """)
    return [{"day": day_names.get(r["dow"], "?"), "hour": r["hour"], "orders": r["orders"]} for r in rows]


@router.get("/cancellation-trend")
def cancellation_trend():
    rows = bq_query("""
        WITH hourly AS (
            SELECT
                TIMESTAMP_TRUNC(event_timestamp, HOUR) AS ts,
                COUNTIF(event_type = 'order_canceled') AS canceled,
                COUNT(*) AS total
            FROM realtime.realtime_orders
            GROUP BY ts
        )
        SELECT
            ts AS timestamp,
            ROUND(SAFE_DIVIDE(canceled, total) * 100, 2) AS rate
        FROM hourly
        ORDER BY ts DESC
        LIMIT 48
    """)
    return list(reversed(rows))
