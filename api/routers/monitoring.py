"""Data Quality & Pipeline Health — queries monitoring tables + MongoDB."""
from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Query

from api.db import bq_query, get_mongo_db

router = APIRouter()


@router.get("/consumers")
def consumers():
    rows = bq_query("""
        SELECT
            consumer_group,
            topics,
            total_lag,
            partitions,
            status,
            measured_at
        FROM monitoring.consumer_health_metrics
        WHERE consumer_group IS NOT NULL
        ORDER BY measured_at DESC
    """)
    seen = set()
    unique = []
    for r in rows:
        cg = r.get("consumer_group")
        if cg and cg not in seen:
            seen.add(cg)
            unique.append(r)
    return unique


@router.get("/collections")
def collections():
    labels = {
        "dim_customers": "Customers",
        "dim_sellers": "Sellers",
        "products": "Products",
        "reviews": "Reviews",
        "experiments": "Experiments",
        "experiment_assignments": "Experiment Assignments",
        "fct_orders_realtime": "Orders (RT)",
        "realtime_metrics": "Metrics (RT)",
        "experiments_realtime": "Experiments (RT)",
        "delivery_alerts": "Delivery Alerts",
        "delivery_events": "Delivery Events",
        "orders": "Orders",
    }
    result = []
    for db_name in ["ecommerce_streaming", "ecommerce_analytics"]:
        try:
            db = get_mongo_db(db_name)
            for name in sorted(db.list_collection_names()):
                coll = db[name]
                doc_count = coll.estimated_document_count()
                latest = coll.find_one(sort=[("_id", -1)])
                ts = ""
                if latest:
                    for field in ("processed_at", "updated_at", "event_timestamp",
                                  "alerted_at", "window_end", "created_at"):
                        if field in latest:
                            ts = str(latest[field])
                            break
                    if not ts and "_id" in latest:
                        try:
                            ts = latest["_id"].generation_time.isoformat()
                        except Exception:
                            pass
                result.append({
                    "collection": f"{db_name}.{name}",
                    "label": labels.get(name, name),
                    "document_count": doc_count,
                    "newest_timestamp": ts,
                })
        except Exception:
            continue
    return result


@router.get("/alerts")
def alerts():
    rows = bq_query("""
        SELECT
            created_at                         AS timestamp,
            'Delivery SLA'                     AS type,
            alert_severity                     AS severity,
            CONCAT('Order ', order_id, ' — ', CAST(days_to_deliver AS STRING), ' days to deliver') AS message,
            CASE WHEN is_late THEN 'open' ELSE 'resolved' END AS status
        FROM realtime.delivery_alerts
        ORDER BY created_at DESC
        LIMIT 20
    """)
    if not rows:
        for db_name in ["ecommerce_analytics", "ecommerce_streaming"]:
            try:
                db = get_mongo_db(db_name)
                if "delivery_alerts" not in db.list_collection_names():
                    continue
                docs = list(
                    db["delivery_alerts"]
                    .find({}, {"_id": 0})
                    .sort("alerted_at", -1)
                    .limit(20)
                )
                if docs:
                    return [
                        {
                            "timestamp": d.get("alerted_at", ""),
                            "type": "Delivery SLA",
                            "severity": d.get("severity", "warning"),
                            "message": f"Order {d.get('order_id', '?')} — {d.get('days_late', 0):.0f} days late",
                            "status": "open",
                        }
                        for d in docs
                    ]
            except Exception:
                continue
        return []
    return rows


@router.get("/lag-history")
def lag_history(hours: int = Query(24, ge=1, le=168)):
    rows = bq_query(f"""
        WITH latest AS (
            SELECT MAX(measured_at) AS mx
            FROM monitoring.consumer_health_metrics
            WHERE consumer_group IS NOT NULL
        )
        SELECT
            chm.measured_at AS timestamp,
            chm.consumer_group,
            chm.total_lag
        FROM monitoring.consumer_health_metrics chm, latest l
        WHERE chm.consumer_group IS NOT NULL
          AND chm.measured_at >= TIMESTAMP_SUB(l.mx, INTERVAL {hours} HOUR)
        ORDER BY chm.measured_at
    """)
    # Pivot: one row per timestamp with consumer group columns
    ts_data: dict[str, dict] = {}
    for r in rows:
        t = r["timestamp"]
        if t not in ts_data:
            ts_data[t] = {"timestamp": t}
        ts_data[t][r["consumer_group"]] = r["total_lag"]

    return list(ts_data.values())


@router.get("/health-score")
def health_score():
    consumer_rows = consumers()
    total = len(consumer_rows)
    healthy = sum(1 for c in consumer_rows if c.get("status") == "healthy")

    if total > 0:
        score = int(healthy / total * 100)
    else:
        fresh = data_freshness()
        fresh_count = sum(1 for f in fresh if f.get("status") == "fresh")
        total_ds = len(fresh) or 1
        score = int(fresh_count / total_ds * 100)

    alert_rows = alerts()
    open_alerts = sum(1 for a in alert_rows if a.get("status") == "open")
    score = max(0, score - open_alerts * 5)

    return {
        "score": score,
        "healthy_consumers": healthy,
        "total_consumers": total,
        "open_alerts": open_alerts,
        "status": "excellent" if score >= 90 else "warning" if score >= 70 else "critical",
    }


@router.get("/throughput")
def throughput(hours: int = Query(24, ge=1, le=168)):
    rows = bq_query(f"""
        WITH latest AS (
            SELECT MAX(window_start) AS mx FROM realtime.realtime_metrics
        )
        SELECT
            TIMESTAMP_TRUNC(rm.window_start, HOUR) AS timestamp,
            rm.metric_name,
            SUM(rm.metric_value) AS val
        FROM realtime.realtime_metrics rm, latest l
        WHERE rm.window_start >= TIMESTAMP_SUB(l.mx, INTERVAL {hours} HOUR)
        GROUP BY timestamp, rm.metric_name
        ORDER BY timestamp
    """)
    ts_data: dict[str, dict] = {}
    metric_map = {
        "total_orders": "orders_stream",
        "total_clicks": "clickstream",
        "total_gmv": "deliveries_stream",
    }
    for r in rows:
        t = r["timestamp"]
        if t not in ts_data:
            ts_data[t] = {"timestamp": t, "orders_stream": 0, "clickstream": 0, "deliveries_stream": 0}
        col = metric_map.get(r["metric_name"])
        if col:
            ts_data[t][col] = round(r["val"], 1)
    return list(ts_data.values())


@router.get("/sla-compliance")
def sla_compliance():
    rows = bq_query("""
        WITH ordered AS (
            SELECT
                metric_date,
                LAG(metric_date) OVER (ORDER BY metric_date) AS prev_date
            FROM marts.fct_daily_metrics
            WHERE metric_date IS NOT NULL
            ORDER BY metric_date DESC
            LIMIT 31
        )
        SELECT
            metric_date AS date,
            COALESCE(DATE_DIFF(metric_date, prev_date, DAY), 1) AS gap_days
        FROM ordered
        WHERE prev_date IS NOT NULL
        ORDER BY metric_date DESC
        LIMIT 30
    """)
    sla_target = 1440
    result = []
    for r in rows:
        gap = r.get("gap_days", 1) or 1
        duration_min = gap * 1440
        result.append({
            "date": r["date"],
            "sla_met": gap <= 1,
            "duration_min": duration_min,
            "sla_target_min": sla_target,
        })
    result.reverse()
    return result


@router.get("/data-freshness")
def data_freshness():
    tables = [
        ("marts", "fct_orders", "daily", 72),
        ("marts", "fct_daily_metrics", "daily", 72),
        ("realtime", "realtime_orders", "batch sync", 72),
        ("realtime", "realtime_metrics", "batch sync", 72),
        ("marts", "clickstream_events", "daily", 72),
        ("marts", "experiment_summary", "daily", 72),
    ]
    result = []
    now = datetime.now(timezone.utc)
    for dataset, table, freq, sla_hours in tables:
        rows = bq_query(f"""
            SELECT TIMESTAMP_MILLIS(last_modified_time) AS last_updated
            FROM `{dataset}.__TABLES__`
            WHERE table_id = '{table}'
        """)
        if rows:
            last = rows[0]["last_updated"]
            try:
                ts = datetime.fromisoformat(last) if isinstance(last, str) else last
                age_hours = (now - ts).total_seconds() / 3600
                status = "fresh" if age_hours <= sla_hours else "stale"
            except Exception:
                status = "unknown"
        else:
            last = ""
            status = "unknown"

        result.append({
            "dataset": f"{dataset}.{table}",
            "last_updated": last if isinstance(last, str) else str(last),
            "update_frequency": freq,
            "sla_hours": sla_hours,
            "status": status,
        })
    return result
