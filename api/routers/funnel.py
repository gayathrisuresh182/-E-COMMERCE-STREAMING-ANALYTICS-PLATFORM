"""Customer Journey & Funnel — queries marts.clickstream_events."""
from __future__ import annotations

from fastapi import APIRouter, Query

from api.db import bq_query

router = APIRouter()

STAGES_ORDER = ["session_start", "product_view", "add_to_cart", "checkout_start", "checkout_complete"]
STAGE_LABELS = {
    "session_start": "Sessions",
    "product_view": "Product Views",
    "add_to_cart": "Add to Cart",
    "checkout_start": "Checkout Start",
    "checkout_complete": "Order Placed",
}


def _build_funnel(rows: list[dict], include_revenue: bool = False) -> list[dict]:
    """Convert stage count rows into funnel format with drop-off rates."""
    stage_counts = {r["stage"]: r["cnt"] for r in rows}
    stage_revenue = {r["stage"]: r.get("revenue", 0) or 0 for r in rows}
    total = stage_counts.get("session_start", 0) or 1

    result = []
    prev = total
    for stage in STAGES_ORDER:
        cnt = stage_counts.get(stage, 0)
        entry: dict = {
            "stage": STAGE_LABELS.get(stage, stage),
            "count": cnt,
            "percentage": round(cnt / total * 100, 1) if total else 0,
            "drop_off_rate": round((1 - cnt / prev) * 100, 1) if prev and stage != "session_start" else 0,
        }
        if include_revenue:
            entry["revenue"] = round(stage_revenue.get(stage, 0), 2)
        prev = cnt or 1
        result.append(entry)
    return result


@router.get("/overall")
def overall():
    rows = bq_query("""
        SELECT
            event_type AS stage,
            COUNT(DISTINCT session_id) AS cnt,
            COALESCE(SUM(CASE WHEN event_type = 'order_placed'
                              THEN 1 END), 0) AS revenue
        FROM marts.clickstream_events
        GROUP BY event_type
    """)
    return _build_funnel(rows, include_revenue=True)


@router.get("/by-variant")
def by_variant(experiment_id: str = Query("exp_001")):
    rows = bq_query(f"""
        SELECT
            variant,
            event_type AS stage,
            COUNT(DISTINCT session_id) AS cnt
        FROM marts.clickstream_events
        WHERE experiment_id = '{experiment_id}'
        GROUP BY variant, event_type
    """)

    ctrl_rows = [r for r in rows if r["variant"] == "control"]
    treat_rows = [r for r in rows if r["variant"] == "treatment"]
    ctrl = _build_funnel(ctrl_rows)
    treat = _build_funnel(treat_rows)

    improvements = []
    for c, t in zip(ctrl, treat):
        imp = round((t["count"] - c["count"]) / max(c["count"], 1) * 100, 1)
        improvements.append({"stage": c["stage"], "improvement_percent": imp})

    return {
        "experiment_id": experiment_id,
        "control": ctrl,
        "treatment": treat,
        "improvements": improvements,
    }


@router.get("/by-device")
def by_device():
    rows = bq_query("""
        WITH session_devices AS (
            SELECT session_id, MAX(device) AS device
            FROM marts.clickstream_events
            WHERE device IS NOT NULL AND device != ''
            GROUP BY session_id
        )
        SELECT
            sd.device,
            COUNT(DISTINCT sd.session_id) AS sessions,
            COUNT(DISTINCT CASE WHEN ce.event_type = 'checkout_complete' THEN sd.session_id END) AS orders,
            SAFE_DIVIDE(
                COUNT(DISTINCT CASE WHEN ce.event_type = 'checkout_complete' THEN sd.session_id END),
                COUNT(DISTINCT sd.session_id)
            ) AS conversion_rate
        FROM session_devices sd
        JOIN marts.clickstream_events ce ON sd.session_id = ce.session_id
        GROUP BY sd.device
        ORDER BY sessions DESC
    """)
    for r in rows:
        r["conversion_rate"] = round(r.get("conversion_rate", 0) or 0, 4)
        r["revenue_per_session"] = 0
    return rows


@router.get("/by-referrer")
def by_referrer():
    rows = bq_query("""
        WITH session_referrers AS (
            SELECT session_id, MAX(referrer) AS referrer
            FROM marts.clickstream_events
            WHERE referrer IS NOT NULL AND referrer != ''
            GROUP BY session_id
        )
        SELECT
            sr.referrer,
            COUNT(DISTINCT sr.session_id) AS sessions,
            COUNT(DISTINCT CASE WHEN ce.event_type = 'checkout_complete' THEN sr.session_id END) AS orders,
            SAFE_DIVIDE(
                COUNT(DISTINCT CASE WHEN ce.event_type = 'checkout_complete' THEN sr.session_id END),
                COUNT(DISTINCT sr.session_id)
            ) AS conversion_rate
        FROM session_referrers sr
        JOIN marts.clickstream_events ce ON sr.session_id = ce.session_id
        GROUP BY sr.referrer
        ORDER BY sessions DESC
    """)
    for r in rows:
        r["conversion_rate"] = round(r.get("conversion_rate", 0) or 0, 4)
    return rows


@router.get("/time-to-convert")
def time_to_convert():
    rows = bq_query("""
        WITH session_times AS (
            SELECT
                session_id,
                TIMESTAMP_DIFF(
                    MAX(CASE WHEN event_type = 'checkout_complete' THEN event_timestamp END),
                    MIN(event_timestamp),
                    MINUTE
                ) AS minutes
            FROM marts.clickstream_events
            GROUP BY session_id
            HAVING MAX(CASE WHEN event_type = 'checkout_complete' THEN 1 ELSE 0 END) = 1
        ),
        bucketed AS (
            SELECT
                CAST(FLOOR(minutes / 5) * 5 AS INT64) AS minutes_start
            FROM session_times
            WHERE minutes BETWEEN 0 AND 59
        )
        SELECT
            CONCAT(CAST(minutes_start AS STRING), '-', CAST(minutes_start + 5 AS STRING)) AS bucket,
            minutes_start,
            COUNT(*) AS count
        FROM bucketed
        GROUP BY minutes_start
        ORDER BY minutes_start
    """)
    return rows


@router.get("/transitions")
def transitions():
    rows = bq_query("""
        WITH ordered AS (
            SELECT
                session_id,
                event_type,
                LEAD(event_type) OVER (PARTITION BY session_id ORDER BY event_timestamp) AS next_event
            FROM marts.clickstream_events
        )
        SELECT
            event_type AS source,
            COALESCE(next_event, 'exit') AS target,
            COUNT(*) AS value
        FROM ordered
        GROUP BY source, target
        ORDER BY value DESC
        LIMIT 20
    """)
    return rows


@router.get("/device-funnel")
def device_funnel():
    rows = bq_query("""
        SELECT
            device,
            event_type AS stage,
            COUNT(DISTINCT session_id) AS cnt
        FROM marts.clickstream_events
        WHERE device IS NOT NULL
        GROUP BY device, event_type
    """)

    result = {}
    devices = set(r["device"] for r in rows)
    for dev in devices:
        dev_rows = [r for r in rows if r["device"] == dev]
        result[dev.lower()] = _build_funnel(dev_rows)
    return result


@router.get("/abandonment")
def abandonment():
    rows = bq_query("""
        WITH funnel AS (
            SELECT
                COUNT(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN session_id END) AS carts,
                COUNT(DISTINCT CASE WHEN event_type = 'checkout_complete' THEN session_id END) AS orders
            FROM marts.clickstream_events
        )
        SELECT
            carts - orders                                  AS abandoned_carts,
            SAFE_DIVIDE(carts - orders, carts) * 100        AS abandonment_rate,
            carts,
            orders
        FROM funnel
    """)
    r = rows[0] if rows else {}
    abandoned = r.get("abandoned_carts", 0) or 0
    return {
        "abandoned_carts": abandoned,
        "abandoned_value": 0,
        "abandonment_rate": round(r.get("abandonment_rate", 0) or 0, 1),
        "top_abandon_stage": "Add to Cart → Order",
        "avg_abandoned_cart_value": 0,
        "recovery_rate": 0,
        "recovered_revenue": 0,
    }


@router.get("/session-metrics")
def session_metrics():
    rows = bq_query("""
        WITH sessions AS (
            SELECT
                session_id,
                TIMESTAMP_DIFF(MAX(event_timestamp), MIN(event_timestamp), SECOND) AS duration_sec,
                COUNT(*) AS page_count,
                MAX(CASE WHEN event_type = 'checkout_complete' THEN 1 ELSE 0 END) AS converted
            FROM marts.clickstream_events
            GROUP BY session_id
        )
        SELECT
            APPROX_QUANTILES(duration_sec, 2)[OFFSET(1)] AS median_session_duration_sec,
            ROUND(AVG(page_count), 1)                     AS avg_pages_per_session,
            ROUND(COUNTIF(page_count <= 2) / COUNT(*) * 100, 1) AS bounce_rate,
            ROUND(COUNTIF(converted = 1) / COUNT(*) * 100, 1) AS return_visit_conversion
        FROM sessions
    """)
    r = rows[0] if rows else {}
    return {
        "median_session_duration_sec": r.get("median_session_duration_sec", 0) or 0,
        "avg_pages_per_session": r.get("avg_pages_per_session", 0) or 0,
        "bounce_rate": r.get("bounce_rate", 0) or 0,
        "return_visit_conversion": 0,
    }
