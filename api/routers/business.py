"""Business Performance — queries marts.fct_orders_unified, fct_daily_metrics, dims."""
from __future__ import annotations

from fastapi import APIRouter, Query

from api.db import bq_query

router = APIRouter()


@router.get("/kpis")
def kpis():
    totals = bq_query("""
        SELECT
            COALESCE(SUM(order_total), 0)   AS total_gmv,
            COUNT(*)                         AS total_orders,
            COALESCE(AVG(order_total), 0)    AS aov
        FROM marts.fct_orders
    """)
    t = totals[0] if totals else {}

    mtd = bq_query("""
        WITH monthly AS (
            SELECT DATE_TRUNC(DATE(order_purchase_timestamp), MONTH) AS m,
                   SUM(order_total) AS gmv, COUNT(*) AS orders
            FROM marts.fct_orders GROUP BY m HAVING COUNT(*) > 100
        ),
        latest AS (SELECT MAX(m) AS latest_month FROM monthly)
        SELECT COALESCE(SUM(o.order_total), 0) AS gmv_mtd, COUNT(*) AS orders_mtd
        FROM marts.fct_orders o, latest
        WHERE DATE(o.order_purchase_timestamp) >= latest.latest_month
    """)
    m = mtd[0] if mtd else {}

    prev_month = bq_query("""
        WITH monthly AS (
            SELECT DATE_TRUNC(DATE(order_purchase_timestamp), MONTH) AS m,
                   COUNT(*) AS orders
            FROM marts.fct_orders GROUP BY m HAVING COUNT(*) > 100
        ),
        latest AS (SELECT MAX(m) AS latest_month FROM monthly)
        SELECT COALESCE(SUM(o.order_total), 0) AS prev_gmv
        FROM marts.fct_orders o, latest
        WHERE DATE(o.order_purchase_timestamp) >= DATE_SUB(latest.latest_month, INTERVAL 1 MONTH)
          AND DATE(o.order_purchase_timestamp) < latest.latest_month
    """)
    pm = prev_month[0] if prev_month else {}

    gmv_mtd = m.get("gmv_mtd", 0) or 0
    prev_gmv = pm.get("prev_gmv", 0) or 0
    growth = round((gmv_mtd - prev_gmv) / prev_gmv * 100, 1) if prev_gmv else 0

    delivery = bq_query("""
        SELECT
            ROUND(AVG(avg_delivery_time_days), 1)           AS avg_delivery_days,
            ROUND(100 - AVG(late_delivery_rate), 1)         AS on_time_delivery_pct
        FROM marts.fct_daily_metrics
        WHERE avg_delivery_time_days IS NOT NULL
          AND total_orders > 10
    """)
    d = delivery[0] if delivery else {}

    sparklines = bq_query("""
        SELECT
            metric_date AS d,
            COALESCE(total_gmv, 0)       AS gmv,
            COALESCE(total_orders, 0)    AS orders,
            COALESCE(avg_order_value, 0) AS aov
        FROM marts.fct_daily_metrics
        WHERE total_orders > 10
          AND total_gmv > 0
        ORDER BY metric_date DESC
        LIMIT 14
    """)
    sparklines.reverse()

    repeat = bq_query("""
        WITH cust_orders AS (
            SELECT c.customer_unique_id, COUNT(*) AS cnt
            FROM marts.fct_orders o
            JOIN staging.stg_customers c ON o.customer_id = c.customer_id
            GROUP BY c.customer_unique_id
        )
        SELECT ROUND(COUNTIF(cnt > 1) / COUNT(*) * 100, 1) AS repeat_rate
        FROM cust_orders
    """)
    rp = repeat[0] if repeat else {}

    item_counts = bq_query("""
        SELECT ROUND(AVG(item_cnt), 2) AS items_per_order
        FROM (
            SELECT order_id, COUNT(*) AS item_cnt
            FROM staging.stg_order_items
            GROUP BY order_id
        )
    """)
    ic = item_counts[0] if item_counts else {}

    reviews = bq_query("""
        SELECT
            COUNT(*) AS total_reviews,
            ROUND(AVG(review_score), 2) AS avg_score
        FROM staging.stg_reviews
    """)
    rv = reviews[0] if reviews else {}
    total_orders_val = t.get("total_orders", 0) or 0
    total_reviews = rv.get("total_reviews", 0) or 0

    return {
        "total_gmv": t.get("total_gmv", 0),
        "gmv_mtd": gmv_mtd,
        "gmv_today": 0,
        "gmv_growth_pct": growth,
        "total_orders": total_orders_val,
        "orders_mtd": m.get("orders_mtd", 0),
        "orders_today": 0,
        "order_growth_pct": 0,
        "aov": round(t.get("aov", 0) or 0, 2),
        "items_per_order": ic.get("items_per_order", 0) or 0,
        "repeat_purchase_rate": rp.get("repeat_rate", 0) or 0,
        "avg_delivery_days": d.get("avg_delivery_days", 0) or 0,
        "on_time_delivery_pct": d.get("on_time_delivery_pct", 0) or 0,
        "review_rate": round(total_reviews / max(total_orders_val, 1) * 100, 1),
        "avg_review_score": rv.get("avg_score", 0) or 0,
        "sparklines": {
            "gmv": [r.get("gmv", 0) or 0 for r in sparklines],
            "orders": [r.get("orders", 0) or 0 for r in sparklines],
            "aov": [round(r.get("aov", 0) or 0, 2) for r in sparklines],
        },
    }


@router.get("/daily-trends")
def daily_trends(days: int = Query(90, ge=7, le=365)):
    rows = bq_query(f"""
        WITH daily AS (
            SELECT
                DATE(order_purchase_timestamp) AS d,
                COALESCE(SUM(order_total), 0)  AS gmv,
                COUNT(*)                        AS orders,
                COALESCE(AVG(order_total), 0)   AS aov,
                COUNTIF(order_status = 'delivered')  AS delivered,
                COUNTIF(order_status = 'shipped')    AS shipped,
                COUNTIF(order_status = 'canceled')   AS canceled,
                COUNTIF(order_status NOT IN ('delivered','shipped','canceled')) AS processing
            FROM marts.fct_orders
            GROUP BY d
            ORDER BY d DESC
            LIMIT {days}
        )
        SELECT
            d AS date,
            gmv,
            AVG(gmv) OVER (ORDER BY d ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS gmv_ma7,
            orders, aov, delivered, shipped, processing, canceled
        FROM daily
        ORDER BY d
    """)
    return rows


@router.get("/by-state")
def by_state():
    return bq_query("""
        SELECT
            customer_state                AS state,
            COALESCE(SUM(order_total), 0) AS gmv,
            COUNT(*)                       AS orders,
            ROUND(AVG(order_total), 2)     AS aov
        FROM marts.fct_orders
        WHERE customer_state IS NOT NULL
        GROUP BY customer_state
        ORDER BY gmv DESC
    """)


@router.get("/top-products")
def top_products(limit: int = Query(15, ge=5, le=50)):
    rows = bq_query(f"""
        SELECT
            COALESCE(p.product_category_name_english, p.product_category_name, 'other') AS category,
            COUNT(*)                                  AS units_sold,
            COALESCE(SUM(oi.price), 0)                AS revenue,
            ROUND(AVG(oi.price), 2)                   AS avg_price,
            ROUND(AVG(r.review_score), 1)             AS review_score
        FROM staging.stg_order_items oi
        JOIN staging.stg_products p ON oi.product_id = p.product_id
        LEFT JOIN staging.stg_reviews r ON oi.order_id = r.order_id
        WHERE p.product_category_name IS NOT NULL
        GROUP BY category
        ORDER BY revenue DESC
        LIMIT {limit}
    """)
    total_rev = sum(r.get("revenue", 0) or 0 for r in rows)
    for i, r in enumerate(rows):
        r["rank"] = i + 1
        r["review_score"] = r.get("review_score") or 0
        r["share_pct"] = round((r.get("revenue", 0) or 0) / max(total_rev, 1) * 100, 1)
    return rows


@router.get("/revenue-by-category")
def revenue_by_category():
    return bq_query("""
        SELECT
            COALESCE(p.product_category_name_english, p.product_category_name) AS category,
            COALESCE(SUM(oi.price), 0) AS revenue
        FROM staging.stg_order_items oi
        JOIN staging.stg_products p ON oi.product_id = p.product_id
        WHERE p.product_category_name IS NOT NULL
        GROUP BY category
        ORDER BY revenue DESC
    """)


@router.get("/wow")
def wow_comparison():
    week_rows = bq_query("""
        WITH daily AS (
            SELECT DATE(order_purchase_timestamp) AS d
            FROM marts.fct_orders
            GROUP BY d
        ),
        weeks AS (
            SELECT DATE_TRUNC(d, WEEK(MONDAY)) AS wk, COUNT(*) AS dc
            FROM daily GROUP BY wk
            HAVING COUNT(*) >= 6
        )
        SELECT CAST(wk AS STRING) AS wk FROM weeks ORDER BY wk DESC LIMIT 2
    """)
    if len(week_rows) < 2:
        return []

    w1 = week_rows[0]["wk"]
    w2 = week_rows[1]["wk"]

    rows = bq_query(f"""
        WITH orders_in_scope AS (
            SELECT
                DATE(order_purchase_timestamp) AS d,
                EXTRACT(DAYOFWEEK FROM DATE(order_purchase_timestamp)) AS dow,
                DATE_TRUNC(DATE(order_purchase_timestamp), WEEK(MONDAY)) AS wk,
                order_total
            FROM marts.fct_orders
            WHERE DATE_TRUNC(DATE(order_purchase_timestamp), WEEK(MONDAY))
                  IN (DATE '{w1}', DATE '{w2}')
        )
        SELECT
            CASE dow
                WHEN 2 THEN 'Mon' WHEN 3 THEN 'Tue' WHEN 4 THEN 'Wed'
                WHEN 5 THEN 'Thu' WHEN 6 THEN 'Fri' WHEN 7 THEN 'Sat' WHEN 1 THEN 'Sun'
            END AS day,
            SUM(CASE WHEN wk = DATE '{w1}'
                     THEN order_total ELSE 0 END) AS this_week,
            SUM(CASE WHEN wk = DATE '{w2}'
                     THEN order_total ELSE 0 END) AS last_week,
            0.0 AS delta_pct
        FROM orders_in_scope
        GROUP BY dow
        ORDER BY dow
    """)
    for r in rows:
        tw = r.get("this_week", 0) or 0
        lw = r.get("last_week", 0) or 0
        r["delta_pct"] = round((tw - lw) / lw * 100, 1) if lw else 0
    return rows


@router.get("/category-trends")
def category_trends():
    rows = bq_query("""
        WITH top_cats AS (
            SELECT COALESCE(p.product_category_name_english, p.product_category_name) AS cat
            FROM staging.stg_order_items oi
            JOIN staging.stg_products p ON oi.product_id = p.product_id
            WHERE p.product_category_name IS NOT NULL
            GROUP BY cat ORDER BY SUM(oi.price) DESC LIMIT 6
        )
        SELECT
            DATE_TRUNC(DATE(o.order_purchase_timestamp), WEEK) AS week,
            COALESCE(p.product_category_name_english, p.product_category_name) AS category,
            COALESCE(SUM(oi.price), 0) AS revenue
        FROM staging.stg_order_items oi
        JOIN marts.fct_orders o ON oi.order_id = o.order_id
        JOIN staging.stg_products p ON oi.product_id = p.product_id
        WHERE COALESCE(p.product_category_name_english, p.product_category_name) IN (SELECT cat FROM top_cats)
        GROUP BY week, category
        ORDER BY week
    """)
    # Pivot: one row per week with category columns
    weeks: dict[str, dict] = {}
    cats = set()
    for r in rows:
        w = r["week"]
        cat = r["category"] or "other"
        cats.add(cat)
        if w not in weeks:
            weeks[w] = {"week": w}
        weeks[w][cat] = r["revenue"]

    result = list(weeks.values())
    for row in result:
        for c in cats:
            row.setdefault(c, 0)
    return sorted(result, key=lambda x: x["week"])


@router.get("/cohort-retention")
def cohort_retention():
    rows = bq_query("""
        WITH orders_with_uid AS (
            SELECT o.order_id, c.customer_unique_id, o.order_purchase_timestamp
            FROM marts.fct_orders o
            JOIN staging.stg_customers c ON o.customer_id = c.customer_id
        ),
        first_orders AS (
            SELECT customer_unique_id,
                   MIN(DATE(order_purchase_timestamp)) AS first_date
            FROM orders_with_uid
            GROUP BY customer_unique_id
        ),
        cohorts AS (
            SELECT customer_unique_id,
                   FORMAT_DATE('%Y-%m', first_date) AS cohort,
                   first_date
            FROM first_orders
        ),
        repeat_orders AS (
            SELECT c.cohort,
                   DATE_DIFF(DATE(o.order_purchase_timestamp), c.first_date, MONTH) AS months_since,
                   o.customer_unique_id
            FROM orders_with_uid o
            JOIN cohorts c ON o.customer_unique_id = c.customer_unique_id
        )
        SELECT cohort, months_since, COUNT(DISTINCT customer_unique_id) AS users
        FROM repeat_orders
        WHERE months_since BETWEEN 0 AND 12
        GROUP BY cohort, months_since
        ORDER BY cohort, months_since
    """)

    cohort_data: dict[str, dict] = {}
    for r in rows:
        c = r["cohort"]
        if c not in cohort_data:
            cohort_data[c] = {"cohort": c, "cohort_size": 0}
        ms = r["months_since"]
        users = r["users"]
        period = f"Month {ms}"
        if ms == 0:
            cohort_data[c]["cohort_size"] = users
        cohort_data[c][period] = users

    result = []
    for c_data in sorted(cohort_data.values(), key=lambda x: x["cohort"]):
        size = c_data.get("cohort_size", 1) or 1
        if size < 50:
            continue
        for i in range(13):
            key = f"Month {i}"
            if key in c_data:
                c_data[key] = round(c_data[key] / size * 100, 1)
            else:
                c_data[key] = None
        result.append(c_data)

    return result[-8:] if len(result) > 8 else result


@router.get("/aov-distribution")
def aov_distribution():
    rows = bq_query("""
        SELECT
            CASE
                WHEN order_total < 50 THEN 'R$0-50'
                WHEN order_total < 100 THEN 'R$50-100'
                WHEN order_total < 200 THEN 'R$100-200'
                WHEN order_total < 500 THEN 'R$200-500'
                WHEN order_total < 1000 THEN 'R$500-1K'
                ELSE 'R$1K+'
            END AS bucket,
            COUNT(*) AS count
        FROM marts.fct_orders
        GROUP BY bucket
    """)
    total = sum(r["count"] for r in rows) or 1
    bucket_order = ["R$0-50", "R$50-100", "R$100-200", "R$200-500", "R$500-1K", "R$1K+"]
    by_bucket = {r["bucket"]: r for r in rows}
    result = []
    for b in bucket_order:
        r = by_bucket.get(b, {"bucket": b, "count": 0})
        r["pct"] = round(r["count"] / total * 100, 1)
        r["bucket"] = b
        result.append(r)
    return result
