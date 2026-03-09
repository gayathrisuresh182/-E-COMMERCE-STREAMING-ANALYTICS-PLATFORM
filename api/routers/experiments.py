"""A/B Testing — queries marts.experiment_summary, experiment_comparison_view, clickstream."""
from __future__ import annotations

import math

from fastapi import APIRouter, Query

from api.db import bq_query

router = APIRouter()


def _compute_ci(ctrl_cr, treat_cr, ctrl_n, treat_n, lift_pct):
    """95% confidence interval for lift percentage."""
    try:
        se = math.sqrt(
            ctrl_cr * (1 - ctrl_cr) / max(ctrl_n, 1)
            + treat_cr * (1 - treat_cr) / max(treat_n, 1)
        )
        se_lift = (se / max(ctrl_cr, 0.001)) * 100
        return round(lift_pct - 1.96 * se_lift, 1), round(lift_pct + 1.96 * se_lift, 1)
    except (ValueError, ZeroDivisionError):
        return lift_pct - 5, lift_pct + 5


def _sample_size_needed(baseline_cr: float, mde: float = 0.02) -> int:
    """Required per-arm sample size for 80% power, alpha=0.05."""
    try:
        z_alpha, z_beta = 1.96, 0.84
        p = baseline_cr
        n = 2 * (z_alpha + z_beta) ** 2 * p * (1 - p) / max(mde ** 2, 1e-8)
        return int(n)
    except (ValueError, ZeroDivisionError):
        return 5000


@router.get("/summary")
def summary():
    rows = bq_query("""
        SELECT
            COUNT(*)                                                          AS total_experiments,
            COUNTIF(LOWER(significant) = 'true' OR significant = 'yes')       AS significant,
            COUNTIF(recommendation = 'LAUNCH')                                AS launch_ready,
            COALESCE(SUM(CASE WHEN recommendation = 'LAUNCH'
                              THEN revenue_impact_monthly END), 0)            AS total_revenue_impact
        FROM marts.experiment_summary
    """)
    r = rows[0] if rows else {}

    avg = bq_query("""
        SELECT
            CAST(AVG(unique_users) AS INT64) AS avg_sample_size
        FROM marts.fct_experiment_results
    """)
    a = avg[0] if avg else {}

    return {
        "total_experiments": r.get("total_experiments", 0),
        "significant": r.get("significant", 0),
        "launch_ready": r.get("launch_ready", 0),
        "total_revenue_impact": r.get("total_revenue_impact", 0),
        "avg_sample_size": a.get("avg_sample_size", 0) or 0,
        "avg_test_duration_days": 18,
    }


@router.get("/list")
def experiment_list():
    rows = bq_query("""
        SELECT *
        FROM marts.experiment_comparison_view
        ORDER BY experiment_id
    """)
    result = []
    for r in rows:
        ctrl_n = r.get("control_users", 0) or 0
        treat_n = r.get("treatment_users", 0) or 0
        ctrl_cr_pct = r.get("control_conversion_rate", 0) or 0
        treat_cr_pct = r.get("treatment_conversion_rate", 0) or 0
        ctrl_cr = ctrl_cr_pct / 100
        treat_cr = treat_cr_pct / 100
        ctrl_rev = r.get("control_revenue", 0) or 0
        treat_rev = r.get("treatment_revenue", 0) or 0
        lift = r.get("lift_percent", 0) or 0
        p_val = r.get("p_value", 1) or 1
        ci_lo, ci_hi = _compute_ci(ctrl_cr, treat_cr, ctrl_n, treat_n, lift)
        needed = _sample_size_needed(ctrl_cr) * 2

        ctrl_conversions = int(ctrl_n * ctrl_cr)
        treat_conversions = int(treat_n * treat_cr)

        result.append({
            "experiment_id": r.get("experiment_id", ""),
            "experiment_name": r.get("experiment_name", ""),
            "p_value": p_val,
            "significant": p_val < 0.05,
            "recommendation": r.get("recommendation", "MONITOR"),
            "revenue_impact_monthly": r.get("revenue_impact_monthly", 0) or 0,
            "lift_percent": round(lift, 2),
            "ci_lower": ci_lo,
            "ci_upper": ci_hi,
            "sample_size_needed": needed,
            "sample_size_current": ctrl_n + treat_n,
            "days_running": 0,
            "control": {
                "variant": "control",
                "users": ctrl_n,
                "conversions": ctrl_conversions,
                "conversion_rate": ctrl_cr_pct,
                "revenue": ctrl_rev,
                "aov": round(ctrl_rev / max(ctrl_conversions, 1), 2),
            },
            "treatment": {
                "variant": "treatment",
                "users": treat_n,
                "conversions": treat_conversions,
                "conversion_rate": treat_cr_pct,
                "revenue": treat_rev,
                "aov": round(treat_rev / max(treat_conversions, 1), 2),
            },
        })
    return result


@router.get("/portfolio")
def portfolio():
    rows = bq_query("""
        SELECT
            experiment_name,
            lift_percent,
            control_users,
            treatment_users,
            control_conversion_rate,
            treatment_conversion_rate,
            p_value,
            significant,
            recommendation
        FROM marts.experiment_comparison_view
        ORDER BY lift_percent DESC
    """)
    result = []
    for r in rows:
        lift = r.get("lift_percent", 0) or 0
        ci_lo, ci_hi = _compute_ci(
            (r.get("control_conversion_rate", 0) or 0) / 100,
            (r.get("treatment_conversion_rate", 0) or 0) / 100,
            r.get("control_users", 0) or 0,
            r.get("treatment_users", 0) or 0,
            lift,
        )
        p_val = r.get("p_value", 1) or 1
        result.append({
            "experiment_name": r.get("experiment_name", ""),
            "lift_percent": lift,
            "ci_lower": ci_lo,
            "ci_upper": ci_hi,
            "significant": p_val < 0.05,
            "recommendation": r.get("recommendation", "MONITOR"),
        })
    return result


@router.get("/cumulative")
def cumulative(experiment_id: str = Query("exp_001")):
    rows = bq_query(f"""
        WITH daily AS (
            SELECT
                DATE(event_timestamp) AS d,
                variant,
                COUNT(DISTINCT session_id)                                             AS sessions,
                COUNT(DISTINCT CASE WHEN event_type = 'checkout_complete' THEN session_id END) AS conversions
            FROM marts.clickstream_events
            WHERE experiment_id = '{experiment_id}'
            GROUP BY d, variant
        ),
        cum AS (
            SELECT
                d, variant,
                SUM(sessions)     OVER (PARTITION BY variant ORDER BY d) AS cum_s,
                SUM(conversions)  OVER (PARTITION BY variant ORDER BY d) AS cum_c,
                ROW_NUMBER()      OVER (PARTITION BY variant ORDER BY d) AS day_num
            FROM daily
        )
        SELECT day_num AS day, variant,
               ROUND(SAFE_DIVIDE(cum_c, cum_s) * 100, 2) AS cr
        FROM cum
        ORDER BY day_num
    """)

    by_day: dict[int, dict] = {}
    for r in rows:
        day = r["day"]
        if day not in by_day:
            by_day[day] = {"day": day, "control": 0, "treatment": 0}
        by_day[day][r["variant"]] = r["cr"]
    return list(by_day.values())


@router.get("/{experiment_id}")
def detail(experiment_id: str):
    all_exps = experiment_list()
    for exp in all_exps:
        if exp["experiment_id"] == experiment_id:
            return exp
    return {"error": "Experiment not found"}
