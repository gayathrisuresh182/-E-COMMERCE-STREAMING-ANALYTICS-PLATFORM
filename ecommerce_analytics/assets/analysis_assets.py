"""
Phase 4F: Statistical analysis as Dagster assets.

Runs ExperimentAnalyzer + BayesianAnalyzer + MultiMetricAnalyzer for each
experiment and aggregates results into an executive summary. Integrates
with the Dagster pipeline so analysis runs automatically (e.g. weekly).

Also provides a Papermill-based asset for notebook execution (visual output).
"""
from __future__ import annotations

import json
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from dagster import asset, get_dagster_logger

from ecommerce_analytics.analysis import (
    BayesianAnalyzer,
    ExperimentCatalog,
    MultiMetricAnalyzer,
    build_experiment_data_from_dagster,
)
from ecommerce_analytics.analysis.stats_framework import ExperimentAnalyzer

logger = get_dagster_logger()
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
ANALYSIS_OUTPUT_DIR = PROJECT_ROOT / "output" / "analysis"


def _run_single_experiment_analysis(
    experiment_id: str,
    raw_experiment_assignments: pd.DataFrame,
    stg_orders: pd.DataFrame,
    stg_order_items: pd.DataFrame,
    stg_reviews: pd.DataFrame,
) -> dict[str, Any]:
    """Run full statistical analysis for one experiment."""
    catalog = ExperimentCatalog()
    meta = catalog.get(experiment_id)

    data = build_experiment_data_from_dagster(
        assignments=raw_experiment_assignments,
        orders=stg_orders,
        order_items=stg_order_items,
        reviews=stg_reviews,
        experiment_id=experiment_id,
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        # Frequentist
        analyzer = ExperimentAnalyzer(data, meta)
        report = analyzer.full_report()
        rec = report["recommendation"]

        # Bayesian
        bayes = BayesianAnalyzer(data, prior_alpha=1.0, prior_beta=1.0)
        bayes_result = bayes.conversion_test()

        # Multi-metric (hierarchical)
        multi = MultiMetricAnalyzer(data)
        multi_result = multi.hierarchical_test()

    ds = report["descriptive_stats"]
    variant_stats = {
        "n_control": int(ds["n_control"]),
        "n_treatment": int(ds["n_treatment"]),
        "conversions_control": int(ds["conversions_control"]),
        "conversions_treatment": int(ds["conversions_treatment"]),
        "conversion_rate_control": float(ds["conversion_rate_control"]),
        "conversion_rate_treatment": float(ds["conversion_rate_treatment"]),
        "revenue_total_control": round(
            float(ds["revenue_mean_control"]) * int(ds["n_control"]), 2
        ),
        "revenue_total_treatment": round(
            float(ds["revenue_mean_treatment"]) * int(ds["n_treatment"]), 2
        ),
    }

    # Sanitize for JSON (numpy types, etc.)
    def _to_json_safe(obj: Any) -> Any:
        if hasattr(obj, "item"):
            return obj.item()
        if isinstance(obj, dict):
            return {k: _to_json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_to_json_safe(x) for x in obj]
        if isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        return str(obj)

    return {
        "experiment_id": experiment_id,
        "experiment_name": meta.name,
        "variant_stats": variant_stats,
        "frequentist": _to_json_safe({
            "p_value": report["hypothesis_test"]["p_value"],
            "significant": report["hypothesis_test"]["significant"],
            "decision": rec["decision"],
            "relative_lift_pct": report["confidence_interval"]["relative_lift_pct"],
            "ci_lower": report["confidence_interval"]["ci_lower"],
            "ci_upper": report["confidence_interval"]["ci_upper"],
            "achieved_power": report["power_analysis"].get("achieved_power"),
            "monthly_revenue_impact": report["business_impact"].get("monthly_revenue_impact"),
        }),
        "bayesian": _to_json_safe({
            "prob_treatment_better": bayes_result.prob_treatment_better,
            "expected_loss_treatment": bayes_result.expected_loss_treatment,
            "recommendation": bayes.recommend(bayes_result)["decision"],
        }),
        "multi_metric": _to_json_safe({
            "decision": multi_result["decision"],
            "primary_significant": (
                bool(multi_result["primary"]["significant"].all())
                if len(multi_result.get("primary", [])) > 0 else None
            ),
        }),
        "recommendation": rec["decision"],
        "p_value": float(report["hypothesis_test"]["p_value"]),
        "revenue_impact_monthly": float(report["business_impact"].get("monthly_revenue_impact", 0) or 0),
    }


def _make_experiment_analysis_asset(experiment_id: str):
    """Factory for experiment analysis assets."""

    @asset(
        name=f"experiment_{experiment_id.replace('-', '_')}_analysis",
        description=f"Statistical analysis for {experiment_id} (frequentist + Bayesian + multi-metric).",
        compute_kind="python",
        group_name="analysis",
    )
    def _asset(
        context,
        raw_experiment_assignments: pd.DataFrame,
        stg_orders: pd.DataFrame,
        stg_order_items: pd.DataFrame,
        stg_reviews: pd.DataFrame,
    ) -> dict[str, Any]:
        result = _run_single_experiment_analysis(
            experiment_id=experiment_id,
            raw_experiment_assignments=raw_experiment_assignments,
            stg_orders=stg_orders,
            stg_order_items=stg_order_items,
            stg_reviews=stg_reviews,
        )

        # Persist JSON to output/analysis/
        ANALYSIS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = ANALYSIS_OUTPUT_DIR / f"{experiment_id}_results.json"
        with open(out_path, "w") as f:
            json.dump(result, f, indent=2)

        context.add_output_metadata({
            "notebook_path": f"notebooks/{experiment_id}_analysis.ipynb",
            "p_value": result["p_value"],
            "recommendation": result["recommendation"],
            "revenue_impact_monthly": result["revenue_impact_monthly"],
            "output_path": str(out_path),
        })

        return result

    return _asset


# Build assets for all 10 experiments
EXPERIMENT_IDS = [
    "exp_001", "exp_002", "exp_003", "exp_004", "exp_005",
    "exp_006", "exp_007", "exp_008", "exp_009", "exp_010",
]

experiment_01_analysis = _make_experiment_analysis_asset("exp_001")
experiment_02_analysis = _make_experiment_analysis_asset("exp_002")
experiment_03_analysis = _make_experiment_analysis_asset("exp_003")
experiment_04_analysis = _make_experiment_analysis_asset("exp_004")
experiment_05_analysis = _make_experiment_analysis_asset("exp_005")
experiment_06_analysis = _make_experiment_analysis_asset("exp_006")
experiment_07_analysis = _make_experiment_analysis_asset("exp_007")
experiment_08_analysis = _make_experiment_analysis_asset("exp_008")
experiment_09_analysis = _make_experiment_analysis_asset("exp_009")
experiment_10_analysis = _make_experiment_analysis_asset("exp_010")

EXPERIMENT_ANALYSIS_ASSETS = [
    experiment_01_analysis,
    experiment_02_analysis,
    experiment_03_analysis,
    experiment_04_analysis,
    experiment_05_analysis,
    experiment_06_analysis,
    experiment_07_analysis,
    experiment_08_analysis,
    experiment_09_analysis,
    experiment_10_analysis,
]


@asset(
    description="Executive summary aggregating statistical results from all 10 experiments.",
    compute_kind="python",
    group_name="analysis",
)
def all_experiments_summary(
    experiment_exp_001_analysis: dict,
    experiment_exp_002_analysis: dict,
    experiment_exp_003_analysis: dict,
    experiment_exp_004_analysis: dict,
    experiment_exp_005_analysis: dict,
    experiment_exp_006_analysis: dict,
    experiment_exp_007_analysis: dict,
    experiment_exp_008_analysis: dict,
    experiment_exp_009_analysis: dict,
    experiment_exp_010_analysis: dict,
) -> dict[str, Any]:
    """Aggregate results from all experiment analyses into an executive summary."""
    results = [
        experiment_exp_001_analysis,
        experiment_exp_002_analysis,
        experiment_exp_003_analysis,
        experiment_exp_004_analysis,
        experiment_exp_005_analysis,
        experiment_exp_006_analysis,
        experiment_exp_007_analysis,
        experiment_exp_008_analysis,
        experiment_exp_009_analysis,
        experiment_exp_010_analysis,
    ]

    launched = sum(1 for r in results if r["recommendation"] == "LAUNCH")
    abandoned = sum(1 for r in results if r["recommendation"] == "ABANDON")
    iterate = sum(1 for r in results if r["recommendation"] == "ITERATE")
    inconclusive = sum(1 for r in results if r["recommendation"] == "INCONCLUSIVE")
    significant = sum(1 for r in results if r["frequentist"].get("significant"))

    total_revenue_impact = sum(r["revenue_impact_monthly"] for r in results)

    summary = {
        "total_experiments": 10,
        "statistically_significant": significant,
        "launched": launched,
        "abandoned": abandoned,
        "iterate": iterate,
        "inconclusive": inconclusive,
        "testing_continue": iterate + inconclusive,
        "total_revenue_impact_monthly": round(total_revenue_impact, 2),
        "total_revenue_impact_annual": round(total_revenue_impact * 12, 2),
        "experiments": [
            {
                "experiment_id": r["experiment_id"],
                "name": r["experiment_name"],
                "recommendation": r["recommendation"],
                "p_value": r["p_value"],
                "significant": r["frequentist"].get("significant"),
                "revenue_impact_monthly": r["revenue_impact_monthly"],
            }
            for r in results
        ],
    }

    # Persist summary
    ANALYSIS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = ANALYSIS_OUTPUT_DIR / "all_experiments_summary.json"
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)

    logger.info(
        "all_experiments_summary: %d launched, %d abandoned, %d iterate, %d inconclusive, "
        "total revenue impact $%.0f/month",
        launched, abandoned, iterate, inconclusive, total_revenue_impact,
    )

    return summary


# ── Papermill (notebook execution) ──────────────────────────────────────────


def _papermill_asset_impl(
    context,
    raw_experiment_assignments: pd.DataFrame,
    stg_orders: pd.DataFrame,
    stg_order_items: pd.DataFrame,
    stg_reviews: pd.DataFrame,
) -> dict[str, Any]:
    """Execute exp_001 notebook via Papermill; save executed notebook + HTML."""
    try:
        import papermill as pm
    except ImportError:
        raise ImportError("papermill is required for notebook execution. pip install papermill")

    notebook_path = PROJECT_ROOT / "notebooks" / "exp_001_free_shipping_threshold.ipynb"
    if not notebook_path.exists():
        raise FileNotFoundError(f"Notebook not found: {notebook_path}")

    executed_dir = ANALYSIS_OUTPUT_DIR / "executed"
    executed_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M")
    out_notebook = executed_dir / f"exp_001_{ts}.ipynb"

    pm.execute_notebook(
        str(notebook_path),
        str(out_notebook),
        parameters={},
        kernel_name="python3",
        log_output=True,
    )

    # Optional: convert to HTML for viewing
    html_path = None
    try:
        from nbconvert import HTMLExporter
        html_path = out_notebook.with_suffix(".html")
        (HTMLExporter().from_filename(str(out_notebook))).write(str(html_path))
    except Exception:
        pass

    return {
        "notebook_path": str(notebook_path),
        "executed_path": str(out_notebook),
        "html_path": str(html_path) if html_path else None,
    }


# Papermill asset: runs exp_001 notebook (requires papermill, nbconvert)
@asset(
    name="experiment_exp_001_notebook",
    description="Execute exp_001 notebook via Papermill (HTML + executed .ipynb).",
    compute_kind="papermill",
    group_name="analysis",
)
def experiment_exp_001_notebook(
    context,
    raw_experiment_assignments: pd.DataFrame,
    stg_orders: pd.DataFrame,
    stg_order_items: pd.DataFrame,
    stg_reviews: pd.DataFrame,
) -> dict[str, Any]:
    result = _papermill_asset_impl(
        context, raw_experiment_assignments, stg_orders, stg_order_items, stg_reviews
    )
    context.add_output_metadata({
        "executed_path": result["executed_path"],
        "html_path": result.get("html_path"),
    })
    return result
