"""Shared data gathering for Excel workbook and CSV export."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def gather_experiment_data():
    """Build variant-level and summary DataFrames for all experiments."""
    from ecommerce_analytics.analysis import ExperimentCatalog, load_experiment_data
    from ecommerce_analytics.analysis.stats_framework import ExperimentAnalyzer

    catalog = ExperimentCatalog()
    variant_rows = []
    summary_rows = []

    for exp_id in catalog.list_ids():
        meta = catalog.get(exp_id)
        try:
            data = load_experiment_data(exp_id)
        except Exception:
            continue

        for variant in ["control", "treatment"]:
            grp = data[data["variant"] == variant]
            n = len(grp)
            conv = int(grp["converted"].sum())
            cr = conv / n * 100 if n else 0
            rev = grp["order_total"].sum()
            aov = grp.loc[grp["converted"], "order_total"].mean() if conv else 0

            variant_rows.append({
                "Experiment": meta.name,
                "Experiment_ID": exp_id,
                "Variant": variant.capitalize(),
                "Users": n,
                "Conversions": conv,
                "Conv_Rate": round(cr, 2),
                "Revenue": round(rev, 2),
                "AOV": round(aov, 2),
            })

        analyzer = ExperimentAnalyzer(data, meta)
        report = analyzer.full_report()
        rec = report["recommendation"]
        pval = report["hypothesis_test"]["p_value"]
        rev_impact = report["business_impact"].get("monthly_revenue_impact", 0) or 0

        summary_rows.append({
            "Experiment_ID": exp_id,
            "Experiment": meta.name,
            "P_Value": pval,
            "Significant": "Yes" if pval < 0.05 else "No",
            "Recommendation": rec["decision"],
            "Revenue_Impact_Monthly": round(rev_impact, 2),
        })

    import pandas as pd
    return pd.DataFrame(variant_rows), pd.DataFrame(summary_rows)
