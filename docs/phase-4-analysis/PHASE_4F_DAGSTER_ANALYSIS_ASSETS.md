# Dagster Analysis Assets — Automated Experiment Evaluation Pipeline

> Statistical analysis integrated into the Dagster orchestration layer so that experiment evaluation runs automatically on schedule, not manually in notebooks, with full lineage from raw data to launch recommendations.

---

## Overview

Jupyter notebooks are excellent for exploratory analysis and presentation. They are terrible for production-grade automated pipelines. Notebooks require manual execution, have hidden state issues, and don't integrate with scheduling, alerting, or dependency management.

This module encodes the statistical analysis workflow as **Dagster assets** — first-class data objects with declared dependencies, automated materialization, metadata tracking, and a visual lineage graph. Each experiment's analysis becomes a reproducible, schedulable computation that feeds into a portfolio summary and downstream reporting.

Both Python-function assets (production) and Papermill notebook assets (presentation) are supported, serving different audiences with the same underlying data.

---

## Asset Lineage

The dependency graph flows from raw data through staging to analysis and aggregation:

```
raw_experiment_assignments ─┐
stg_orders ─────────────────┼──► experiment_exp_001_analysis ──┐
stg_order_items ────────────┤    experiment_exp_002_analysis ───┤
stg_reviews ────────────────┘    ...                           ├──► all_experiments_summary
                                 experiment_exp_010_analysis ──┘
```

> **Why lineage matters:** When `stg_orders` is refreshed with new data, Dagster knows that all 10 experiment analyses and the summary are stale. One command re-materializes the entire chain. Without lineage, an analyst would need to manually re-run 11 assets in the correct order.

---

## Asset Definitions

### Experiment Analysis Assets (Python Functions)

Each `experiment_exp_XX_analysis` asset encapsulates the full analysis pipeline:

| Aspect | Detail |
|--------|--------|
| **Dependencies** | `raw_experiment_assignments`, `stg_orders`, `stg_order_items`, `stg_reviews` |
| **Computation** | Runs `ExperimentAnalyzer` (frequentist), `BayesianAnalyzer`, and `MultiMetricAnalyzer` |
| **Output (file)** | `output/analysis/exp_XXX_results.json` |
| **Output (Dagster)** | Dict returned to IOManager for downstream consumption |
| **Metadata** | `p_value`, `recommendation`, `revenue_impact_monthly`, `output_path` |

The metadata is surfaced in the Dagit UI, enabling operational visibility without opening JSON files:

```
experiment_exp_001_analysis
  p_value: 1.12e-11
  recommendation: LAUNCH
  revenue_impact_monthly: R$199,000
```

### All Experiments Summary

| Aspect | Detail |
|--------|--------|
| **Dependencies** | All 10 `experiment_exp_XXX_analysis` assets |
| **Computation** | Aggregates results, applies cross-experiment FDR correction |
| **Output** | `output/analysis/all_experiments_summary.json` |
| **Key fields** | Launch count, iterate count, abandon count, total revenue impact |

This asset is the single source of truth for the experiment portfolio. Downstream consumers (weekly reports, dashboards, executive summaries) depend on it rather than querying individual experiment results.

### Papermill Notebook Asset (Presentation)

| Aspect | Detail |
|--------|--------|
| **Asset** | `experiment_exp_001_notebook` |
| **Dependencies** | Same upstream data as experiment analysis |
| **Computation** | `papermill.execute_notebook()` on the Jupyter template |
| **Output** | Timestamped `.ipynb` and `.html` in `output/analysis/executed/` |

Papermill assets serve a different audience than function assets. Executives and product managers want a rendered notebook with charts. The data engineering team wants JSON. Both are derived from the same pipeline.

---

## Scheduling

| Schedule | Cron Expression | Job | Description |
|----------|----------------|-----|-------------|
| `weekly_analysis_schedule` | `0 6 * * 0` | `weekly_statistical_analysis` | Sundays 06:00 (Sao Paulo timezone) |

The schedule triggers materialization of all 10 experiment analysis assets, the summary, and optionally the Papermill notebook. This cadence ensures results are fresh for Monday morning reviews.

---

## Why Dagster Over Alternatives

| Consideration | Dagster Assets | Airflow Tasks | Manual Notebooks |
|---------------|---------------|---------------|-----------------|
| Dependency tracking | Native; declared in code | Manual; DAG-level only | None |
| Incremental re-computation | Materializes only stale assets | Re-runs entire DAG | Re-runs everything manually |
| Metadata in UI | p-values, recommendations visible | Logs only | Must open notebook |
| Testability | Standard Python unit tests | Task-level testing | Difficult |
| Data lineage | Visual graph in Dagit | DAG view (task-level) | None |
| Notebook support | Papermill integration | Possible but not native | Native but manual |

> **Design rationale:** The experiment analysis is computationally light (seconds per experiment). The value of Dagster is not parallelism or scale — it's **lineage, scheduling, and metadata**. When a stakeholder asks "when was the last analysis run and what did it find?", the answer is in Dagit, not in someone's notebook.

---

## Notebook Versioning

Executed notebooks are timestamped for auditability:

```
output/analysis/executed/
  exp_001_2026-03-02_0600.ipynb
  exp_001_2026-03-02_0600.html
  exp_001_2026-03-09_0600.ipynb
  exp_001_2026-03-09_0600.html
```

Dagster tracks materialization history per asset, providing a complete timeline of when each analysis was run and what the inputs were.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Python functions as primary | JSON output + Dagster metadata | Testable, fast, CI-friendly |
| Papermill as secondary | Rendered notebooks for stakeholders | Visual presentation without manual execution |
| One asset per experiment | `experiment_exp_XXX_analysis` | Independent materialization; clear lineage |
| Summary as separate asset | `all_experiments_summary` | Aggregation logic is distinct from per-experiment analysis |
| Weekly schedule | Sunday 06:00 | Fresh results for Monday standup; avoids weekday compute contention |

---

## Production Considerations

- **Failure isolation:** If one experiment's analysis fails (e.g., missing data), the other 9 still materialize. The summary asset handles missing inputs gracefully by reporting the failure.
- **Idempotency:** Assets produce the same output given the same inputs. Re-materialization is safe and doesn't create duplicates.
- **Cost:** Each materialization reads from BigQuery staging tables. With 10 experiments at ~50K rows each, the query cost is negligible (<$0.01 per run on BigQuery's free tier).
- **Alerting:** Dagster sensors can trigger alerts if a materialization fails or if a key metric (e.g., guardrail violation) changes unexpectedly. The weekly report pipeline (Phase 4I) consumes the summary asset downstream.
- **Scaling:** The current architecture handles 10 experiments trivially. For 100+ experiments, consider batching experiments into groups and parallelizing materialization.

---

*Last updated: March 2026*
