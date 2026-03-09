# Excel Statistical Analysis Workbook — Power Query, Formulas, and Executive Dashboard

> A fully-structured Excel workbook that demonstrates A/B test analysis using native Excel capabilities: Power Query for data loading, statistical formulas for hypothesis testing, conditional formatting for decision visualization, and an executive dashboard summarizing the experiment portfolio.

---

## Overview

Not every stakeholder has access to Python, BigQuery, or Dagit. Excel remains the universal language of business analysis. This workbook bridges the gap between the statistical analysis pipeline and business consumers who need to explore, validate, and present experiment results in a familiar environment.

The workbook is generated programmatically from experiment results, ensuring consistency with the Python analysis pipeline. It includes formulas that replicate the core statistical tests (Z-test, t-test, confidence intervals) so that results can be verified independently of the codebase.

---

## Workbook Architecture

### Sheet 1: Data_Import

Power Query connection point for refreshable data loading.

| Component | Purpose |
|-----------|---------|
| Power Query source | `experiment_results.csv` exported from the analysis pipeline |
| Refresh strategy | Manual or on-open; M code template provided |
| Data path | `output/analysis/experiment_results.csv` |

Power Query enables the workbook to refresh with updated results without manual copy-paste. When the analysis pipeline produces new CSV output, opening the workbook (or clicking Refresh) pulls the latest data.

### Sheet 2: Experiment_Results

Raw experiment data formatted as an Excel Table (`ExperimentResults`).

| Column | Description | Example |
|--------|-------------|---------|
| Experiment | Experiment name | Free Shipping Threshold |
| Variant | Control or Treatment | Treatment |
| Users | Sample size | 49,659 |
| Conversions | Converted users | 6,679 |
| Conv_Rate | Conversion rate (decimal) | 0.1345 |
| Revenue | Total revenue (R$) | R$927,000 |
| AOV | Average order value | R$138.79 |
| Lift_% | Relative lift vs. control | 11.95% |

### Sheet 3: Statistical_Tests

Replicates the Python analysis using native Excel formulas. This serves two purposes: independent verification, and demonstration that the statistical concepts work outside of a coding environment.

**Key formulas implemented:**

| Test | Excel Formula |
|------|--------------|
| Z-statistic | `=(p_treat - p_ctrl) / SQRT(p_pool * (1 - p_pool) * (1/n1 + 1/n2))` |
| Two-tailed p-value | `=2 * (1 - NORM.S.DIST(ABS(z_score), TRUE))` |
| Chi-square test | `=CHISQ.TEST(actual_range, expected_range)` |
| T-test (revenue) | `=T.TEST(control_revenue, treatment_revenue, 2, 2)` |
| 95% CI half-width | `=CONFIDENCE.NORM(0.05, STDEV.S(range), COUNT(range))` |

> **Why replicate in Excel:** Stakeholders often ask "how did you calculate this?" Showing the formula in a cell they can click on builds trust in a way that a Python script cannot. It also validates the pipeline — if Excel and Python disagree, something is wrong.

### Sheet 4: Charts

Visual comparison of experiment results using native Excel charts.

| Chart | Type | Content |
|-------|------|---------|
| Conversion Rate Comparison | Grouped bar | Control vs. Treatment for each experiment |
| Lift by Experiment | Sorted bar | Relative lift (%) across all 10 experiments |
| P-Value Distribution | Bar with threshold line | All p-values with alpha = 0.05 reference |

### Sheet 5: Executive_Summary

A single-page dashboard designed for leadership review.

| Component | Detail |
|-----------|--------|
| KPI tiles | Total experiments, significant count, launch recommendations, total revenue impact |
| Summary table | Experiment, p-value, significance (Yes/No), recommendation, monthly revenue impact |
| Conditional formatting | Green cells for p < 0.05 (significant), red for p >= 0.05 |
| Decision column | LAUNCH / ITERATE / ABANDON with color coding |

---

## Data Pipeline Integration

The workbook sits at the end of the data pipeline, consuming output from the Dagster analysis assets:

```
BigQuery staging tables
    → Dagster experiment analysis assets
        → all_experiments_summary.json
            → export_experiment_csv.py
                → experiment_results.csv
                    → Excel Power Query refresh
```

### CSV Export

```bash
python scripts/export_experiment_csv.py
```

This script reads the analysis results and produces a flat CSV optimized for Power Query consumption. Column names, data types, and formatting are designed for clean import without manual transformation.

---

## Power Query Configuration

### M Code Template

A Power Query M code template is provided at `output/analysis/power_query_m_code.pq`. To set up:

1. In Excel: **Data** > **Get Data** > **From File** > **From Text/CSV**
2. Select `output/analysis/experiment_results.csv`
3. Click **Transform Data** > **Advanced Editor**
4. Replace the auto-generated code with the M template
5. Adjust the file path in the first line
6. Click **Close & Load**

The M code handles type inference, column renaming, and null handling so the loaded table matches the expected schema exactly.

---

## Excel Formulas Reference

| Formula | Purpose | Notes |
|---------|---------|-------|
| `=T.TEST(range1, range2, 2, 2)` | Two-sample t-test | Tails=2 (two-tailed), Type=2 (equal variance) |
| `=CHISQ.TEST(actual, expected)` | Chi-square test | For contingency tables |
| `=CONFIDENCE.NORM(0.05, STDEV.S(r), COUNT(r))` | 95% CI half-width | For normally distributed metrics |
| `=NORM.S.DIST(z, TRUE)` | Standard normal CDF | Building block for p-values |
| `=2*(1-NORM.S.DIST(ABS(z), TRUE))` | Two-tailed p-value | From Z-statistic |
| `=INDEX(col, MATCH(1, (A:A=x)*(B:B="Control"), 0))` | Multi-criteria lookup | Retrieves control values for comparison |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| CSV over ODBC for BigQuery | Simplicity; no driver installation | Portfolio project; ODBC adds complexity for reviewers |
| Power Query over VBA | Modern, declarative, auditable | VBA is fragile and harder to review |
| Replicate stats in formulas | Independent verification | Builds trust; demonstrates Excel proficiency |
| Programmatic workbook generation | `openpyxl` via Python script | Consistent formatting; reproducible from CI |
| Single-page executive summary | Dashboard pattern | Leadership doesn't scroll through 5 sheets |

---

## Production Considerations

- **Refresh automation:** In a production environment, Windows Task Scheduler can run `export_experiment_csv.py` daily. The workbook refreshes on open if configured under **Data > Connections > Properties > Refresh data when opening the file**.
- **Distribution:** The generated `.xlsx` file can be attached to the weekly report email (Phase 4I) or uploaded to SharePoint/OneDrive for self-service access.
- **Limitations:** Excel formulas cannot replicate the full Bayesian analysis or multiple testing corrections. These are documented in the workbook with references to the Python pipeline for the complete analysis.
- **Version control:** The workbook is generated, not hand-edited. The generation script (`build_excel_workbook.py`) is version-controlled; the `.xlsx` output is treated as a build artifact.
- **Accessibility:** For stakeholders who prefer Google Sheets, the CSV can be loaded into Sheets with a `=IMPORTDATA()` formula or via BigQuery's Sheets connector.

---

*Last updated: March 2026*
