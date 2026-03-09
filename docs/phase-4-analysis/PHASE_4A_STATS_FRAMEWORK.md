# Statistical Analysis Framework — Test Selection, Significance, and Automated Evaluation

> A reusable statistical framework that selects the right hypothesis test for each metric type, enforces pre-registered significance thresholds, and produces automated launch/abandon recommendations across 10 e-commerce A/B experiments.

---

## Overview

Every A/B test ultimately reduces to a question: **did this change cause a real difference, or is it noise?** Answering that question incorrectly in either direction has real cost. A false positive ships a feature that doesn't help (or actively hurts). A false negative kills a feature that would have generated revenue.

This framework eliminates ad-hoc statistical choices by encoding a **decision tree** that selects the appropriate test based on metric type and data characteristics, applies pre-registered thresholds, and outputs a structured recommendation. The goal is reproducibility: two analysts running the same experiment should always reach the same conclusion.

---

## Statistical Test Decision Tree

Choosing the wrong test invalidates results. The decision tree below is encoded in `ExperimentAnalyzer` and executes automatically — no manual test selection required.

```
What is the metric type?
│
├── BINARY (conversion, click-through, yes/no)
│   │
│   ├── n >= 30 per variant
│   │   └── Z-test for two proportions
│   │       (Normal approximation to binomial is valid)
│   │
│   └── n < 30 per variant
│       └── Fisher's exact test
│           (Small-sample exact computation)
│
├── CONTINUOUS (revenue, AOV, time-on-site)
│   │
│   ├── Shapiro-Wilk p >= 0.05 (cannot reject normality)
│   │   └── Welch's t-test (unequal variance by default)
│   │
│   └── Shapiro-Wilk p < 0.05 (non-normal distribution)
│       └── Mann-Whitney U test (rank-based, no distributional assumption)
│
└── COUNT (items per order, events per session)
    └── Chi-square or Poisson test
```

> **Why Welch's over Student's t-test:** Welch's t-test does not assume equal variance between groups and performs well even when variances are equal. It dominates Student's t-test in practice, which is why most modern experimentation platforms (Optimizely, Statsig) default to it.

### Test Implementation Reference

| Test | Function | Library | When to Use |
|------|----------|---------|-------------|
| Z-test for proportions | `proportions_ztest` | `statsmodels.stats.proportion` | Binary metrics, large samples |
| Fisher's exact | `fisher_exact` | `scipy.stats` | Binary metrics, small samples |
| Chi-square | `chi2_contingency` | `scipy.stats` | Contingency table cross-validation |
| Welch's t-test | `ttest_ind(equal_var=False)` | `scipy.stats` | Continuous metrics, normal data |
| Mann-Whitney U | `mannwhitneyu` | `scipy.stats` | Continuous metrics, non-normal data |
| Multiple testing | `multipletests` | `statsmodels.stats.multitest` | Cross-experiment correction |

### Assumption Checks (Automated)

| Check | Test | Threshold | Fallback |
|-------|------|-----------|----------|
| Normality | Shapiro-Wilk | p < 0.05 rejects normality | Mann-Whitney U |
| Equal variance | Levene's test | p < 0.05 rejects equality | Welch's t-test (already default) |
| Randomization balance | Chi-square on variant counts | p < 0.05 flags SRM | Manual investigation |

---

## Significance Thresholds

These thresholds are pre-registered, not chosen after seeing data. Changing them post-hoc is a form of p-hacking.

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Alpha (Type I error) | 0.05 | Industry standard; 1-in-20 false positive rate |
| Confidence level | 95% | Complement of alpha |
| Test sidedness | Two-tailed | No directional prior; treatment could be worse |
| Minimum practical lift | 10% relative | Below this, implementation cost exceeds benefit |
| Target power (1 - beta) | 80% | Balances sample size requirements with detection ability |
| Multiple testing correction | Benjamini-Hochberg FDR | Controls false discovery rate across experiment portfolio |

> **Why two-tailed:** One-tailed tests increase power but assume treatment can only help. In e-commerce, a checkout change that removes friction might also remove trust signals. Two-tailed tests protect against detecting harm.

> **Why 10% minimum lift:** For a 12% baseline conversion rate, 10% relative lift is 1.2 percentage points. Smaller effects are real but often not worth the engineering effort to ship and maintain.

---

## 9-Section Analysis Workflow

Each experiment is evaluated through a standardized pipeline. This structure mirrors what companies like Booking.com and Airbnb use internally — a fixed protocol prevents cherry-picking analyses.

### Section 1: Experiment Context

Loads experiment metadata from the catalog. Displays hypothesis, variants, MDE, sample targets, and guardrail metrics. The hypothesis is declared **before** data examination — a critical safeguard against HARKing.

### Section 2: Data Extraction

Joins assignment data with orders, items, and reviews. Runs a **randomization balance check** (chi-square on variant split) to detect Sample Ratio Mismatch before any analysis begins. An SRM invalidates all downstream results.

### Section 3: Descriptive Statistics

Sample sizes, conversion rates, and revenue summaries per variant. Visualizations (conversion bar charts, revenue histograms, box plots) provide intuition before formal testing.

### Section 4: Statistical Testing

The decision tree selects exactly one test — no shopping across tests for a favorable p-value. Reports the test statistic, p-value, and the automated assumption checks that led to test selection.

### Section 5: Confidence Intervals

95% CIs for absolute and relative lift. Wald interval for proportions, Welch-Satterthwaite for means. The CI width communicates precision: a statistically significant result with a wide CI spanning 1% to 30% lift tells a very different story than one spanning 10% to 14%.

### Section 6: Effect Size

Cohen's h (proportions) or Cohen's d (continuous). Reports magnitude classification (negligible / small / medium / large) and a practical significance check against the minimum lift threshold.

> **Why effect size matters separately:** A p-value of 0.001 with Cohen's h of 0.01 means you detected a real but tiny effect. Statistical significance without practical significance wastes engineering resources.

### Section 7: Power Analysis

Retrospective achieved power, sample adequacy check, minimum sample size for 80% power, and a power curve showing how power varies with sample size. Underpowered non-significant results are labeled **inconclusive**, not "no effect."

### Section 8: Business Impact

Projects monthly and annual revenue impact using configurable traffic assumptions. Includes CI-bounded range for the projection — executives should see the best-case and worst-case, not just the point estimate.

### Section 9: Recommendation

Automated decision engine with four outcomes:

| Decision | Criteria |
|----------|----------|
| **LAUNCH** | Statistically significant AND practical lift >= threshold AND positive direction |
| **ITERATE** | Statistically significant AND positive but lift below practical threshold |
| **INCONCLUSIVE** | Not significant AND underpowered (test was too small to detect the effect) |
| **ABANDON** | Not significant with adequate power, or significant but negative direction |

---

## Conversion Simulation Design

Experiment assignments are synthetic overlays on the Olist e-commerce dataset. Conversion outcomes are simulated deterministically to enable reproducible analysis:

```
hash = SHA-256(customer_id | experiment_id | "outcome")
probability = hash mod 10,000 / 10,000

threshold_control   = baseline_rate
threshold_treatment = baseline_rate * (1 + MDE / 100)

converted = probability < threshold
```

| Property | Why It Matters |
|----------|----------------|
| **Deterministic** | Same customer always gets the same outcome across runs |
| **Reproducible** | Any analyst re-running the analysis gets identical results |
| **Realistic** | Rates match experiment design (e.g., 12% baseline, ~13.2% treatment) |
| **Variant-aware** | Treatment group has a higher conversion rate by design |

---

## Common Pitfalls Addressed

| Pitfall | How This Framework Prevents It |
|---------|-------------------------------|
| **Peeking** | Sample sizes pre-defined in catalog; power analysis validates adequacy before analysis |
| **HARKing** | Hypothesis declared in Section 1 before data examination |
| **P-hacking** | Decision tree selects exactly one test per metric; no shopping |
| **Multiple testing** | FDR correction applied across all 10 experiments |
| **Statistical != practical** | Separate significance and practical-lift checks in Sections 4 and 6 |
| **Assumption violations** | Shapiro-Wilk and Levene's tests checked; non-parametric fallback automated |
| **Underpowered conclusions** | Section 7 flags inadequate power; non-significant results labeled inconclusive |

> **Industry context:** Booking.com runs thousands of experiments per year and found that most "successful" experiments from underpowered tests failed to replicate. Pre-registered power analysis and fixed decision rules are the antidote.

---

## Verified Results — All 10 Experiments

| # | Experiment | CR Control | CR Treatment | p-value | Relative Lift | Power | Decision |
|---|------------|------------|--------------|---------|---------------|-------|----------|
| 1 | Free Shipping Threshold | 12.01% | 13.45% | 1.12e-11 | +12.0% | 1.00 | LAUNCH |
| 2 | Payment Installments | 8.08% | 9.30% | 7.29e-12 | +15.1% | 1.00 | LAUNCH |
| 3 | One-Page Checkout | 14.96% | 15.85% | 1.05e-04 | +6.0% | 0.97 | ITERATE |
| 4 | Review Solicitation | 21.95% | 26.30% | 6.60e-58 | +19.8% | 1.00 | LAUNCH |
| 5 | Product Recommendations | 9.88% | 11.13% | 1.06e-10 | +12.7% | 1.00 | LAUNCH |
| 6 | Delivery Speed Options | 14.20% | 15.24% | 3.86e-06 | +7.3% | 1.00 | ITERATE |
| 7 | Seller Ratings Display | 10.94% | 12.06% | 2.84e-08 | +10.2% | 1.00 | LAUNCH |
| 8 | Bundle Discounts | 24.90% | 29.07% | 6.09e-48 | +15.2% | 1.00 | LAUNCH |
| 9 | Guest Checkout | 8.93% | 10.28% | 5.65e-13 | +15.1% | 1.00 | LAUNCH |
| 10 | Dynamic Pricing | 13.01% | 13.56% | 1.26e-02 | +7.5% | 0.65 | ITERATE |

All 10 experiments survive Benjamini-Hochberg FDR correction at alpha = 0.05.

> **Critical observation:** Experiment 10 (Dynamic Pricing) has only 65% achieved power. Its +7.5% lift is real but the test was borderline underpowered — a 5% MDE with a 13% baseline requires ~43K users per variant. Had this result been non-significant, the correct conclusion would be **inconclusive**, not "pricing doesn't matter."

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Automated test selection | Decision tree in code | Eliminates analyst discretion; prevents test shopping |
| Two-tailed by default | No directional assumption | Treatments can harm; one-tailed masks negative effects |
| Welch's over Student's | Unequal variance default | Robust regardless of variance equality; no downside |
| FDR over Bonferroni | Benjamini-Hochberg | Better power for 10+ simultaneous experiments |
| Practical significance gate | 10% relative lift minimum | Prevents shipping trivially small improvements |
| Deterministic simulation | SHA-256 hashing | Reproducible across environments and re-runs |

---

## Production Considerations

- **Scaling:** The framework processes ~50K users per experiment in seconds. For millions of users, switch to BigQuery SQL-based test statistics to avoid data transfer.
- **Monitoring:** Dagster asset metadata exposes p-values and recommendations in the Dagit UI for operational visibility.
- **Freshness:** Weekly schedule ensures results reflect the latest data without manual intervention.
- **Failure modes:** If data extraction returns zero rows (e.g., BigQuery downtime), the analyzer raises an explicit error rather than producing misleading results.
- **Extensibility:** Adding a new experiment requires only a catalog entry — the framework auto-discovers and analyzes it.

---

## Programmatic API

```python
from ecommerce_analytics.analysis import (
    ExperimentCatalog,
    ExperimentAnalyzer,
    load_experiment_data,
    correct_multiple_tests,
)

catalog = ExperimentCatalog()
data = load_experiment_data("exp_001")
meta = catalog.get("exp_001")

analyzer = ExperimentAnalyzer(data, meta)
report = analyzer.full_report()
```

| Class / Function | Purpose |
|------------------|---------|
| `ExperimentCatalog` | Load and query experiment metadata |
| `load_experiment_data(exp_id)` | Build analysis-ready DataFrame with simulated outcomes |
| `ExperimentAnalyzer` | Full 9-section analysis pipeline |
| `correct_multiple_tests(p_values)` | Benjamini-Hochberg FDR correction |
| `cohens_d`, `cohens_h` | Effect size calculations |
| `required_sample_size` | Prospective sample size estimation |

---

*Last updated: March 2026*
