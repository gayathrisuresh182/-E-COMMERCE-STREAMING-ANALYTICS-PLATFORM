"""Statistical analysis framework for A/B experiment evaluation.

Provides:
- ``ExperimentCatalog`` – loads experiment metadata from the JSON catalog.
- ``load_experiment_data`` – joins assignments with orders to build an
  analysis-ready DataFrame.
- ``ExperimentAnalyzer`` – runs the full statistical workflow (descriptive
  stats, hypothesis tests, confidence intervals, effect sizes, power analysis,
  and business-impact projection).
"""

from __future__ import annotations

import json
import math
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.stats.power import NormalIndPower, TTestIndPower
from statsmodels.stats.proportion import proportions_ztest

ROOT = Path(__file__).resolve().parents[2]

# ── Experiment catalog ──────────────────────────────────────────────────────

CATALOG_PATH = ROOT / "docs" / "experiment_catalog.json"
ASSIGNMENTS_PATH = ROOT / "output" / "experiment_assignments.csv"

# Olist staging data lives under the Dagster storage directory after
# materialisation; fall back to raw CSVs for standalone notebook use.
_DAGSTER_STORAGE = ROOT / "storage"


@dataclass
class ExperimentMeta:
    """Metadata for a single experiment from the catalog."""

    id: str
    name: str
    primary_metric: str
    mde_percent: float
    mde_type: str
    test_type: str  # "chi_square" | "t_test"
    sample_per_variant: int
    randomization_unit: str
    split: str
    variants: list[str]
    guardrail_metrics: list[str]


class ExperimentCatalog:
    """Loads and queries the experiment catalog JSON."""

    def __init__(self, path: str | Path = CATALOG_PATH) -> None:
        with open(path) as f:
            raw = json.load(f)
        self._experiments: dict[str, ExperimentMeta] = {}
        for exp in raw["experiments"]:
            meta = ExperimentMeta(**exp)
            self._experiments[meta.id] = meta

    def get(self, experiment_id: str) -> ExperimentMeta:
        return self._experiments[experiment_id]

    def list_ids(self) -> list[str]:
        return sorted(self._experiments.keys())

    def summary(self) -> pd.DataFrame:
        rows = []
        for m in self._experiments.values():
            rows.append(
                {
                    "experiment_id": m.id,
                    "name": m.name,
                    "primary_metric": m.primary_metric,
                    "test_type": m.test_type,
                    "mde_%": m.mde_percent,
                    "sample/variant": m.sample_per_variant,
                }
            )
        return pd.DataFrame(rows)


# ── Data loading ────────────────────────────────────────────────────────────


def _resolve_orders() -> pd.DataFrame:
    """Load stg_orders from Dagster storage or raw CSV fallback."""
    dagster_path = _DAGSTER_STORAGE / "stg_orders"
    if dagster_path.exists():
        return pd.read_pickle(next(dagster_path.glob("*")))

    for candidate in [
        ROOT / "raw" / "olist" / "olist_orders_dataset.csv",
        ROOT / "raw" / "olist_orders_dataset.csv",
    ]:
        if candidate.exists():
            return pd.read_csv(candidate)

    raise FileNotFoundError(
        "Cannot locate orders data. Materialise stg_orders in Dagster "
        "or place olist_orders_dataset.csv in raw/ or raw/olist/."
    )


def _resolve_order_items() -> pd.DataFrame:
    dagster_path = _DAGSTER_STORAGE / "stg_order_items"
    if dagster_path.exists():
        return pd.read_pickle(next(dagster_path.glob("*")))

    for candidate in [
        ROOT / "raw" / "olist" / "olist_order_items_dataset.csv",
        ROOT / "raw" / "olist_order_items_dataset.csv",
    ]:
        if candidate.exists():
            return pd.read_csv(candidate)

    raise FileNotFoundError(
        "Cannot locate order-items data. Materialise stg_order_items in "
        "Dagster or place olist_order_items_dataset.csv in raw/ or raw/olist/."
    )


def _resolve_reviews() -> pd.DataFrame:
    dagster_path = _DAGSTER_STORAGE / "stg_reviews"
    if dagster_path.exists():
        return pd.read_pickle(next(dagster_path.glob("*")))

    for candidate in [
        ROOT / "raw" / "olist" / "olist_order_reviews_dataset.csv",
        ROOT / "raw" / "olist_order_reviews_dataset.csv",
    ]:
        if candidate.exists():
            return pd.read_csv(candidate)

    return pd.DataFrame()


_BASELINE_RATES: dict[str, float] = {
    "exp_001": 0.12,
    "exp_002": 0.08,
    "exp_003": 0.15,
    "exp_004": 0.22,
    "exp_005": 0.10,
    "exp_006": 0.14,
    "exp_007": 0.11,
    "exp_008": 0.25,
    "exp_009": 0.09,
    "exp_010": 0.13,
}


def _simulate_conversion(
    row: pd.Series,
    experiment_id: str,
    baseline_rate: float,
    lift_pct: float,
) -> bool:
    """Deterministic hash-based conversion simulation.

    Uses SHA-256 of ``customer_id|experiment_id|outcome`` to produce a
    reproducible pseudo-random number in [0, 1].  The threshold differs
    between control (baseline) and treatment (baseline * (1 + lift/100)).
    """
    import hashlib

    seed_str = f"{row['customer_id']}|{experiment_id}|outcome"
    h = int(hashlib.sha256(seed_str.encode()).hexdigest(), 16)
    prob = (h % 10_000) / 10_000.0

    if row["variant"] == "treatment":
        return prob < baseline_rate * (1 + lift_pct / 100)
    return prob < baseline_rate


def build_experiment_data_from_dagster(
    assignments: pd.DataFrame,
    orders: pd.DataFrame,
    order_items: pd.DataFrame,
    reviews: pd.DataFrame,
    experiment_id: str,
) -> pd.DataFrame:
    """Build analysis-ready DataFrame from Dagster-passed DataFrames.

    Same logic as load_experiment_data but accepts DataFrames instead of
    reading from disk. Used by Dagster analysis assets.
    """
    catalog = ExperimentCatalog()
    meta = catalog.get(experiment_id)

    asn = assignments[assignments["experiment_id"] == experiment_id].copy()
    asn["customer_id"] = asn["customer_id"].astype(str).str.strip()

    ord_df = orders.copy()
    ord_df["customer_id"] = ord_df["customer_id"].astype(str).str.strip()

    order_totals = (
        order_items.groupby("order_id")
        .agg(order_total=("price", "sum"), item_count=("order_item_id", "count"))
        .reset_index()
    )
    order_totals["order_id"] = order_totals["order_id"].astype(str).str.strip()
    ord_df = ord_df.drop(columns=["order_total"], errors="ignore")
    ord_enriched = ord_df.merge(order_totals, on="order_id", how="left")
    ord_enriched["order_total"] = ord_enriched["order_total"].fillna(0.0)
    ord_enriched["item_count"] = ord_enriched["item_count"].fillna(0).astype(int)

    cust_orders = (
        ord_enriched.sort_values("order_purchase_timestamp")
        .groupby("customer_id")
        .agg(
            order_id=("order_id", "first"),
            order_status=("order_status", "first"),
            order_total=("order_total", "sum"),
            item_count=("item_count", "sum"),
            order_purchase_timestamp=("order_purchase_timestamp", "first"),
            order_delivered_customer_date=("order_delivered_customer_date", "first"),
            order_estimated_delivery_date=("order_estimated_delivery_date", "first"),
        )
        .reset_index()
    )

    merged = asn.merge(cust_orders, on="customer_id", how="left")

    baseline = _BASELINE_RATES.get(experiment_id, 0.12)
    lift = meta.mde_percent

    merged["converted"] = merged.apply(
        _simulate_conversion,
        axis=1,
        experiment_id=experiment_id,
        baseline_rate=baseline,
        lift_pct=lift,
    )

    merged.loc[~merged["converted"], "order_total"] = 0.0
    merged.loc[~merged["converted"], "item_count"] = 0
    merged["order_total"] = merged["order_total"].fillna(0.0)
    merged["item_count"] = merged["item_count"].fillna(0).astype(int)

    if not reviews.empty and "order_id" in reviews.columns:
        review_scores = reviews.groupby("order_id")["review_score"].mean().reset_index()
        review_scores["order_id"] = review_scores["order_id"].astype(str).str.strip()
        merged = merged.merge(review_scores, on="order_id", how="left")
    else:
        merged["review_score"] = np.nan

    merged.loc[~merged["converted"], "review_score"] = np.nan

    return merged


def load_experiment_data(
    experiment_id: str,
    assignments_path: str | Path = ASSIGNMENTS_PATH,
) -> pd.DataFrame:
    """Build an analysis-ready DataFrame for *experiment_id*.

    Since experiment assignments are synthetic overlays on the full Olist
    customer base (where every customer already has an order), conversion
    outcomes are simulated deterministically using a hash of
    ``customer_id + experiment_id``.  This produces realistic, reproducible
    conversion rates with a controlled treatment effect so that statistical
    tests can be meaningfully demonstrated.

    Key columns:
    - ``converted`` (bool): simulated conversion outcome
    - ``order_total`` (float): real order total (zeroed for non-converters)
    - ``item_count`` (int): real item count (zeroed for non-converters)
    - ``review_score`` (float): real review score (NaN for non-converters)
    """
    catalog = ExperimentCatalog()
    meta = catalog.get(experiment_id)

    assignments = pd.read_csv(assignments_path)
    assignments = assignments[assignments["experiment_id"] == experiment_id].copy()
    assignments["customer_id"] = assignments["customer_id"].astype(str).str.strip()

    orders = _resolve_orders()
    orders["customer_id"] = orders["customer_id"].astype(str).str.strip()

    order_items = _resolve_order_items()

    order_totals = (
        order_items.groupby("order_id")
        .agg(order_total=("price", "sum"), item_count=("order_item_id", "count"))
        .reset_index()
    )

    orders_enriched = orders.merge(order_totals, on="order_id", how="left")

    cust_orders = (
        orders_enriched.sort_values("order_purchase_timestamp")
        .groupby("customer_id")
        .agg(
            order_id=("order_id", "first"),
            order_status=("order_status", "first"),
            order_total=("order_total", "sum"),
            item_count=("item_count", "sum"),
            order_purchase_timestamp=("order_purchase_timestamp", "first"),
            order_delivered_customer_date=("order_delivered_customer_date", "first"),
            order_estimated_delivery_date=("order_estimated_delivery_date", "first"),
        )
        .reset_index()
    )

    merged = assignments.merge(cust_orders, on="customer_id", how="left")

    baseline = _BASELINE_RATES.get(experiment_id, 0.12)
    lift = meta.mde_percent

    merged["converted"] = merged.apply(
        _simulate_conversion,
        axis=1,
        experiment_id=experiment_id,
        baseline_rate=baseline,
        lift_pct=lift,
    )

    merged.loc[~merged["converted"], "order_total"] = 0.0
    merged.loc[~merged["converted"], "item_count"] = 0
    merged["order_total"] = merged["order_total"].fillna(0.0)
    merged["item_count"] = merged["item_count"].fillna(0).astype(int)

    reviews = _resolve_reviews()
    if not reviews.empty and "order_id" in reviews.columns:
        review_scores = (
            reviews.groupby("order_id")["review_score"].mean().reset_index()
        )
        merged = merged.merge(review_scores, on="order_id", how="left")
    else:
        merged["review_score"] = np.nan

    merged.loc[~merged["converted"], "review_score"] = np.nan

    return merged


# ── Statistical test helpers ────────────────────────────────────────────────


@dataclass
class TestResult:
    """Container for the output of a single statistical test."""

    test_name: str
    statistic: float
    p_value: float
    significant: bool
    alpha: float = 0.05
    interpretation: str = ""
    details: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        sig = "SIGNIFICANT" if self.significant else "not significant"
        return (
            f"{self.test_name}: stat={self.statistic:.4f}, "
            f"p={self.p_value:.4f} ({sig} at alpha={self.alpha})"
        )


def _z_test_proportions(
    successes: tuple[int, int],
    totals: tuple[int, int],
    alpha: float = 0.05,
) -> TestResult:
    """Two-sided Z-test for two independent proportions."""
    count = np.array(successes)
    nobs = np.array(totals)
    stat, pval = proportions_ztest(count, nobs, alternative="two-sided")
    return TestResult(
        test_name="Z-test for proportions",
        statistic=float(stat),
        p_value=float(pval),
        significant=pval < alpha,
        alpha=alpha,
    )


def _chi_square_test(
    successes: tuple[int, int],
    totals: tuple[int, int],
    alpha: float = 0.05,
) -> TestResult:
    """Chi-square test of independence for 2x2 contingency table."""
    s_ctrl, s_treat = successes
    n_ctrl, n_treat = totals
    table = np.array(
        [[s_ctrl, n_ctrl - s_ctrl], [s_treat, n_treat - s_treat]]
    )
    chi2, pval, dof, _ = stats.chi2_contingency(table, correction=True)
    return TestResult(
        test_name="Chi-square test",
        statistic=float(chi2),
        p_value=float(pval),
        significant=pval < alpha,
        alpha=alpha,
        details={"dof": dof},
    )


def _fishers_exact(
    successes: tuple[int, int],
    totals: tuple[int, int],
    alpha: float = 0.05,
) -> TestResult:
    """Fisher's exact test for small samples."""
    s_ctrl, s_treat = successes
    n_ctrl, n_treat = totals
    table = np.array(
        [[s_ctrl, n_ctrl - s_ctrl], [s_treat, n_treat - s_treat]]
    )
    odds_ratio, pval = stats.fisher_exact(table, alternative="two-sided")
    return TestResult(
        test_name="Fisher's exact test",
        statistic=float(odds_ratio),
        p_value=float(pval),
        significant=pval < alpha,
        alpha=alpha,
        details={"odds_ratio": float(odds_ratio)},
    )


def _welch_t_test(
    control: np.ndarray,
    treatment: np.ndarray,
    alpha: float = 0.05,
) -> TestResult:
    """Welch's t-test (unequal variance)."""
    stat, pval = stats.ttest_ind(control, treatment, equal_var=False)
    return TestResult(
        test_name="Welch's t-test",
        statistic=float(stat),
        p_value=float(pval),
        significant=pval < alpha,
        alpha=alpha,
    )


def _mann_whitney(
    control: np.ndarray,
    treatment: np.ndarray,
    alpha: float = 0.05,
) -> TestResult:
    """Mann-Whitney U test (non-parametric alternative to t-test)."""
    stat, pval = stats.mannwhitneyu(
        control, treatment, alternative="two-sided"
    )
    return TestResult(
        test_name="Mann-Whitney U test",
        statistic=float(stat),
        p_value=float(pval),
        significant=pval < alpha,
        alpha=alpha,
    )


# ── Confidence intervals ───────────────────────────────────────────────────


def _ci_proportion_diff(
    p1: float, n1: int, p2: float, n2: int, confidence: float = 0.95
) -> tuple[float, float]:
    """Wald CI for the difference in two proportions (p2 - p1)."""
    diff = p2 - p1
    se = math.sqrt(p1 * (1 - p1) / n1 + p2 * (1 - p2) / n2)
    z = stats.norm.ppf(1 - (1 - confidence) / 2)
    return (diff - z * se, diff + z * se)


def _ci_mean_diff(
    control: np.ndarray,
    treatment: np.ndarray,
    confidence: float = 0.95,
) -> tuple[float, float]:
    """CI for difference in means using Welch-Satterthwaite df."""
    diff = float(treatment.mean() - control.mean())
    se = math.sqrt(treatment.var(ddof=1) / len(treatment) + control.var(ddof=1) / len(control))
    nu_num = (treatment.var(ddof=1) / len(treatment) + control.var(ddof=1) / len(control)) ** 2
    nu_den = (
        (treatment.var(ddof=1) / len(treatment)) ** 2 / (len(treatment) - 1)
        + (control.var(ddof=1) / len(control)) ** 2 / (len(control) - 1)
    )
    dof = nu_num / nu_den if nu_den > 0 else len(control) + len(treatment) - 2
    t_crit = stats.t.ppf(1 - (1 - confidence) / 2, dof)
    return (diff - t_crit * se, diff + t_crit * se)


# ── Effect size ────────────────────────────────────────────────────────────


def cohens_d(control: np.ndarray, treatment: np.ndarray) -> float:
    """Cohen's d for two independent samples."""
    n1, n2 = len(control), len(treatment)
    s_pooled = math.sqrt(
        ((n1 - 1) * control.var(ddof=1) + (n2 - 1) * treatment.var(ddof=1))
        / (n1 + n2 - 2)
    )
    if s_pooled == 0:
        return 0.0
    return float((treatment.mean() - control.mean()) / s_pooled)


def cohens_h(p1: float, p2: float) -> float:
    """Cohen's h for difference between two proportions."""
    return float(2 * (math.asin(math.sqrt(p2)) - math.asin(math.sqrt(p1))))


def interpret_effect_size(
    d: float, metric_type: Literal["continuous", "proportion"] = "continuous"
) -> str:
    """Cohen's benchmarks: |d| < 0.2 negligible, < 0.5 small, < 0.8 medium, else large."""
    d_abs = abs(d)
    if d_abs < 0.2:
        return "negligible"
    if d_abs < 0.5:
        return "small"
    if d_abs < 0.8:
        return "medium"
    return "large"


# ── Power analysis ─────────────────────────────────────────────────────────


def achieved_power_proportions(
    p_control: float,
    p_treatment: float,
    n_control: int,
    n_treatment: int,
    alpha: float = 0.05,
) -> float:
    """Retrospective power for a two-proportion Z-test."""
    h = cohens_h(p_control, p_treatment)
    ratio = n_treatment / n_control if n_control > 0 else 1.0
    try:
        power = NormalIndPower().solve_power(
            effect_size=abs(h),
            nobs1=n_control,
            alpha=alpha,
            ratio=ratio,
            alternative="two-sided",
        )
    except Exception:
        power = np.nan
    return float(power)


def achieved_power_means(
    control: np.ndarray,
    treatment: np.ndarray,
    alpha: float = 0.05,
) -> float:
    """Retrospective power for a two-sample t-test."""
    d = cohens_d(control, treatment)
    ratio = len(treatment) / len(control) if len(control) > 0 else 1.0
    try:
        power = TTestIndPower().solve_power(
            effect_size=abs(d),
            nobs1=len(control),
            alpha=alpha,
            ratio=ratio,
            alternative="two-sided",
        )
    except Exception:
        power = np.nan
    return float(power)


def required_sample_size(
    effect_size: float,
    alpha: float = 0.05,
    power: float = 0.80,
    test_type: str = "chi_square",
    ratio: float = 1.0,
) -> int:
    """Minimum sample size per group to detect *effect_size*."""
    if test_type in ("chi_square", "z_test"):
        n = NormalIndPower().solve_power(
            effect_size=abs(effect_size),
            alpha=alpha,
            power=power,
            ratio=ratio,
            alternative="two-sided",
        )
    else:
        n = TTestIndPower().solve_power(
            effect_size=abs(effect_size),
            alpha=alpha,
            power=power,
            ratio=ratio,
            alternative="two-sided",
        )
    return int(math.ceil(n))


# ── Multiple testing correction ────────────────────────────────────────────


def correct_multiple_tests(
    p_values: list[float],
    method: str = "fdr_bh",
    alpha: float = 0.05,
) -> pd.DataFrame:
    """Apply multiple testing correction (Benjamini-Hochberg by default)."""
    from statsmodels.stats.multitest import multipletests

    reject, corrected, _, _ = multipletests(p_values, alpha=alpha, method=method)
    return pd.DataFrame(
        {
            "original_p": p_values,
            "corrected_p": corrected,
            "reject_null": reject,
        }
    )


# ── Main analyzer ──────────────────────────────────────────────────────────


class ExperimentAnalyzer:
    """End-to-end statistical analysis for a single experiment.

    Usage::

        catalog = ExperimentCatalog()
        data = load_experiment_data("exp_001")
        analyzer = ExperimentAnalyzer(data, catalog.get("exp_001"))
        report = analyzer.full_report()
    """

    def __init__(
        self,
        data: pd.DataFrame,
        meta: ExperimentMeta,
        alpha: float = 0.05,
        min_lift_pct: float = 10.0,
    ) -> None:
        self.data = data
        self.meta = meta
        self.alpha = alpha
        self.min_lift_pct = min_lift_pct

        self.control = data[data["variant"] == "control"]
        self.treatment = data[data["variant"] == "treatment"]

    # ── Section 3: Descriptive statistics ──────────────────────────────

    def descriptive_stats(self) -> dict[str, Any]:
        """Sample sizes, conversion rates, revenue summary."""
        n_ctrl = len(self.control)
        n_treat = len(self.treatment)

        conv_ctrl = self.control["converted"].sum()
        conv_treat = self.treatment["converted"].sum()
        cr_ctrl = conv_ctrl / n_ctrl if n_ctrl else 0
        cr_treat = conv_treat / n_treat if n_treat else 0

        rev_ctrl = self.control["order_total"]
        rev_treat = self.treatment["order_total"]

        return {
            "n_control": n_ctrl,
            "n_treatment": n_treat,
            "conversions_control": int(conv_ctrl),
            "conversions_treatment": int(conv_treat),
            "conversion_rate_control": round(cr_ctrl, 4),
            "conversion_rate_treatment": round(cr_treat, 4),
            "revenue_mean_control": round(float(rev_ctrl.mean()), 2),
            "revenue_mean_treatment": round(float(rev_treat.mean()), 2),
            "revenue_median_control": round(float(rev_ctrl.median()), 2),
            "revenue_median_treatment": round(float(rev_treat.median()), 2),
            "revenue_std_control": round(float(rev_ctrl.std()), 2),
            "revenue_std_treatment": round(float(rev_treat.std()), 2),
        }

    # ── Section 4: Hypothesis test ────────────────────────────────────

    def select_test(self) -> str:
        """Decision-tree logic for choosing the appropriate test."""
        if self.meta.test_type == "t_test":
            ctrl_vals = self.control["order_total"].dropna().values
            treat_vals = self.treatment["order_total"].dropna().values
            if len(ctrl_vals) < 20 or len(treat_vals) < 20:
                return "mann_whitney"
            _, p_norm_ctrl = stats.shapiro(
                np.random.choice(ctrl_vals, min(500, len(ctrl_vals)), replace=False)
            )
            _, p_norm_treat = stats.shapiro(
                np.random.choice(treat_vals, min(500, len(treat_vals)), replace=False)
            )
            if p_norm_ctrl < 0.05 or p_norm_treat < 0.05:
                return "mann_whitney"
            return "welch_t"

        n_ctrl = len(self.control)
        n_treat = len(self.treatment)
        if n_ctrl < 30 or n_treat < 30:
            return "fisher"
        return "z_test"

    def run_test(self) -> TestResult:
        """Run the appropriate hypothesis test and return a TestResult."""
        test_name = self.select_test()
        ds = self.descriptive_stats()

        if test_name == "z_test":
            return _z_test_proportions(
                successes=(ds["conversions_control"], ds["conversions_treatment"]),
                totals=(ds["n_control"], ds["n_treatment"]),
                alpha=self.alpha,
            )
        if test_name == "fisher":
            return _fishers_exact(
                successes=(ds["conversions_control"], ds["conversions_treatment"]),
                totals=(ds["n_control"], ds["n_treatment"]),
                alpha=self.alpha,
            )
        if test_name == "welch_t":
            ctrl_vals = self.control["order_total"].dropna().values
            treat_vals = self.treatment["order_total"].dropna().values
            return _welch_t_test(ctrl_vals, treat_vals, self.alpha)
        if test_name == "mann_whitney":
            ctrl_vals = self.control["order_total"].dropna().values
            treat_vals = self.treatment["order_total"].dropna().values
            return _mann_whitney(ctrl_vals, treat_vals, self.alpha)

        raise ValueError(f"Unknown test: {test_name}")

    def check_assumptions(self) -> dict[str, Any]:
        """Verify normality and variance homogeneity."""
        ctrl_vals = self.control["order_total"].dropna().values
        treat_vals = self.treatment["order_total"].dropna().values

        results: dict[str, Any] = {}
        if len(ctrl_vals) >= 8 and len(treat_vals) >= 8:
            sample_c = np.random.choice(ctrl_vals, min(500, len(ctrl_vals)), replace=False)
            sample_t = np.random.choice(treat_vals, min(500, len(treat_vals)), replace=False)
            _, p_c = stats.shapiro(sample_c)
            _, p_t = stats.shapiro(sample_t)
            results["shapiro_control_p"] = round(float(p_c), 4)
            results["shapiro_treatment_p"] = round(float(p_t), 4)
            results["normal_control"] = p_c >= 0.05
            results["normal_treatment"] = p_t >= 0.05

            _, p_lev = stats.levene(ctrl_vals, treat_vals)
            results["levene_p"] = round(float(p_lev), 4)
            results["equal_variance"] = p_lev >= 0.05
        else:
            results["note"] = "Sample too small for assumption checks"
        return results

    # ── Section 5: Confidence intervals ───────────────────────────────

    def confidence_interval(self, confidence: float = 0.95) -> dict[str, Any]:
        """95% CI for the treatment effect (lift)."""
        ds = self.descriptive_stats()

        if self.meta.test_type != "t_test":
            p1 = ds["conversion_rate_control"]
            p2 = ds["conversion_rate_treatment"]
            ci = _ci_proportion_diff(
                p1, ds["n_control"], p2, ds["n_treatment"], confidence
            )
            abs_lift = p2 - p1
            rel_lift = abs_lift / p1 * 100 if p1 > 0 else 0.0
            return {
                "metric": "conversion_rate",
                "absolute_lift": round(abs_lift, 4),
                "relative_lift_pct": round(rel_lift, 2),
                "ci_lower": round(ci[0], 4),
                "ci_upper": round(ci[1], 4),
                "contains_zero": ci[0] <= 0 <= ci[1],
                "confidence": confidence,
            }

        ctrl_vals = self.control["order_total"].dropna().values
        treat_vals = self.treatment["order_total"].dropna().values
        ci = _ci_mean_diff(ctrl_vals, treat_vals, confidence)
        abs_lift = float(treat_vals.mean() - ctrl_vals.mean())
        rel_lift = abs_lift / ctrl_vals.mean() * 100 if ctrl_vals.mean() != 0 else 0.0
        return {
            "metric": self.meta.primary_metric,
            "absolute_lift": round(abs_lift, 2),
            "relative_lift_pct": round(rel_lift, 2),
            "ci_lower": round(ci[0], 2),
            "ci_upper": round(ci[1], 2),
            "contains_zero": ci[0] <= 0 <= ci[1],
            "confidence": confidence,
        }

    # ── Section 6: Effect size ────────────────────────────────────────

    def effect_size(self) -> dict[str, Any]:
        ds = self.descriptive_stats()

        if self.meta.test_type != "t_test":
            h = cohens_h(
                ds["conversion_rate_control"],
                ds["conversion_rate_treatment"],
            )
            return {
                "metric_type": "proportion",
                "cohens_h": round(h, 4),
                "magnitude": interpret_effect_size(h),
                "absolute_diff": round(
                    ds["conversion_rate_treatment"] - ds["conversion_rate_control"], 4
                ),
                "relative_lift_pct": round(
                    (ds["conversion_rate_treatment"] - ds["conversion_rate_control"])
                    / ds["conversion_rate_control"]
                    * 100
                    if ds["conversion_rate_control"] > 0
                    else 0,
                    2,
                ),
            }

        ctrl_vals = self.control["order_total"].dropna().values
        treat_vals = self.treatment["order_total"].dropna().values
        d = cohens_d(ctrl_vals, treat_vals)
        return {
            "metric_type": "continuous",
            "cohens_d": round(d, 4),
            "magnitude": interpret_effect_size(d),
            "absolute_diff": round(float(treat_vals.mean() - ctrl_vals.mean()), 2),
            "relative_lift_pct": round(
                (treat_vals.mean() - ctrl_vals.mean()) / ctrl_vals.mean() * 100
                if ctrl_vals.mean() != 0
                else 0,
                2,
            ),
        }

    # ── Section 7: Power analysis ─────────────────────────────────────

    def power_analysis(self) -> dict[str, Any]:
        ds = self.descriptive_stats()

        if self.meta.test_type != "t_test":
            power = achieved_power_proportions(
                ds["conversion_rate_control"],
                ds["conversion_rate_treatment"],
                ds["n_control"],
                ds["n_treatment"],
                self.alpha,
            )
            mde_h = cohens_h(
                ds["conversion_rate_control"],
                ds["conversion_rate_control"] * (1 + self.meta.mde_percent / 100),
            )
            n_needed = required_sample_size(
                mde_h, self.alpha, 0.80, "chi_square"
            )
        else:
            ctrl_vals = self.control["order_total"].dropna().values
            treat_vals = self.treatment["order_total"].dropna().values
            power = achieved_power_means(ctrl_vals, treat_vals, self.alpha)
            d = cohens_d(ctrl_vals, treat_vals)
            mde_d = abs(d) if abs(d) > 0 else 0.2
            n_needed = required_sample_size(mde_d, self.alpha, 0.80, "t_test")

        return {
            "achieved_power": round(power, 4) if not np.isnan(power) else None,
            "adequate": power >= 0.80 if not np.isnan(power) else False,
            "target_power": 0.80,
            "planned_sample_per_variant": self.meta.sample_per_variant,
            "actual_n_control": ds["n_control"],
            "actual_n_treatment": ds["n_treatment"],
            "min_sample_for_80pct_power": n_needed,
        }

    # ── Section 8: Business impact ────────────────────────────────────

    def business_impact(
        self,
        monthly_traffic: int = 100_000,
        avg_revenue_per_user: float | None = None,
    ) -> dict[str, Any]:
        """Project annualised revenue impact of the treatment effect."""
        ds = self.descriptive_stats()
        ci = self.confidence_interval()

        if avg_revenue_per_user is None:
            avg_revenue_per_user = ds["revenue_mean_treatment"]

        if self.meta.test_type != "t_test":
            abs_lift = ci["absolute_lift"]
            monthly_extra_conversions = monthly_traffic * abs_lift
            monthly_revenue_delta = monthly_extra_conversions * avg_revenue_per_user
        else:
            abs_lift = ci["absolute_lift"]
            monthly_revenue_delta = monthly_traffic * abs_lift

        return {
            "monthly_extra_conversions": round(
                monthly_traffic * ci.get("absolute_lift", 0), 0
            ),
            "monthly_revenue_impact": round(monthly_revenue_delta, 2),
            "annual_revenue_impact": round(monthly_revenue_delta * 12, 2),
            "ci_annual_lower": round(
                ci["ci_lower"] * monthly_traffic * avg_revenue_per_user * 12
                if self.meta.test_type != "t_test"
                else ci["ci_lower"] * monthly_traffic * 12,
                2,
            ),
            "ci_annual_upper": round(
                ci["ci_upper"] * monthly_traffic * avg_revenue_per_user * 12
                if self.meta.test_type != "t_test"
                else ci["ci_upper"] * monthly_traffic * 12,
                2,
            ),
            "assumptions": {
                "monthly_traffic": monthly_traffic,
                "avg_revenue_per_user": avg_revenue_per_user,
            },
        }

    # ── Section 9: Recommendation ─────────────────────────────────────

    def recommendation(self) -> dict[str, Any]:
        """Automated recommendation based on all analysis sections."""
        test = self.run_test()
        ci = self.confidence_interval()
        es = self.effect_size()
        pwr = self.power_analysis()

        statistically_significant = test.significant
        practically_significant = abs(ci["relative_lift_pct"]) >= self.min_lift_pct
        positive_direction = ci["absolute_lift"] > 0
        adequate_power = pwr.get("adequate", False)

        if statistically_significant and practically_significant and positive_direction:
            decision = "LAUNCH"
            reasoning = (
                f"Statistically significant (p={test.p_value:.4f}), "
                f"practically meaningful ({ci['relative_lift_pct']:+.1f}% lift), "
                f"and positive direction."
            )
        elif statistically_significant and positive_direction:
            decision = "ITERATE"
            reasoning = (
                f"Statistically significant (p={test.p_value:.4f}) but lift "
                f"({ci['relative_lift_pct']:+.1f}%) below practical threshold "
                f"of {self.min_lift_pct}%. Consider amplifying treatment."
            )
        elif not statistically_significant and not adequate_power:
            decision = "INCONCLUSIVE"
            reasoning = (
                f"Not significant (p={test.p_value:.4f}) and underpowered "
                f"(power={pwr['achieved_power']}). Collect more data."
            )
        elif not statistically_significant:
            decision = "ABANDON"
            reasoning = (
                f"Not significant (p={test.p_value:.4f}) with adequate power. "
                f"Treatment has no detectable effect."
            )
        else:
            decision = "ABANDON"
            reasoning = (
                f"Significant but negative direction "
                f"({ci['relative_lift_pct']:+.1f}% lift). Treatment hurts."
            )

        return {
            "decision": decision,
            "reasoning": reasoning,
            "statistically_significant": statistically_significant,
            "practically_significant": practically_significant,
            "positive_direction": positive_direction,
            "adequate_power": adequate_power,
        }

    # ── Full report ───────────────────────────────────────────────────

    def full_report(self) -> dict[str, Any]:
        """Run all analysis sections and return a consolidated report."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return {
                "experiment": {
                    "id": self.meta.id,
                    "name": self.meta.name,
                    "primary_metric": self.meta.primary_metric,
                    "test_type": self.meta.test_type,
                },
                "descriptive_stats": self.descriptive_stats(),
                "test_selected": self.select_test(),
                "hypothesis_test": {
                    "test_name": (r := self.run_test()).test_name,
                    "statistic": r.statistic,
                    "p_value": r.p_value,
                    "significant": r.significant,
                },
                "assumptions": self.check_assumptions(),
                "confidence_interval": self.confidence_interval(),
                "effect_size": self.effect_size(),
                "power_analysis": self.power_analysis(),
                "business_impact": self.business_impact(),
                "recommendation": self.recommendation(),
            }

    def summary_table(self) -> pd.DataFrame:
        """Single-row summary for cross-experiment comparison."""
        rpt = self.full_report()
        return pd.DataFrame(
            [
                {
                    "experiment_id": self.meta.id,
                    "experiment_name": self.meta.name,
                    "n_control": rpt["descriptive_stats"]["n_control"],
                    "n_treatment": rpt["descriptive_stats"]["n_treatment"],
                    "cr_control": rpt["descriptive_stats"]["conversion_rate_control"],
                    "cr_treatment": rpt["descriptive_stats"]["conversion_rate_treatment"],
                    "p_value": rpt["hypothesis_test"]["p_value"],
                    "significant": rpt["hypothesis_test"]["significant"],
                    "relative_lift_%": rpt["confidence_interval"]["relative_lift_pct"],
                    "ci_lower": rpt["confidence_interval"]["ci_lower"],
                    "ci_upper": rpt["confidence_interval"]["ci_upper"],
                    "power": rpt["power_analysis"]["achieved_power"],
                    "effect_magnitude": rpt["effect_size"]["magnitude"],
                    "decision": rpt["recommendation"]["decision"],
                }
            ]
        )
