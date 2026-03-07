"""Multiple testing correction for multi-metric A/B experiments.

Implements three correction strategies:

1. **Bonferroni** — divide alpha by *k*; controls family-wise error rate
   (FWER).  Conservative but simple.
2. **Benjamini-Hochberg (FDR)** — controls *expected proportion* of false
   discoveries.  More powerful than Bonferroni.
3. **Hierarchical (gate-keeping)** — primary metric tested first without
   penalty; secondary metrics tested only if primary is significant;
   guardrail metrics tested unconditionally (separate safety question).

The ``MultiMetricAnalyzer`` class ties everything together: it runs all
tests for an experiment, applies corrections, and produces a unified
results table with a hierarchical recommendation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.stats.multitest import multipletests
from statsmodels.stats.proportion import proportions_ztest


# ── Metric definitions per experiment ──────────────────────────────────────

@dataclass(frozen=True)
class MetricSpec:
    """Specification for a single metric in an experiment."""
    name: str
    role: str                        # "primary" | "secondary" | "guardrail"
    metric_type: str                 # "proportion" | "continuous"
    direction: str = "two-sided"     # "two-sided" | "less" | "greater"
    column: str | None = None        # DataFrame column for continuous metrics


# Standard set of metrics available from the experiment data
STANDARD_METRICS: list[MetricSpec] = [
    MetricSpec("conversion_rate", "primary", "proportion"),
    MetricSpec("aov", "secondary", "continuous", column="order_total"),
    MetricSpec("items_per_order", "secondary", "continuous", column="item_count"),
    MetricSpec("revenue_per_user", "secondary", "continuous", column="order_total"),
    MetricSpec("review_score", "guardrail", "continuous",
               direction="greater", column="review_score"),
]


# ── Single-metric test runner ──────────────────────────────────────────────


def _run_proportion_test(
    control: pd.DataFrame,
    treatment: pd.DataFrame,
    alternative: str = "two-sided",
) -> dict[str, Any]:
    """Z-test for two proportions (conversion rate)."""
    n_c, n_t = len(control), len(treatment)
    s_c = int(control["converted"].sum())
    s_t = int(treatment["converted"].sum())
    cr_c = s_c / n_c if n_c else 0
    cr_t = s_t / n_t if n_t else 0

    z, p = proportions_ztest(
        np.array([s_c, s_t]), np.array([n_c, n_t]), alternative=alternative,
    )
    return {
        "statistic": float(z),
        "p_value": float(p),
        "control_value": cr_c,
        "treatment_value": cr_t,
        "absolute_diff": cr_t - cr_c,
        "relative_diff_pct": (cr_t - cr_c) / cr_c * 100 if cr_c > 0 else 0.0,
    }


def _run_continuous_test(
    control: pd.DataFrame,
    treatment: pd.DataFrame,
    column: str,
    use_converters_only: bool = False,
    alternative: str = "two-sided",
) -> dict[str, Any]:
    """Mann-Whitney U test for a continuous metric."""
    if use_converters_only:
        c_vals = control.loc[control["converted"], column].dropna().values
        t_vals = treatment.loc[treatment["converted"], column].dropna().values
    else:
        c_vals = control[column].dropna().values
        t_vals = treatment[column].dropna().values

    if len(c_vals) < 5 or len(t_vals) < 5:
        return {
            "statistic": np.nan, "p_value": 1.0,
            "control_value": float(c_vals.mean()) if len(c_vals) else 0,
            "treatment_value": float(t_vals.mean()) if len(t_vals) else 0,
            "absolute_diff": 0.0, "relative_diff_pct": 0.0,
        }

    u, p = stats.mannwhitneyu(c_vals, t_vals, alternative=alternative)
    c_mean = float(c_vals.mean())
    t_mean = float(t_vals.mean())
    return {
        "statistic": float(u),
        "p_value": float(p),
        "control_value": c_mean,
        "treatment_value": t_mean,
        "absolute_diff": t_mean - c_mean,
        "relative_diff_pct": (t_mean - c_mean) / c_mean * 100 if c_mean != 0 else 0.0,
    }


# ── Correction methods ────────────────────────────────────────────────────


def bonferroni_correct(
    p_values: list[float], alpha: float = 0.05,
) -> pd.DataFrame:
    reject, corrected, _, _ = multipletests(p_values, alpha=alpha, method="bonferroni")
    return pd.DataFrame({
        "original_p": p_values,
        "corrected_p": corrected,
        "reject": reject,
        "method": "bonferroni",
    })


def holm_correct(
    p_values: list[float], alpha: float = 0.05,
) -> pd.DataFrame:
    reject, corrected, _, _ = multipletests(p_values, alpha=alpha, method="holm")
    return pd.DataFrame({
        "original_p": p_values,
        "corrected_p": corrected,
        "reject": reject,
        "method": "holm",
    })


def fdr_correct(
    p_values: list[float], alpha: float = 0.05,
) -> pd.DataFrame:
    reject, corrected, _, _ = multipletests(p_values, alpha=alpha, method="fdr_bh")
    return pd.DataFrame({
        "original_p": p_values,
        "corrected_p": corrected,
        "reject": reject,
        "method": "fdr_bh",
    })


def compare_corrections(
    p_values: list[float],
    metric_names: list[str],
    alpha: float = 0.05,
) -> pd.DataFrame:
    """Apply all three correction methods side-by-side."""
    rows = []
    for method_name, method_fn in [
        ("none", lambda p, a: pd.DataFrame({"corrected_p": p, "reject": [v < a for v in p]})),
        ("bonferroni", bonferroni_correct),
        ("holm", holm_correct),
        ("fdr_bh", fdr_correct),
    ]:
        result = method_fn(p_values, alpha)
        for i, name in enumerate(metric_names):
            rows.append({
                "metric": name,
                "method": method_name,
                "original_p": p_values[i],
                "corrected_p": float(result["corrected_p"].iloc[i]),
                "significant": bool(result["reject"].iloc[i]),
            })
    return pd.DataFrame(rows)


# ── Family-wise error rate simulation ──────────────────────────────────────


def simulate_fwer(
    n_tests: int = 5,
    alpha: float = 0.05,
    n_simulations: int = 50_000,
    seed: int = 42,
) -> dict[str, Any]:
    """Simulate the multiple-testing inflation problem.

    Under the global null (no effect anywhere), run *n_tests* independent
    tests and measure how often at least one falsely rejects.
    """
    rng = np.random.default_rng(seed)
    # Each simulation: n_tests independent p-values from Uniform(0,1)
    p_matrix = rng.uniform(0, 1, size=(n_simulations, n_tests))

    # Uncorrected: any p < alpha
    any_reject_none = (p_matrix < alpha).any(axis=1).mean()

    # Bonferroni
    any_reject_bonf = (p_matrix < alpha / n_tests).any(axis=1).mean()

    # FDR (applied per simulation)
    any_reject_fdr = 0.0
    for row in p_matrix:
        reject, _, _, _ = multipletests(row, alpha=alpha, method="fdr_bh")
        if reject.any():
            any_reject_fdr += 1
    any_reject_fdr /= n_simulations

    theoretical_fwer = 1 - (1 - alpha) ** n_tests

    return {
        "n_tests": n_tests,
        "alpha": alpha,
        "theoretical_fwer": round(theoretical_fwer, 4),
        "simulated_fwer_none": round(float(any_reject_none), 4),
        "simulated_fwer_bonferroni": round(float(any_reject_bonf), 4),
        "simulated_fwer_fdr": round(float(any_reject_fdr), 4),
    }


# ── Multi-metric analyzer ─────────────────────────────────────────────────


class MultiMetricAnalyzer:
    """Run multiple metrics for a single experiment with hierarchical testing.

    Workflow
    --------
    1. Test **primary** metric uncorrected.
    2. If primary significant, test **secondary** metrics with correction.
    3. Always test **guardrail** metrics (one-sided, no correction).
    4. Combine results with an overall recommendation.

    Parameters
    ----------
    data : DataFrame
        Experiment data with ``variant``, ``converted``, ``order_total``, etc.
    metrics : list[MetricSpec] | None
        Metric specifications.  Defaults to ``STANDARD_METRICS``.
    alpha : float
        Significance threshold (default 0.05).
    correction : str
        Correction method for secondary metrics: ``"bonferroni"``,
        ``"holm"``, or ``"fdr_bh"``.
    """

    def __init__(
        self,
        data: pd.DataFrame,
        metrics: list[MetricSpec] | None = None,
        alpha: float = 0.05,
        correction: str = "holm",
    ) -> None:
        self.data = data
        self.metrics = metrics or STANDARD_METRICS
        self.alpha = alpha
        self.correction = correction

        self.control = data[data["variant"] == "control"]
        self.treatment = data[data["variant"] == "treatment"]

    def _test_metric(self, spec: MetricSpec) -> dict[str, Any]:
        """Run the appropriate test for a single metric."""
        if spec.metric_type == "proportion":
            alt = spec.direction
            return _run_proportion_test(self.control, self.treatment, alt)

        col = spec.column or "order_total"
        converters_only = spec.name in ("aov", "items_per_order")
        alt = spec.direction
        return _run_continuous_test(
            self.control, self.treatment, col, converters_only, alt,
        )

    def run_all(self) -> pd.DataFrame:
        """Test every metric and return raw (uncorrected) results."""
        rows = []
        for spec in self.metrics:
            result = self._test_metric(spec)
            rows.append({
                "metric": spec.name,
                "role": spec.role,
                "type": spec.metric_type,
                "control": result["control_value"],
                "treatment": result["treatment_value"],
                "absolute_diff": result["absolute_diff"],
                "relative_diff_%": round(result["relative_diff_pct"], 2),
                "statistic": result["statistic"],
                "p_value": result["p_value"],
            })
        return pd.DataFrame(rows)

    def hierarchical_test(self) -> dict[str, Any]:
        """Execute the full hierarchical testing procedure.

        Returns a dict with ``primary``, ``secondary``, ``guardrail``
        DataFrames plus an overall ``recommendation``.
        """
        raw = self.run_all()

        # ── Step 1: Primary metric (no correction) ─────────────────
        primary = raw[raw["role"] == "primary"].copy()
        primary["corrected_p"] = primary["p_value"]
        primary["significant"] = primary["p_value"] < self.alpha
        primary_passed = bool(primary["significant"].all())

        # ── Step 2: Secondary metrics (corrected, gated on primary) ──
        secondary = raw[raw["role"] == "secondary"].copy()
        if primary_passed and len(secondary) > 0:
            sec_p = secondary["p_value"].tolist()
            _, corrected, _, _ = multipletests(
                sec_p, alpha=self.alpha, method=self.correction,
            )
            secondary["corrected_p"] = corrected
            secondary["significant"] = corrected < self.alpha
            secondary["note"] = "tested (primary passed gate)"
        elif len(secondary) > 0:
            secondary["corrected_p"] = np.nan
            secondary["significant"] = False
            secondary["note"] = "not tested (primary failed gate)"
        else:
            secondary["corrected_p"] = pd.Series(dtype=float)
            secondary["significant"] = pd.Series(dtype=bool)
            secondary["note"] = pd.Series(dtype=str)

        # ── Step 3: Guardrail metrics (always tested, no correction) ──
        guardrail = raw[raw["role"] == "guardrail"].copy()
        guardrail["corrected_p"] = guardrail["p_value"]
        guardrail["significant"] = guardrail["p_value"] < self.alpha
        # For guardrails, "significant" in the harmful direction = problem
        guardrail["triggered"] = (
            guardrail["significant"] & (guardrail["absolute_diff"] < 0)
        )

        # ── Recommendation ─────────────────────────────────────────
        guardrail_ok = not guardrail["triggered"].any() if len(guardrail) > 0 else True
        any_secondary_sig = bool(secondary["significant"].any()) if len(secondary) > 0 else False

        if not primary_passed:
            decision = "FAIL"
            reasoning = "Primary metric not significant; experiment did not meet success criterion."
        elif not guardrail_ok:
            triggered_names = guardrail.loc[guardrail["triggered"], "metric"].tolist()
            decision = "BLOCKED"
            reasoning = (
                f"Primary significant but guardrail(s) triggered: "
                f"{', '.join(triggered_names)}. Do not launch."
            )
        elif any_secondary_sig:
            sig_names = secondary.loc[secondary["significant"], "metric"].tolist()
            decision = "STRONG LAUNCH"
            reasoning = (
                f"Primary significant, guardrails OK, and secondary metric(s) "
                f"also significant after {self.correction} correction: "
                f"{', '.join(sig_names)}."
            )
        else:
            decision = "LAUNCH"
            reasoning = (
                "Primary significant and guardrails OK. Secondary metrics "
                "not significant after correction — launch on primary alone."
            )

        return {
            "primary": primary,
            "secondary": secondary,
            "guardrail": guardrail,
            "all_results": pd.concat(
                [primary, secondary, guardrail], ignore_index=True,
            ),
            "primary_passed": primary_passed,
            "guardrail_ok": guardrail_ok,
            "correction_method": self.correction,
            "decision": decision,
            "reasoning": reasoning,
        }

    def summary_table(self) -> pd.DataFrame:
        """One-row-per-metric table with role, p-values, and significance."""
        ht = self.hierarchical_test()
        return ht["all_results"][
            ["metric", "role", "control", "treatment",
             "relative_diff_%", "p_value", "corrected_p", "significant"]
        ]
