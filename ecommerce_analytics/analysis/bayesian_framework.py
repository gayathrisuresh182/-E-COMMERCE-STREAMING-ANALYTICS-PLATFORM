"""Bayesian A/B testing framework.

Complements the frequentist ``ExperimentAnalyzer`` with a fully Bayesian
workflow built on conjugate Beta-Binomial models (for conversion) and
Normal-Normal models (for continuous metrics like revenue).

Key outputs
-----------
- Posterior distributions for each variant
- P(treatment > control) via Monte Carlo
- 95 % Highest-Density credible intervals
- Expected loss for each decision
- Risk-of-choosing-wrong quantification
- Rope (Region of Practical Equivalence) analysis
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats as sp_stats


# ── Data containers ────────────────────────────────────────────────────────


@dataclass
class BayesianResult:
    """Full output of a Bayesian A/B analysis."""

    # Posterior parameters
    control_alpha: float
    control_beta: float
    treatment_alpha: float
    treatment_beta: float

    # Monte-Carlo outputs
    prob_treatment_better: float
    prob_control_better: float

    # Credible intervals (95 %)
    control_ci: tuple[float, float]
    treatment_ci: tuple[float, float]
    lift_ci: tuple[float, float]

    # Expected loss
    expected_loss_control: float
    expected_loss_treatment: float

    # Point estimates
    control_mean: float
    treatment_mean: float
    lift_mean: float
    lift_relative_pct: float

    # Samples (kept for plotting)
    control_samples: np.ndarray = field(repr=False, default_factory=lambda: np.array([]))
    treatment_samples: np.ndarray = field(repr=False, default_factory=lambda: np.array([]))
    lift_samples: np.ndarray = field(repr=False, default_factory=lambda: np.array([]))


@dataclass
class BayesianContinuousResult:
    """Bayesian result for a continuous metric (e.g. revenue)."""

    prob_treatment_better: float
    prob_control_better: float
    control_ci: tuple[float, float]
    treatment_ci: tuple[float, float]
    lift_ci: tuple[float, float]
    expected_loss_control: float
    expected_loss_treatment: float
    control_mean: float
    treatment_mean: float
    lift_mean: float
    lift_relative_pct: float
    control_samples: np.ndarray = field(repr=False, default_factory=lambda: np.array([]))
    treatment_samples: np.ndarray = field(repr=False, default_factory=lambda: np.array([]))
    lift_samples: np.ndarray = field(repr=False, default_factory=lambda: np.array([]))


# ── Core engine ────────────────────────────────────────────────────────────


class BayesianAnalyzer:
    """Bayesian A/B test analyser.

    Parameters
    ----------
    data : DataFrame
        Experiment data with ``variant`` and ``converted`` columns, plus
        ``order_total`` for continuous-metric analysis.
    prior_alpha, prior_beta : float
        Beta prior hyper-parameters for conversion rate.
        Defaults to Beta(1, 1) — the uninformative uniform prior.
    n_samples : int
        Number of Monte-Carlo draws from the posteriors.
    seed : int
        RNG seed for reproducibility.

    Example
    -------
    >>> ba = BayesianAnalyzer(data, prior_alpha=1, prior_beta=1)
    >>> result = ba.conversion_test()
    >>> print(f"P(treat > ctrl) = {result.prob_treatment_better:.1%}")
    """

    def __init__(
        self,
        data: pd.DataFrame,
        prior_alpha: float = 1.0,
        prior_beta: float = 1.0,
        n_samples: int = 100_000,
        seed: int = 42,
    ) -> None:
        self.data = data
        self.prior_alpha = prior_alpha
        self.prior_beta = prior_beta
        self.n_samples = n_samples
        self.rng = np.random.default_rng(seed)

        self.control = data[data["variant"] == "control"]
        self.treatment = data[data["variant"] == "treatment"]

        self.n_ctrl = len(self.control)
        self.n_treat = len(self.treatment)
        self.conv_ctrl = int(self.control["converted"].sum())
        self.conv_treat = int(self.treatment["converted"].sum())

    # ── Conversion rate (Beta-Binomial) ────────────────────────────────

    def conversion_test(self) -> BayesianResult:
        """Run the full Bayesian conversion-rate analysis."""

        # Posterior parameters (conjugate update)
        a_ctrl = self.prior_alpha + self.conv_ctrl
        b_ctrl = self.prior_beta + (self.n_ctrl - self.conv_ctrl)
        a_treat = self.prior_alpha + self.conv_treat
        b_treat = self.prior_beta + (self.n_treat - self.conv_treat)

        # Monte Carlo draws
        ctrl_samples = self.rng.beta(a_ctrl, b_ctrl, self.n_samples)
        treat_samples = self.rng.beta(a_treat, b_treat, self.n_samples)
        lift_samples = treat_samples - ctrl_samples

        # Probabilities
        prob_treat = float((treat_samples > ctrl_samples).mean())
        prob_ctrl = 1.0 - prob_treat

        # Credible intervals (2.5th – 97.5th percentile = 95 % CI)
        ctrl_ci = (float(np.percentile(ctrl_samples, 2.5)),
                   float(np.percentile(ctrl_samples, 97.5)))
        treat_ci = (float(np.percentile(treat_samples, 2.5)),
                    float(np.percentile(treat_samples, 97.5)))
        lift_ci = (float(np.percentile(lift_samples, 2.5)),
                   float(np.percentile(lift_samples, 97.5)))

        # Expected loss
        loss_ctrl = float(np.maximum(treat_samples - ctrl_samples, 0).mean())
        loss_treat = float(np.maximum(ctrl_samples - treat_samples, 0).mean())

        # Point estimates
        ctrl_mean = float(ctrl_samples.mean())
        treat_mean = float(treat_samples.mean())
        lift_mean = float(lift_samples.mean())
        lift_rel = lift_mean / ctrl_mean * 100 if ctrl_mean > 0 else 0.0

        return BayesianResult(
            control_alpha=a_ctrl,
            control_beta=b_ctrl,
            treatment_alpha=a_treat,
            treatment_beta=b_treat,
            prob_treatment_better=prob_treat,
            prob_control_better=prob_ctrl,
            control_ci=ctrl_ci,
            treatment_ci=treat_ci,
            lift_ci=lift_ci,
            expected_loss_control=loss_ctrl,
            expected_loss_treatment=loss_treat,
            control_mean=ctrl_mean,
            treatment_mean=treat_mean,
            lift_mean=lift_mean,
            lift_relative_pct=round(lift_rel, 2),
            control_samples=ctrl_samples,
            treatment_samples=treat_samples,
            lift_samples=lift_samples,
        )

    # ── Continuous metric (Normal approximation) ───────────────────────

    def continuous_test(self, column: str = "order_total") -> BayesianContinuousResult:
        """Bayesian comparison of a continuous metric between variants.

        Uses a Normal-Normal conjugate model: with a vague prior the
        posterior for each group mean is approximately
        Normal(sample_mean, sample_se^2).
        """
        ctrl_vals = self.control[column].dropna().values.astype(float)
        treat_vals = self.treatment[column].dropna().values.astype(float)

        ctrl_mean, ctrl_se = float(ctrl_vals.mean()), float(ctrl_vals.std(ddof=1) / np.sqrt(len(ctrl_vals)))
        treat_mean, treat_se = float(treat_vals.mean()), float(treat_vals.std(ddof=1) / np.sqrt(len(treat_vals)))

        ctrl_samples = self.rng.normal(ctrl_mean, ctrl_se, self.n_samples)
        treat_samples = self.rng.normal(treat_mean, treat_se, self.n_samples)
        lift_samples = treat_samples - ctrl_samples

        prob_treat = float((treat_samples > ctrl_samples).mean())

        ctrl_ci = (float(np.percentile(ctrl_samples, 2.5)),
                   float(np.percentile(ctrl_samples, 97.5)))
        treat_ci = (float(np.percentile(treat_samples, 2.5)),
                    float(np.percentile(treat_samples, 97.5)))
        lift_ci = (float(np.percentile(lift_samples, 2.5)),
                   float(np.percentile(lift_samples, 97.5)))

        loss_ctrl = float(np.maximum(treat_samples - ctrl_samples, 0).mean())
        loss_treat = float(np.maximum(ctrl_samples - treat_samples, 0).mean())

        lift_mean = float(lift_samples.mean())
        lift_rel = lift_mean / ctrl_mean * 100 if ctrl_mean != 0 else 0.0

        return BayesianContinuousResult(
            prob_treatment_better=prob_treat,
            prob_control_better=1.0 - prob_treat,
            control_ci=ctrl_ci,
            treatment_ci=treat_ci,
            lift_ci=lift_ci,
            expected_loss_control=loss_ctrl,
            expected_loss_treatment=loss_treat,
            control_mean=ctrl_mean,
            treatment_mean=treat_mean,
            lift_mean=lift_mean,
            lift_relative_pct=round(lift_rel, 2),
            control_samples=ctrl_samples,
            treatment_samples=treat_samples,
            lift_samples=lift_samples,
        )

    # ── ROPE analysis ──────────────────────────────────────────────────

    @staticmethod
    def rope_analysis(
        lift_samples: np.ndarray,
        rope_low: float = -0.005,
        rope_high: float = 0.005,
    ) -> dict[str, float]:
        """Region of Practical Equivalence analysis.

        Classifies the posterior lift distribution into three zones:
        below ROPE (treatment worse), inside ROPE (practically equivalent),
        above ROPE (treatment better).
        """
        below = float((lift_samples < rope_low).mean())
        inside = float(((lift_samples >= rope_low) & (lift_samples <= rope_high)).mean())
        above = float((lift_samples > rope_high).mean())
        return {
            "rope_low": rope_low,
            "rope_high": rope_high,
            "prob_below_rope": round(below, 4),
            "prob_inside_rope": round(inside, 4),
            "prob_above_rope": round(above, 4),
            "conclusion": (
                "Practically equivalent"
                if inside > 0.90
                else "Treatment better" if above > 0.95
                else "Treatment worse" if below > 0.95
                else "Inconclusive"
            ),
        }

    # ── Prior sensitivity ──────────────────────────────────────────────

    def prior_sensitivity(
        self,
        priors: list[tuple[float, float]] | None = None,
    ) -> pd.DataFrame:
        """Re-run the conversion test under several priors.

        Demonstrates how robust the conclusion is to prior choice.
        """
        if priors is None:
            priors = [
                (1, 1),        # uninformative
                (0.5, 0.5),    # Jeffreys
                (10, 90),      # weakly informative (~10 % rate)
                (100, 900),    # moderately informative (~10 % rate)
            ]

        rows: list[dict[str, Any]] = []
        for pa, pb in priors:
            old_a, old_b = self.prior_alpha, self.prior_beta
            self.prior_alpha, self.prior_beta = pa, pb
            res = self.conversion_test()
            self.prior_alpha, self.prior_beta = old_a, old_b
            rows.append({
                "prior": f"Beta({pa}, {pb})",
                "prior_mean": round(pa / (pa + pb), 4),
                "P(treat > ctrl)": round(res.prob_treatment_better, 4),
                "lift_mean": round(res.lift_mean, 5),
                "lift_ci_low": round(res.lift_ci[0], 5),
                "lift_ci_high": round(res.lift_ci[1], 5),
                "E[loss|ctrl]": round(res.expected_loss_control, 5),
                "E[loss|treat]": round(res.expected_loss_treatment, 5),
            })
        return pd.DataFrame(rows)

    # ── Sequential analysis (cumulative) ───────────────────────────────

    def sequential_probabilities(
        self,
        steps: int = 20,
    ) -> pd.DataFrame:
        """Simulate how P(treatment > control) evolves as data accumulates.

        Splits the observed data into ``steps`` cumulative slices and
        computes the posterior probability at each checkpoint.
        """
        indices_ctrl = np.arange(self.n_ctrl)
        indices_treat = np.arange(self.n_treat)
        self.rng.shuffle(indices_ctrl)
        self.rng.shuffle(indices_treat)

        ctrl_conv_arr = self.control["converted"].values[indices_ctrl]
        treat_conv_arr = self.treatment["converted"].values[indices_treat]

        checkpoints = np.linspace(1, 1.0, steps)
        rows: list[dict[str, Any]] = []

        for frac in checkpoints:
            nc = max(int(self.n_ctrl * frac), 10)
            nt = max(int(self.n_treat * frac), 10)

            sc = int(ctrl_conv_arr[:nc].sum())
            st = int(treat_conv_arr[:nt].sum())

            a_c = self.prior_alpha + sc
            b_c = self.prior_beta + (nc - sc)
            a_t = self.prior_alpha + st
            b_t = self.prior_beta + (nt - st)

            c_samp = self.rng.beta(a_c, b_c, 50_000)
            t_samp = self.rng.beta(a_t, b_t, 50_000)
            prob = float((t_samp > c_samp).mean())

            rows.append({
                "fraction": round(frac, 2),
                "n_control": nc,
                "n_treatment": nt,
                "cr_control": round(sc / nc, 4),
                "cr_treatment": round(st / nt, 4),
                "P(treat>ctrl)": round(prob, 4),
            })

        return pd.DataFrame(rows)

    # ── Decision summary ───────────────────────────────────────────────

    def recommend(
        self,
        result: BayesianResult | None = None,
        threshold: float = 0.95,
        loss_threshold: float = 0.001,
    ) -> dict[str, Any]:
        """Decision recommendation based on Bayesian criteria.

        Parameters
        ----------
        threshold : float
            Minimum P(better) to declare a winner (default 95 %).
        loss_threshold : float
            Maximum acceptable expected loss in conversion-rate units
            (default 0.1 pp).
        """
        if result is None:
            result = self.conversion_test()

        if result.prob_treatment_better >= threshold:
            decision = "LAUNCH TREATMENT"
            reasoning = (
                f"P(treatment > control) = {result.prob_treatment_better:.1%} "
                f"exceeds {threshold:.0%} threshold. Expected loss if wrong "
                f"is only {result.expected_loss_treatment:.5f}."
            )
        elif result.prob_control_better >= threshold:
            decision = "KEEP CONTROL"
            reasoning = (
                f"P(control > treatment) = {result.prob_control_better:.1%}. "
                f"Treatment is likely worse."
            )
        elif (result.expected_loss_treatment < loss_threshold
              and result.expected_loss_control < loss_threshold):
            decision = "PRACTICALLY EQUIVALENT"
            reasoning = (
                f"Expected loss for either choice is < {loss_threshold:.4f}. "
                f"Variants are practically indistinguishable."
            )
        else:
            decision = "CONTINUE TESTING"
            reasoning = (
                f"P(treatment > control) = {result.prob_treatment_better:.1%} — "
                f"not yet decisive. Collect more data."
            )

        return {
            "decision": decision,
            "reasoning": reasoning,
            "prob_treatment_better": result.prob_treatment_better,
            "expected_loss_treatment": result.expected_loss_treatment,
            "expected_loss_control": result.expected_loss_control,
            "lift_mean": result.lift_mean,
            "lift_ci": result.lift_ci,
        }
