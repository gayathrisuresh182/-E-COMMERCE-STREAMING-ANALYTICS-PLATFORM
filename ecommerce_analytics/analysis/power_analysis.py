"""Statistical power analysis for A/B experiment design validation.

Provides three analysis modes:

1. **Prospective** — before the experiment: "how many users do I need?"
2. **Retrospective** — after the experiment: "did I have enough power?"
3. **Sensitivity** — "what effect sizes could I detect at 80 % power?"

Also includes a full-catalog audit that compares planned vs actual sample
sizes for every experiment and flags underpowered designs.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats as sp_stats
from statsmodels.stats.power import NormalIndPower, TTestIndPower

from ecommerce_analytics.analysis.stats_framework import (
    ExperimentCatalog,
    ExperimentMeta,
    _BASELINE_RATES,
    cohens_h,
    cohens_d,
    load_experiment_data,
)


# ── Core calculators ──────────────────────────────────────────────────────


def sample_size_calculator(
    baseline_rate: float,
    mde_relative: float,
    alpha: float = 0.05,
    power: float = 0.80,
    ratio: float = 1.0,
    daily_traffic: int | None = None,
) -> dict[str, Any]:
    """Prospective sample-size calculation for a proportion test.

    Parameters
    ----------
    baseline_rate : float
        Control-group conversion rate (e.g. 0.12 for 12 %).
    mde_relative : float
        Minimum detectable effect as relative lift (e.g. 0.10 for 10 %).
    alpha : float
        Significance level.
    power : float
        Target statistical power.
    ratio : float
        Treatment-to-control allocation ratio (1.0 = equal).
    daily_traffic : int or None
        If provided, calculates experiment runtime in days.

    Returns
    -------
    dict with keys: n_per_variant, n_total, effect_size_h,
    target_rate, runtime_days (if daily_traffic given).
    """
    target_rate = baseline_rate * (1 + mde_relative)
    h = abs(cohens_h(baseline_rate, target_rate))

    n = NormalIndPower().solve_power(
        effect_size=h,
        alpha=alpha,
        power=power,
        ratio=ratio,
        alternative="two-sided",
    )
    n_per = int(math.ceil(n))
    n_total = int(math.ceil(n_per * (1 + ratio)))

    result: dict[str, Any] = {
        "baseline_rate": baseline_rate,
        "mde_relative": mde_relative,
        "target_rate": round(target_rate, 6),
        "effect_size_h": round(h, 4),
        "alpha": alpha,
        "power": power,
        "n_per_variant": n_per,
        "n_total": n_total,
    }
    if daily_traffic is not None and daily_traffic > 0:
        result["runtime_days"] = round(n_total / daily_traffic, 1)
    return result


def sample_size_continuous(
    baseline_mean: float,
    baseline_std: float,
    mde_relative: float,
    alpha: float = 0.05,
    power: float = 0.80,
    ratio: float = 1.0,
) -> dict[str, Any]:
    """Prospective sample-size calculation for a t-test."""
    target_mean = baseline_mean * (1 + mde_relative)
    d = abs(target_mean - baseline_mean) / baseline_std if baseline_std > 0 else 0.2

    n = TTestIndPower().solve_power(
        effect_size=d,
        alpha=alpha,
        power=power,
        ratio=ratio,
        alternative="two-sided",
    )
    n_per = int(math.ceil(n))
    return {
        "baseline_mean": baseline_mean,
        "baseline_std": baseline_std,
        "mde_relative": mde_relative,
        "target_mean": round(target_mean, 4),
        "effect_size_d": round(d, 4),
        "alpha": alpha,
        "power": power,
        "n_per_variant": n_per,
        "n_total": int(math.ceil(n_per * (1 + ratio))),
    }


# ── Retrospective power ──────────────────────────────────────────────────


def retrospective_power(
    p_control: float,
    p_treatment: float,
    n_control: int,
    n_treatment: int,
    alpha: float = 0.05,
) -> dict[str, Any]:
    """Achieved power given observed conversion rates and sample sizes."""
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
        power = float("nan")

    return {
        "p_control": p_control,
        "p_treatment": p_treatment,
        "observed_lift": round((p_treatment - p_control) / p_control * 100, 2)
        if p_control > 0 else 0.0,
        "effect_size_h": round(abs(h), 4),
        "n_control": n_control,
        "n_treatment": n_treatment,
        "achieved_power": round(float(power), 4),
        "adequate": float(power) >= 0.80,
        "alpha": alpha,
    }


# ── Sensitivity analysis ─────────────────────────────────────────────────


def sensitivity_analysis(
    n_per_variant: int,
    alpha: float = 0.05,
    test_type: str = "chi_square",
    ratio: float = 1.0,
    power_levels: list[float] | None = None,
) -> pd.DataFrame:
    """What effect sizes are detectable at various power levels?"""
    if power_levels is None:
        power_levels = [0.50, 0.60, 0.70, 0.80, 0.90, 0.95]

    engine = NormalIndPower() if test_type != "t_test" else TTestIndPower()
    rows = []
    for pwr in power_levels:
        try:
            es = engine.solve_power(
                effect_size=None,
                nobs1=n_per_variant,
                alpha=alpha,
                power=pwr,
                ratio=ratio,
                alternative="two-sided",
            )
        except Exception:
            es = float("nan")
        rows.append({
            "power": pwr,
            "min_detectable_effect": round(float(es), 4),
        })
    return pd.DataFrame(rows)


def power_curve(
    effect_sizes: np.ndarray | None = None,
    n_per_variant: int = 5000,
    alpha: float = 0.05,
    test_type: str = "chi_square",
    ratio: float = 1.0,
) -> pd.DataFrame:
    """Power as a function of effect size for a fixed sample size."""
    if effect_sizes is None:
        effect_sizes = np.linspace(0.005, 0.15, 80)

    engine = NormalIndPower() if test_type != "t_test" else TTestIndPower()
    rows = []
    for es in effect_sizes:
        try:
            pwr = engine.solve_power(
                effect_size=float(es),
                nobs1=n_per_variant,
                alpha=alpha,
                ratio=ratio,
                alternative="two-sided",
            )
        except Exception:
            pwr = float("nan")
        rows.append({"effect_size": float(es), "power": float(pwr)})
    return pd.DataFrame(rows)


def sample_size_curve(
    effect_size: float,
    alpha: float = 0.05,
    test_type: str = "chi_square",
    ratio: float = 1.0,
    n_range: np.ndarray | None = None,
) -> pd.DataFrame:
    """Power as a function of sample size for a fixed effect."""
    if n_range is None:
        n_range = np.arange(200, 60_001, 200)

    engine = NormalIndPower() if test_type != "t_test" else TTestIndPower()
    rows = []
    for n in n_range:
        try:
            pwr = engine.solve_power(
                effect_size=effect_size,
                nobs1=int(n),
                alpha=alpha,
                ratio=ratio,
                alternative="two-sided",
            )
        except Exception:
            pwr = float("nan")
        rows.append({"n_per_variant": int(n), "power": float(pwr)})
    return pd.DataFrame(rows)


# ── Full-catalog audit ────────────────────────────────────────────────────


@dataclass
class ExperimentPowerReport:
    """Power audit for a single experiment."""
    experiment_id: str
    experiment_name: str
    test_type: str
    baseline_rate: float
    mde_percent: float
    planned_n: int
    actual_n_control: int
    actual_n_treatment: int
    required_n: int
    observed_cr_control: float
    observed_cr_treatment: float
    observed_lift_pct: float
    prospective_power: float
    achieved_power: float
    adequate: bool
    overpowered: bool


def audit_all_experiments(
    alpha: float = 0.05,
    target_power: float = 0.80,
) -> pd.DataFrame:
    """Run prospective + retrospective power analysis for all 10 experiments.

    Returns a DataFrame with one row per experiment comparing planned
    sample sizes to actual, required n for 80 % power, and achieved power.
    """
    catalog = ExperimentCatalog()
    reports: list[dict[str, Any]] = []

    for exp_id in catalog.list_ids():
        meta = catalog.get(exp_id)
        baseline = _BASELINE_RATES.get(exp_id, 0.12)

        # Prospective: required sample size at MDE
        target_rate = baseline * (1 + meta.mde_percent / 100)
        h_mde = abs(cohens_h(baseline, target_rate))

        try:
            n_required = int(math.ceil(
                NormalIndPower().solve_power(
                    effect_size=h_mde, alpha=alpha,
                    power=target_power, ratio=1.0, alternative="two-sided",
                )
            ))
        except Exception:
            n_required = -1

        # Prospective power at planned n
        try:
            prosp_power = NormalIndPower().solve_power(
                effect_size=h_mde, nobs1=meta.sample_per_variant,
                alpha=alpha, ratio=1.0, alternative="two-sided",
            )
        except Exception:
            prosp_power = float("nan")

        # Load actual data for retrospective
        try:
            data = load_experiment_data(exp_id)
            ctrl = data[data["variant"] == "control"]
            treat = data[data["variant"] == "treatment"]
            n_c, n_t = len(ctrl), len(treat)
            cr_c = ctrl["converted"].mean()
            cr_t = treat["converted"].mean()
        except Exception:
            n_c, n_t = 0, 0
            cr_c, cr_t = 0.0, 0.0

        # Retrospective power at observed effect
        h_obs = abs(cohens_h(cr_c, cr_t)) if cr_c > 0 and cr_t > 0 else 0.0
        try:
            ach_power = NormalIndPower().solve_power(
                effect_size=h_obs, nobs1=n_c, alpha=alpha,
                ratio=n_t / n_c if n_c > 0 else 1.0, alternative="two-sided",
            )
        except Exception:
            ach_power = float("nan")

        lift_pct = (cr_t - cr_c) / cr_c * 100 if cr_c > 0 else 0.0

        reports.append({
            "experiment_id": meta.id,
            "experiment_name": meta.name,
            "test_type": meta.test_type,
            "baseline_rate": round(baseline, 4),
            "mde_%": meta.mde_percent,
            "planned_n": meta.sample_per_variant,
            "required_n_80%": n_required,
            "actual_n_ctrl": n_c,
            "actual_n_treat": n_t,
            "prospective_power": round(float(prosp_power), 4),
            "observed_cr_ctrl": round(cr_c, 4),
            "observed_cr_treat": round(cr_t, 4),
            "observed_lift_%": round(lift_pct, 2),
            "achieved_power": round(float(ach_power), 4),
            "adequate": float(ach_power) >= target_power,
            "overpowered": float(ach_power) >= 0.99,
        })

    return pd.DataFrame(reports)
