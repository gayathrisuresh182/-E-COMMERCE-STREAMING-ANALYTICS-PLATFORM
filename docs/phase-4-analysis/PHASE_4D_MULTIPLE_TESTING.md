# Multiple Testing Correction — Controlling False Discoveries Across Metrics and Experiments

> When you test 50 hypotheses at alpha = 0.05, you expect 2.5 false positives by pure chance. Multiple testing correction prevents the experimentation platform from becoming a false-positive factory.

---

## Overview

A single A/B test at alpha = 0.05 has a 5% false positive rate. That's acceptable. But this platform runs **10 experiments**, each with **5 metrics** (1 primary + 3 secondary + 1 guardrail). That's 50 hypothesis tests. Without correction, there's a **92.3% probability** of at least one spurious "significant" result across the portfolio.

Multiple testing correction is the difference between a rigorous experimentation program and one that ships features based on noise. This module implements three correction methods (Bonferroni, Holm, Benjamini-Hochberg) and a hierarchical gate-keeping strategy that protects the primary metric's power while controlling error across the full metric family.

---

## The Problem — Intuitive Explanation

Imagine rolling a 20-sided die. The chance of rolling a 1 is 5% (1/20). Now roll it 50 times. The chance of **never** rolling a 1 is (19/20)^50 = 7.7%. That means there's a 92.3% chance you roll at least one 1.

Each hypothesis test is like rolling that die. The "1" is a false positive. More tests mean more rolls.

| Tests (k) | P(at least 1 false positive) | Intuition |
|-----------|------------------------------|-----------|
| 1 | 5.0% | Single die roll |
| 3 | 14.3% | Noticeable risk |
| 5 | **22.6%** | 1-in-4 experiments produces a false positive |
| 10 | 40.1% | Nearly coin flip |
| 50 | **92.3%** | Almost guaranteed spurious result |

Monte Carlo simulation (50,000 runs under the global null) confirmed: without correction, 22.5% of experiments with 5 metrics produce at least one spurious "significant" result.

> **Industry context:** Microsoft's experimentation platform (ExP) runs thousands of tests per week. They discovered that without correction, teams were regularly shipping features "validated" by p-hacked secondary metrics. Their solution: hierarchical testing with mandatory pre-registration of primary metrics.

---

## Three Correction Methods

### 1. Bonferroni — Simple but Conservative

**Idea:** Divide alpha equally among all tests. Reject if p < alpha / k.

For 5 tests at alpha = 0.05: reject if p < 0.01.

| Aspect | Detail |
|--------|--------|
| Controls | **Family-Wise Error Rate (FWER)** — probability of any false positive |
| Strengths | Simplest to explain; guaranteed FWER control; no assumptions about test independence |
| Weaknesses | Severely reduces power; with 50 tests, threshold becomes p < 0.001 |
| Best for | Safety-critical decisions; regulatory contexts; small number of tests |

> **When Bonferroni hurts:** If you're testing 10 metrics and the real effect is moderate, Bonferroni may cause you to miss it. You're paying a power tax to control an error rate that may be too strict for business decisions.

### 2. Holm (Step-Down Bonferroni) — Strictly Better than Bonferroni

**Idea:** Sort p-values smallest to largest. Test each sequentially with an increasingly lenient threshold.

**Procedure:**
1. Sort p-values: p(1) <= p(2) <= ... <= p(k)
2. For the smallest: reject if p(1) < alpha / k (same as Bonferroni)
3. For the second smallest: reject if p(2) < alpha / (k - 1) (slightly easier)
4. Continue until a test fails — stop rejecting

| Aspect | Detail |
|--------|--------|
| Controls | **FWER** — same guarantee as Bonferroni |
| Strengths | **Uniformly more powerful** than Bonferroni; no reason to ever use Bonferroni instead |
| Weaknesses | Still conservative for large test families |
| Best for | 1 primary + 2-5 secondary metrics within an experiment |

> **Why Holm dominates Bonferroni:** Holm controls the same error rate (FWER) but rejects more true effects. There is literally no scenario where Bonferroni outperforms Holm. If you're using Bonferroni for anything other than a quick mental calculation, switch to Holm.

### 3. Benjamini-Hochberg (FDR) — Power-Optimized for Exploration

**Idea:** Instead of controlling the probability of **any** false positive, control the expected **proportion** of false positives among rejected hypotheses.

**Procedure:**
1. Sort p-values: p(1) <= p(2) <= ... <= p(k)
2. Find the largest i where p(i) <= (i / k) * alpha
3. Reject all hypotheses 1 through i

| Aspect | Detail |
|--------|--------|
| Controls | **False Discovery Rate (FDR)** — expected fraction of rejections that are false |
| Strengths | Much more powerful than FWER methods; scales well to many tests |
| Weaknesses | Allows some false positives (controlled fraction); requires independence or PRDS |
| Best for | 10+ tests; cross-experiment correction; exploratory analysis |

> **FDR intuition:** If you reject 10 hypotheses with FDR = 5%, you expect 0.5 of them to be false discoveries. For business decisions, this is usually acceptable — you'd rather find 9.5 real effects and 0.5 false ones than find only 5 real effects with zero false ones.

### FWER Simulation Results (50,000 runs, k=5, global null)

| Method | Actual FWER | Actual FDR |
|--------|-------------|------------|
| No correction | 22.5% | -- |
| Bonferroni | 5.0% | << 5% |
| Holm | 5.0% | << 5% |
| Benjamini-Hochberg | 5.1% | 5.0% |

All three methods successfully control their target error rate. BH is slightly above 5% FWER because it controls FDR, not FWER — this is expected and acceptable.

---

## Hierarchical Gate-Keeping Strategy

The correction methods above treat all tests equally. But in practice, not all metrics are equal. The primary metric is the one driving the launch decision. Secondary metrics provide context. Guardrails are safety checks.

The hierarchical strategy preserves **full alpha for the primary metric** while still controlling error:

```
Step 1: PRIMARY metric — tested at alpha = 0.05 (no correction)
   |
   |-- Not significant --> STOP (experiment failed on primary)
   |-- Significant --> proceed to Step 2
        |
Step 2: SECONDARY metrics — tested with Holm correction among secondary family
        |
Step 3: GUARDRAIL metrics — always tested (separate safety family)
   |
   |-- Triggered (treatment significantly worse) --> BLOCK launch
   |-- Not triggered --> proceed with LAUNCH decision
```

### Why This Works

| Principle | Rationale |
|-----------|-----------|
| Primary gets full alpha | No power loss for the most important test; gate-keeping structure controls FWER |
| Gate-keeping controls overall error | If the primary test is a true null (no effect), secondary tests are never run, so they can't produce false positives |
| Guardrails are a separate family | Safety is a different question than efficacy; a harm check shouldn't be penalized by correction for efficacy tests |
| Secondary metrics use Holm | Best available FWER control within the secondary family |

### Metric Family Specification

| Role | Metric | Type | Direction |
|------|--------|------|-----------|
| Primary | conversion_rate | proportion | two-sided |
| Secondary | aov | continuous | two-sided |
| Secondary | items_per_order | continuous | two-sided |
| Secondary | revenue_per_user | continuous | two-sided |
| Guardrail | review_score | continuous | one-sided (greater = safe) |

---

## Method Selection Guide

| Scenario | Recommended | Why |
|----------|-------------|-----|
| 1 primary + 1-2 secondary within one experiment | Holm | Small family; FWER appropriate |
| 1 primary + 3-5 secondary within one experiment | Holm or FDR | Holm if power is sufficient; FDR if you need to preserve detection |
| 10+ metrics in an exploratory analysis | FDR (BH) | FWER methods kill power with many tests |
| Across 10 experiments (primary p-values only) | FDR (BH) | Controls false discovery across the portfolio |
| Safety-critical or regulatory decision | Bonferroni | Simplest to justify; most conservative |
| Guardrail metrics | No correction | Separate family; safety question should not be diluted |

> **Common mistake:** Applying Bonferroni correction across all 50 tests (10 experiments x 5 metrics) when the hierarchical approach is available. This crushes power for no benefit. Structure your tests into families and apply correction within each family.

---

## Verified Results

### Experiment 001 — Hierarchical Test

| Step | Metric | Raw p-value | Corrected p | Significant | Notes |
|------|--------|-------------|-------------|-------------|-------|
| Primary | conversion_rate | 1.12e-11 | -- | Yes | Gate passed; proceed |
| Secondary | aov | 0.08 | 0.24 | No | Holm corrected (3 tests) |
| Secondary | items_per_order | 0.19 | 0.38 | No | Holm corrected (3 tests) |
| Secondary | revenue_per_user | 9.5e-6 | 2.9e-5 | Yes | Survives Holm correction |
| Guardrail | review_score | 0.65 | -- | No | Not triggered; safe |

**Decision:** STRONG LAUNCH (primary significant, revenue/user also significant after correction, guardrails clear)

### Cross-Experiment Correction (10 Primary p-values)

| Method | Experiments Significant |
|--------|------------------------|
| No correction | 10/10 |
| Bonferroni | 9/10 |
| Holm | 9/10 |
| FDR (BH) | 10/10 |

All experiments survive FDR correction. Bonferroni and Holm lose experiment 10 (Dynamic Pricing, p = 0.013, Bonferroni threshold = 0.005) — this is the power cost of FWER control.

---

## Best Practices

| Practice | Why It Matters |
|----------|----------------|
| Pre-register primary/secondary/guardrail metrics | Prevents post-hoc metric selection (HARKing) |
| Never add metrics after seeing results | Adding metrics after peeking is a form of p-hacking that correction cannot fix |
| Use hierarchical testing | Protects primary metric power while controlling error |
| Separate guardrails from efficacy metrics | Different question (safety vs. improvement); different correction family |
| Report both raw and corrected p-values | Transparency; lets readers assess the correction's impact |
| Apply cross-experiment FDR | When running many experiments simultaneously, portfolio-level correction is essential |

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Hierarchical over flat correction | Gate-keeping strategy | Preserves full power for primary metric |
| Holm for secondary metrics | Step-down Bonferroni | Dominates Bonferroni; appropriate for 3-5 tests |
| BH for cross-experiment | FDR control | 10 experiments; FWER too conservative |
| Separate guardrail family | No correction penalty | Safety checks should not be weakened by efficacy testing |
| Pre-registration required | All metrics declared before data | Only way to make correction meaningful |

---

## Production Considerations

- **Metric registration:** In a production system, metric families should be registered in a configuration store (e.g., experiment catalog) before the experiment starts. Post-hoc additions should be flagged and excluded from corrected results.
- **Scalability:** FDR correction is O(k log k) — sorting p-values. It scales trivially to thousands of tests.
- **Dashboard display:** Show both raw and corrected p-values. Color-code: green if significant after correction, yellow if raw-significant but correction-insignificant, red if raw-insignificant.
- **Audit trail:** Log which correction method was applied and the family structure. This is critical for reproducibility and for explaining results to stakeholders.
- **Failure mode:** If an experiment's primary metric fails, the hierarchical strategy stops analysis of secondary metrics. This is a feature, not a bug — it prevents fishing in secondary metrics when the primary signal is absent.

---

*Last updated: March 2026*
