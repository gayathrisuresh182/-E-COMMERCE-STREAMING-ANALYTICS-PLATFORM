# Experimentation Playbook — A Practitioner's Guide to Running Rigorous A/B Tests

> The complete guide for designing, running, analyzing, and deciding on A/B experiments. Follow this playbook to avoid the most common mistakes in experimentation and make defensible launch decisions backed by statistical evidence.

---

## Overview

Experimentation is not a statistics exercise — it's a **decision-making system**. A well-run experimentation program turns product intuition into evidence, protects revenue from bad changes, and compounds learning over time.

This playbook codifies the methodology behind the platform's 10 e-commerce experiments. It covers when to test (and when not to), how to design experiments that produce trustworthy results, how to interpret output correctly, how to avoid the pitfalls that plague most A/B testing programs, and how to make launch decisions that balance statistical rigor with business pragmatism.

The principles here are drawn from the practices of high-maturity experimentation teams at companies like Microsoft, Booking.com, Netflix, and Airbnb, adapted for an e-commerce context.

---

## When to Run an A/B Test

### Good Candidates

| Category | Examples | Why A/B Testing Works |
|----------|----------|----------------------|
| Conversion funnel | Checkout flow, payment options, pricing | High traffic; clear binary outcome; directly tied to revenue |
| User-facing features | UI changes, navigation, search ranking | Observable behavior change; measurable engagement |
| Marketing | Messaging, ad creative, landing pages | High volume; fast iteration cycle |
| Operational changes | Email sequences, notification timing, support workflows | Measurable downstream metrics; randomizable |

**Requirements for a valid A/B test:**
- A **measurable outcome** tied to the hypothesis
- **Sufficient traffic** to reach statistical power within a reasonable timeframe
- **Randomization is possible** (can assign users independently)
- **Short enough** to measure the outcome within weeks, not months

### When NOT to Run an A/B Test

| Situation | Reason | Better Approach |
|-----------|--------|-----------------|
| Tiny user segments | Cannot reach adequate power; months to run | Qualitative research; quasi-experiments |
| Long-term effects | Compounding, seasonality, and attrition confound results | Longitudinal studies; holdout groups |
| Randomization impossible | Account-level or geo-based constraints | Difference-in-differences; synthetic controls |
| Qualitative question | "Do users understand this UI?" | Usability testing; user interviews |
| One-off events | No repeatability; no valid control group | Pre/post analysis with caveats |
| Legal or regulatory changes | Must apply uniformly to all users | Compliance-driven; no experiment needed |
| Infrastructure changes | Backend migration, database switch | Canary deploy; performance monitoring |

> **Decision rule:** If you cannot (1) randomize users, (2) define a measurable primary metric, and (3) reach 80% power within 4 weeks, an A/B test is probably not the right tool.

---

## Experiment Design Checklist

### Phase 1: Pre-Experiment Design

| Step | Description | Why It Matters |
|------|-------------|----------------|
| Formulate hypothesis | Clear, directional, testable statement | Prevents HARKing; anchors the analysis |
| Define primary metric | Single metric that determines success | Prevents metric shopping; focuses the test |
| Define secondary metrics | Additional context (engagement, revenue, retention) | Enriches understanding without driving the decision |
| Define guardrail metrics | Safety constraints (error rates, refunds, support tickets) | Prevents shipping changes that harm other metrics |
| Set success criteria | Alpha, power, MDE, and business ROI threshold | Pre-commits to decision rules before seeing data |
| Calculate sample size | Power analysis for target MDE and power | Prevents underpowered tests and wasted traffic |
| Estimate runtime | Sample size / daily traffic | Ensures the test fits the business window |
| Design randomization | User ID hash, consistent assignment, 50/50 split | Ensures valid causal inference |
| Verify instrumentation | Event tracking, metric computation, data pipeline | Garbage in, garbage out |

> **The most important step is defining the primary metric before the experiment starts.** Everything else follows from this. If you define it after seeing results, the analysis is invalid regardless of how sophisticated the statistics are.

### Phase 2: During the Experiment

| Step | Description | Frequency |
|------|-------------|-----------|
| Monitor sample accumulation | Track progress toward planned sample size | Daily |
| Check for SRM | Sample Ratio Mismatch — is the 50/50 split holding? | Daily |
| Monitor guardrails | Watch for critical degradation | Daily |
| Maintain no-peeking discipline | Do not make decisions before reaching planned sample | Continuous |
| Log external events | Promotions, outages, competitor actions that could confound | As they occur |

> **Why SRM matters:** If you expect 50/50 but observe 47/53, something is wrong with randomization. The experimental results are unreliable regardless of what the p-value says. Common causes: redirect-based assignment where one variant loads slower, bot filtering that affects variants differently, or assignment bugs.

### Phase 3: Post-Experiment Analysis

| Step | Description |
|------|-------------|
| Run pre-specified statistical test | Exactly the test declared in Phase 1 — no shopping |
| Calculate confidence intervals | CIs communicate precision; p-values alone do not |
| Assess practical significance | Compare observed lift to MDE; is this worth shipping? |
| Check achieved power | Was the sample adequate? If not, non-significance is inconclusive |
| Apply multiple testing correction | Holm for secondary metrics; BH for cross-experiment |
| Check guardrails | Any violations block launch regardless of primary result |
| Document everything | Results, decision, rationale, dissenting views |
| Make launch decision | Use the decision framework below |
| Plan post-launch monitoring | Track metrics for 30-60 days after full rollout |

---

## Statistical Test Selection

```
What type of metric?
|
+-- Binary (conversion, click-through, yes/no)
|   +-- n >= 30 per variant  --> Z-test for two proportions
|   +-- n < 30               --> Fisher's exact test
|
+-- Continuous (revenue, time-on-site, AOV)
|   +-- Normal distribution (Shapiro-Wilk p >= 0.05)
|   |   --> Welch's t-test (default; handles unequal variance)
|   +-- Non-normal distribution
|       --> Mann-Whitney U test (rank-based)
|
+-- Count (items per user, events per session)
    --> Poisson test or Chi-square
```

| Test | When to Use | Python Implementation |
|------|-------------|----------------------|
| Z-test for proportions | Binary, n >= 30 | `proportions_ztest` (statsmodels) |
| Chi-square | Binary, contingency table | `chi2_contingency` (scipy) |
| Fisher's exact | Binary, n < 30 | `fisher_exact` (scipy) |
| Welch's t-test | Continuous, normal | `ttest_ind(equal_var=False)` (scipy) |
| Mann-Whitney U | Continuous, non-normal | `mannwhitneyu` (scipy) |

---

## Interpreting Results Correctly

### P-Value: What It Is and What It Isn't

| Statement | Correct? |
|-----------|----------|
| "p = 0.03 means there's a 97% chance the treatment works" | **Wrong.** P-value is not the probability that the hypothesis is true |
| "p = 0.03 means: if the null were true, there's a 3% chance of data this extreme" | **Correct.** It's a statement about the data, not the hypothesis |
| "p = 0.06 means the treatment doesn't work" | **Wrong.** It means the evidence is insufficient at alpha = 0.05; the treatment may still work |
| "p = 0.001 means the effect is large" | **Wrong.** P-values reflect sample size and effect size together; a tiny effect with huge sample gives tiny p |

### Confidence Intervals Tell the Full Story

| CI Characteristic | Interpretation |
|-------------------|----------------|
| Excludes zero | Statistically significant; directional effect supported |
| Includes zero | Not significant; effect could be zero |
| Narrow (e.g., +8% to +14%) | Precise estimate; result is informative |
| Wide (e.g., -5% to +25%) | Imprecise; need more data |

> **Best practice:** Always report confidence intervals alongside p-values. A p-value of 0.001 with a CI of [+0.1%, +0.3%] tells a very different story than p = 0.04 with a CI of [+5%, +20%]. The first is precisely estimated to be trivially small. The second is uncertain but potentially large.

### Effect Size Benchmarks

| Measure | Small | Medium | Large |
|---------|-------|--------|-------|
| Cohen's d (continuous) | 0.2 | 0.5 | 0.8 |
| Cohen's h (proportions) | 0.2 | 0.5 | 0.8 |
| Relative lift (e-commerce) | < 5% | 5-15% | > 15% |

---

## Common Pitfalls and How to Avoid Them

### 1. Peeking at Results

**The problem:** Checking p-values daily and stopping when you see significance. Each peek inflates the false positive rate. Checking 10 times during an experiment raises alpha from 5% to approximately 14%.

**The fix:** Pre-commit to a sample size. If early stopping is necessary, use sequential testing methods (O'Brien-Fleming boundaries) or Bayesian monitoring, which are designed for valid continuous monitoring.

### 2. HARKing (Hypothesizing After Results are Known)

**The problem:** Running the analysis, finding that secondary metric X improved, and then claiming X was the hypothesis all along. This invalidates the entire inferential framework.

**The fix:** Document the hypothesis and primary metric in the experiment catalog **before** the experiment starts. Phase 1 of the checklist enforces this.

### 3. P-Hacking / Metric Shopping

**The problem:** Testing 10 metrics and reporting the one that's significant. Running the test with and without outliers, with different date windows, with different segments, until something "works."

**The fix:** Pre-register all metrics and analysis decisions. Apply multiple testing correction (Holm within experiment, BH across experiments). Report all results, not just the flattering ones.

### 4. Ignoring Practical Significance

**The problem:** Celebrating p = 0.001 on a +0.5% relative lift. The effect is real but trivially small — not worth the engineering effort to ship.

**The fix:** Set a Minimum Detectable Effect (MDE) before the experiment. Require both statistical significance (p < 0.05) AND practical significance (lift >= MDE) for a LAUNCH decision.

### 5. Sample Ratio Mismatch (SRM)

**The problem:** The 50/50 randomization is actually 47/53. This usually indicates a bug in the assignment logic, a crawler/bot imbalance, or a redirect that causes differential dropout.

**The detection:** Chi-square test on variant assignment counts. If p < 0.01, investigate before trusting any results.

**The fix:** Audit the randomization implementation. Common causes: client-side assignment where slow-loading variants cause users to leave before being counted, or server-side filtering that disproportionately affects one variant.

### 6. Simpson's Paradox

**The problem:** Treatment wins overall but loses in every segment (or vice versa). This happens when segment sizes differ between variants due to unequal randomization across segments.

**The fix:** Always check segment-level results (device type, region, new vs. returning) alongside the aggregate. If directions conflict, investigate the underlying mechanism.

---

## Launch Decision Framework

### Decision Matrix

| Statistical Result | Practical Impact | Guardrails | Decision |
|-------------------|------------------|------------|----------|
| Significant (p < 0.05) | Lift >= MDE | All clear | **LAUNCH** |
| Significant | Lift > 0 but < MDE | All clear | **ITERATE** — effect is real but too small; consider amplification |
| Not significant | Large observed lift | All clear | **EXTEND** — likely underpowered; collect more data |
| Not significant | Small observed lift | All clear | **ABANDON** — effect is likely null or too small to detect |
| Significant | Any | Guardrail violated | **DO NOT LAUNCH** — fix the violation first |
| Not significant | Any | Guardrail violated | **ABANDON** — no benefit and possible harm |

### Recommendation Template

```
EXPERIMENT:   [Name / ID]
DATE:         [Analysis date]
ANALYST:      [Name]
DECISION:     [LAUNCH / ITERATE / EXTEND / ABANDON]

STATISTICAL EVIDENCE:
  Primary metric:    [Conversion rate / Revenue / etc.]
  Observed lift:     [X% relative / X pp absolute]
  P-value:           [X]
  95% CI:            [lower, upper]
  Achieved power:    [X%]

PRACTICAL ASSESSMENT:
  MDE threshold:     [X% relative]
  Exceeds MDE:       [Yes / No]
  Effect size:       [Cohen's d/h = X, classification]

BUSINESS IMPACT:
  Monthly revenue:   R$[X] ([CI lower] to [CI upper])
  Annual projection: R$[X]
  Implementation:    [Cost / complexity estimate]

GUARDRAILS:
  [Metric 1]:        [Pass / Fail — details]
  [Metric 2]:        [Pass / Fail — details]

RISKS AND CAVEATS:
  [List concerns, limitations, external factors]

RECOMMENDATION:
  [Detailed justification for the decision]
```

---

## Quick Reference

| Parameter | Default | Notes |
|-----------|---------|-------|
| Alpha (Type I error) | 0.05 | Adjust to 0.01 for high-stakes decisions |
| Power (1 - beta) | 80% | Use 90% for revenue-critical experiments |
| Test sidedness | Two-tailed | One-tailed only with strong directional prior |
| MDE (relative) | 10% | Adjust per experiment based on business value |
| Multiple testing | Holm (within), BH FDR (across) | Hierarchical strategy preserves primary metric power |
| Minimum runtime | 7 days | Captures day-of-week effects even if sample size is met earlier |

---

## Experimentation Maturity Model

| Level | Characteristics | This Platform |
|-------|----------------|---------------|
| **1 - Ad hoc** | No process; gut decisions; occasional informal tests | -- |
| **2 - Emerging** | Some A/B tests; inconsistent methodology; no correction | -- |
| **3 - Structured** | Pre-registered hypotheses; standardized analysis; power analysis | Current level |
| **4 - Advanced** | Bayesian analysis; sequential testing; automated pipelines; portfolio optimization | Partially implemented |
| **5 - World-class** | Experiment-driven culture; causal inference; long-term holdouts; ML-optimized allocation | Aspirational |

This platform operates at Level 3-4: pre-registered hypotheses, automated analysis pipelines (Dagster), both frequentist and Bayesian methods, multiple testing correction, and power analysis. The experimentation playbook ensures consistency across all experiments.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Fixed-horizon over sequential | Simpler; avoids early-stopping complexity | Sequential testing adds value for high-traffic platforms; overkill for 10 experiments |
| Two-tailed default | Conservative | One-tailed masks potential harm from treatments |
| 10% MDE default | Practical for e-commerce | Smaller effects are real but rarely worth shipping for most features |
| Hierarchical metric families | Primary/Secondary/Guardrail | Preserves power for the most important test |
| Document before analyze | Pre-registration ethos | The single most important defense against invalid inference |

---

## Production Considerations

- **Experiment governance:** In a production environment, experiment proposals should go through a review process (design doc, power analysis, metric registration) before traffic is allocated.
- **Traffic management:** With limited traffic, experiments compete for users. A traffic allocation system (e.g., layers/slots as used by Google) prevents collisions.
- **Interaction effects:** When experiments run simultaneously, they can interact. Isolation (each user in at most one experiment) is simplest but wasteful. Layered randomization is more efficient but harder to analyze.
- **Cultural adoption:** The hardest part of experimentation is not the statistics — it's getting teams to accept results they disagree with. The playbook's fixed decision rules depersonalize the decision.
- **Continuous improvement:** After each quarter, review the experiment portfolio: What fraction of launches held their gains? What was the average time-to-decision? Where did the process break down?

---

*Last updated: March 2026*
