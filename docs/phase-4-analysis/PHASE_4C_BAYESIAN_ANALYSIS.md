# Bayesian A/B Testing Framework — Posterior Inference and Decision-Making Under Uncertainty

> A conjugate Bayesian analysis pipeline that complements the frequentist framework, providing probability statements ("99.9% chance treatment is better"), expected loss quantification, and the ability to monitor experiments continuously without inflating error rates.

---

## Overview

Frequentist p-values answer the wrong question for most business stakeholders. A p-value of 0.03 does **not** mean there's a 97% chance the treatment works. It means: if the treatment had zero effect, there's a 3% chance of seeing data this extreme. That distinction confuses even experienced analysts.

Bayesian analysis answers the question people actually ask: **"What is the probability that treatment B is better than control A?"** It produces direct probability statements, incorporates prior knowledge, and allows continuous monitoring — a critical advantage when stakeholders demand early results.

This framework implements both Beta-Binomial (conversion rates) and Normal-Normal (continuous metrics) conjugate models, running alongside the frequentist analysis for cross-validation.

---

## When Bayesian Beats Frequentist — and When It Doesn't

| Scenario | Better Approach | Why |
|----------|----------------|-----|
| Communicating to executives | **Bayesian** | "95% probability of improvement" is universally understood |
| Continuous monitoring / early stopping | **Bayesian** | No p-value inflation; posterior is valid at any sample size |
| Incorporating historical data | **Bayesian** | Informative priors encode prior experiment results |
| Quantifying cost of wrong decision | **Bayesian** | Expected loss directly measures business risk |
| Regulatory / academic publication | **Frequentist** | P-values and CIs are the accepted standard |
| Running 100+ experiments / week | **Frequentist** | Faster computation; less analyst judgment on priors |
| Small teams, limited stats expertise | **Frequentist** | Simpler interpretation rules; less prior specification risk |
| Cross-validating results | **Both** | Agreement between methods strengthens confidence |

> **Industry practice:** Netflix uses Bayesian methods for their experimentation platform because it enables "anytime valid" inference — product teams can check results daily without statistical penalty. Booking.com runs frequentist as default but layers Bayesian analysis for high-stakes decisions. Most mature experimentation platforms (Optimizely, VWO) now offer both.

---

## Mathematical Framework

### Conjugate Beta-Binomial Model (Conversion Rates)

The Beta distribution is the conjugate prior for the Binomial likelihood, which means the posterior is also Beta — no MCMC sampling needed.

```
Prior:      theta ~ Beta(alpha_0, beta_0)
Likelihood: X | theta ~ Binomial(n, theta)
Posterior:  theta | X ~ Beta(alpha_0 + successes, beta_0 + failures)
```

With an uninformative Beta(1, 1) prior (uniform over [0, 1]):

```
Control:   Beta(1 + 5981, 1 + 43801) = Beta(5982, 43802)
Treatment: Beta(1 + 6679, 1 + 42980) = Beta(6680, 42981)
```

The prior contributes 2 pseudo-observations against ~50,000 real observations, making it negligible. This is by design — with large samples, the data should overwhelm the prior.

### Monte Carlo Probability Estimation

Draw 100,000 samples from each posterior. The probability that treatment beats control is the fraction of draws where the treatment sample exceeds the control sample:

```python
P(treatment > control) = mean(theta_treatment_samples > theta_control_samples)
```

### Expected Loss

Expected loss quantifies the **cost of choosing the wrong variant**:

```
E[loss | choose control]   = E[max(theta_treat - theta_ctrl, 0)]
E[loss | choose treatment] = E[max(theta_ctrl - theta_treat, 0)]
```

Choose the variant with lower expected loss. Unlike a p-value threshold, expected loss is on the metric's natural scale (e.g., percentage points of conversion), making it directly interpretable for business decisions.

> **Why expected loss matters:** A p-value tells you whether an effect exists. Expected loss tells you **how much you lose by picking wrong**. If expected loss from choosing control is 1.4 pp and from choosing treatment is 0.0 pp, the decision is obvious regardless of where p falls relative to 0.05.

### ROPE Analysis (Region of Practical Equivalence)

ROPE separates statistical significance from practical significance natively within the Bayesian framework. The lift posterior is classified into three zones relative to a practical equivalence region (e.g., [-0.5 pp, +0.5 pp]):

| Zone | Interpretation |
|------|----------------|
| Below ROPE | Treatment is practically worse |
| Inside ROPE | Variants are practically equivalent — doesn't matter which ships |
| Above ROPE | Treatment is practically better |

This eliminates the "statistically significant but trivially small" problem that plagues frequentist analysis.

---

## Normal-Normal Model (Continuous Metrics)

For continuous metrics like revenue per user, the framework uses a Normal-Normal conjugate model:

```
Prior:     mu ~ Normal(mu_0, sigma_0^2)
Data:      X_1, ..., X_n ~ Normal(mu, sigma^2)
Posterior: mu | X ~ Normal(posterior_mean, posterior_variance)
```

With a weakly informative prior (large sigma_0), the posterior mean converges to the sample mean and the posterior variance to the standard error squared.

---

## Prior Specification and Sensitivity

### Available Priors

| Prior | Parameters | Use Case |
|-------|-----------|----------|
| Uninformative | Beta(1, 1) | No prior knowledge; let data speak |
| Jeffreys | Beta(0.5, 0.5) | Maximum ignorance prior; slight edge preference |
| Weakly informative | Beta(10, 90) | Expect ~10% conversion; wide uncertainty |
| Moderately informative | Beta(50, 450) | Confident in ~10% baseline from prior experiments |

### Prior Sensitivity Analysis

The framework automatically re-runs the analysis under all four priors. If the conclusion changes with the prior, the result is **not robust** and more data is needed.

For experiment 001, all four priors produce P(treatment > control) > 99.9%. The conclusion is **completely robust** to prior specification because the sample size (~50K per variant) overwhelms any reasonable prior.

> **Critical consideration:** Prior sensitivity matters most for small samples. With 500 users per variant, a Beta(50, 450) prior centered at 10% will noticeably pull the posterior. With 50,000 users, it's irrelevant. Always run sensitivity analysis, but expect it to matter only for early-stage or low-traffic experiments.

---

## Sequential Monitoring

One of Bayesian analysis's strongest practical advantages: you can check results at any point without inflating error rates. The posterior is valid after 100 observations or 100,000.

The framework computes P(treatment > control) and expected loss at 30 evenly-spaced checkpoints throughout the experiment. This produces a **convergence plot** showing how beliefs evolve as data accumulates.

For experiment 001, the probability crossed 95% well before the full sample was collected — the experiment could have been stopped earlier, freeing traffic for other tests.

> **Contrast with frequentist:** Checking a p-value 10 times during an experiment inflates the Type I error rate from 5% to ~14% (Armitage et al., 1969). Bayesian posterior probabilities have no such penalty, though they can still be miscalibrated with poor priors.

---

## Verified Results — Experiment 001

### Conversion Rate (Beta-Binomial)

| Metric | Value |
|--------|-------|
| P(treatment > control) | 100.00% |
| Posterior mean (control) | 12.02% |
| Posterior mean (treatment) | 13.45% |
| Absolute lift | +1.43 pp (+11.9%) |
| 95% Credible Interval | [+1.02, +1.85] pp |
| E[loss if choose control] | 0.01434 |
| E[loss if choose treatment] | 0.00000 |
| ROPE conclusion | Treatment better |
| **Decision** | **LAUNCH TREATMENT** |

### Revenue per User (Normal-Normal)

| Metric | Value |
|--------|-------|
| P(treatment > control) | 99.91% |
| Control mean | R$16.91 |
| Treatment mean | R$18.67 |
| Lift | R$+1.77 (+10.4%) |
| 95% Credible Interval | [R$0.66, R$2.88] |

---

## Frequentist vs. Bayesian Comparison

| Aspect | Frequentist | Bayesian |
|--------|-------------|----------|
| Core output | p = 1.12e-11 | P(treat > ctrl) = 100.0% |
| What it answers | "How surprising is this data if null is true?" | "How probable is it that treatment is better?" |
| Interval | CI [1.02, 1.85] pp | CrI [1.02, 1.85] pp |
| Interpretation of interval | "95% of such intervals contain the true value" | "95% probability the true value is in this interval" |
| Effect quantification | Cohen's h = 0.0431 | E[loss of choosing control] = 0.01434 |
| Decision | **LAUNCH** | **LAUNCH TREATMENT** |
| Prior specification | Not applicable | Beta(1,1) — robust to alternatives |
| Early stopping | Not valid (inflates alpha) | Valid at any sample size |

Both methods **agree** on the decision, which is the strongest endorsement of the result. When frequentist and Bayesian approaches diverge, it typically indicates a borderline result that warrants caution.

> **Note on interval coincidence:** The CI and CrI are nearly identical here because the uninformative prior contributes negligible information. With informative priors or small samples, they will diverge — the CrI will be pulled toward the prior.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Conjugate models over MCMC | Beta-Binomial, Normal-Normal | Exact posteriors; no sampling convergence issues; millisecond computation |
| Uninformative default prior | Beta(1, 1) | Conservative; lets data dominate; avoids debates about prior |
| 100K Monte Carlo samples | Balance of precision and speed | Standard error of P(better) estimate is ~0.001 |
| ROPE width of 0.5 pp | Half of typical natural variation | Conservative practical equivalence zone |
| Run both frameworks | Frequentist + Bayesian | Cross-validation; serves different audiences |

---

## Production Considerations

- **Computation:** Conjugate models compute in milliseconds. No GPU or MCMC infrastructure needed. This scales to hundreds of experiments.
- **Prior governance:** In a production setting, priors should be logged and versioned. A team wiki or config file should document the rationale for each prior choice.
- **Stakeholder reporting:** Default reports show Bayesian results prominently (probability statements are more intuitive) with frequentist results as a technical appendix.
- **Alert thresholds:** Bayesian monitoring enables alerts when P(treatment worse) > 5% (stop for harm) or P(treatment better) > 99% with expected loss < 0.001 (early success).
- **Caveats:** Bayesian analysis is not a license to ignore sample size planning. Small samples with strong priors can produce confident-looking posteriors driven by the prior, not the data.

---

*Last updated: March 2026*
