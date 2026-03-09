# Automated Weekly Report Pipeline — From Analysis to Stakeholder Inbox

> A Dagster-orchestrated pipeline that generates a weekly experiment report (HTML email, PDF, charts), delivers it to stakeholders via email, and maintains an archive of all past reports for auditability.

---

## Overview

Statistical analysis that lives in notebooks is analysis that nobody reads. The gap between "results are available" and "stakeholders are informed" is where most experimentation programs lose momentum. Product managers don't log into Dagit. Executives don't open Jupyter notebooks.

This pipeline closes that gap by automatically generating a formatted report every Monday morning, rendering experiment results as an HTML email with inline charts, and delivering it to configured recipients. The entire flow — from data freshness check through analysis, report generation, and email delivery — is orchestrated by Dagster with full lineage and failure handling.

---

## Pipeline Architecture

### Asset Lineage

```
experiment_exp_001_analysis ──┐
experiment_exp_002_analysis ──┤
...                           ├──> all_experiments_summary ──> weekly_experiment_report
experiment_exp_010_analysis ──┘                                        |
                                                                       | (sensor)
                                                                       v
                                                            weekly_report_email_job
```

The report asset depends on the experiment summary, which depends on all 10 individual experiment analyses. Materializing the report triggers the full upstream chain if any dependency is stale.

### Execution Flow

1. **Schedule** fires `weekly_report_job` at Monday 08:00 (Sao Paulo timezone)
2. **Dagster** materializes `weekly_experiment_report` and all upstream dependencies
3. **Report asset** generates HTML, PDF, and PNG chart files
4. **Asset sensor** detects the new materialization and triggers `weekly_report_email_job`
5. **Email op** sends the HTML report with inline chart images (or writes a dry-run preview)

> **Why sensor-based email:** Decoupling report generation from email delivery means the report can be materialized manually (for testing or ad-hoc review) without triggering an email. The sensor only fires on successful materialization, preventing partial or errored reports from being distributed.

---

## Report Structure

### HTML Email

The email is designed for readability in email clients (Outlook, Gmail) with inline CSS and embedded images.

| Section | Content |
|---------|---------|
| **Portfolio summary** | Total experiments, significant count, launch recommendations, projected revenue impact |
| **Per-experiment detail** | Variant comparison table (Users, Conversions, Conv Rate, Revenue, Lift), p-value, 95% CI, recommendation |
| **Inline charts** | One conversion rate comparison bar chart per experiment, embedded via `cid:` references |

Charts are attached as inline images with Content-ID headers (e.g., `Content-ID: exp_01_chart`). The HTML references them via `cid:exp_01_chart`, which displays correctly in most email clients without requiring external image hosting.

### PDF Report

Same content as the HTML email, formatted for archival and printing.

| Aspect | Detail |
|--------|--------|
| Primary renderer | `reportlab` (pure Python, no system dependencies) |
| Enhanced renderer | `weasyprint` (if installed; better CSS support) |
| Archive path | `output/analysis/reports/weekly_YYYY-MM-DD.pdf` |

### Report Customization by Audience

Different stakeholders need different levels of detail. The Jinja2 template supports conditional rendering for audience-specific variants.

| Audience | Content Focus | Typical Length |
|----------|---------------|----------------|
| **Executive** | KPI tiles, total revenue impact, key wins and risks | 1 page |
| **Product** | Full statistical detail, p-values, CIs, per-experiment recommendations | 5-10 pages |
| **Data Science** | Methodology notes, assumption checks, Bayesian comparison, power analysis | 20+ pages |

To add audience variants, extend the Jinja2 template with conditional blocks or create separate templates (e.g., `weekly_report_executive.html`).

---

## Output Paths

| Path | Content |
|------|---------|
| `output/analysis/reports/weekly_report.html` | Latest HTML report |
| `output/analysis/reports/weekly_YYYY-MM-DD.pdf` | Timestamped PDF archive |
| `output/analysis/reports/charts/exp_01_chart.png` | Individual experiment chart (1 per experiment) |
| `output/analysis/reports/weekly_report_email_preview.html` | Dry-run preview (when email not configured) |

---

## Email Delivery

### Supported Providers

| Provider | Configuration | Use Case |
|----------|--------------|----------|
| `dry_run` (default) | No configuration needed | Local development; writes preview HTML |
| `sendgrid` | API key via `EMAIL_API_KEY` | Production; free tier supports 100 emails/day |
| `ses` | AWS credentials + region | Production; integrates with existing AWS infrastructure |

### Configuration

Email settings are loaded from environment variables or `.env`:

```bash
# SendGrid
EMAIL_PROVIDER=sendgrid
EMAIL_API_KEY=SG.xxxxx
EMAIL_FROM=analytics@company.com
EMAIL_RECIPIENTS=pm@company.com,ds@company.com

# Amazon SES
EMAIL_PROVIDER=ses
EMAIL_FROM=analytics@company.com
EMAIL_RECIPIENTS=pm@company.com,ds@company.com
EMAIL_REGION=us-east-1
```

> **Security note:** Email credentials should never be committed to version control. The `.env` file is gitignored. In production, use Dagster's resource configuration or a secrets manager.

---

## Scheduling

| Schedule | Cron | Job | Description |
|----------|------|-----|-------------|
| `weekly_report_schedule` | `0 8 * * 1` | `weekly_report_job` | Mondays 08:00 Sao Paulo |

### Why Monday Morning

- Sunday's `weekly_analysis_schedule` (06:00) refreshes all experiment analyses
- Monday's report schedule (08:00) generates and delivers reports based on fresh data
- Stakeholders receive the report at the start of the work week, enabling action during the week

### Schedule Dependencies

```
Sunday  06:00  →  weekly_analysis_schedule (refreshes analysis)
Monday  08:00  →  weekly_report_schedule (generates report)
Monday  08:01+ →  report sensor (triggers email delivery)
```

---

## Template System

The HTML report is rendered from a Jinja2 template at `ecommerce_analytics/templates/weekly_report.html`. The template receives:

| Variable | Content |
|----------|---------|
| `summary` | Portfolio-level aggregates (counts, total impact) |
| `experiments` | List of per-experiment results (variant stats, p-values, CIs, recommendations) |
| `report_date` | Generation timestamp |
| `charts` | File paths to generated PNG charts |

The template uses inline CSS (not external stylesheets) for email client compatibility. Tables use alternating row colors and bold headers for readability.

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Jinja2 over report builders | Template-based HTML | Maximum control over layout; familiar to web developers; version-controllable |
| Inline CSS over stylesheets | Email client compatibility | Most email clients strip `<link>` and `<style>` tags |
| CID-embedded images over URLs | No external hosting needed | Images render offline; no CDN dependency |
| Sensor-triggered email | Decoupled from report generation | Manual materialization doesn't spam inboxes |
| Dry-run default | No email configuration required | Works out of the box for development and review |
| Reportlab for PDF | Pure Python; no system deps | WeasyPrint produces better output but requires system libraries |

---

## Production Considerations

- **Delivery reliability:** If the email provider fails, the report is still generated and stored locally. A retry sensor can re-attempt delivery on the next Dagster tick.
- **Recipient management:** For production use, integrate with a mailing list (e.g., Google Group, internal distribution list) rather than hardcoded email addresses. This enables self-service subscription.
- **Report SLA:** The full pipeline (analysis + report + email) should complete within 30 minutes. If analysis takes longer due to data volume growth, consider caching intermediate results or pre-computing summaries.
- **Archival:** PDF reports are timestamped and retained indefinitely. For compliance environments, consider uploading to cloud storage (S3) with lifecycle policies.
- **Monitoring:** A Dagster sensor can alert if the weekly report was not materialized by Monday 09:00 (1 hour after schedule), indicating a pipeline failure that needs attention.
- **A/B on the report itself:** Meta-experimentation — test different report formats, levels of detail, or delivery times to optimize stakeholder engagement with experiment results.

---

*Last updated: March 2026*
