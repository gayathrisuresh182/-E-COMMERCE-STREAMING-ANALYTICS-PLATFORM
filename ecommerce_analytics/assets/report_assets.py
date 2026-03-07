"""
Phase 4I: Automated weekly experiment report (HTML email + PDF + charts).

Generates an executive summary of all experiment results, with visualizations,
formatted for email delivery and PDF archival.
"""
from __future__ import annotations

import io
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from dagster import asset, get_dagster_logger
from jinja2 import Environment, FileSystemLoader

logger = get_dagster_logger()
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
REPORTS_DIR = PROJECT_ROOT / "output" / "analysis" / "reports"
TEMPLATES_DIR = PROJECT_ROOT / "ecommerce_analytics" / "templates"


def _format_ci_pct(ci_abs: float, cr_control: float) -> str:
    """Convert absolute CI to relative % for display."""
    if cr_control <= 0:
        return "0"
    return f"{ci_abs / cr_control * 100:.1f}"


def _render_html(
    summary: dict[str, Any],
    experiments: list[dict[str, Any]],
    week_start: str,
    week_end: str,
) -> str:
    """Render HTML report from Jinja2 template."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    template = env.get_template("weekly_report.html")
    return template.render(
        summary=summary,
        experiments=experiments,
        week_start=week_start,
        week_end=week_end,
        report_date=datetime.now().strftime("%Y-%m-%d %H:%M"),
    )


def _get_variant_stats(exp: dict[str, Any]) -> dict[str, Any]:
    """Get variant_stats from experiment, with fallback for pre-Phase-4I materializations."""
    vs = exp.get("variant_stats")
    if vs:
        return vs
    # Fallback for old materializations without variant_stats
    logger.warning(
        "experiment %s missing variant_stats; re-materialize analysis assets for full report",
        exp.get("experiment_id", "?"),
    )
    return {
        "n_control": 0,
        "n_treatment": 0,
        "conversions_control": 0,
        "conversions_treatment": 0,
        "conversion_rate_control": 0.0,
        "conversion_rate_treatment": 0.0,
        "revenue_total_control": 0.0,
        "revenue_total_treatment": 0.0,
    }


def _generate_chart(exp: dict[str, Any], chart_path: Path) -> None:
    """Generate a bar chart for one experiment (Control vs Treatment conversion rate)."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        logger.warning("matplotlib not available; skipping chart generation")
        return

    vs = _get_variant_stats(exp)
    cr_ctrl = vs["conversion_rate_control"] * 100
    cr_treat = vs["conversion_rate_treatment"] * 100

    fig, ax = plt.subplots(figsize=(6, 4))
    variants = ["Control", "Treatment"]
    rates = [cr_ctrl, cr_treat]
    colors = ["#4299e1", "#48bb78"]
    bars = ax.bar(variants, rates, color=colors, edgecolor="white", linewidth=1.2)

    ax.set_ylabel("Conversion Rate (%)")
    ax.set_title(f"{exp['experiment_name']}\nConversion Rate Comparison")
    ax.set_ylim(0, max(rates) * 1.2 if max(rates) > 0 else 5)

    for bar, rate in zip(bars, rates):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.1,
            f"{rate:.2f}%",
            ha="center",
            va="bottom",
            fontsize=10,
        )

    plt.tight_layout()
    chart_path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(chart_path, dpi=100, bbox_inches="tight")
    plt.close()


def _generate_pdf(html: str, pdf_path: Path) -> bool:
    """Generate PDF from HTML. Uses weasyprint if available, else reportlab fallback."""
    try:
        from weasyprint import HTML
        HTML(string=html).write_pdf(str(pdf_path))
        return True
    except ImportError:
        pass

    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import letter
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

        doc = SimpleDocTemplate(
            str(pdf_path),
            pagesize=letter,
            rightMargin=inch,
            leftMargin=inch,
            topMargin=inch,
            bottomMargin=inch,
        )
        styles = getSampleStyleSheet()
        story = []
        story.append(Paragraph("Weekly Experimentation Report", styles["Title"]))
        story.append(Paragraph(
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            styles["Normal"],
        ))
        story.append(Spacer(1, 0.3 * inch))
        story.append(Paragraph(
            "See HTML report for full content. PDF generated via reportlab.",
            styles["Normal"],
        ))
        doc.build(story)
        return True
    except ImportError:
        logger.warning("Neither weasyprint nor reportlab available; PDF not generated")
        return False


@asset(
    name="weekly_experiment_report",
    description="Weekly experiment report (HTML email + PDF + PNG charts).",
    compute_kind="python",
    group_name="analysis",
)
def weekly_experiment_report(
    all_experiments_summary: dict[str, Any],
    experiment_exp_001_analysis: dict[str, Any],
    experiment_exp_002_analysis: dict[str, Any],
    experiment_exp_003_analysis: dict[str, Any],
    experiment_exp_004_analysis: dict[str, Any],
    experiment_exp_005_analysis: dict[str, Any],
    experiment_exp_006_analysis: dict[str, Any],
    experiment_exp_007_analysis: dict[str, Any],
    experiment_exp_008_analysis: dict[str, Any],
    experiment_exp_009_analysis: dict[str, Any],
    experiment_exp_010_analysis: dict[str, Any],
) -> dict[str, Any]:
    """Generate HTML report, PDF, and PNG charts for weekly experiment summary."""
    results = [
        experiment_exp_001_analysis,
        experiment_exp_002_analysis,
        experiment_exp_003_analysis,
        experiment_exp_004_analysis,
        experiment_exp_005_analysis,
        experiment_exp_006_analysis,
        experiment_exp_007_analysis,
        experiment_exp_008_analysis,
        experiment_exp_009_analysis,
        experiment_exp_010_analysis,
    ]

    # Build experiment list with CI in %
    experiments_for_template = []
    charts_dir = REPORTS_DIR / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    chart_paths = []

    for i, exp in enumerate(results):
        freq = exp.get("frequentist", {})
        vs = _get_variant_stats(exp)
        cr_ctrl = vs.get("conversion_rate_control", 0) or 0.001
        ci_lower = freq.get("ci_lower", 0)
        ci_upper = freq.get("ci_upper", 0)

        exp_data = {
            **exp,
            "variant_stats": vs,
            "ci_lower_pct": _format_ci_pct(ci_lower, cr_ctrl),
            "ci_upper_pct": _format_ci_pct(ci_upper, cr_ctrl),
            "chart_cid": f"exp_{i+1:02d}_chart",
        }
        experiments_for_template.append(exp_data)

        # Generate chart
        chart_path = charts_dir / f"exp_{i+1:02d}_chart.png"
        _generate_chart(exp, chart_path)
        if chart_path.exists():
            chart_paths.append(str(chart_path))
            exp_data["chart_path"] = str(chart_path)

    # Week range (e.g. Mon–Sun)
    today = datetime.now()
    week_end = (today - timedelta(days=today.weekday()) + timedelta(days=6)).strftime("%b %d, %Y")
    week_start = (today - timedelta(days=today.weekday())).strftime("%b %d, %Y")

    html = _render_html(
        summary=all_experiments_summary,
        experiments=experiments_for_template,
        week_start=week_start,
        week_end=week_end,
    )

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    html_path = REPORTS_DIR / "weekly_report.html"
    html_path.write_text(html, encoding="utf-8")

    pdf_filename = f"weekly_{datetime.now().strftime('%Y-%m-%d')}.pdf"
    pdf_path = REPORTS_DIR / pdf_filename
    pdf_ok = _generate_pdf(html, pdf_path)
    pdf_path_str = str(pdf_path) if pdf_ok and pdf_path.exists() else None

    logger.info(
        "weekly_experiment_report: HTML=%s, PDF=%s, charts=%d",
        html_path,
        pdf_path_str,
        len(chart_paths),
    )

    return {
        "html": html,
        "html_path": str(html_path),
        "pdf_path": pdf_path_str,
        "charts": chart_paths,
        "summary": all_experiments_summary,
        "experiments": experiments_for_template,
    }
