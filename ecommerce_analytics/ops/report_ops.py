"""
Phase 4I: Ops for sending the weekly experiment report via email.

Supports SendGrid, AWS SES, or dry-run (write to file for local testing).
"""
from __future__ import annotations

import os
from pathlib import Path

from dagster import op, get_dagster_logger

logger = get_dagster_logger()
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
REPORTS_DIR = PROJECT_ROOT / "output" / "analysis" / "reports"


@op(
    name="send_experiment_report_email",
    description="Send weekly experiment report via email (SendGrid/SES) or dry-run.",
)
def send_experiment_report_email(context) -> dict:
    """
    Read the generated report from disk and send it via email.
    Recipients and provider are configured via resources.
    For local testing without email config: writes to output and logs.
    """
    html_path = REPORTS_DIR / "weekly_report.html"
    charts_dir = REPORTS_DIR / "charts"

    if not html_path.exists():
        logger.warning("Report HTML not found at %s; skipping email", html_path)
        return {"sent": False, "reason": "report_not_found"}

    html = html_path.read_text(encoding="utf-8")

    # Collect chart attachments
    chart_paths = []
    if charts_dir.exists():
        chart_paths = sorted(charts_dir.glob("exp_*.png"))

    # Get email config from resources, fallback to env vars (subprocess may not get serialized config)
    email_config = {}
    try:
        email_config = getattr(context.resources, "email", None) or {}
    except Exception:
        pass

    recipients = email_config.get("recipients", [])
    provider = email_config.get("provider", "dry_run")

    # Fallback: read from env when resource config is empty
    if not recipients:
        recipients_str = os.getenv("EMAIL_RECIPIENTS", "")
        recipients = [r.strip() for r in recipients_str.split(",") if r.strip()]
    if provider == "dry_run" and os.getenv("EMAIL_PROVIDER"):
        provider = os.getenv("EMAIL_PROVIDER", "dry_run")
        email_config = {**email_config, "provider": provider, "recipients": recipients}
        if os.getenv("EMAIL_FROM"):
            email_config["from_email"] = os.getenv("EMAIL_FROM")
        if os.getenv("EMAIL_REGION"):
            email_config["region"] = os.getenv("EMAIL_REGION")
    subject = f"Weekly Experimentation Report - {context.run_id[:8] if context.run_id else 'Report'}"

    if provider == "dry_run" or not recipients:
        # Local testing: write to output, log
        dry_run_path = REPORTS_DIR / "weekly_report_email_preview.html"
        dry_run_path.write_text(html, encoding="utf-8")
        logger.info(
            "Email dry-run: report written to %s (would send to %s with %d charts)",
            dry_run_path,
            recipients or "stakeholders@example.com",
            len(chart_paths),
        )
        return {
            "sent": False,
            "dry_run": True,
            "html_path": str(dry_run_path),
            "charts_count": len(chart_paths),
        }

    # Send via provider
    try:
        if provider == "sendgrid":
            _send_via_sendgrid(html, chart_paths, recipients, subject, email_config)
        elif provider == "ses":
            _send_via_ses(html, chart_paths, recipients, subject, email_config)
        else:
            logger.warning("Unknown email provider %s; dry-run", provider)
            return {"sent": False, "reason": f"unknown_provider_{provider}"}

        logger.info("Email sent to %d recipients", len(recipients))
        return {"sent": True, "recipients": recipients, "charts_count": len(chart_paths)}
    except Exception as e:
        logger.exception("Failed to send email: %s", e)
        return {"sent": False, "error": str(e)}


def _send_via_sendgrid(
    html: str,
    chart_paths: list[Path],
    recipients: list[str],
    subject: str,
    config: dict,
) -> None:
    """Send via SendGrid API."""
    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import (
            Attachment,
            Content,
            Email,
            Mail,
            FileContent,
            FileName,
            FileType,
            Disposition,
        )
    except ImportError as e:
        raise ImportError("sendgrid package required for SendGrid: pip install sendgrid") from e

    message = Mail(
        from_email=Email(config.get("from_email", "reports@example.com")),
        to_emails=[Email(r) for r in recipients],
        subject=subject,
        html_content=Content("text/html", html),
    )

    for i, path in enumerate(chart_paths):
        content = path.read_bytes()
        encoded = __import__("base64").b64encode(content).decode()
        attachment = Attachment(
            FileContent(encoded),
            FileName(path.name),
            FileType("image/png"),
            Disposition("inline"),
        )
        attachment.content_id = f"exp_{i+1:02d}_chart"
        message.attachment = message.attachment or []
        message.attachment.append(attachment)

    client = SendGridAPIClient(config.get("api_key", ""))
    response = client.send(message)
    if response.status_code >= 400:
        raise RuntimeError(f"SendGrid error: {response.status_code} {response.body}")


def _send_via_ses(
    html: str,
    chart_paths: list[Path],
    recipients: list[str],
    subject: str,
    config: dict,
) -> None:
    """Send via AWS SES."""
    try:
        import boto3
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        from email.mime.image import MIMEImage
    except ImportError as e:
        raise ImportError("boto3 required for SES") from e

    client = boto3.client("ses", region_name=config.get("region", "us-east-1"))
    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"] = config.get("from_email", "reports@example.com")
    msg["To"] = ", ".join(recipients)

    part = MIMEText(html, "html")
    msg.attach(part)

    for i, path in enumerate(chart_paths):
        with open(path, "rb") as f:
            img = MIMEImage(f.read())
        img.add_header("Content-ID", f"<exp_{i+1:02d}_chart>")
        img.add_header("Content-Disposition", "inline", filename=path.name)
        msg.attach(img)

    client.send_raw_email(
        Source=msg["From"],
        Destinations=recipients,
        RawMessage={"Data": msg.as_string()},
    )
