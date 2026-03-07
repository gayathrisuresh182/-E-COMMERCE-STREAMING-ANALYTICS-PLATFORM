"""
Resource factory: build Dagster resources from environment/config.
Use .env for local overrides (EMAIL_PROVIDER, EMAIL_RECIPIENTS, etc.).
"""
import os
from pathlib import Path
from typing import Any

from dagster import resource

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(_PROJECT_ROOT / ".env")
    load_dotenv(_PROJECT_ROOT / "docs" / ".env")  # fallback if .env lives in docs/
except ImportError:
    pass


@resource
def kafka_resource(context) -> dict[str, Any]:
    """Kafka bootstrap servers for producers/consumers."""
    return {
        "bootstrap_servers": context.resource_config.get(
            "bootstrap_servers", "localhost:9092"
        ).split(","),
        "topic_orders": "orders-stream",
        "topic_clickstream": "clickstream",
        "topic_payments": "payments-stream",
        "topic_deliveries": "deliveries-stream",
        "topic_reviews": "reviews-stream",
        "topic_experiments": "experiments-stream",
    }


@resource
def bigquery_resource(context) -> dict[str, Any]:
    """BigQuery connection config. Use GOOGLE_APPLICATION_CREDENTIALS for auth."""
    return {
        "project": context.resource_config.get("project", ""),
        "dataset_staging": context.resource_config.get("dataset_staging", "staging"),
        "dataset_marts": context.resource_config.get("dataset_marts", "marts"),
        "dataset_realtime": context.resource_config.get("dataset_realtime", "realtime"),
        "dataset_experiments": context.resource_config.get("dataset_experiments", "experiments"),
    }


@resource
def mongodb_resource(context) -> dict[str, Any]:
    """MongoDB connection string and database name."""
    return {
        "connection_string": context.resource_config.get(
            "connection_string", "mongodb://localhost:27017"
        ),
        "database": context.resource_config.get("database", "ecommerce_streaming"),
    }


@resource
def slack_resource(context) -> dict[str, Any]:
    """Slack webhook URL for alerts (optional)."""
    return {
        "webhook_url": context.resource_config.get("webhook_url", ""),
        "enabled": bool(context.resource_config.get("webhook_url")),
    }


def _get_email_config() -> dict[str, Any]:
    """Email config from env vars. Set EMAIL_PROVIDER, EMAIL_RECIPIENTS, EMAIL_FROM."""
    recipients = os.getenv("EMAIL_RECIPIENTS", "")
    recipients_list = [r.strip() for r in recipients.split(",") if r.strip()]
    return {
        "provider": os.getenv("EMAIL_PROVIDER", "dry_run"),
        "recipients": recipients_list,
        "from_email": os.getenv("EMAIL_FROM", "reports@example.com"),
        "api_key": os.getenv("EMAIL_API_KEY", ""),
        "region": os.getenv("EMAIL_REGION", "us-east-1"),
    }


@resource
def email_resource(context) -> dict[str, Any]:
    """Email config for report delivery (SendGrid, SES, or dry_run)."""
    cfg = context.resource_config
    return {
        "provider": cfg.get("provider", "dry_run"),
        "recipients": cfg.get("recipients", []),
        "from_email": cfg.get("from_email", "reports@example.com"),
        "api_key": cfg.get("api_key", ""),
        "region": cfg.get("region", "us-east-1"),
    }


def get_resources() -> dict[str, Any]:
    """
    Build resource dict for Definitions.
    In production, use Dagster's env vars or secrets:
    - Dagster Cloud / env: DAGSTER_KAFKA_BOOTSTRAP_SERVERS, etc.
    """
    return {
        "kafka": kafka_resource.configured({
            "bootstrap_servers": "localhost:9092",
        }),
        "bigquery": bigquery_resource.configured({
            "project": "your-gcp-project",
            "dataset_staging": "staging",
            "dataset_marts": "marts",
            "dataset_realtime": "realtime",
            "dataset_experiments": "experiments",
        }),
        "mongodb": mongodb_resource.configured({
            "connection_string": "mongodb://localhost:27017",
            "database": "ecommerce_streaming",
        }),
        "slack": slack_resource.configured({
            "webhook_url": "",  # Set in .env or dagster.yaml
        }),
        "email": email_resource.configured(_get_email_config()),
    }
