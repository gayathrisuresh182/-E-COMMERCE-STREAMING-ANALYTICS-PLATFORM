"""Dagster resources: connections to Kafka, BigQuery, MongoDB, Slack."""
from ecommerce_analytics.resources.config import get_resources

__all__ = ["get_resources"]
