"""Dagster sensors: Kafka lag monitoring, stream freshness, report email."""
from ecommerce_analytics.sensors.report_sensors import weekly_report_email_sensor
from ecommerce_analytics.sensors.stream_sensors import (
    SENSORS,
    kafka_consumer_health_sensor,
    realtime_freshness_sensor,
)

SENSORS = [
    *SENSORS,
    weekly_report_email_sensor,
]

__all__ = [
    "SENSORS",
    "kafka_consumer_health_sensor",
    "realtime_freshness_sensor",
    "weekly_report_email_sensor",
]
