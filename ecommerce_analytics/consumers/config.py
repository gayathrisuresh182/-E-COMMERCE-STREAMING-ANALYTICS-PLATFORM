"""Shared configuration for Kafka consumers."""
from __future__ import annotations

import os

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092").split(",")
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce_analytics")

# Topics
TOPIC_ORDERS = "orders-stream"
TOPIC_CLICKSTREAM = "clickstream"
TOPIC_PAYMENTS = "payments-stream"
TOPIC_DELIVERIES = "deliveries-stream"
TOPIC_REVIEWS = "reviews-stream"

# DLQ topics (created lazily by consumers)
DLQ_PREFIX = "dlq_"

# Consumer groups
GROUP_ORDER_PROCESSORS = "order-processors"
GROUP_METRICS_AGGREGATOR = "metrics-aggregator"
GROUP_EXPERIMENT_TRACKER = "experiment-tracker"
GROUP_DELIVERY_MONITOR = "delivery-monitor"

# Processing
MAX_POLL_RECORDS = 100
POLL_TIMEOUT_MS = 10_000
BATCH_SIZE = 100
MAX_RETRIES = 3

# Windowing (metrics aggregator)
WINDOW_SIZE_SECONDS = 300  # 5 minutes
WINDOW_GRACE_SECONDS = 60  # late-data grace period

# Caching
CACHE_REFRESH_SECONDS = 3600  # 1 hour

# Monitoring
METRICS_LOG_INTERVAL = 60  # log metrics every 60 seconds
