"""Base producer with Kafka client, retry, and logging."""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:9092").split(",")
LOG = logging.getLogger("kafka_producers")

# Retry when multiple producers start at once and one hits NoBrokersAvailable
CREATE_PRODUCER_RETRIES = 5
CREATE_PRODUCER_BACKOFF_SEC = 2


def create_producer(acks: str = "all") -> KafkaProducer:
    """Create Kafka producer. No compression (avoids snappy on Windows). Retries on NoBrokersAvailable."""
    last_err = None
    for attempt in range(CREATE_PRODUCER_RETRIES):
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                acks=acks,
                retries=3,
                request_timeout_ms=10000,
                metadata_max_age_ms=5000,
                max_block_ms=15000,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                compression_type=None,
            )
        except NoBrokersAvailable as e:
            last_err = e
            if attempt < CREATE_PRODUCER_RETRIES - 1:
                LOG.warning("NoBrokersAvailable (attempt %d/%d), retrying in %ds...", attempt + 1, CREATE_PRODUCER_RETRIES, CREATE_PRODUCER_BACKOFF_SEC)
                time.sleep(CREATE_PRODUCER_BACKOFF_SEC)
            else:
                raise
    raise last_err


def close_producer(producer: KafkaProducer, timeout: float = 10.0) -> None:
    """Flush and close producer with timeout to avoid indefinite hang."""
    if producer is None:
        return
    try:
        producer.flush(timeout=timeout)
    except Exception as e:
        LOG.warning("Flush failed: %s", e)
    try:
        producer.close(timeout=timeout)
    except Exception as e:
        LOG.warning("Close failed: %s", e)


def send_with_retry(
    producer: KafkaProducer,
    topic: str,
    value: dict,
    key: Optional[bytes] = None,
    max_retries: int = 3,
) -> bool:
    """Send message with exponential backoff retry. Returns True on success."""
    for attempt in range(max_retries):
        try:
            future = producer.send(topic, value=value, key=key)
            future.get(timeout=30)
            return True
        except KafkaError as e:
            if attempt == max_retries - 1:
                LOG.error("Send failed after %d retries: %s", max_retries, e)
                return False
            time.sleep(2 ** attempt)
    return False
