"""
Base Kafka consumer with shared patterns:
  - Manual offset commit (exactly-once semantics)
  - Dead-letter queue (DLQ)
  - Error handling with retries and exponential backoff
  - Monitoring metrics (lag, throughput, error rate, latency)
  - Graceful shutdown
"""
from __future__ import annotations

import json
import logging
import signal
import threading
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError

from ecommerce_analytics.consumers.config import (
    DLQ_PREFIX,
    KAFKA_BOOTSTRAP,
    MAX_POLL_RECORDS,
    MAX_RETRIES,
    METRICS_LOG_INTERVAL,
    POLL_TIMEOUT_MS,
)

LOG = logging.getLogger("consumers")


class ConsumerMetrics:
    """Lightweight in-process metrics tracker."""

    def __init__(self) -> None:
        self.messages_processed = 0
        self.messages_failed = 0
        self.batches_processed = 0
        self.dlq_sent = 0
        self._window_start = time.monotonic()
        self._window_processed = 0
        self._latencies: list[float] = []

    def record_success(self, latency_ms: float = 0.0) -> None:
        self.messages_processed += 1
        self._window_processed += 1
        if latency_ms > 0:
            self._latencies.append(latency_ms)

    def record_failure(self) -> None:
        self.messages_failed += 1

    def record_dlq(self) -> None:
        self.dlq_sent += 1

    def record_batch(self) -> None:
        self.batches_processed += 1

    def snapshot(self) -> dict[str, Any]:
        elapsed = max(time.monotonic() - self._window_start, 0.001)
        throughput = self._window_processed / elapsed
        avg_latency = (
            sum(self._latencies) / len(self._latencies) if self._latencies else 0.0
        )
        total = self.messages_processed + self.messages_failed
        error_rate = (self.messages_failed / total * 1000) if total else 0.0
        snapshot = {
            "total_processed": self.messages_processed,
            "total_failed": self.messages_failed,
            "dlq_sent": self.dlq_sent,
            "throughput_per_sec": round(throughput, 2),
            "avg_latency_ms": round(avg_latency, 2),
            "error_rate_per_1k": round(error_rate, 2),
        }
        self._window_start = time.monotonic()
        self._window_processed = 0
        self._latencies.clear()
        return snapshot


class BaseConsumer(ABC):
    """
    Abstract Kafka consumer with:
    - Manual offset commits
    - DLQ for unprocessable messages
    - Retries with exponential backoff
    - Graceful shutdown via SIGINT/SIGTERM
    - Periodic metrics logging
    """

    def __init__(
        self,
        topics: list[str],
        group_id: str,
        *,
        poll_timeout_ms: int = POLL_TIMEOUT_MS,
        max_poll_records: int = MAX_POLL_RECORDS,
    ) -> None:
        self.topics = topics
        self.group_id = group_id
        self.poll_timeout_ms = poll_timeout_ms
        self.max_poll_records = max_poll_records

        self._running = False
        self._consumer: KafkaConsumer | None = None
        self._dlq_producer: KafkaProducer | None = None
        self.metrics = ConsumerMetrics()

    @property
    def name(self) -> str:
        return self.__class__.__name__

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _create_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            *self.topics,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            max_poll_records=self.max_poll_records,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=-1,
        )

    def _create_dlq_producer(self) -> KafkaProducer:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            retries=2,
        )

    def start(self) -> None:
        """Create Kafka connections and register signal handlers."""
        LOG.info("[%s] Starting (topics=%s, group=%s)", self.name, self.topics, self.group_id)
        self._consumer = self._create_consumer()
        self._dlq_producer = self._create_dlq_producer()
        self._running = True
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self._handle_signal)
            signal.signal(signal.SIGTERM, self._handle_signal)
        self.on_start()

    def stop(self) -> None:
        """Graceful shutdown: flush, close connections."""
        LOG.info("[%s] Shutting down…", self.name)
        self._running = False
        if self._consumer:
            try:
                self._consumer.close(autocommit=False)
            except Exception:
                pass
        if self._dlq_producer:
            try:
                self._dlq_producer.flush(timeout=5)
                self._dlq_producer.close(timeout=5)
            except Exception:
                pass
        self.on_stop()
        LOG.info("[%s] Stopped. Final metrics: %s", self.name, self.metrics.snapshot())

    def _handle_signal(self, signum: int, frame: Any) -> None:
        LOG.info("[%s] Received signal %d, stopping…", self.name, signum)
        self._running = False

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Infinite poll-process-commit loop."""
        self.start()
        last_metrics_log = time.monotonic()
        try:
            while self._running:
                raw = self._consumer.poll(
                    timeout_ms=self.poll_timeout_ms,
                    max_records=self.max_poll_records,
                )
                if not raw:
                    continue

                messages: list[dict[str, Any]] = []
                for tp, records in raw.items():
                    for record in records:
                        messages.append(record.value)

                if messages:
                    self._process_with_error_handling(messages)

                # Periodic metrics logging
                if time.monotonic() - last_metrics_log >= METRICS_LOG_INTERVAL:
                    LOG.info("[%s] Metrics: %s", self.name, self.metrics.snapshot())
                    last_metrics_log = time.monotonic()
        finally:
            self.stop()

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def _process_with_error_handling(self, messages: list[dict]) -> None:
        """Process a batch, handle errors, commit offsets on success."""
        valid: list[dict] = []
        for msg in messages:
            try:
                self.validate(msg)
                valid.append(msg)
            except Exception as exc:
                LOG.warning("[%s] Invalid message → DLQ: %s", self.name, exc)
                self._send_to_dlq(msg, reason=str(exc))
                self.metrics.record_failure()

        if valid:
            t0 = time.monotonic()
            success = self._process_with_retry(valid)
            latency = (time.monotonic() - t0) * 1000
            if success:
                for _ in valid:
                    self.metrics.record_success(latency_ms=latency / len(valid))
                self.metrics.record_batch()
            else:
                for msg in valid:
                    self._send_to_dlq(msg, reason="processing_failed_after_retries")
                    self.metrics.record_failure()

        # Commit offset after successful processing (or after DLQ routing)
        try:
            self._consumer.commit()
        except KafkaError as exc:
            LOG.error("[%s] Offset commit failed: %s", self.name, exc)

    def _process_with_retry(self, messages: list[dict]) -> bool:
        """Retry processing up to MAX_RETRIES with exponential backoff."""
        for attempt in range(MAX_RETRIES):
            try:
                self.process_batch(messages)
                return True
            except Exception as exc:
                wait = 2**attempt
                LOG.warning(
                    "[%s] Batch failed (attempt %d/%d): %s – retrying in %ds",
                    self.name, attempt + 1, MAX_RETRIES, exc, wait,
                )
                if attempt < MAX_RETRIES - 1:
                    time.sleep(wait)
        return False

    # ------------------------------------------------------------------
    # DLQ
    # ------------------------------------------------------------------

    def _send_to_dlq(self, message: dict, *, reason: str) -> None:
        """Send unprocessable message to a dead-letter topic."""
        dlq_topic = DLQ_PREFIX + (self.topics[0] if self.topics else "unknown")
        envelope = {
            "original_message": message,
            "error_reason": reason,
            "consumer": self.name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            self._dlq_producer.send(dlq_topic, value=envelope)
            self.metrics.record_dlq()
        except Exception as exc:
            LOG.error("[%s] DLQ send failed: %s", self.name, exc)

    # ------------------------------------------------------------------
    # Hooks for subclasses
    # ------------------------------------------------------------------

    def on_start(self) -> None:
        """Called once after Kafka consumer is created (setup caches, connections)."""

    def on_stop(self) -> None:
        """Called during shutdown (close external connections)."""

    def validate(self, message: dict) -> None:
        """
        Validate a single message. Raise ValueError for malformed data.
        Default: require non-empty dict.
        """
        if not isinstance(message, dict) or not message:
            raise ValueError("Message must be a non-empty dict")

    @abstractmethod
    def process_batch(self, messages: list[dict]) -> None:
        """
        Process a validated batch of messages.
        Write to sinks, enrich, aggregate, etc.
        Raise on failure to trigger retry.
        """
