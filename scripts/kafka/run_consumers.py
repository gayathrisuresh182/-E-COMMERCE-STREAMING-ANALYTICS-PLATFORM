#!/usr/bin/env python3
"""
Run Kafka consumers for real-time stream processing.

Usage:
  python scripts/run_consumers.py                 # all consumers in parallel threads
  python scripts/run_consumers.py --consumer orders  # single consumer
  python scripts/run_consumers.py --list           # list available consumers
  python scripts/run_consumers.py --test           # quick connectivity test (5s)
"""
from __future__ import annotations
import argparse, logging, signal, sys, threading, time

sys.path.insert(0, ".")

from ecommerce_analytics.consumers.order_processor import OrderStreamProcessor
from ecommerce_analytics.consumers.metrics_aggregator import MetricsAggregator
from ecommerce_analytics.consumers.experiment_tracker import ExperimentTracker
from ecommerce_analytics.consumers.delivery_monitor import DeliverySLAMonitor

CONSUMERS = {
    "orders": OrderStreamProcessor,
    "metrics": MetricsAggregator,
    "experiments": ExperimentTracker,
    "deliveries": DeliverySLAMonitor,
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOG = logging.getLogger("run_consumers")


def run_single(name: str, timeout: float | None = None) -> None:
    cls = CONSUMERS[name]
    consumer = cls()
    if timeout:
        def stopper():
            time.sleep(timeout)
            consumer._running = False
        t = threading.Thread(target=stopper, daemon=True)
        t.start()
    LOG.info("Starting consumer: %s", name)
    consumer.run()


def run_all(timeout: float | None = None) -> None:
    instances = []
    threads = []
    for name, cls in CONSUMERS.items():
        c = cls()
        instances.append(c)
        t = threading.Thread(target=c.run, name=name, daemon=True)
        threads.append(t)

    for t in threads:
        t.start()
        LOG.info("Started thread: %s", t.name)

    def shutdown(sig, frame):
        LOG.info("Shutting down all consumers...")
        for c in instances:
            c._running = False
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    if timeout:
        time.sleep(timeout)
        for c in instances:
            c._running = False
    else:
        for t in threads:
            t.join()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Kafka consumers")
    parser.add_argument("--consumer", choices=list(CONSUMERS.keys()),
                        help="Run a single consumer")
    parser.add_argument("--list", action="store_true", help="List consumers")
    parser.add_argument("--test", action="store_true",
                        help="Quick test (5 seconds then exit)")
    args = parser.parse_args()

    if args.list:
        for name, cls in CONSUMERS.items():
            print(f"  {name:15s} {cls.__name__}")
        return

    timeout = 5.0 if args.test else None

    if args.consumer:
        run_single(args.consumer, timeout=timeout)
    else:
        run_all(timeout=timeout)


if __name__ == "__main__":
    main()
