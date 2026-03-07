"""
Phase 2D: Clickstream event producer.
Replays Phase 2C events.ndjson with compressed timing.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from .base import close_producer, create_producer, send_with_retry
from .replay_clock import ReplayClock

LOG = logging.getLogger("clickstream_producer")
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
EVENTS_PATH = PROJECT_ROOT / "output" / "events.ndjson"


def parse_ts(s: str) -> datetime | None:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def run_clickstream_producer(
    replay_hours: float = 48.0,
    limit: int = 0,
    dry_run: bool = False,
) -> int:
    """Produce clickstream events. Returns count sent."""
    if not EVENTS_PATH.exists():
        LOG.error("Missing %s. Run generate_clickstream_events.py first.", EVENTS_PATH)
        return 0

    events = []
    max_read = (limit * 100) if limit > 0 else None  # For --test limit=10, read ~1000 lines
    with open(EVENTS_PATH, encoding="utf-8") as f:
        for i, line in enumerate(f):
            if max_read is not None and i >= max_read:
                break
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
                ts = parse_ts(ev.get("timestamp"))
                if ts:
                    events.append((ts, ev))
            except json.JSONDecodeError:
                continue

    if not events:
        return 0

    events.sort(key=lambda x: x[0])
    if limit > 0:
        events = events[:limit]

    first_ts = events[0][0]
    last_ts = events[-1][0]
    clock = ReplayClock(first_ts, last_ts, replay_hours)

    producer = create_producer() if not dry_run else None
    sent = 0
    start_wall = time.time()

    try:
        for ts, ev in events:
            replay_at = clock.replay_time(ts)
            now = datetime.now(timezone.utc)
            if replay_at > now:
                sleep_secs = (replay_at - now).total_seconds()
                if sleep_secs > 0:
                    time.sleep(sleep_secs)

            session_id = ev.get("session_id", "")
            key = session_id.encode() if session_id else None

            if producer and send_with_retry(producer, "clickstream", ev, key=key):
                sent += 1
            elif dry_run:
                sent += 1

            if sent > 0 and sent % 5000 == 0:
                elapsed = time.time() - start_wall
                rate = sent / (elapsed / 3600) if elapsed > 0 else 0
                LOG.info("  Clickstream sent: %d (%.0f/hour)", sent, rate)
    finally:
        close_producer(producer)

    return sent
