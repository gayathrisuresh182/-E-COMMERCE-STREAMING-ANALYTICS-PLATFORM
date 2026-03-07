"""
Phase 2D: Replay clock for time compression.

Maps historical timestamps (2016-2018) to a compressed replay window (e.g. 48 hours).
Preserves relative ordering and gaps between events.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional


class ReplayClock:
    """Maps historical timestamps to replay timestamps within a compressed window."""

    def __init__(
        self,
        first_historical: datetime,
        last_historical: datetime,
        replay_duration_hours: float = 48.0,
        start_at: Optional[datetime] = None,
    ):
        self.first = first_historical
        self.last = last_historical
        self.replay_span = timedelta(hours=replay_duration_hours)
        self.start = start_at or datetime.now(timezone.utc)
        hist_span = (last_historical - first_historical).total_seconds()
        self.factor = self.replay_span.total_seconds() / hist_span if hist_span > 0 else 1.0

    def replay_time(self, historical_ts: datetime) -> datetime:
        """Convert historical timestamp to replay (wall-clock) time."""
        delta = (historical_ts - self.first).total_seconds()
        return self.start + timedelta(seconds=delta * self.factor)
