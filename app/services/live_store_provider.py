from __future__ import annotations

import threading
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class LiveSnapshot:
    vehicles: tuple[dict[str, Any], ...] = ()
    ok: bool = False
    error: str = ""
    last_attempt_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    raw_count: int = 0
    fetch_duration_ms: int = 0
    source: str = ""
    stale: bool = True
    age_seconds: Optional[int] = None

    @property
    def active_count(self) -> int:
        return len(self.vehicles)

    def with_age(self, now: datetime) -> "LiveSnapshot":
        age = max(0, int((now - self.last_success_at).total_seconds())) if self.last_success_at else None
        return replace(self, age_seconds=age)


class LiveSnapshotProvider:
    def __init__(self, initial: Optional[LiveSnapshot] = None):
        self._snapshot = initial or LiveSnapshot()
        self._lock = threading.Lock()

    def get(self) -> LiveSnapshot:
        with self._lock:
            return self._snapshot

    def replace(self, snapshot: LiveSnapshot) -> LiveSnapshot:
        with self._lock:
            previous, self._snapshot = self._snapshot, snapshot
            return previous
