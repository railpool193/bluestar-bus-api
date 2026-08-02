from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Callable, Optional

from app.services.live_store_provider import LiveSnapshot, LiveSnapshotProvider
from app.services.siri_client import SIRIClient
from app.services.siri_parser import parse_vehicle_monitoring
from app.utils.time_utils import now_london


class LiveRefreshService:
    def __init__(self, *, client: SIRIClient, provider: LiveSnapshotProvider, interval_seconds: int, max_age_seconds: int, operator_filter: str, now: Callable[[], datetime] = now_london, parser=parse_vehicle_monitoring):
        self.client, self.provider = client, provider
        self.interval_seconds, self.max_age_seconds, self.operator_filter = interval_seconds, max_age_seconds, operator_filter
        self.now, self.parser = now, parser
        self._fetch_lock, self._state_lock = threading.Lock(), threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def snapshot(self) -> LiveSnapshot:
        return self.provider.get().with_age(self.now())

    def refresh(self, force: bool = False) -> bool:
        current, attempted_at = self.snapshot(), self.now()
        if not force and current.last_attempt_at and (attempted_at - current.last_attempt_at).total_seconds() < self.interval_seconds:
            return False
        if not self._fetch_lock.acquire(blocking=False):
            return False
        started = time.perf_counter()
        try:
            payload = self.client.download()
            parsed = self.parser(payload, reference_time=attempted_at, operator_filter=self.operator_filter, max_age_seconds=self.max_age_seconds)
            duration = int(round((time.perf_counter() - started) * 1000))
            self.provider.replace(LiveSnapshot(tuple(dict(vehicle) for vehicle in parsed.vehicles), True, "", attempted_at, attempted_at, parsed.raw_count, duration, self.client.source, False, 0))
            return True
        except Exception as exc:
            duration = int(round((time.perf_counter() - started) * 1000))
            previous = self.provider.get()
            self.provider.replace(LiveSnapshot(previous.vehicles, False, str(exc), attempted_at, previous.last_success_at, previous.raw_count, duration, self.client.source, True, previous.with_age(attempted_at).age_seconds))
            return False
        finally:
            self._fetch_lock.release()

    def _run(self) -> None:
        while not self._stop.is_set():
            self.refresh(force=True)
            self._stop.wait(self.interval_seconds)

    def start(self) -> None:
        with self._state_lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._run, name="siri-live-refresh", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=2)
