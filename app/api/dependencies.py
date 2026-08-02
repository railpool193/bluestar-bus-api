from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider
from app.services.live_store_provider import LiveSnapshot, LiveSnapshotProvider


@dataclass(frozen=True)
class APIRuntime:
    gtfs_provider: GTFSStoreProvider
    live_provider: LiveSnapshotProvider
    gtfs_zip_path: Path
    gtfs_directory_path: Path
    now: Callable[[], datetime]

    def gtfs_snapshot(self) -> GTFSStore:
        return self.gtfs_provider.get()

    def live_snapshot(self) -> LiveSnapshot:
        return self.live_provider.get()
