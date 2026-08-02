from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider
from app.services.live_store_provider import LiveSnapshot, LiveSnapshotProvider
from app.services.fleet_registry import FleetRegistryProvider


@dataclass(frozen=True)
class APIRuntime:
    gtfs_provider: GTFSStoreProvider
    live_provider: LiveSnapshotProvider
    gtfs_zip_path: Path
    gtfs_directory_path: Path
    now: Callable[[], datetime]
    fleet_provider: FleetRegistryProvider | None = None
    fleet_operator_id: str = "BLUS"

    def gtfs_snapshot(self) -> GTFSStore:
        return self.gtfs_provider.get()

    def live_snapshot(self) -> LiveSnapshot:
        return self.live_provider.get()


@dataclass(frozen=True)
class StatusRuntime:
    gtfs_provider: GTFSStoreProvider
    live_provider: LiveSnapshotProvider
    gtfs_refresh_snapshot: Callable[[], Mapping[str, Any]]
    now: Callable[[], datetime]
    live_max_age_seconds: int
    live_operator_filter: str
    live_refresh_interval_seconds: int
    fleet_refresh_snapshot: Callable[[], Mapping[str, Any]] = lambda: {}

    def gtfs_snapshot(self) -> GTFSStore:
        return self.gtfs_provider.get()

    def live_snapshot(self) -> LiveSnapshot:
        return self.live_provider.get()


@dataclass(frozen=True)
class SearchRuntime:
    gtfs_provider: GTFSStoreProvider

    def gtfs_snapshot(self) -> GTFSStore:
        return self.gtfs_provider.get()


@dataclass(frozen=True)
class StopRuntime:
    gtfs_provider: GTFSStoreProvider
    live_provider: LiveSnapshotProvider
    now: Callable[[], datetime]
    departure_window_minutes: int
    departure_limit: int
    matching_minutes: int
    fleet_provider: FleetRegistryProvider | None = None
    fleet_operator_id: str = "BLUS"

    def gtfs_snapshot(self) -> GTFSStore:
        return self.gtfs_provider.get()

    def live_snapshot(self) -> LiveSnapshot:
        return self.live_provider.get()


@dataclass(frozen=True)
class TripRuntime:
    gtfs_provider: GTFSStoreProvider
    live_provider: LiveSnapshotProvider
    now: Callable[[], datetime]
    fleet_provider: FleetRegistryProvider | None = None
    fleet_operator_id: str = "BLUS"

    def gtfs_snapshot(self) -> GTFSStore:
        return self.gtfs_provider.get()

    def live_snapshot(self) -> LiveSnapshot:
        return self.live_provider.get()


@dataclass(frozen=True)
class RouteRuntime:
    gtfs_provider: GTFSStoreProvider
    live_provider: LiveSnapshotProvider
    now: Callable[[], datetime]

    def gtfs_snapshot(self) -> GTFSStore:
        return self.gtfs_provider.get()

    def live_snapshot(self) -> LiveSnapshot:
        return self.live_provider.get()
