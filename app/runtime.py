from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from app.config import Settings
from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_refresh import GTFSRefreshService
from app.services.gtfs_store_provider import GTFSStoreProvider, GTFSStoreProxy
from app.services.live_refresh import LiveRefreshService
from app.services.live_store_provider import LiveSnapshotProvider
from app.services.siri_client import SIRIClient, SIRIClientConfig
from app.utils.time_utils import now_london


class LiveStoreProxy:
    """Read-only compatibility facade over an application's live provider."""

    def __init__(self, provider: LiveSnapshotProvider):
        self.provider = provider

    def fetch(self, force: bool = False) -> list[dict[str, Any]]:
        return [dict(vehicle) for vehicle in self.provider.get().vehicles]

    @property
    def vehicles(self): return self.fetch()
    @property
    def ok(self): return self.provider.get().ok
    @property
    def error(self): return self.provider.get().error
    @property
    def last_fetch(self): return self.provider.get().last_attempt_at
    @property
    def raw_count(self): return self.provider.get().raw_count


@dataclass
class ApplicationRuntime:
    settings: Settings
    gtfs_provider: GTFSStoreProvider
    live_provider: LiveSnapshotProvider
    gtfs_refresh: GTFSRefreshService
    live_refresh: LiveRefreshService
    now: Callable[[], datetime]
    gtfs: GTFSStoreProxy
    live_store: LiveStoreProxy
    endpoints: dict[str, Callable[..., Any]] = field(default_factory=dict)

    @property
    def gtfs_zip_path(self): return self.settings.gtfs_zip_path
    @property
    def gtfs_directory_path(self): return self.settings.gtfs_directory_path

    def initialize_local_gtfs(self) -> GTFSStore:
        candidate = GTFSStore(
            zip_path=self.gtfs_zip_path,
            directory_path=self.gtfs_directory_path,
        ).load()
        if candidate.loaded:
            self.gtfs_provider.replace(candidate)
        return candidate


def create_runtime(
    settings: Settings | None = None,
    *,
    now: Callable[[], datetime] | None = None,
) -> ApplicationRuntime:
    selected = settings or Settings.from_env()
    gtfs_provider = GTFSStoreProvider(
        GTFSStore(
            zip_path=selected.gtfs_zip_path,
            directory_path=selected.gtfs_directory_path,
        )
    )
    live_provider = LiveSnapshotProvider()

    def build_candidate(path):
        return GTFSStore().load_from_path(path)

    def activate_candidate(candidate) -> None:
        gtfs_provider.replace(candidate)

    gtfs_refresh = GTFSRefreshService(
        source_url=selected.gtfs_download_url,
        target_path=selected.gtfs_zip_path,
        metadata_path=selected.gtfs_metadata_path,
        interval_seconds=selected.gtfs_refresh_interval_seconds,
        timeout_seconds=selected.gtfs_download_timeout_seconds,
        max_download_bytes=selected.gtfs_max_download_bytes,
        max_uncompressed_bytes=selected.gtfs_max_uncompressed_bytes,
        max_attempts=selected.gtfs_refresh_attempts,
        enabled=selected.gtfs_auto_refresh,
        build_candidate=build_candidate,
        activate_candidate=activate_candidate,
    )
    live_refresh = LiveRefreshService(
        client=SIRIClient(SIRIClientConfig.from_env()),
        provider=live_provider,
        interval_seconds=selected.live_cache_ttl_seconds,
        max_age_seconds=selected.live_max_age_seconds,
        operator_filter=selected.live_operator_filter,
    )
    return ApplicationRuntime(
        settings=selected,
        gtfs_provider=gtfs_provider,
        live_provider=live_provider,
        gtfs_refresh=gtfs_refresh,
        live_refresh=live_refresh,
        now=now or (lambda: now_london()),
        gtfs=GTFSStoreProxy(gtfs_provider),
        live_store=LiveStoreProxy(live_provider),
    )
