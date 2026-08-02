import unittest
from datetime import datetime, timedelta

from app.api.dependencies import StatusRuntime
from app.api.health import create_health_router
from app.api.status import create_status_router
from app.main import app
from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider
from app.services.live_store_provider import LiveSnapshot, LiveSnapshotProvider
from app.utils.time_utils import LONDON
from tests.route_helpers import application_routes


NOW = datetime(2026, 8, 2, 10, 0, tzinfo=LONDON)


class CountingGTFSProvider(GTFSStoreProvider):
    def __init__(self, initial):
        super().__init__(initial)
        self.get_count = 0

    def get(self):
        self.get_count += 1
        return super().get()


class CountingLiveProvider(LiveSnapshotProvider):
    def __init__(self, initial):
        super().__init__(initial)
        self.get_count = 0

    def get(self):
        self.get_count += 1
        return super().get()


class CountingRefreshSnapshot:
    def __init__(self, value):
        self.value = value
        self.call_count = 0

    def __call__(self):
        self.call_count += 1
        return self.value


def status_endpoint(store, live, refresh):
    gtfs_provider = CountingGTFSProvider(store)
    live_provider = CountingLiveProvider(live)
    refresh_snapshot = CountingRefreshSnapshot(refresh)
    _, endpoint = create_status_router(
        runtime=StatusRuntime(
            gtfs_provider=gtfs_provider,
            live_provider=live_provider,
            gtfs_refresh_snapshot=refresh_snapshot,
            now=lambda: NOW,
            live_max_age_seconds=360,
            live_operator_filter="BLUS",
            live_refresh_interval_seconds=8,
        )
    )
    return endpoint, gtfs_provider, live_provider, refresh_snapshot


class HealthStatusRouterTests(unittest.TestCase):
    def test_routes_and_openapi_are_registered_once_before_fallback(self):
        routes = application_routes(app)
        fallback_index = next(
            index for index, route in enumerate(routes)
            if getattr(route, "path", None) == "/{path:path}"
        )
        for path in ("/health", "/api/status"):
            matches = [
                index for index, route in enumerate(routes)
                if getattr(route, "path", None) == path
            ]
            self.assertEqual(len(matches), 1)
            self.assertLess(matches[0], fallback_index)
            self.assertIn(path, app.openapi()["paths"])

    def test_health_is_pure_and_keeps_exact_contract(self):
        calls = []
        _, endpoint = create_health_router(
            app_name="Test app",
            now=lambda: calls.append("now") or NOW,
        )
        self.assertEqual(
            endpoint(),
            {"ok": True, "app": "Test app", "time": NOW.isoformat()},
        )
        self.assertEqual(calls, ["now"])

    def test_status_uses_each_snapshot_exactly_once_and_keeps_contract(self):
        store = GTFSStore()
        store.loaded = True
        store.source = "cached.zip"
        store.agency = {"A": {}}
        store.stops = {"S1": {}, "S2": {}}
        store.routes = {"R": {}}
        store.trips = {"T": {}}
        store.stop_times_by_trip = {"T": []}
        store.stop_departures_index = {"S1": []}
        store.shapes = {"SH": []}
        live = LiveSnapshot(
            vehicles=({"vehicleRef": "V1"},),
            ok=True,
            last_attempt_at=NOW - timedelta(seconds=3),
            last_success_at=NOW - timedelta(seconds=5),
            raw_count=2,
            fetch_duration_ms=15,
            stale=False,
        )
        refresh = {
            "source": "https://example.test/gtfs.zip?token=secret&format=gtfs",
            "enabled": True,
            "running": False,
            "lastCheckedAt": "checked",
            "lastUpdatedAt": "updated",
            "lastSuccessfulLoadAt": "loaded",
            "sha256": "abc",
            "etag": "etag",
            "lastModified": "modified",
            "usingCachedData": True,
            "refreshIntervalSeconds": 21600,
            "lastError": None,
            "metadataPersistenceError": None,
        }
        endpoint, gtfs_provider, live_provider, refresh_snapshot = status_endpoint(store, live, refresh)

        response = endpoint()

        self.assertEqual((gtfs_provider.get_count, live_provider.get_count, refresh_snapshot.call_count), (1, 1, 1))
        self.assertEqual(set(response), {"live", "gtfs", "fleetMetadata", "serverTime", "timezone"})
        self.assertEqual(
            set(response["live"]),
            {"ok", "activeCount", "rawCount", "maxAgeSeconds", "operatorFilter", "error", "lastFetchTime", "lastAttemptAt", "lastSuccessAt", "stale", "ageSeconds", "fetchDurationMs", "refreshIntervalSeconds"},
        )
        self.assertEqual(
            set(response["gtfs"]),
            {"ok", "loaded", "error", "source", "activeDataSource", "refreshEnabled", "refreshRunning", "lastCheckedAt", "lastUpdatedAt", "lastSuccessfulLoadAt", "sha256", "etag", "lastModified", "usingCachedData", "refreshIntervalSeconds", "lastError", "metadataPersistenceError", "counts"},
        )
        self.assertEqual(response["live"]["ageSeconds"], 5)
        self.assertEqual(response["gtfs"]["counts"]["stops"], 2)
        self.assertNotIn("secret", response["gtfs"]["source"])

    def test_status_does_not_load_missing_gtfs_or_start_refresh(self):
        store = GTFSStore()
        store.error = "missing"
        store.load = lambda: self.fail("status must not load GTFS")
        store.load_from_path = lambda _path: self.fail("status must not load GTFS")
        endpoint, gtfs_provider, live_provider, refresh_snapshot = status_endpoint(
            store,
            LiveSnapshot(error="offline", stale=True),
            {},
        )

        response = endpoint()

        self.assertFalse(response["gtfs"]["loaded"])
        self.assertEqual(response["gtfs"]["lastError"], "missing")
        self.assertEqual(response["live"]["error"], "offline")
        self.assertEqual((gtfs_provider.get_count, live_provider.get_count, refresh_snapshot.call_count), (1, 1, 1))


if __name__ == "__main__":
    unittest.main()
