import unittest
from unittest.mock import patch

from app.main import app, gtfs_refresh, gtfs_refresh_status
import main as legacy


class UnavailableStore:
    loaded = False
    error = "missing test GTFS"
    source = ""
    agency = {}
    stops = {}
    routes = {}
    trips = {}
    stop_times_by_trip = {}
    stop_departures_index = {}
    shapes = {}

    def load(self):
        return self


class HealthTests(unittest.TestCase):
    def test_expected_routes_are_registered(self):
        paths = {route.path for route in app.routes}
        for path in (
            "/health", "/api/status", "/api/gtfs/refresh/status", "/api/search",
            "/api/stops/{stop_id}/departures", "/api/routes/{line}",
            "/api/trips/{trip_id}", "/api/vehicles", "/api/map", "/",
        ):
            self.assertIn(path, paths)

    def test_refresh_status_masks_query_values(self):
        status = gtfs_refresh_status()
        self.assertIn("enabled", status)
        self.assertNotIn("token=secret", status["source"])

    def test_health_never_triggers_refresh(self):
        with patch.object(gtfs_refresh, "refresh") as refresh:
            response = legacy.health()
        self.assertTrue(response["ok"])
        refresh.assert_not_called()

    def test_api_status_contains_refresh_diagnostics(self):
        fake_refresh = {
            "source": "https://example.test/feed.zip",
            "enabled": True,
            "running": False,
            "lastCheckedAt": "2026-01-01T00:00:00+00:00",
            "lastUpdatedAt": "2026-01-01T00:00:00+00:00",
            "lastSuccessfulLoadAt": "2026-01-01T00:00:00+00:00",
            "sha256": "abc",
            "etag": '"v1"',
            "lastModified": None,
            "usingCachedData": False,
            "refreshIntervalSeconds": 21600,
            "lastError": None,
        }
        original_store = legacy.gtfs
        try:
            legacy.gtfs = UnavailableStore()
            with patch.object(gtfs_refresh, "snapshot", return_value=fake_refresh), patch.object(
                legacy.live_store, "fetch", return_value=[]
            ):
                status = legacy.status()
        finally:
            legacy.gtfs = original_store
        self.assertFalse(status["gtfs"]["loaded"])
        self.assertTrue(status["gtfs"]["refreshEnabled"])
        self.assertEqual(status["gtfs"]["refreshIntervalSeconds"], 21600)
        self.assertEqual(status["gtfs"]["etag"], '"v1"')

    def test_gtfs_data_endpoints_return_503_when_unavailable(self):
        original_store = legacy.gtfs
        try:
            legacy.gtfs = UnavailableStore()
            responses = [
                legacy.api_search("x"),
                legacy.api_stop_departures("S"),
                legacy.api_trip("T"),
                legacy.api_route("1"),
                legacy.api_map("1"),
            ]
        finally:
            legacy.gtfs = original_store
        self.assertTrue(all(response.status_code == 503 for response in responses))


if __name__ == "__main__":
    unittest.main()
