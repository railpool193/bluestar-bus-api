import json
import unittest
from datetime import datetime
from unittest.mock import patch

from fastapi.responses import JSONResponse

from app.api.dependencies import RouteRuntime
from app.api.routes import create_routes_router
from app.main import app, live_refresh
from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider
from app.services.live_store_provider import LiveSnapshot, LiveSnapshotProvider
from app.utils.time_utils import LONDON
import main as legacy


NOW = datetime(2026, 8, 3, 1, 0, tzinfo=LONDON)


def response_body(value):
    if isinstance(value, JSONResponse):
        return value.status_code, json.loads(value.body)
    return 200, value


def api_error(text, status):
    return JSONResponse({"ok": False, "error": text}, status_code=status)


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


def route_store():
    store = GTFSStore()
    store.loaded = True
    store.stops = {
        "S1": {"stop_id": "S1", "stop_name": "First", "code": "A", "lat": 50.8, "lon": -1.3},
        "S2": {"stop_id": "S2", "lat": None, "lon": None},
    }
    store.routes = {
        "R1": {"route_id": "R1", "route_short_name": "U1", "route_long_name": "First to Terminal"},
        "LEGACY-ID": {"route_id": "LEGACY-ID", "route_short_name": "X9", "route_long_name": "Fallback"},
    }
    store.route_by_short["U1"] = ["R1"]
    store.route_by_short["X9"] = ["LEGACY-ID"]
    store.calendar = {
        "CURRENT": {"monday": "1", "start_date": "20260101", "end_date": "20261231"},
        "PREVIOUS": {"sunday": "1", "start_date": "20260101", "end_date": "20261231"},
        "INACTIVE": {"tuesday": "1", "start_date": "20260101", "end_date": "20261231"},
    }
    trips = [
        ("T-CURRENT", "CURRENT", "0", "Winchester Bus Station"),
        ("T-DUP", "CURRENT", "0", "Winchester Bus Station"),
        ("T-PREVIOUS", "PREVIOUS", "1", "Southampton City Centre"),
        ("T-INACTIVE", "INACTIVE", "2", "Inactive Place"),
    ]
    for index in range(7):
        trips.append((f"T-EXTRA-{index}", "CURRENT", str(index + 3), f"Destination {index}"))
    for trip_id, service_id, direction_id, headsign in trips:
        store.trips[trip_id] = {"trip_id": trip_id, "route_id": "R1", "service_id": service_id, "direction_id": direction_id, "trip_headsign": headsign, "line": "U1"}
        store.stop_times_by_trip[trip_id] = [
            {"trip_id": trip_id, "stop_id": "S1", "stop_sequence": "1"},
            {"trip_id": trip_id, "stop_id": "S2", "stop_sequence": "2"},
        ]
    store.trips["T-FALLBACK"] = {"trip_id": "T-FALLBACK", "route_id": "LEGACY-ID", "service_id": "CURRENT", "direction_id": "0", "trip_headsign": "Fallback", "line": "X9"}
    store.stop_times_by_trip["T-FALLBACK"] = [{"trip_id": "T-FALLBACK", "stop_id": "S1", "stop_sequence": "1"}]
    return store


def vehicle():
    return {"line": "U1", "fleet": "1234", "vehicleRef": "V1", "destination": "Winchester", "latitude": 50.9, "longitude": -1.4}


class RouteLegacyContractTests(unittest.TestCase):
    def setUp(self):
        self.store = route_store()
        self.store_patch = patch.object(legacy.gtfs_provider, "get", return_value=self.store)
        self.now_patch = patch.object(legacy, "now_london", return_value=NOW)
        self.live_patch = patch.object(legacy.live_snapshot_provider, "get", return_value=LiveSnapshot((vehicle(),), True))
        self.store_patch.start(); self.now_patch.start(); self.live_patch.start()

    def tearDown(self):
        self.live_patch.stop(); self.now_patch.stop(); self.store_patch.stop()

    def test_contract_short_name_case_whitespace_and_live_vehicle(self):
        response = legacy.api_route(" u1 ")
        self.assertEqual(set(response), {"ok", "line", "routes", "vehicles", "directions"})
        self.assertEqual(response["line"], "u1")
        self.assertEqual(response["routes"], [self.store.routes["R1"]])
        self.assertEqual(response["vehicles"], [vehicle()])
        self.assertEqual(set(response["directions"][0]), {"directionId", "destination", "tripId", "stops"})
        self.assertEqual(set(response["directions"][0]["stops"][0]), {"id", "name", "code", "lat", "lon", "sequence"})

    def test_route_id_fallback_and_unknown(self):
        self.store.route_by_short["LEGACY-ID"] = []
        self.assertEqual(legacy.api_route("legacy-id")["routes"], [self.store.routes["LEGACY-ID"]])
        status, body = response_body(legacy.api_route("UNKNOWN"))
        self.assertEqual((status, body), (404, {"ok": False, "error": "Route not found"}))

    def test_current_previous_service_dedup_order_and_limit(self):
        response = legacy.api_route("U1")
        self.assertEqual(len(response["directions"]), 6)
        self.assertEqual([item["tripId"] for item in response["directions"][:2]], ["T-CURRENT", "T-PREVIOUS"])
        self.assertNotIn("T-DUP", [item["tripId"] for item in response["directions"]])
        self.assertNotIn("T-INACTIVE", [item["tripId"] for item in response["directions"]])
        self.assertEqual(response["directions"][0]["destination"], "Winchester")

    def test_direction_stop_fallbacks_and_last_stop_are_preserved(self):
        stops = legacy.api_route("U1")["directions"][0]["stops"]
        self.assertEqual([stop["id"] for stop in stops], ["S1", "S2"])
        self.assertEqual(stops[0], {"id": "S1", "name": "First", "code": "A", "lat": 50.8, "lon": -1.3, "sequence": "1"})
        self.assertEqual(stops[1], {"id": "S2", "name": "S2", "code": "BUS", "lat": None, "lon": None, "sequence": "2"})

    def test_siri_absence_keeps_routes_and_directions(self):
        self.live_patch.stop()
        self.live_patch = patch.object(legacy.live_snapshot_provider, "get", return_value=LiveSnapshot(error="offline", stale=True))
        self.live_patch.start()
        response = legacy.api_route("U1")
        self.assertEqual(response["vehicles"], [])
        self.assertTrue(response["routes"])
        self.assertTrue(response["directions"])


class RouteRouterBoundaryTests(unittest.TestCase):
    def make_endpoint(self, store, live=LiveSnapshot()):
        gtfs_provider = CountingGTFSProvider(store)
        live_provider = CountingLiveProvider(live)
        now_calls = []
        _, endpoint = create_routes_router(
            runtime=RouteRuntime(
                gtfs_provider,
                live_provider,
                lambda: now_calls.append(NOW) or NOW,
            ),
            unavailable_response=api_error,
        )
        return endpoint, gtfs_provider, live_provider, now_calls

    def test_route_openapi_and_all_api_routes_precede_fallback(self):
        path = "/api/routes/{line}"
        matches = [index for index, route in enumerate(app.routes) if getattr(route, "path", None) == path]
        fallback_index = next(index for index, route in enumerate(app.routes) if getattr(route, "path", None) == "/{path:path}")
        self.assertEqual(len(matches), 1)
        self.assertLess(matches[0], fallback_index)
        self.assertEqual(list(app.openapi()["paths"]).count(path), 1)
        self.assertTrue(all(index < fallback_index for index, route in enumerate(app.routes) if str(getattr(route, "path", "")).startswith("/api/")))

    def test_snapshots_and_time_are_read_once_without_io(self):
        store = route_store()
        store.load = lambda: self.fail("route must not load GTFS")
        store.load_from_path = lambda _path: self.fail("route must not load GTFS")
        snapshot = LiveSnapshot((vehicle(),), True)
        endpoint, gtfs_provider, live_provider, now_calls = self.make_endpoint(store, snapshot)
        with patch.object(live_refresh.client, "download") as download, patch("app.services.siri_parser.parse_vehicle_monitoring") as parse_xml:
            response = endpoint("U1")
        self.assertEqual((gtfs_provider.get_count, live_provider.get_count, len(now_calls)), (1, 1, 1))
        self.assertEqual(response["vehicles"][0]["fleet"], "1234")
        download.assert_not_called()
        parse_xml.assert_not_called()

    def test_missing_gtfs_and_unknown_route_statuses_without_load(self):
        missing = GTFSStore()
        missing.error = "missing"
        missing.load = lambda: self.fail("route must not load GTFS")
        endpoint, gtfs_provider, live_provider, now_calls = self.make_endpoint(missing)
        self.assertEqual(endpoint("U1").status_code, 503)
        self.assertEqual((gtfs_provider.get_count, live_provider.get_count, len(now_calls)), (1, 1, 1))
        unknown, _, _, _ = self.make_endpoint(route_store())
        self.assertEqual(unknown("UNKNOWN").status_code, 404)

    def test_empty_or_stale_live_and_empty_active_services_are_compatible(self):
        store = route_store()
        store.calendar = {}
        endpoint, _, _, _ = self.make_endpoint(store, LiveSnapshot(error="offline", stale=True))
        response = endpoint("U1")
        self.assertEqual(response["vehicles"], [])
        self.assertTrue(any(item["tripId"] == "T-INACTIVE" for item in response["directions"]))
        self.assertEqual(len(response["directions"]), 6)

    def test_response_mutation_does_not_modify_snapshots(self):
        store = route_store()
        original_route = dict(store.routes["R1"])
        source_vehicle = vehicle()
        snapshot = LiveSnapshot((source_vehicle,), True, stale=True)
        endpoint, _, _, _ = self.make_endpoint(store, snapshot)
        response = endpoint("U1")
        response["routes"][0]["route_short_name"] = "changed"
        response["vehicles"][0]["fleet"] = "changed"
        response["directions"][0]["stops"][0]["name"] = "changed"
        self.assertEqual(store.routes["R1"], original_route)
        self.assertEqual(snapshot.vehicles[0]["fleet"], "1234")
        self.assertEqual(store.stops["S1"]["stop_name"], "First")


if __name__ == "__main__":
    unittest.main()
