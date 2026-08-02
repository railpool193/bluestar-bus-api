import unittest
from datetime import datetime
from pathlib import Path

from fastapi.responses import JSONResponse

from app.api.dependencies import APIRuntime
from app.api.map import create_map_router
from app.api.vehicles import create_vehicles_router
from app.main import app
from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider
from app.services.live_store_provider import LiveSnapshot, LiveSnapshotProvider
from app.utils.time_utils import LONDON
from tests.route_helpers import application_routes


NOW = datetime(2026, 8, 2, 10, 0, tzinfo=LONDON)


class CountingGTFSProvider(GTFSStoreProvider):
    def __init__(self, initial): super().__init__(initial); self.get_count = 0
    def get(self): self.get_count += 1; return super().get()


class CountingLiveProvider(LiveSnapshotProvider):
    def __init__(self, initial=None): super().__init__(initial); self.get_count = 0
    def get(self): self.get_count += 1; return super().get()


def loaded_store(*, shape=True):
    store = GTFSStore(); store.loaded = True
    store.route_by_short["1"] = ["R1"]
    store.trips = {"T1": {"trip_id": "T1", "route_id": "R1", "service_id": "S", "shape_id": "SH" if shape else ""}}
    store.shapes = {"SH": [{"lat": 50.9, "lon": -1.4, "seq": 1}]} if shape else {}
    store.stops = {"A": {"stop_id": "A", "lat": 50.8, "lon": -1.3}}
    store.stop_times_by_trip["T1"] = [{"stop_id": "A", "stop_sequence": 1}]
    return store


def runtime(store, live):
    return APIRuntime(store, live, Path("missing.zip"), Path("missing-dir"), lambda: NOW)


class APIRouterMigrationTests(unittest.TestCase):
    def test_routes_and_openapi_are_registered_exactly_once_before_fallback(self):
        for path in ("/api/vehicles", "/api/map"):
            self.assertEqual(sum(getattr(route, "path", None) == path for route in application_routes(app)), 1)
            self.assertIn(path, app.openapi()["paths"])
        routes = application_routes(app)
        fallback_index = next(index for index, route in enumerate(routes) if getattr(route, "path", None) == "/{path:path}")
        for path in ("/api/vehicles", "/api/map"):
            api_index = next(index for index, route in enumerate(routes) if getattr(route, "path", None) == path)
            self.assertLess(api_index, fallback_index)

    def test_vehicles_empty_success_filter_stale_and_snapshot_immutability(self):
        vehicles = ({"line": "1", "fleet": "1", "destination": "A"}, {"line": "2", "fleet": "2", "destination": "B"})
        live = CountingLiveProvider(LiveSnapshot(vehicles, False, "offline", stale=True))
        _, endpoint = create_vehicles_router(runtime=runtime(CountingGTFSProvider(loaded_store()), live))
        before = live.get().vehicles
        live.get_count = 0
        response = endpoint("1")
        self.assertEqual(set(response), {"ok", "line", "vehicles", "now"})
        self.assertEqual([vehicle["line"] for vehicle in response["vehicles"]], ["1"])
        self.assertEqual(live.get_count, 1)
        response["vehicles"][0]["fleet"] = "changed"
        self.assertEqual(live.get().vehicles, before)
        live.replace(LiveSnapshot(error="SIRI failed", stale=True))
        self.assertEqual(endpoint()["vehicles"], [])

    def test_map_uses_one_snapshot_each_and_keeps_shape_contract(self):
        gtfs = CountingGTFSProvider(loaded_store())
        live = CountingLiveProvider(LiveSnapshot(({"line": "1", "latitude": 50.9, "longitude": -1.4, "bearing": 90, "fleet": "1", "destination": "A", "live": True},), True))
        _, endpoint = create_map_router(runtime=runtime(gtfs, live), unavailable_response=lambda text, status: JSONResponse({"ok": False, "error": text}, status_code=status))
        response = endpoint("1")
        self.assertEqual(set(response), {"ok", "line", "vehicles", "shapes", "center", "now"})
        self.assertEqual(response["shapes"][0]["points"][0]["lat"], 50.9)
        self.assertEqual(response["vehicles"][0]["bearing"], 90)
        self.assertEqual((gtfs.get_count, live.get_count), (1, 1))

    def test_map_missing_shape_fallback_stale_live_and_gtfs_503(self):
        stale = CountingLiveProvider(LiveSnapshot(({"line": "1", "fleet": "old"},), False, "offline", stale=True))
        _, endpoint = create_map_router(runtime=runtime(CountingGTFSProvider(loaded_store(shape=False)), stale), unavailable_response=lambda text, status: JSONResponse({"ok": False, "error": text}, status_code=status))
        response = endpoint("1")
        self.assertEqual(response["shapes"][0]["shapeId"], "T1")
        self.assertEqual(response["shapes"][0]["points"], [{"lat": 50.8, "lon": -1.3}])
        self.assertEqual(response["vehicles"][0]["fleet"], "old")
        unavailable = GTFSStore(); unavailable.error = "missing"
        _, missing_endpoint = create_map_router(runtime=runtime(CountingGTFSProvider(unavailable), CountingLiveProvider()), unavailable_response=lambda text, status: JSONResponse({"ok": False, "error": text}, status_code=status))
        self.assertEqual(missing_endpoint("1").status_code, 503)


if __name__ == "__main__": unittest.main()
