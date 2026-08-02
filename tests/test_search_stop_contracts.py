import json
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from fastapi.responses import JSONResponse

from app.api.dependencies import SearchRuntime, StopRuntime
from app.api.search import create_search_router
from app.api.stops import create_stops_router
from app.main import app, live_refresh
from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider
from app.services.live_store_provider import LiveSnapshot
from app.services.live_store_provider import LiveSnapshotProvider
from app.services.fleet_registry import FleetRegistryProvider, FleetSnapshot
from app.utils.time_utils import LONDON
import main as legacy
from tests.route_helpers import application_routes


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


def contract_store() -> GTFSStore:
    store = GTFSStore()
    store.loaded = True
    store.stops = {
        "S1": {"stop_id": "S1", "stop_name": "Central Station [A]", "name": "Central Station [A]", "code": "A", "lat": 50.9, "lon": -1.4},
        "S2": {"stop_id": "S2", "stop_name": "University Interchange [B]", "name": "University Interchange [B]", "code": "UNI", "lat": 50.91, "lon": -1.41},
    }
    store.routes = {
        "R2": {"route_id": "R2", "route_short_name": "U1A", "route_long_name": "University to Airport"},
        "R1": {"route_id": "R1", "route_short_name": "U1", "route_long_name": "Central to University"},
    }
    store.calendar = {
        "DAILY": {**{day: "1" for day in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")}, "start_date": "20260101", "end_date": "20261231"}
    }
    store.stop_departures_index = {
        "S1": [
            {"trip_id": "T24", "service_id": "DAILY", "route_id": "R1", "line": "U1", "stop_id": "S1", "stop_name": "Central Station [A]", "stop_sequence": 1, "departure_time": "25:05:00", "headsign": "University Interchange", "headsign_full": "University Interchange", "pickup_type": "0", "is_last_stop": False},
            {"trip_id": "TFUTURE", "service_id": "DAILY", "route_id": "R1", "line": "U1", "stop_id": "S1", "stop_name": "Central Station [A]", "stop_sequence": 2, "departure_time": "01:20:00", "headsign": "University Interchange", "headsign_full": "University Interchange", "pickup_type": "0", "is_last_stop": False},
            {"trip_id": "TLAST", "service_id": "DAILY", "route_id": "R1", "line": "U1", "stop_id": "S1", "stop_sequence": 3, "departure_time": "01:10:00", "headsign": "Depot", "pickup_type": "0", "is_last_stop": True},
        ]
    }
    return store


class SearchStopLegacyContractTests(unittest.TestCase):
    def setUp(self):
        self.store = contract_store()
        self.store_patch = patch.object(legacy.gtfs_provider, "get", return_value=self.store)
        self.now_patch = patch("app.runtime.now_london", return_value=NOW)
        self.live_patch = patch.object(legacy.live_snapshot_provider, "get", return_value=LiveSnapshot())
        self.store_patch.start()
        self.now_patch.start()
        self.live_patch.start()

    def tearDown(self):
        self.live_patch.stop()
        self.now_patch.stop()
        self.store_patch.stop()

    def test_search_empty_and_result_shapes(self):
        self.assertEqual(legacy.api_search(), {"ok": True, "query": "", "stops": [], "routes": []})
        _, stop_result = response_body(legacy.api_search("central"))
        self.assertEqual(set(stop_result["stops"][0]), {"id", "stop_id", "name", "code", "lat", "lon"})
        _, route_result = response_body(legacy.api_search("airport"))
        self.assertEqual(set(route_result["routes"][0]), {"id", "routeId", "line", "name", "subtitle", "routeColor"})

    def test_search_is_partial_case_insensitive_and_supports_codes_and_lines(self):
        self.assertEqual(legacy.api_search("STAT")["stops"][0]["stop_id"], "S1")
        self.assertEqual(legacy.api_search("uni")["stops"][0]["code"], "UNI")
        self.assertEqual([item["line"] for item in legacy.api_search("u1")["routes"]], ["U1", "U1A"])

    def test_search_preserves_order_and_limits(self):
        self.store.stops = {
            f"S{index:02d}": {"stop_id": f"S{index:02d}", "stop_name": f"Match {index:02d}", "code": "M", "lat": None, "lon": None}
            for index in range(55)
        }
        self.store.routes = {
            f"R{index:02d}": {"route_id": f"R{index:02d}", "route_short_name": f"M{index:02d}", "route_long_name": "Match"}
            for index in range(45)
        }
        result = legacy.api_search("match")
        self.assertEqual(len(result["stops"]), 50)
        self.assertEqual([item["stop_id"] for item in result["stops"][:2]], ["S00", "S01"])
        self.assertEqual(len(result["routes"]), 40)
        self.assertEqual([item["line"] for item in result["routes"][:2]], ["M00", "M01"])

    def test_departure_contract_previous_service_day_and_last_stop(self):
        status, result = response_body(legacy.api_stop_departures("S1"))
        self.assertEqual(status, 200)
        self.assertEqual(set(result), {"ok", "stop", "departures", "now"})
        departure = next(item for item in result["departures"] if item["tripId"] == "T24")
        self.assertEqual(
            set(departure),
            {"tripId", "trip_id", "serviceDate", "line", "routeId", "routeColor", "stopId", "stopName", "stopSequence", "destination", "destinationFull", "scheduledTime", "scheduledTimeIso", "displayTime", "displayTimeIso", "minutes", "minutesText", "live", "isDue", "vehicleRef", "fleet", "delayMinutes", "currentStopRef", "currentStopName", "vehicleAtStop"},
        )
        self.assertEqual(departure["serviceDate"], "2026-08-02")
        self.assertEqual(departure["scheduledTime"], "01:05")
        self.assertTrue(departure["isDue"] is False)
        self.assertFalse(any(item["tripId"] == "TLAST" for item in result["departures"]))

    def test_departure_unknown_stop_and_window(self):
        status, result = response_body(legacy.api_stop_departures("UNKNOWN"))
        self.assertEqual((status, result), (404, {"ok": False, "error": "Stop not found"}))
        narrow = legacy.api_stop_departures("S1", minutes=10)
        self.assertFalse(any(item["tripId"] == "TFUTURE" for item in narrow["departures"]))
        wide = legacy.api_stop_departures("S1", minutes=30)
        self.assertTrue(any(item["tripId"] == "TFUTURE" for item in wide["departures"]))

    def test_departure_live_due_delay_and_scheduled_fallback(self):
        vehicle = {"line": "U1", "destination": "University", "destinationFull": "University Interchange", "liveTime": (NOW + timedelta(minutes=1)).isoformat(), "currentStopRef": "S1", "vehicleAtStop": True, "vehicleRef": "V1", "fleet": "1001", "delayMinutes": -4}
        self.live_patch.stop()
        self.live_patch = patch.object(legacy.live_snapshot_provider, "get", return_value=LiveSnapshot((vehicle,), True))
        self.live_patch.start()
        result = legacy.api_stop_departures("S1")
        live = next(item for item in result["departures"] if item["tripId"] == "T24")
        self.assertEqual((live["live"], live["isDue"], live["minutesText"]), (True, True, "Due"))
        self.assertEqual((live["vehicleRef"], live["fleet"], live["delayMinutes"]), ("V1", "1001", -4))
        self.assertEqual((live["currentStopRef"], live["currentStopName"], live["vehicleAtStop"]), ("S1", "", True))


class SearchStopRouterBoundaryTests(unittest.TestCase):
    def test_past_timetable_is_removed_but_delayed_live_is_kept(self):
        store = contract_store()
        store.stop_departures_index["S1"] = [
            {**store.stop_departures_index["S1"][0], "trip_id": "PAST", "departure_time": "24:57:00"},
            {**store.stop_departures_index["S1"][0], "trip_id": "DELAYED", "departure_time": "24:55:00"},
        ]
        vehicle = {"line": "U1", "destinationFull": "University Interchange", "liveTime": (NOW + timedelta(minutes=3)).isoformat(), "fleet": "1804", "datedVehicleJourneyRef": "DELAYED"}
        _, endpoint = create_stops_router(
            runtime=StopRuntime(CountingGTFSProvider(store), CountingLiveProvider(LiveSnapshot((vehicle,), True)), lambda: NOW, 120, 80, 38),
            unavailable_response=api_error,
        )
        departures = endpoint("S1")["departures"]
        self.assertEqual([item["tripId"] for item in departures], ["DELAYED"])
        self.assertTrue(departures[0]["live"])
    def test_live_departure_is_enriched_after_matching(self):
        store = contract_store()
        vehicle = {"operator": "BLUS", "line": "U1", "destinationFull": "University Interchange", "liveTime": (NOW + timedelta(minutes=5)).isoformat(), "currentStopRef": "S1", "fleet": "1804", "datedVehicleJourneyRef": "T24"}
        fleet = FleetRegistryProvider(FleetSnapshot(({"operatorId": "BLUS", "fleetCode": "1804", "registration": "HJ25BYG", "vehicleType": "ADL Enviro400 MMC", "withdrawn": False},)))
        _, endpoint = create_stops_router(
            runtime=StopRuntime(CountingGTFSProvider(store), CountingLiveProvider(LiveSnapshot((vehicle,), True)), lambda: NOW, 120, 80, 38, fleet, "BLUS"),
            unavailable_response=api_error,
        )
        departure = next(item for item in endpoint("S1")["departures"] if item["tripId"] == "T24")
        self.assertEqual((departure["registration"], departure["vehicleType"]), ("HJ25BYG", "ADL Enviro400 MMC"))
    def test_routes_and_openapi_are_registered_once_before_fallback(self):
        routes = application_routes(app)
        fallback_index = next(
            index for index, route in enumerate(routes)
            if getattr(route, "path", None) == "/{path:path}"
        )
        for path in ("/api/search", "/api/stops/{stop_id}/departures"):
            matches = [
                index for index, route in enumerate(routes)
                if getattr(route, "path", None) == path
            ]
            self.assertEqual(len(matches), 1)
            self.assertLess(matches[0], fallback_index)
            self.assertEqual(list(app.openapi()["paths"]).count(path), 1)

    def test_search_reads_one_gtfs_snapshot_and_never_loads_or_reads_live(self):
        store = contract_store()
        store.load = lambda: self.fail("search must not load GTFS")
        store.load_from_path = lambda _path: self.fail("search must not load GTFS")
        gtfs_provider = CountingGTFSProvider(store)
        _, endpoint = create_search_router(
            runtime=SearchRuntime(gtfs_provider),
            unavailable_response=api_error,
        )
        with patch.object(live_refresh.client, "download") as download:
            result = endpoint("central")
        self.assertEqual(result["stops"][0]["stop_id"], "S1")
        self.assertEqual(gtfs_provider.get_count, 1)
        download.assert_not_called()

    def test_search_missing_gtfs_is_503_without_load(self):
        store = GTFSStore()
        store.error = "missing"
        store.load = lambda: self.fail("search must not load GTFS")
        provider = CountingGTFSProvider(store)
        _, endpoint = create_search_router(
            runtime=SearchRuntime(provider),
            unavailable_response=api_error,
        )
        response = endpoint("x")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(provider.get_count, 1)

    def test_stop_reads_each_request_snapshot_once_without_fetch_or_mutation(self):
        store = contract_store()
        original_departures = [dict(item) for item in store.stop_departures_index["S1"]]
        gtfs_provider = CountingGTFSProvider(store)
        live_snapshot = LiveSnapshot(error="SIRI offline", stale=True)
        live_provider = CountingLiveProvider(live_snapshot)
        now_calls = []
        _, endpoint = create_stops_router(
            runtime=StopRuntime(
                gtfs_provider=gtfs_provider,
                live_provider=live_provider,
                now=lambda: now_calls.append(NOW) or NOW,
                departure_window_minutes=120,
                departure_limit=80,
                matching_minutes=38,
            ),
            unavailable_response=api_error,
        )
        store.load = lambda: self.fail("departures must not load GTFS")
        with patch.object(live_refresh.client, "download") as download:
            response = endpoint("S1")
        self.assertTrue(response["departures"])
        self.assertTrue(all(not item["live"] for item in response["departures"]))
        self.assertEqual((gtfs_provider.get_count, live_provider.get_count, len(now_calls)), (1, 1, 1))
        self.assertEqual(store.stop_departures_index["S1"], original_departures)
        self.assertIs(live_provider.get(), live_snapshot)
        download.assert_not_called()

    def test_stop_limit_unknown_and_missing_gtfs_responses(self):
        store = contract_store()
        store.stop_departures_index["S1"] = [
            {**store.stop_departures_index["S1"][1], "trip_id": f"T{index}", "stop_sequence": index}
            for index in range(5)
        ]
        _, limited = create_stops_router(
            runtime=StopRuntime(
                CountingGTFSProvider(store),
                CountingLiveProvider(LiveSnapshot()),
                lambda: NOW,
                120,
                2,
                38,
            ),
            unavailable_response=api_error,
        )
        self.assertEqual(len(limited("S1")["departures"]), 2)
        self.assertEqual(limited("UNKNOWN").status_code, 404)

        missing = GTFSStore()
        missing.error = "missing"
        missing.load = lambda: self.fail("departures must not load GTFS")
        _, unavailable = create_stops_router(
            runtime=StopRuntime(
                CountingGTFSProvider(missing),
                CountingLiveProvider(LiveSnapshot()),
                lambda: NOW,
                120,
                80,
                38,
            ),
            unavailable_response=api_error,
        )
        self.assertEqual(unavailable("S1").status_code, 503)


if __name__ == "__main__":
    unittest.main()
