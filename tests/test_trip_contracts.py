import json
import unittest
from datetime import datetime
from unittest.mock import patch

from fastapi.responses import JSONResponse

from app.api.dependencies import TripRuntime
from app.api.trips import create_trips_router
from app.main import app, live_refresh
from app.services.gtfs_loader import GTFSStore
from app.services.gtfs_store_provider import GTFSStoreProvider
from app.services.live_store_provider import LiveSnapshot, LiveSnapshotProvider
from app.utils.time_utils import LONDON
import main as legacy


NOW = datetime(2026, 8, 3, 1, 0, tzinfo=LONDON)
TOP_LEVEL_KEYS = {"ok", "trip", "route", "serviceDate", "stops", "live", "delayLabel", "currentSequence", "shape", "now"}
STOP_KEYS = {"stopId", "name", "sequence", "lat", "lon", "scheduledTime", "scheduledTimeIso", "displayTime", "displayTimeIso", "minutes", "rightLabel", "live", "current", "past"}


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


def trip_store(*, with_shape=True):
    store = GTFSStore()
    store.loaded = True
    store.stops = {
        "S1": {"stop_id": "S1", "stop_name": "First", "lat": 50.80, "lon": -1.30},
        "S2": {"stop_id": "S2", "stop_name": "Central", "lat": 50.90, "lon": -1.40},
        "S3": {"stop_id": "S3", "stop_name": "Terminal", "lat": 51.00, "lon": -1.50},
    }
    store.routes = {"R1": {"route_id": "R1", "route_short_name": "U1", "route_long_name": "First to Terminal"}}
    store.trips = {"T1": {"trip_id": "T1", "route_id": "R1", "service_id": "DAILY", "line": "U1", "trip_headsign": "Terminal Stop C", "direction_id": "0", "shape_id": "SH1" if with_shape else ""}}
    store.stop_times_by_trip["T1"] = [
        {"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1, "departure_time": "24:50:00"},
        {"trip_id": "T1", "stop_id": "S2", "stop_sequence": 2, "departure_time": "25:00:00"},
        {"trip_id": "T1", "stop_id": "S3", "stop_sequence": 3, "departure_time": "25:20:00"},
    ]
    store.shapes = {"SH1": [{"lat": 50.8, "lon": -1.3, "seq": 1}, {"lat": 51.0, "lon": -1.5, "seq": 2}]} if with_shape else {}
    return store


def live_vehicle(**changes):
    value = {
        "line": "U1",
        "destination": "Terminal Stop C",
        "destinationFull": "Terminal Stop C",
        "liveTime": NOW.isoformat(),
        "currentStopRef": "S2",
        "currentStopName": "Central",
        "vehicleAtStop": True,
        "fleet": "1234",
        "vehicleRef": "BLUS-1234",
        "codes": ["1234"],
        "datedVehicleJourneyRef": "T1",
        "delayMinutes": 5,
    }
    value.update(changes)
    return value


class TripLegacyContractTests(unittest.TestCase):
    def setUp(self):
        self.store = trip_store()
        self.store_patch = patch.object(legacy.gtfs_provider, "get", return_value=self.store)
        self.now_patch = patch.object(legacy, "now_london", return_value=NOW)
        self.time_now_patch = patch("app.utils.time_utils.now_london", return_value=NOW)
        self.live_patch = patch.object(legacy.live_snapshot_provider, "get", return_value=LiveSnapshot())
        self.store_patch.start(); self.now_patch.start(); self.time_now_patch.start(); self.live_patch.start()

    def tearDown(self):
        self.live_patch.stop(); self.time_now_patch.stop(); self.now_patch.stop(); self.store_patch.stop()

    def replace_live(self, snapshot):
        self.live_patch.stop()
        self.live_patch = patch.object(legacy.live_snapshot_provider, "get", return_value=snapshot)
        self.live_patch.start()

    def test_timetable_contract_and_24_hour_time(self):
        response = legacy.api_trip("T1", service_date="2026-08-02")
        self.assertEqual(set(response), TOP_LEVEL_KEYS)
        self.assertEqual(set(response["stops"][0]), STOP_KEYS)
        self.assertEqual(response["serviceDate"], "2026-08-02")
        self.assertEqual(response["stops"][1]["scheduledTime"], "01:00")
        self.assertEqual(response["stops"][1]["displayTime"], "01:00")
        self.assertTrue(all(not stop["live"] and not stop["current"] for stop in response["stops"]))
        self.assertEqual((response["live"], response["delayLabel"], response["currentSequence"]), (None, "--", None))
        self.assertEqual(response["trip"]["destinationFull"], "Terminal [C]")
        self.assertEqual(response["route"]["route_id"], "R1")
        self.assertEqual(response["shape"], self.store.shapes["SH1"])

    def test_default_valid_and_invalid_service_date(self):
        self.assertEqual(legacy.api_trip("T1")["serviceDate"], "2026-08-03")
        self.assertEqual(legacy.api_trip("T1", service_date="2026-08-02")["serviceDate"], "2026-08-02")
        self.assertEqual(legacy.api_trip("T1", service_date="invalid")["serviceDate"], "2026-08-03")

    def test_unknown_trip_is_404(self):
        status, body = response_body(legacy.api_trip("UNKNOWN"))
        self.assertEqual((status, body), (404, {"ok": False, "error": "Trip not found"}))

    def test_live_current_future_past_and_positive_delay(self):
        self.replace_live(LiveSnapshot((live_vehicle(),), True))
        response = legacy.api_trip("T1", service_date="2026-08-02", vehicle="1234")
        first, current, future = response["stops"]
        self.assertEqual((response["delayLabel"], response["currentSequence"]), ("+5", 2))
        self.assertTrue(first["past"])
        self.assertEqual((current["current"], current["rightLabel"]), (True, "LIVE"))
        self.assertEqual((future["live"], future["displayTime"], future["rightLabel"]), (True, "01:25", "25'"))

    def test_delay_label_variants_and_current_due(self):
        for delay, expected in ((-2, "-2"), (0, "+0"), (5, "+5")):
            self.replace_live(LiveSnapshot((live_vehicle(delayMinutes=delay),), True))
            self.assertEqual(legacy.api_trip("T1", "2026-08-02")["delayLabel"], expected)
        self.replace_live(LiveSnapshot((live_vehicle(delayMinutes=None, vehicleAtStop=False),), True))
        response = legacy.api_trip("T1", "2026-08-02")
        self.assertEqual(response["delayLabel"], "LIVE")
        self.assertEqual(response["stops"][1]["rightLabel"], "Due")

    def test_shape_falls_back_to_stop_coordinates(self):
        self.store.trips["T1"]["shape_id"] = ""
        self.store.shapes = {}
        response = legacy.api_trip("T1", "2026-08-02")
        self.assertEqual(response["shape"], [{"lat": 50.8, "lon": -1.3}, {"lat": 50.9, "lon": -1.4}, {"lat": 51.0, "lon": -1.5}])


class TripRouterBoundaryTests(unittest.TestCase):
    def make_endpoint(self, store, live=LiveSnapshot()):
        gtfs_provider = CountingGTFSProvider(store)
        live_provider = CountingLiveProvider(live)
        now_calls = []
        _, endpoint = create_trips_router(
            runtime=TripRuntime(
                gtfs_provider,
                live_provider,
                lambda: now_calls.append(NOW) or NOW,
            ),
            unavailable_response=api_error,
        )
        return endpoint, gtfs_provider, live_provider, now_calls

    def test_route_and_openapi_are_registered_once_before_fallback(self):
        path = "/api/trips/{trip_id}"
        matches = [
            index for index, route in enumerate(app.routes)
            if getattr(route, "path", None) == path
        ]
        fallback_index = next(
            index for index, route in enumerate(app.routes)
            if getattr(route, "path", None) == "/{path:path}"
        )
        self.assertEqual(len(matches), 1)
        self.assertLess(matches[0], fallback_index)
        self.assertEqual(list(app.openapi()["paths"]).count(path), 1)

    def test_each_snapshot_and_time_are_read_once_without_io(self):
        store = trip_store()
        store.load = lambda: self.fail("trip must not load GTFS")
        store.load_from_path = lambda _path: self.fail("trip must not load GTFS")
        snapshot = LiveSnapshot((live_vehicle(),), True)
        endpoint, gtfs_provider, live_provider, now_calls = self.make_endpoint(store, snapshot)
        with patch.object(live_refresh.client, "download") as download, patch(
            "app.services.siri_parser.parse_vehicle_monitoring"
        ) as parse_xml:
            response = endpoint("T1", "2026-08-02", "1234")
        self.assertEqual((gtfs_provider.get_count, live_provider.get_count, len(now_calls)), (1, 1, 1))
        self.assertEqual(response["live"]["fleet"], "1234")
        download.assert_not_called()
        parse_xml.assert_not_called()

    def test_vehicle_hint_is_forwarded_to_matching(self):
        weak = live_vehicle(vehicleRef="weak", fleet="1111", codes=["1111"], datedVehicleJourneyRef="other")
        hinted = live_vehicle(vehicleRef="hinted", fleet="2222", codes=["2222"], datedVehicleJourneyRef="other", currentStopRef="S3")
        endpoint, _, _, _ = self.make_endpoint(trip_store(), LiveSnapshot((weak, hinted), True))
        self.assertEqual(endpoint("T1", "2026-08-02", "2222")["live"]["vehicleRef"], "hinted")

    def test_missing_gtfs_and_unknown_trip_keep_status_codes_without_load(self):
        missing = GTFSStore()
        missing.error = "missing"
        missing.load = lambda: self.fail("trip must not load GTFS")
        unavailable, gtfs_provider, live_provider, now_calls = self.make_endpoint(missing)
        self.assertEqual(unavailable("T1").status_code, 503)
        self.assertEqual((gtfs_provider.get_count, live_provider.get_count, len(now_calls)), (1, 1, 1))

        unknown, _, _, _ = self.make_endpoint(trip_store())
        self.assertEqual(unknown("UNKNOWN").status_code, 404)

    def test_invalid_date_uses_same_reference_time_and_snapshots_are_unchanged(self):
        store = trip_store()
        original_trip = dict(store.trips["T1"])
        vehicle = live_vehicle()
        snapshot = LiveSnapshot((vehicle,), True)
        endpoint, gtfs_provider, live_provider, now_calls = self.make_endpoint(store, snapshot)
        response = endpoint("T1", "not-a-date")
        self.assertEqual(response["serviceDate"], NOW.date().isoformat())
        self.assertEqual(response["now"], NOW.isoformat())
        self.assertEqual((gtfs_provider.get_count, live_provider.get_count, len(now_calls)), (1, 1, 1))
        response["trip"]["line"] = "changed"
        response["live"]["fleet"] = "changed"
        self.assertEqual(store.trips["T1"], original_trip)
        self.assertEqual(snapshot.vehicles[0]["fleet"], "1234")

    def test_mismatched_live_vehicle_does_not_create_false_live_stops(self):
        endpoint, _, _, _ = self.make_endpoint(
            trip_store(),
            LiveSnapshot((live_vehicle(line="OTHER", destinationFull="Elsewhere"),), True),
        )
        response = endpoint("T1", "2026-08-02")
        self.assertIsNone(response["live"])
        self.assertEqual(response["delayLabel"], "--")
        self.assertTrue(all(not stop["live"] and not stop["current"] for stop in response["stops"]))


if __name__ == "__main__":
    unittest.main()
