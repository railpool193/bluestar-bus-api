import asyncio
import json
import tempfile
import unittest
import urllib.parse
import zipfile
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from app.main import app, gtfs_refresh, live_refresh
from app.services.live_store_provider import LiveSnapshot
import main as legacy


FIXED_NOW = datetime(2026, 8, 2, 10, 0, tzinfo=legacy.LONDON)


def write_contract_gtfs(path: Path) -> None:
    files = {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA,Agency,https://example.test,Europe/London\n",
        "stops.txt": (
            "stop_id,stop_name,stop_lat,stop_lon,platform_code\n"
            "S1,Central Stop [A],50.900,-1.400,A\n"
            "S2,Terminal Stop [B],50.910,-1.410,B\n"
        ),
        "routes.txt": "route_id,agency_id,route_short_name,route_long_name,route_type\nR1,A,1,Central to Terminal,3\n",
        "trips.txt": "route_id,service_id,trip_id,trip_headsign,direction_id,shape_id\nR1,SUN,T1,Terminal Stop,0,SH1\n",
        "stop_times.txt": (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type\n"
            "T1,10:15:00,10:15:00,S1,1,0\n"
            "T1,10:30:00,10:30:00,S2,2,0\n"
        ),
        "calendar.txt": (
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date\n"
            "SUN,0,0,0,0,0,0,1,20260101,20261231\n"
        ),
        "shapes.txt": (
            "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
            "SH1,50.910,-1.410,2\n"
            "SH1,50.900,-1.400,1\n"
        ),
    }
    with zipfile.ZipFile(path, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)


async def asgi_get(path: str, query: str = ""):
    messages = []
    received = False

    async def receive():
        nonlocal received
        if not received:
            received = True
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.disconnect"}

    async def send(message):
        messages.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("ascii"),
        "query_string": query.encode("ascii"),
        "headers": [(b"host", b"testserver")],
        "client": ("127.0.0.1", 1234),
        "server": ("testserver", 80),
        "root_path": "",
    }
    await app(scope, receive, send)
    start = next(message for message in messages if message["type"] == "http.response.start")
    body = b"".join(message.get("body", b"") for message in messages if message["type"] == "http.response.body")
    return start["status"], json.loads(body) if body else None


def get(path: str, params=None):
    query = urllib.parse.urlencode(params or {})
    return asyncio.run(asgi_get(path, query))


class UnavailableStore:
    loaded = False
    error = "fixture unavailable"
    source = ""
    agency = stops = routes = trips = stop_times_by_trip = stop_departures_index = shapes = {}

    def load(self):
        return self


class APIContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        fixture = Path(cls.temp.name) / "contract.zip"
        write_contract_gtfs(fixture)
        cls.store = legacy.GTFSStore().load_from_path(fixture)
        if not cls.store.loaded:
            raise RuntimeError(cls.store.error)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        self.previous_live = legacy.live_snapshot_provider.get()
        legacy.live_snapshot_provider.replace(LiveSnapshot())
        self.store_patch = patch.object(legacy.gtfs_provider, "get", return_value=self.store)
        self.now_patch = patch("app.runtime.now_london", return_value=FIXED_NOW)
        self.live_patch = patch.object(legacy.live_store, "fetch", return_value=[])
        self.refresh_patch = patch.object(
            gtfs_refresh,
            "snapshot",
            return_value={
                "source": "https://example.test/gtfs.zip",
                "enabled": True,
                "running": False,
                "usingCachedData": True,
                "refreshIntervalSeconds": 21600,
                "lastError": None,
            },
        )
        self.store_patch.start()
        self.now_patch.start()
        self.live_patch.start()
        self.refresh_patch.start()

    def tearDown(self):
        self.refresh_patch.stop()
        self.live_patch.stop()
        self.now_patch.stop()
        self.store_patch.stop()
        legacy.live_snapshot_provider.replace(self.previous_live)

    def test_health_contract(self):
        status, body = get("/health")
        self.assertEqual(status, 200)
        self.assertTrue({"ok", "app", "time"}.issubset(body))

    def test_status_contract(self):
        status, body = get("/api/status")
        self.assertEqual(status, 200)
        self.assertTrue({"live", "gtfs", "serverTime", "timezone"}.issubset(body))
        self.assertTrue({"ok", "loaded", "source", "counts", "refreshEnabled"}.issubset(body["gtfs"]))

    def test_search_contract(self):
        status, body = get("/api/search", {"q": "Central"})
        self.assertEqual(status, 200)
        self.assertTrue({"ok", "query", "stops", "routes"}.issubset(body))
        self.assertIsInstance(body["stops"], list)
        self.assertTrue({"id", "stop_id", "name", "code", "lat", "lon"}.issubset(body["stops"][0]))

    def test_departures_contract_and_invalid_stop(self):
        status, body = get("/api/stops/S1/departures")
        self.assertEqual(status, 200)
        self.assertTrue({"ok", "stop", "departures", "now"}.issubset(body))
        self.assertIsInstance(body["departures"], list)
        self.assertTrue({"tripId", "line", "destination", "scheduledTime", "live"}.issubset(body["departures"][0]))
        invalid_status, invalid = get("/api/stops/UNKNOWN/departures")
        self.assertEqual(invalid_status, 404)
        self.assertEqual(invalid["error"], "Stop not found")

    def test_trip_contract_and_invalid_trip(self):
        status, body = get("/api/trips/T1", {"service_date": "2026-08-02"})
        self.assertEqual(status, 200)
        self.assertTrue({"ok", "trip", "route", "stops", "shape", "live"}.issubset(body))
        self.assertTrue({"stopId", "name", "sequence", "scheduledTime", "live", "past"}.issubset(body["stops"][0]))
        self.assertEqual(get("/api/trips/UNKNOWN")[0], 404)

    def test_route_contract_and_invalid_route(self):
        status, body = get("/api/routes/1")
        self.assertEqual(status, 200)
        self.assertTrue({"ok", "line", "routes", "vehicles", "directions"}.issubset(body))
        self.assertTrue({"directionId", "destination", "tripId", "stops"}.issubset(body["directions"][0]))
        self.assertEqual(get("/api/routes/UNKNOWN")[0], 404)

    def test_vehicles_contract(self):
        status, body = get("/api/vehicles", {"line": "1"})
        self.assertEqual(status, 200)
        self.assertTrue({"ok", "line", "vehicles", "now"}.issubset(body))
        self.assertIsInstance(body["vehicles"], list)

    def test_map_contract(self):
        status, body = get("/api/map", {"line": "1"})
        self.assertEqual(status, 200)
        self.assertTrue({"ok", "line", "vehicles", "shapes", "center", "now"}.issubset(body))
        self.assertTrue({"shapeId", "points"}.issubset(body["shapes"][0]))

    def test_gtfs_endpoints_return_503_without_store(self):
        self.store_patch.stop()
        self.store_patch = patch.object(legacy.gtfs_provider, "get", return_value=UnavailableStore())
        self.store_patch.start()
        with patch.object(legacy, "GTFSStore", return_value=UnavailableStore()):
            paths = [
                ("/api/search", {"q": "x"}),
                ("/api/stops/S1/departures", None),
                ("/api/trips/T1", None),
                ("/api/routes/1", None),
                ("/api/map", {"line": "1"}),
            ]
            for path, params in paths:
                status, body = get(path, params)
                self.assertEqual(status, 503, path)
                self.assertFalse(body["ok"])
                self.assertIn("GTFS data unavailable", body["error"])

    def test_health_and_status_never_trigger_live_network(self):
        with patch.object(live_refresh.client, "download") as download:
            self.assertEqual(get("/health")[0], 200)
            self.assertEqual(get("/api/status")[0], 200)
        download.assert_not_called()

    def test_siri_failure_preserves_scheduled_departures_and_empty_vehicles(self):
        legacy.live_snapshot_provider.replace(LiveSnapshot(error="SIRI offline", stale=True))
        status_code, status = get("/api/status")
        self.assertEqual(status_code, 200)
        self.assertEqual(status["live"]["error"], "SIRI offline")
        departures_code, departures = get("/api/stops/S1/departures")
        self.assertEqual(departures_code, 200)
        self.assertTrue(departures["departures"])
        self.assertTrue(all(not item["live"] for item in departures["departures"]))
        vehicles_code, vehicles = get("/api/vehicles")
        self.assertEqual(vehicles_code, 200)
        self.assertEqual(vehicles["vehicles"], [])

    def test_live_fields_flow_through_route_trip_and_map_contracts(self):
        vehicle = {"line": "1", "destination": "Terminal", "destinationFull": "Terminal Stop", "fleet": "1234", "vehicleRef": "V1234", "codes": ["1234", "T1"], "datedVehicleJourneyRef": "T1", "currentStopRef": "S1", "currentStopName": "Central Stop", "vehicleAtStop": True, "liveTime": "2026-08-02T10:15:00+01:00", "delayMinutes": 0, "latitude": 50.9, "longitude": -1.4}
        legacy.live_snapshot_provider.replace(LiveSnapshot((vehicle,), True))
        self.assertEqual(get("/api/routes/1")[1]["vehicles"][0]["fleet"], "1234")
        self.assertEqual(get("/api/trips/T1", {"service_date": "2026-08-02"})[1]["live"]["fleet"], "1234")
        self.assertEqual(get("/api/map", {"line": "1"})[1]["vehicles"][0]["fleet"], "1234")


if __name__ == "__main__":
    unittest.main()
