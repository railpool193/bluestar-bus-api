import io
import json
import os
import tempfile
import threading
import unittest
import urllib.error
import zipfile
from pathlib import Path
from unittest.mock import patch

from app.config import DEFAULT_GTFS_DOWNLOAD_URL, Settings
from app.services.gtfs_refresh import GTFSRefreshService
import main as legacy


class FakeResponse(io.BytesIO):
    def __init__(self, payload=b"", *, status=200, headers=None):
        super().__init__(payload)
        self.status = status
        self.headers = headers or {}


class FakeStore:
    def __init__(self, loaded=True, error=""):
        self.loaded = loaded
        self.error = error


def gtfs_bytes(*, calendar=True, calendar_dates=False, missing=None, unsafe=False):
    files = {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\nA,Agency,https://example.com,Europe/London\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\nS,Stop,50,-1\n",
        "routes.txt": "route_id,route_short_name,route_type\nR,1,3\n",
        "trips.txt": "route_id,service_id,trip_id\nR,WK,T\n",
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT,10:00:00,10:00:00,S,1\n",
    }
    if calendar:
        files["calendar.txt"] = "service_id,monday,start_date,end_date\nWK,1,20260101,20261231\n"
    if calendar_dates:
        files["calendar_dates.txt"] = "service_id,date,exception_type\nWK,20260101,1\n"
    if missing:
        files.pop(missing, None)
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w", compression=zipfile.ZIP_STORED) as archive:
        for name, content in files.items():
            archive.writestr(f"../{name}" if unsafe and name == "agency.txt" else name, content)
    return stream.getvalue()


class SettingsTests(unittest.TestCase):
    def test_default_source_and_interval(self):
        with patch.dict(os.environ, {}, clear=True):
            settings = Settings.from_env()
        self.assertEqual(settings.gtfs_download_url, DEFAULT_GTFS_DOWNLOAD_URL)
        self.assertEqual(settings.gtfs_refresh_interval_seconds, 21600)
        self.assertTrue(settings.gtfs_auto_refresh)

    def test_environment_priority_and_primary_interval_name(self):
        env = {
            "GTFS_DOWNLOAD_URL": "https://primary.example/feed.zip",
            "GTFS_URL": "https://compat.example/feed.zip",
            "GTFS_REFRESH_SECONDS": "7200",
            "GTFS_REFRESH_INTERVAL_SECONDS": "9000",
            "GTFS_AUTO_REFRESH": "no",
        }
        with patch.dict(os.environ, env, clear=True):
            settings = Settings.from_env()
        self.assertEqual(settings.gtfs_download_url, env["GTFS_DOWNLOAD_URL"])
        self.assertEqual(settings.gtfs_refresh_interval_seconds, 7200)
        self.assertFalse(settings.gtfs_auto_refresh)

    def test_compatibility_url_and_interval_names(self):
        with patch.dict(
            os.environ,
            {"GTFS_URL": "https://compat.example/feed.zip", "GTFS_REFRESH_INTERVAL_SECONDS": "8000"},
            clear=True,
        ):
            settings = Settings.from_env()
        self.assertEqual(settings.gtfs_download_url, "https://compat.example/feed.zip")
        self.assertEqual(settings.gtfs_refresh_interval_seconds, 8000)


class GTFSRefreshTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.target = self.root / "active.zip"
        self.metadata = self.root / "data" / "gtfs" / "metadata.json"
        self.activated = []
        self.candidates = []

    def tearDown(self):
        self.temp.cleanup()

    def service(self, opener, *, builder=None, max_download=1_048_576, max_uncompressed=2_097_152, attempts=3):
        def default_builder(path):
            self.candidates.append(Path(path).read_bytes())
            return FakeStore()

        return GTFSRefreshService(
            source_url="https://example.test/feed.zip?token=secret",
            target_path=self.target,
            metadata_path=self.metadata,
            interval_seconds=21600,
            timeout_seconds=5,
            max_download_bytes=max_download,
            max_uncompressed_bytes=max_uncompressed,
            max_attempts=attempts,
            enabled=True,
            build_candidate=builder or default_builder,
            activate_candidate=self.activated.append,
            opener=opener,
            sleep=lambda _seconds: None,
        )

    def test_successful_first_download_and_metadata(self):
        payload = gtfs_bytes()
        service = self.service(lambda *_args, **_kwargs: FakeResponse(payload, headers={"ETag": '"v1"'}))
        self.assertTrue(service.refresh())
        self.assertEqual(self.target.read_bytes(), payload)
        self.assertEqual(len(self.activated), 1)
        metadata = json.loads(self.metadata.read_text(encoding="utf-8"))
        self.assertEqual(metadata["etag"], '"v1"')
        self.assertEqual(metadata["downloadedBytes"], len(payload))
        self.assertNotIn("secret", metadata["source"])
        self.assertIn("token=%2A%2A%2A", metadata["source"])
        self.assertIsNotNone(metadata["lastSuccessfulLoadAt"])

    def test_gtfs_store_loads_from_explicit_candidate_path(self):
        candidate = self.root / "candidate.zip"
        candidate.write_bytes(gtfs_bytes())
        store = legacy.GTFSStore().load_from_path(candidate)
        self.assertTrue(store.loaded, store.error)
        self.assertIn("S", store.stops)
        self.assertIn("R", store.routes)
        self.assertIn("T", store.trips)

    def test_activation_order_is_candidate_then_zip_then_store(self):
        old = gtfs_bytes(calendar=False, calendar_dates=True)
        new = gtfs_bytes()
        self.target.write_bytes(old)
        events = []

        def builder(path):
            self.assertEqual(self.target.read_bytes(), old)
            self.assertEqual(Path(path).read_bytes(), new)
            events.append("candidate")
            return FakeStore()

        def activate(candidate):
            self.assertTrue(candidate.loaded)
            self.assertEqual(self.target.read_bytes(), new)
            events.append("activate")

        service = GTFSRefreshService(
            source_url="https://example.test/feed.zip",
            target_path=self.target,
            metadata_path=self.metadata,
            interval_seconds=21600,
            timeout_seconds=5,
            max_download_bytes=1_048_576,
            max_uncompressed_bytes=2_097_152,
            max_attempts=1,
            enabled=True,
            build_candidate=builder,
            activate_candidate=activate,
            opener=lambda *_args, **_kwargs: FakeResponse(new),
            sleep=lambda _seconds: None,
        )
        self.assertTrue(service.refresh())
        self.assertEqual(events, ["candidate", "activate"])

    def test_invalid_zip_and_empty_response_preserve_old_zip(self):
        self.target.write_bytes(b"old")
        for payload in (b"not a zip", b""):
            service = self.service(lambda *_args, _payload=payload, **_kwargs: FakeResponse(_payload))
            self.assertFalse(service.refresh())
            self.assertEqual(self.target.read_bytes(), b"old")
            self.assertTrue(service.snapshot()["lastError"])

    def test_oversized_download_is_rejected(self):
        payload = gtfs_bytes()
        service = self.service(
            lambda *_args, **_kwargs: FakeResponse(payload, headers={"Content-Length": str(len(payload) + 10)}),
            max_download=max(1, len(payload)),
        )
        service.max_download_bytes = len(payload) - 1
        self.assertFalse(service.refresh())
        self.assertFalse(self.target.exists())

    def test_missing_required_file_and_calendar_are_rejected(self):
        for payload in (
            gtfs_bytes(missing="stops.txt"),
            gtfs_bytes(calendar=False, calendar_dates=False),
        ):
            service = self.service(lambda *_args, _payload=payload, **_kwargs: FakeResponse(_payload))
            self.assertFalse(service.refresh())
            self.assertFalse(self.target.exists())

    def test_calendar_dates_alone_is_accepted(self):
        payload = gtfs_bytes(calendar=False, calendar_dates=True)
        service = self.service(lambda *_args, **_kwargs: FakeResponse(payload))
        self.assertTrue(service.refresh())

    def test_unsafe_path_and_uncompressed_limit_are_rejected(self):
        unsafe = gtfs_bytes(unsafe=True)
        service = self.service(lambda *_args, **_kwargs: FakeResponse(unsafe))
        self.assertFalse(service.refresh())
        payload = gtfs_bytes()
        service = self.service(lambda *_args, **_kwargs: FakeResponse(payload), max_uncompressed=1)
        service.max_uncompressed_bytes = 1
        self.assertFalse(service.refresh())

    def test_crc_failure_is_rejected(self):
        payload = bytearray(gtfs_bytes())
        marker = payload.find(b"Agency")
        self.assertGreater(marker, 0)
        payload[marker] ^= 0x01
        service = self.service(lambda *_args, **_kwargs: FakeResponse(bytes(payload)))
        self.assertFalse(service.refresh())
        self.assertIn("CRC", service.snapshot()["lastError"])

    def test_304_uses_conditional_headers_without_activation(self):
        self.metadata.parent.mkdir(parents=True)
        self.metadata.write_text(
            json.dumps({"etag": '"old"', "lastModified": "Wed, 01 Jan 2025 00:00:00 GMT"}),
            encoding="utf-8",
        )
        requests = []

        def opener(request, **_kwargs):
            requests.append(dict(request.header_items()))
            raise urllib.error.HTTPError(request.full_url, 304, "Not Modified", {}, None)

        service = self.service(opener)
        self.assertFalse(service.refresh())
        headers = {key.lower(): value for key, value in requests[0].items()}
        self.assertEqual(headers["if-none-match"], '"old"')
        self.assertEqual(headers["if-modified-since"], "Wed, 01 Jan 2025 00:00:00 GMT")
        self.assertEqual(self.activated, [])
        self.assertTrue(service.snapshot()["usingCachedData"])
        self.assertIsNotNone(service.snapshot()["lastCheckedAt"])

    def test_unchanged_sha_does_not_replace_or_reload(self):
        payload = gtfs_bytes()
        self.target.write_bytes(payload)
        service = self.service(lambda *_args, **_kwargs: FakeResponse(payload))
        self.assertFalse(service.refresh())
        self.assertEqual(self.candidates, [])
        self.assertEqual(self.activated, [])
        self.assertTrue(service.snapshot()["usingCachedData"])

    def test_changed_sha_activates_candidate(self):
        self.target.write_bytes(gtfs_bytes(calendar=False, calendar_dates=True))
        payload = gtfs_bytes()
        service = self.service(lambda *_args, **_kwargs: FakeResponse(payload))
        self.assertTrue(service.refresh())
        self.assertEqual(self.target.read_bytes(), payload)
        self.assertEqual(len(self.activated), 1)

    def test_candidate_failure_preserves_zip_and_active_store(self):
        old = gtfs_bytes(calendar=False, calendar_dates=True)
        self.target.write_bytes(old)
        service = self.service(
            lambda *_args, **_kwargs: FakeResponse(gtfs_bytes()),
            builder=lambda _path: FakeStore(False, "candidate failed"),
        )
        self.assertFalse(service.refresh())
        self.assertEqual(self.target.read_bytes(), old)
        self.assertEqual(self.activated, [])
        self.assertIn("candidate failed", service.snapshot()["lastError"])

    def test_activation_failure_rolls_back_active_zip(self):
        old = gtfs_bytes(calendar=False, calendar_dates=True)
        self.target.write_bytes(old)
        service = GTFSRefreshService(
            source_url="https://example.test/feed.zip",
            target_path=self.target,
            metadata_path=self.metadata,
            interval_seconds=21600,
            timeout_seconds=5,
            max_download_bytes=1_048_576,
            max_uncompressed_bytes=2_097_152,
            max_attempts=1,
            enabled=True,
            build_candidate=lambda _path: FakeStore(),
            activate_candidate=lambda _candidate: (_ for _ in ()).throw(RuntimeError("activation failed")),
            opener=lambda *_args, **_kwargs: FakeResponse(gtfs_bytes()),
            sleep=lambda _seconds: None,
        )
        self.assertFalse(service.refresh())
        self.assertEqual(self.target.read_bytes(), old)
        self.assertIn("activation failed", service.snapshot()["lastError"])

    def test_only_one_refresh_runs_at_a_time(self):
        entered = threading.Event()
        release = threading.Event()

        def opener(*_args, **_kwargs):
            entered.set()
            release.wait(5)
            return FakeResponse(gtfs_bytes())

        service = self.service(opener)
        thread = threading.Thread(target=service.refresh)
        thread.start()
        self.assertTrue(entered.wait(2))
        self.assertFalse(service.refresh())
        release.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())

    def test_retry_is_bounded(self):
        attempts = []

        def opener(*_args, **_kwargs):
            attempts.append(True)
            raise urllib.error.URLError("temporary")

        service = self.service(opener, attempts=3)
        self.assertFalse(service.refresh())
        self.assertEqual(len(attempts), 3)

    def test_corrupt_metadata_does_not_block_startup(self):
        self.metadata.parent.mkdir(parents=True)
        self.metadata.write_text("{broken", encoding="utf-8")
        service = self.service(lambda *_args, **_kwargs: FakeResponse(gtfs_bytes()))
        self.assertTrue(service.enabled)
        self.assertIn("source", service.snapshot())


if __name__ == "__main__":
    unittest.main()
