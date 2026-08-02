import threading
import unittest
from datetime import datetime, timedelta
from unittest.mock import Mock

from app.services.live_refresh import LiveRefreshService
from app.services.live_store_provider import LiveSnapshot, LiveSnapshotProvider
from app.services.siri_parser import ParsedSIRI
from app.utils.time_utils import LONDON


NOW = datetime(2026, 8, 2, 10, 0, tzinfo=LONDON)


class Client:
    source = "https://example.test/live?api_key=%2A%2A%2A"
    def __init__(self): self.calls = 0
    def download(self): self.calls += 1; return b"xml"


class LiveServiceTests(unittest.TestCase):
    def test_empty_replace_and_old_snapshot_survives(self):
        provider = LiveSnapshotProvider()
        old = provider.get()
        new = LiveSnapshot(({"line": "1"},), True)
        provider.replace(new)
        self.assertEqual(old.vehicles, ())
        self.assertIs(provider.get(), new)

    def test_success_ttl_and_failed_fetch_preserves_vehicles(self):
        client, provider = Client(), LiveSnapshotProvider()
        parser = Mock(return_value=ParsedSIRI(({"line": "1"},), 2))
        service = LiveRefreshService(client=client, provider=provider, interval_seconds=8, max_age_seconds=360, operator_filter="BLUS", now=lambda: NOW, parser=parser)
        self.assertTrue(service.refresh())
        self.assertFalse(service.refresh())
        self.assertEqual(client.calls, 1)
        client.download = Mock(side_effect=OSError("offline"))
        self.assertFalse(service.refresh(force=True))
        snapshot = provider.get()
        self.assertEqual(snapshot.vehicles[0]["line"], "1")
        self.assertTrue(snapshot.stale)
        self.assertIn("offline", snapshot.error)

    def test_only_one_fetch_and_start_is_idempotent_and_stop_works(self):
        entered, release = threading.Event(), threading.Event()
        client, provider = Client(), LiveSnapshotProvider()
        def download(): entered.set(); release.wait(2); return b"xml"
        client.download = download
        service = LiveRefreshService(client=client, provider=provider, interval_seconds=60, max_age_seconds=360, operator_filter="", parser=lambda *_a, **_k: ParsedSIRI((), 0))
        thread = threading.Thread(target=lambda: service.refresh(force=True)); thread.start(); self.assertTrue(entered.wait(1))
        self.assertFalse(service.refresh(force=True)); release.set(); thread.join(2)
        service.start(); first = service._thread; service.start(); self.assertIs(service._thread, first); service.stop(); self.assertFalse(first.is_alive())

    def test_concurrent_get_replace(self):
        provider, errors = LiveSnapshotProvider(), []
        def reader():
            for _ in range(1000):
                if not isinstance(provider.get().vehicles, tuple): errors.append(1)
        threads = [threading.Thread(target=reader) for _ in range(3)]
        for thread in threads: thread.start()
        for index in range(100): provider.replace(LiveSnapshot(({"line": str(index)},), True))
        for thread in threads: thread.join()
        self.assertEqual(errors, [])


if __name__ == "__main__": unittest.main()
