import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest.mock import patch

from app.services.bustimes_vehicle_client import BustimesVehicleClient, BustimesVehicleClientConfig, BustimesVehicleError
from app.services.fleet_refresh import FleetRefreshService
from app.services.fleet_registry import FleetRegistryProvider, FleetSnapshot, adapt_bustimes_vehicle, enrich_vehicle, normalize_registration


FIXTURE = Path(__file__).parent / "fixtures" / "bustimes" / "vehicles_page.json"
PAGE = json.loads(FIXTURE.read_text(encoding="utf-8"))


class Response(io.BytesIO):
    def __init__(self, value, content_type="application/json", declared=None):
        payload = value if isinstance(value, bytes) else json.dumps(value).encode()
        super().__init__(payload)
        self.status = 200
        self.headers = {"Content-Type": content_type}
        if declared is not None: self.headers["Content-Length"] = str(declared)


def record(**changes):
    raw = json.loads(json.dumps(PAGE["results"][0]))
    raw.update(changes)
    return adapt_bustimes_vehicle(raw, "2026-08-02T12:00:00+00:00")


class BustimesVehicleClientTests(unittest.TestCase):
    def client(self, opener, **changes):
        config = BustimesVehicleClientConfig(max_bytes=4096, max_pages=3, max_records=10, attempts=1, **changes)
        return BustimesVehicleClient(config, opener=opener, sleep=lambda _seconds: None)

    def test_operator_filtered_fixture_and_adapter_fields(self):
        requests = []
        client = self.client(lambda request, **_kwargs: requests.append(request.full_url) or Response(PAGE))
        values = client.fetch("blus", fetched_at="NOW")
        self.assertIn("operator=BLUS", requests[0]); self.assertEqual(len(values), 1)
        value = values[0]
        self.assertEqual((value["fleetCode"], value["registration"], value["vehicleType"], value["fuel"]), ("1804", "HJ25 BYG", "ADL Enviro400 MMC", "diesel"))
        self.assertTrue(value["doubleDecker"]); self.assertFalse(value["electric"])
        self.assertEqual(value["specialFeatures"], ["USB-A", "USB-C"])
        self.assertNotIn("notes", value); self.assertNotIn("left", value)

    def test_pagination_and_lookup_filters(self):
        first = {**PAGE, "next": "https://bustimes.org/api/vehicles/?operator=BLUS&limit=1&offset=1"}
        second = {**PAGE, "results": [{**PAGE["results"][0], "id": 2, "fleet_code": "1805"}]}
        responses = iter([Response(first), Response(second)])
        client = self.client(lambda *_args, **_kwargs: next(responses))
        self.assertEqual(len(client.fetch("BLUS", fetched_at="NOW")), 2)
        urls = []
        client = self.client(lambda request, **_kwargs: urls.append(request.full_url) or Response(PAGE))
        client.lookup_fleet("BLUS", "1804", fetched_at="NOW"); client.lookup_registration("BLUS", "HJ25BYG", fetched_at="NOW")
        self.assertIn("fleet_code=1804", urls[0]); self.assertIn("reg=HJ25BYG", urls[1])

    def test_empty_result_and_wrong_operator_are_safe(self):
        client = self.client(lambda *_args, **_kwargs: Response({"count": 0, "next": None, "results": []}))
        self.assertEqual(client.fetch("BLUS", fetched_at="NOW"), ())
        wrong = {**PAGE, "results": [{**PAGE["results"][0], "operator": {"id": "OTHER"}}]}
        client = self.client(lambda *_args, **_kwargs: Response(wrong))
        self.assertEqual(client.fetch("BLUS", fetched_at="NOW"), ())

    def test_invalid_json_content_type_timeout_and_size(self):
        for opener in (
            lambda *_a, **_k: Response(b"broken"),
            lambda *_a, **_k: Response(PAGE, "text/html"),
            lambda *_a, **_k: Response(PAGE, declared=5000),
            lambda *_a, **_k: (_ for _ in ()).throw(TimeoutError("timeout")),
        ):
            with self.assertRaises((BustimesVehicleError, TimeoutError)):
                self.client(opener).fetch("BLUS", fetched_at="NOW")

    def test_foreign_next_loop_and_page_limit_are_rejected(self):
        foreign = {**PAGE, "next": "https://evil.example/api/vehicles/?offset=1"}
        with self.assertRaises(BustimesVehicleError): self.client(lambda *_a, **_k: Response(foreign)).fetch("BLUS", fetched_at="NOW")
        loop = {**PAGE, "next": "https://bustimes.org/api/vehicles/?operator=BLUS&limit=10"}
        with self.assertRaises(BustimesVehicleError): self.client(lambda *_a, **_k: Response(loop)).fetch("BLUS", fetched_at="NOW")


class FleetRegistryTests(unittest.TestCase):
    def test_exact_operator_fleet_and_registration_normalization(self):
        snapshot = FleetSnapshot((record(),))
        self.assertEqual(snapshot.resolve("blus", " 1804 ")[0]["vehicleType"], "ADL Enviro400 MMC")
        self.assertEqual(normalize_registration("hj25 byg"), "HJ25BYG")

    def test_withdrawn_preference_and_ambiguity(self):
        active, withdrawn = record(), record(sourceVehicleId=2)
        withdrawn["withdrawn"] = True
        self.assertFalse(FleetSnapshot((active, withdrawn)).resolve("BLUS", "1804")[1])
        self.assertEqual(FleetSnapshot((withdrawn,)).resolve("BLUS", "1804"), (None, False))
        other = dict(active); other["sourceVehicleId"] = 3
        self.assertEqual(FleetSnapshot((active, other)).resolve("BLUS", "1804"), (None, True))

    def test_enrichment_missing_ambiguous_and_snapshot_immutability(self):
        original = {"operator": "BLUS", "fleet": "1804", "line": "1"}
        provider = FleetRegistryProvider(FleetSnapshot((record(),)))
        enriched = enrich_vehicle(original, provider.get())
        self.assertEqual((enriched["registration"], enriched["vehicleType"]), ("HJ25 BYG", "ADL Enviro400 MMC"))
        self.assertEqual(original, {"operator": "BLUS", "fleet": "1804", "line": "1"})
        self.assertEqual(enrich_vehicle(original, FleetSnapshot()), original)
        duplicate = dict(record()); duplicate["sourceVehicleId"] = 2
        self.assertTrue(enrich_vehicle(original, FleetSnapshot((record(), duplicate)))["vehicleMetadataAmbiguous"])


class FleetRefreshTests(unittest.TestCase):
    def test_atomic_cache_and_old_snapshot_survives_failure(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); provider = FleetRegistryProvider()
            client = unittest.mock.Mock(); client.fetch.return_value = (record(),)
            service = FleetRefreshService(client=client, provider=provider, operator_id="BLUS", cache_path=root/"vehicles.json", metadata_path=root/"metadata.json", enabled=True)
            with patch("app.services.fleet_refresh.os.replace", wraps=__import__("os").replace) as replace_call:
                self.assertTrue(service.refresh()); self.assertTrue(replace_call.called)
            self.assertEqual(provider.get().records[0]["fleetCode"], "1804")
            client.fetch.side_effect = TimeoutError("offline")
            self.assertFalse(service.refresh()); self.assertEqual(provider.get().records[0]["fleetCode"], "1804")
            self.assertTrue(service.snapshot()["usingCachedData"])

    def test_import_and_endpoint_style_reads_do_not_fetch(self):
        client = unittest.mock.Mock(); provider = FleetRegistryProvider()
        with tempfile.TemporaryDirectory() as folder:
            service = FleetRefreshService(client=client, provider=provider, operator_id="BLUS", cache_path=Path(folder)/"missing.json", metadata_path=Path(folder)/"meta.json", enabled=False)
            provider.get(); service.snapshot(); client.fetch.assert_not_called()

    def test_fetched_timestamp_does_not_force_cache_replacement(self):
        first, second = record(), record()
        second["fetchedAt"] = "LATER"
        self.assertEqual(FleetRefreshService._canonical((first,)), FleetRefreshService._canonical((second,)))


if __name__ == "__main__": unittest.main()
