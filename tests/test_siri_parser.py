import unittest
from datetime import datetime
from pathlib import Path

from app.services.siri_parser import SIRIParseError, parse_vehicle_monitoring
from app.utils.time_utils import LONDON


FIXTURES = Path(__file__).parent / "fixtures" / "siri"
NOW = datetime(2026, 8, 2, 11, 0, tzinfo=LONDON)


class SIRIParserTests(unittest.TestCase):
    def parse(self, name, **kwargs):
        return parse_vehicle_monitoring((FIXTURES / name).read_bytes(), reference_time=NOW, max_age_seconds=360, **kwargs)

    def test_namespaced_full_vehicle_schema(self):
        parsed = self.parse("namespaced_vehicle_monitoring.xml", operator_filter="BLUS")
        self.assertEqual(parsed.raw_count, 1)
        vehicle = parsed.vehicles[0]
        expected = {"line", "lineNorm", "destination", "destinationFull", "operator", "vehicleRef", "fleet", "datedVehicleJourneyRef", "blockRef", "codes", "latitude", "longitude", "bearing", "recordedAt", "currentStopRef", "currentStopName", "vehicleAtStop", "aimedTime", "expectedTime", "liveTime", "delayMinutes", "status"}
        self.assertEqual(set(vehicle), expected)
        self.assertEqual(vehicle["delayMinutes"], 5)
        self.assertTrue(vehicle["vehicleAtStop"])
        self.assertEqual(vehicle["fleet"], "1234")

    def test_non_namespaced_and_invalid_coordinate(self):
        vehicle = self.parse("non_namespaced_vehicle_monitoring.xml").vehicles[0]
        self.assertEqual(vehicle["line"], "U1")
        self.assertIsNone(vehicle["latitude"])

    def test_multiple_operators_and_filter(self):
        self.assertEqual(len(self.parse("multiple_operators.xml").vehicles), 2)
        parsed = self.parse("multiple_operators.xml", operator_filter="BLUS")
        self.assertEqual(parsed.raw_count, 2)
        self.assertEqual([vehicle["operator"] for vehicle in parsed.vehicles], ["BLUS"])

    def test_missing_optional_and_missing_timestamp_are_allowed(self):
        vehicle = self.parse("missing_optional_fields.xml").vehicles[0]
        self.assertEqual(vehicle["recordedAt"], "")
        self.assertEqual(vehicle["status"], "Moving")

    def test_stale_vehicle_is_filtered(self):
        self.assertEqual(self.parse("stale_vehicle.xml").vehicles, ())

    def test_malformed_and_doctype_are_rejected(self):
        for name in ("malformed.xml", "doctype.xml"):
            with self.assertRaises(SIRIParseError, msg=name):
                self.parse(name)

    def test_response_size_limit(self):
        with self.assertRaises(SIRIParseError):
            parse_vehicle_monitoring(b"<Siri/>", reference_time=NOW, max_xml_bytes=2)


if __name__ == "__main__": unittest.main()
