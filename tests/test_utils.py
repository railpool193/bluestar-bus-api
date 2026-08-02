import unittest
from datetime import date, datetime

from app.utils.geo_utils import haversine_m
from app.utils.text_utils import clean_text, destination_match, extract_codes, line_norm, safe_float, safe_int
from app.utils.time_utils import LONDON, gtfs_time_to_datetime, hhmm, minutes_until, parse_iso_dt


class UtilityTests(unittest.TestCase):
    def test_text_helpers_preserve_legacy_behaviour(self):
        self.assertEqual(clean_text("  Central_Stop  "), "Central Stop")
        self.assertEqual(line_norm(" u 1 "), "U1")
        self.assertEqual(safe_int("2.9"), 2)
        self.assertEqual(safe_float("50.9"), 50.9)
        self.assertTrue(destination_match("Winchester Bus Station", "Winchester"))
        self.assertIn("1234", extract_codes("vehicle-001234"))

    def test_time_helpers_support_gtfs_hours_after_midnight(self):
        value = gtfs_time_to_datetime(date(2026, 8, 2), "25:15:30")
        self.assertEqual(value, datetime(2026, 8, 3, 1, 15, 30, tzinfo=LONDON))
        self.assertEqual(hhmm(value), "01:15")
        self.assertEqual(minutes_until(value, reference=datetime(2026, 8, 3, 1, 0, 30, tzinfo=LONDON)), 15)
        self.assertEqual(parse_iso_dt("2026-08-02T10:00:00Z").tzinfo, LONDON)

    def test_haversine(self):
        self.assertEqual(haversine_m(50, -1, 50, -1), 0)
        self.assertIsNone(haversine_m("bad", -1, 50, -1))


if __name__ == "__main__":
    unittest.main()
