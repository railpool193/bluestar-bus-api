import unittest
from datetime import date, datetime

from app.services.departure_service import enrich_departure
from app.services.gtfs_loader import GTFSStore
from app.services.live_matching import find_live_for_trip, match_live_to_departure, match_live_to_departures
from app.utils.time_utils import LONDON


NOW = datetime(2026, 8, 2, 10, 0, tzinfo=LONDON)


def store_fixture():
    store = GTFSStore(); store.loaded = True
    store.stops = {"S1": {"stop_id": "S1", "stop_name": "Central"}, "S2": {"stop_id": "S2", "stop_name": "Terminal"}}
    store.stop_times_by_trip["T1"] = [{"trip_id": "T1", "stop_id": "S1", "stop_sequence": 1, "departure_time": "10:05:00"}, {"trip_id": "T1", "stop_id": "S2", "stop_sequence": 2}]
    return store


def vehicle(**changes):
    value = {"line": "1", "destinationFull": "Winchester", "liveTime": "2026-08-02T10:05:00+01:00", "currentStopRef": "S1", "currentStopName": "Central", "vehicleAtStop": False, "fleet": "1234", "vehicleRef": "BLUS-1234", "codes": ["1234"], "datedVehicleJourneyRef": "T1", "delayMinutes": 5}
    value.update(changes); return value


class LiveMatchingTests(unittest.TestCase):
    def departures(self, *minutes):
        return [
            ({"trip_id": f"T{minute}", "line": "1", "headsign_full": "Winchester", "stop_id": "S1"}, NOW.replace(minute=minute))
            for minute in minutes
        ]

    def test_line_destination_window_and_no_candidate(self):
        departure = {"line": "1", "headsign_full": "Winchester", "stop_id": "S1"}
        self.assertIsNotNone(match_live_to_departure(store_fixture(), departure, NOW.replace(minute=5), [vehicle()], matching_minutes=38))
        self.assertIsNone(match_live_to_departure(store_fixture(), departure, NOW.replace(minute=5), [vehicle(line="2")], matching_minutes=38))
        self.assertIsNone(match_live_to_departure(store_fixture(), departure, NOW.replace(minute=5), [vehicle(destinationFull="Romsey")], matching_minutes=38))
        self.assertIsNone(match_live_to_departure(store_fixture(), departure, NOW.replace(minute=50), [vehicle()], matching_minutes=38))

    def test_stop_and_at_stop_weight_choose_best(self):
        departure = {"line": "1", "headsign_full": "Winchester", "stop_id": "S1"}
        weak, strong = vehicle(vehicleRef="weak", currentStopRef="S2"), vehicle(vehicleRef="strong", vehicleAtStop=True)
        self.assertEqual(match_live_to_departure(store_fixture(), departure, NOW.replace(minute=5), [weak, strong], matching_minutes=38)["vehicleRef"], "strong")

    def test_trip_reference_and_fleet_hint(self):
        store = store_fixture(); trip = {"trip_id": "T1", "line": "1", "trip_headsign": "Winchester"}
        matched, sequence = find_live_for_trip(store, trip, date(2026, 8, 2), [vehicle()], vehicle_hint="1234")
        self.assertEqual(matched["fleet"], "1234"); self.assertEqual(sequence, 1)

    def test_departure_due_delay_early_and_fallback(self):
        departure = {"trip_id": "T1", "line": "1", "route_id": "R", "stop_id": "S1", "stop_name": "Central", "stop_sequence": 1, "headsign_full": "Winchester"}
        scheduled = NOW.replace(minute=1)
        due = enrich_departure(store_fixture(), departure, date(2026, 8, 2), scheduled, [vehicle(liveTime="2026-08-02T10:01:00+01:00")], reference_time=NOW, matching_minutes=38)
        self.assertTrue(due["live"]); self.assertTrue(due["isDue"]); self.assertEqual(due["delayMinutes"], 5)
        fallback = enrich_departure(store_fixture(), departure, date(2026, 8, 2), scheduled, [], reference_time=NOW, matching_minutes=38)
        self.assertFalse(fallback["live"]); self.assertEqual(fallback["vehicleRef"], "")

    def test_one_vehicle_is_allocated_to_only_one_of_three_departures(self):
        matches = match_live_to_departures(
            store_fixture(), self.departures(5, 15, 25),
            [vehicle(datedVehicleJourneyRef="")], matching_minutes=38,
        )
        self.assertEqual(list(matches), [0])

    def test_two_vehicles_are_allocated_to_two_departures(self):
        matches = match_live_to_departures(
            store_fixture(), self.departures(5, 15),
            [vehicle(vehicleRef="V1", datedVehicleJourneyRef="", liveTime="2026-08-02T10:05:00+01:00"),
             vehicle(vehicleRef="V2", datedVehicleJourneyRef="", liveTime="2026-08-02T10:15:00+01:00")],
            matching_minutes=38,
        )
        self.assertEqual({index: item["vehicleRef"] for index, item in matches.items()}, {0: "V1", 1: "V2"})

    def test_trip_reference_has_priority_over_closer_generic_vehicle(self):
        departures = [({"trip_id": "T1", "line": "1", "headsign_full": "Winchester", "stop_id": "S1"}, NOW.replace(minute=15))]
        matches = match_live_to_departures(
            store_fixture(), departures,
            [vehicle(vehicleRef="generic", datedVehicleJourneyRef="", liveTime="2026-08-02T10:15:00+01:00"),
             vehicle(vehicleRef="trip", datedVehicleJourneyRef="T1", liveTime="2026-08-02T10:10:00+01:00")],
            matching_minutes=38,
        )
        self.assertEqual(matches[0]["vehicleRef"], "trip")

    def test_different_trip_reference_cannot_match(self):
        departure = {"trip_id": "T2", "line": "1", "headsign_full": "Winchester", "stop_id": "S1"}
        self.assertIsNone(match_live_to_departure(store_fixture(), departure, NOW.replace(minute=5), [vehicle(datedVehicleJourneyRef="T1")], matching_minutes=38))

    def test_departure_outside_time_window_cannot_match(self):
        matches = match_live_to_departures(
            store_fixture(), self.departures(50),
            [vehicle(datedVehicleJourneyRef="")], matching_minutes=38,
        )
        self.assertEqual(matches, {})


if __name__ == "__main__": unittest.main()
