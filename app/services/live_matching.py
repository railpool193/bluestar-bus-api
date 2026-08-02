from __future__ import annotations

from datetime import date, datetime
from typing import Any, Mapping, Optional, Sequence, Tuple

from app.services.gtfs_loader import GTFSStore
from app.utils.text_utils import clean_text, destination_match, extract_codes, human_name, line_norm, norm, safe_int
from app.utils.time_utils import gtfs_time_to_datetime, parse_iso_dt


def stop_same(stop: Mapping[str, Any], stop_ref: str = "", stop_name: str = "") -> bool:
    if stop_ref and clean_text(stop_ref).upper() == clean_text(stop.get("stop_id")).upper():
        return True
    first, second = norm(stop_name), norm(stop.get("stop_name"))
    return bool(first and second and (first in second or second in first))


def _reference_matches(expected: Any, actual: Any) -> bool:
    expected_value, actual_value = norm(expected), norm(actual)
    return bool(expected_value and actual_value and (expected_value == actual_value or expected_value in actual_value))


def departure_match_score(store: GTFSStore, departure: Mapping[str, Any], scheduled_at: datetime, vehicle: Mapping[str, Any], *, matching_minutes: int) -> Optional[tuple[int, float]]:
    if line_norm(vehicle.get("line")) != line_norm(departure.get("line")):
        return None
    if not destination_match(vehicle.get("destinationFull") or vehicle.get("destination"), departure.get("headsign_full") or departure.get("headsign")):
        return None
    live_at = parse_iso_dt(vehicle.get("liveTime"))
    if not live_at:
        return None
    difference = abs((live_at - scheduled_at).total_seconds()) / 60
    if difference > matching_minutes:
        return None
    trip_ref, trip_id = clean_text(vehicle.get("datedVehicleJourneyRef")), clean_text(departure.get("trip_id"))
    if trip_ref and trip_id and not _reference_matches(trip_id, trip_ref):
        return None
    score = max(0, 100 - int(difference * 3))
    if trip_ref and trip_id:
        score += 1000
    block_ref, block_id = clean_text(vehicle.get("blockRef")), clean_text(departure.get("block_id"))
    if block_ref and block_id:
        if not _reference_matches(block_id, block_ref):
            return None
        score += 400
    if stop_same(store.stops.get(departure.get("stop_id"), {}), vehicle.get("currentStopRef", ""), vehicle.get("currentStopName", "")):
        score += 80
    if vehicle.get("vehicleAtStop"):
        score += 20
    if vehicle.get("fleet"):
        score += 5
    return score, difference


def match_live_to_departure(store: GTFSStore, departure: Mapping[str, Any], scheduled_at: datetime, vehicles: Sequence[Mapping[str, Any]], *, matching_minutes: int) -> Optional[Mapping[str, Any]]:
    best, best_key = None, None
    for vehicle in vehicles:
        scored = departure_match_score(store, departure, scheduled_at, vehicle, matching_minutes=matching_minutes)
        if scored is None:
            continue
        score, difference = scored
        key = (score, -difference)
        if best_key is None or key > best_key:
            best, best_key = vehicle, key
    return best


def match_live_to_departures(store: GTFSStore, departures: Sequence[tuple[Mapping[str, Any], datetime]], vehicles: Sequence[Mapping[str, Any]], *, matching_minutes: int) -> dict[int, Mapping[str, Any]]:
    """Allocate each live vehicle to at most one time-compatible departure."""
    candidates: list[tuple[int, float, int, int]] = []
    for departure_index, (departure, scheduled_at) in enumerate(departures):
        for vehicle_index, vehicle in enumerate(vehicles):
            scored = departure_match_score(store, departure, scheduled_at, vehicle, matching_minutes=matching_minutes)
            if scored is not None:
                score, difference = scored
                candidates.append((score, -difference, departure_index, vehicle_index))
    matched: dict[int, Mapping[str, Any]] = {}
    used_vehicles: set[int] = set()
    for _score, _negative_difference, departure_index, vehicle_index in sorted(candidates, reverse=True):
        if departure_index in matched or vehicle_index in used_vehicles:
            continue
        matched[departure_index] = vehicles[vehicle_index]
        used_vehicles.add(vehicle_index)
    return matched


def find_live_for_trip(store: GTFSStore, trip: Mapping[str, Any], service_day: date, vehicles: Sequence[Mapping[str, Any]], *, vehicle_hint: str = "") -> Tuple[Optional[Mapping[str, Any]], Optional[int]]:
    line, destination, trip_id = trip.get("line", ""), human_name(trip.get("trip_headsign") or trip.get("destination") or ""), trip.get("trip_id", "")
    hint_codes, rows, first = extract_codes(vehicle_hint), store.stop_times_by_trip.get(trip_id, []), store.trip_first_stop(trip_id)
    first_at = gtfs_time_to_datetime(service_day, first.get("departure_time") or first.get("arrival_time") or "")
    best, best_sequence, best_score = None, None, -9999
    for vehicle in vehicles:
        if line_norm(vehicle.get("line")) != line_norm(line) or not destination_match(vehicle.get("destinationFull") or vehicle.get("destination"), destination):
            continue
        score = 30
        if hint_codes and hint_codes.intersection(set(vehicle.get("codes", []))): score += 400
        if trip_id and (trip_id in clean_text(vehicle.get("datedVehicleJourneyRef")) or norm(trip_id) in norm(vehicle.get("datedVehicleJourneyRef"))): score += 300
        sequence = None
        for row in rows:
            if stop_same(store.stops.get(row.get("stop_id"), {}), vehicle.get("currentStopRef", ""), vehicle.get("currentStopName", "")):
                sequence, score = safe_int(row.get("stop_sequence"), 0), score + 160
                break
        live_at = parse_iso_dt(vehicle.get("liveTime"))
        if first_at and live_at:
            difference = abs((live_at - first_at).total_seconds()) / 60
            if difference < 90: score += max(0, 60 - int(difference))
        if score > best_score: best, best_sequence, best_score = vehicle, sequence, score
    return best, best_sequence
