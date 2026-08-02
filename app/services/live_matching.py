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


def match_live_to_departure(store: GTFSStore, departure: Mapping[str, Any], scheduled_at: datetime, vehicles: Sequence[Mapping[str, Any]], *, matching_minutes: int) -> Optional[Mapping[str, Any]]:
    best, best_score = None, -9999
    for vehicle in vehicles:
        if line_norm(vehicle.get("line")) != line_norm(departure.get("line")):
            continue
        if not destination_match(vehicle.get("destinationFull") or vehicle.get("destination"), departure.get("headsign_full") or departure.get("headsign")):
            continue
        score, live_at = 0, parse_iso_dt(vehicle.get("liveTime"))
        if live_at:
            difference = abs((live_at - scheduled_at).total_seconds()) / 60
            if difference > matching_minutes:
                continue
            score += max(0, 100 - int(difference * 3))
        if stop_same(store.stops.get(departure.get("stop_id"), {}), vehicle.get("currentStopRef", ""), vehicle.get("currentStopName", "")):
            score += 80
        if vehicle.get("vehicleAtStop"):
            score += 20
        if vehicle.get("fleet"):
            score += 5
        if score > best_score:
            best, best_score = vehicle, score
    return best


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
