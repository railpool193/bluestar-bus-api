from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


def normalize_operator(value: Any) -> str:
    return str(value or "").strip().upper()


def normalize_fleet_code(value: Any) -> str:
    return str(value or "").strip().casefold()


def normalize_registration(value: Any) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def _text(value: Any, limit: int = 200) -> str:
    return str(value or "").strip()[:limit]


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def adapt_bustimes_vehicle(value: Mapping[str, Any], fetched_at: str) -> dict[str, Any] | None:
    operator = value.get("operator") if isinstance(value.get("operator"), Mapping) else {}
    vehicle_type = value.get("vehicle_type") if isinstance(value.get("vehicle_type"), Mapping) else {}
    livery = value.get("livery") if isinstance(value.get("livery"), Mapping) else {}
    garage = value.get("garage") if isinstance(value.get("garage"), Mapping) else {}
    fleet_code = _text(value.get("fleet_code"))
    operator_id = _text(operator.get("id"))
    if not fleet_code or not operator_id:
        return None
    features = value.get("special_features") if isinstance(value.get("special_features"), list) else []
    return {
        "fleetCode": fleet_code,
        "registration": _text(value.get("reg")),
        "previousRegistration": _text(value.get("previous_reg")),
        "vehicleType": _text(vehicle_type.get("name")),
        "vehicleStyle": _text(vehicle_type.get("style")),
        "fuel": _text(vehicle_type.get("fuel")),
        "doubleDecker": _optional_bool(vehicle_type.get("double_decker")),
        "coach": _optional_bool(vehicle_type.get("coach")),
        "electric": _optional_bool(vehicle_type.get("electric")),
        "livery": _text(livery.get("name")),
        "branding": _text(value.get("branding")),
        "garageCode": _text(garage.get("code")),
        "garage": _text(garage.get("name")),
        "vehicleName": _text(value.get("name")),
        "specialFeatures": [_text(item, 80) for item in features[:50] if _text(item, 80)],
        "withdrawn": _optional_bool(value.get("withdrawn")),
        "operatorId": operator_id,
        "operatorSlug": _text(operator.get("slug")),
        "operatorName": _text(operator.get("name")),
        "source": "bustimes.org",
        "sourceVehicleId": value.get("id") if isinstance(value.get("id"), int) else None,
        "sourceSlug": _text(value.get("slug")),
        "fetchedAt": fetched_at,
    }


@dataclass(frozen=True)
class FleetSnapshot:
    records: tuple[dict[str, Any], ...] = ()
    fetched_at: str | None = None
    stale: bool = False

    def resolve(self, operator: Any, fleet_code: Any, registration: Any = "") -> tuple[dict[str, Any] | None, bool]:
        operator_key, fleet_key = normalize_operator(operator), normalize_fleet_code(fleet_code)
        candidates = [record for record in self.records if normalize_operator(record.get("operatorId")) == operator_key and normalize_fleet_code(record.get("fleetCode")) == fleet_key] if operator_key and fleet_key else []
        active = [record for record in candidates if record.get("withdrawn") is False]
        selected = active
        if len(selected) == 1:
            return dict(selected[0]), False
        if len(selected) > 1:
            return None, True
        registration_key = normalize_registration(registration)
        if operator_key and registration_key:
            by_registration = [record for record in self.records if normalize_operator(record.get("operatorId")) == operator_key and normalize_registration(record.get("registration")) == registration_key and record.get("withdrawn") is False]
            if len(by_registration) == 1:
                return dict(by_registration[0]), False
            if len(by_registration) > 1:
                return None, True
        return None, False


class FleetRegistryProvider:
    def __init__(self, snapshot: FleetSnapshot | None = None):
        self._snapshot = snapshot or FleetSnapshot()
        self._lock = threading.Lock()

    def get(self) -> FleetSnapshot:
        with self._lock:
            return self._snapshot

    def replace(self, snapshot: FleetSnapshot) -> FleetSnapshot:
        with self._lock:
            previous = self._snapshot
            self._snapshot = snapshot
            return previous


def enrich_vehicle(vehicle: Mapping[str, Any], snapshot: FleetSnapshot, *, operator: str = "BLUS") -> dict[str, Any]:
    result = dict(vehicle)
    record, ambiguous = snapshot.resolve(vehicle.get("operator") or operator, vehicle.get("fleet"), vehicle.get("registration"))
    if ambiguous:
        result["vehicleMetadataAmbiguous"] = True
        return result
    if not record:
        return result
    result.update({
        "registration": record.get("registration") or None,
        "vehicleType": record.get("vehicleType") or None,
        "fuel": record.get("fuel") or None,
        "doubleDecker": record.get("doubleDecker"),
        "coach": record.get("coach"),
        "electric": record.get("electric"),
        "livery": record.get("livery") or None,
        "branding": record.get("branding") or None,
        "garage": record.get("garage") or None,
        "vehicleName": record.get("vehicleName") or None,
        "specialFeatures": list(record.get("specialFeatures") or []),
        "vehicleMetadataSource": "bustimes.org",
        "vehicleMetadataStale": snapshot.stale,
        "vehicleMetadataAmbiguous": False,
    })
    return result
