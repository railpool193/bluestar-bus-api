from __future__ import annotations

from typing import Any, Mapping, Sequence

from app.utils.text_utils import line_norm


def vehicles_for_line(vehicles: Sequence[Mapping[str, Any]], line: str = "") -> list[dict[str, Any]]:
    result = [dict(vehicle) for vehicle in vehicles if not line or line_norm(vehicle.get("line")) == line_norm(line)]
    result.sort(key=lambda vehicle: (line_norm(vehicle.get("line")), vehicle.get("destination", ""), vehicle.get("fleet", "")))
    return result
