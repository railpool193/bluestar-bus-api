from __future__ import annotations

from math import asin, cos, radians, sin, sqrt
from typing import Any, Optional


def haversine_m(lat1: Any, lon1: Any, lat2: Any, lon2: Any) -> Optional[float]:
    try:
        first_lat, first_lon = float(lat1), float(lon1)
        second_lat, second_lon = float(lat2), float(lon2)
        radius = 6_371_000.0
        latitude_delta = radians(second_lat - first_lat)
        longitude_delta = radians(second_lon - first_lon)
        value = sin(latitude_delta / 2) ** 2 + cos(radians(first_lat)) * cos(radians(second_lat)) * sin(longitude_delta / 2) ** 2
        return 2 * radius * asin(sqrt(value))
    except Exception:
        return None
