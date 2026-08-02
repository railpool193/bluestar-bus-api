from __future__ import annotations

from typing import Any

from app.services.gtfs_loader import GTFSStore
from app.utils.text_utils import clean_text, line_norm, norm


def search_gtfs(store: GTFSStore, query_value: str = "") -> dict[str, Any]:
    query = clean_text(query_value)
    normalized_query = norm(query)
    stops: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []
    if normalized_query:
        for stop_id, stop in store.stops.items():
            haystack = norm(
                stop.get("stop_name", "") + " " + stop_id + " " + stop.get("code", "")
            )
            if normalized_query in haystack:
                stops.append(
                    {
                        "id": stop_id,
                        "stop_id": stop_id,
                        "name": stop.get("stop_name", stop_id),
                        "code": stop.get("code", "BUS"),
                        "lat": stop.get("lat"),
                        "lon": stop.get("lon"),
                    }
                )
                if len(stops) >= 50:
                    break
        for route_id, route in store.routes.items():
            line = route.get("route_short_name", "")
            haystack = norm(line + " " + route.get("route_long_name", "") + " " + route_id)
            if normalized_query in haystack or normalized_query == norm(line):
                routes.append(
                    {
                        "id": line,
                        "routeId": route_id,
                        "line": line,
                        "name": line,
                        "subtitle": route.get("route_long_name", ""),
                    }
                )
        routes.sort(
            key=lambda route: (
                0 if line_norm(route.get("line")) == line_norm(query) else 1,
                route.get("line"),
            )
        )
    return {"ok": True, "query": query, "stops": stops[:50], "routes": routes[:40]}
