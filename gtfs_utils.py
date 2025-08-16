import json

# Ide töltsd be a GTFS adataidat
stops_data = [
    {"stop_id": "1980SN12619A", "stop_name": "Southampton, Hanover Buildings [CU]"},
    {"stop_id": "1980SN12620B", "stop_name": "Southampton, Central Station"},
    # ide jöhet az összes Bluestar megálló...
]

def is_loaded():
    return len(stops_data) > 0

def search_stop(query: str):
    query_lower = query.lower()
    results = [
        {"stop_id": stop["stop_id"], "display_name": stop["stop_name"]}
        for stop in stops_data if query_lower in stop["stop_name"].lower()
    ]
    return results
