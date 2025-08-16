import asyncio

def is_available():
    # később ide jöhet valós ellenőrzés
    return True

async def get_next_departures(stop_id: str, minutes: int = 60):
    # itt kell majd a valós API-t meghívni
    # most csak teszt adat
    if stop_id == "1980SN12619A":
        return [
            {"line": "1", "destination": "Winchester", "departure": "2025-08-16T22:45:00"},
            {"line": "3", "destination": "Hedge End", "departure": "2025-08-16T22:55:00"}
        ]
    return []
