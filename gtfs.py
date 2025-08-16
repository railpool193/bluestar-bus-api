import csv
import datetime
import logging
from pathlib import Path

logger = logging.getLogger("bluestar")

DATA_DIR = Path(__file__).parent / "data"

# Fájlok betöltése
stops = {}
stop_times = []
trips = {}
routes = {}

def load_data():
    global stops, stop_times, trips, routes

    # stops.txt
    with open(DATA_DIR / "stops.txt", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stops[row["stop_id"]] = row

    logger.info(f"Betöltve {len(stops)} megálló.")

    # trips.txt
    with open(DATA_DIR / "trips.txt", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            trips[row["trip_id"]] = row

    logger.info(f"Betöltve {len(trips)} járat.")

    # routes.txt
    with open(DATA_DIR / "routes.txt", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            routes[row["route_id"]] = row

    logger.info(f"Betöltve {len(routes)} útvonal.")

    # stop_times.txt
    with open(DATA_DIR / "stop_times.txt", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            stop_times.append(row)

    logger.info(f"Betöltve {len(stop_times)} megálló-időpont.")


def search_stops(query: str):
    """Megálló keresése név szerint"""
    query_lower = query.lower()
    results = [
        {"stop_id": s["stop_id"], "name": s["stop_name"]}
        for s in stops.values()
        if query_lower in s["stop_name"].lower()
    ]
    logger.info(f"Keresés: '{query}' → {len(results)} találat")
    return results


def get_next_departures(stop_id: str, minutes: int = 60):
    """Következő indulások lekérése"""
    now = datetime.datetime.now()
    future = now + datetime.timedelta(minutes=minutes)

    logger.info(f"Indulások keresése: stop_id={stop_id}, intervallum={now.time()} - {future.time()}")

    departures = []

    for st in stop_times:
        if st["stop_id"] != stop_id:
            continue

        trip_id = st["trip_id"]
        trip = trips.get(trip_id)
        if not trip:
            logger.warning(f"Hiányzó trip: {trip_id}")
            continue

        route = routes.get(trip["route_id"])
        if not route:
            logger.warning(f"Hiányzó route: {trip['route_id']}")
            continue

        # Indulási idő feldolgozás
        dep_time_str = st["departure_time"]
        try:
            h, m, s = map(int, dep_time_str.split(":"))
            dep_time = now.replace(hour=h % 24, minute=m, second=s)
        except Exception as e:
            logger.error(f"Hibás időformátum: {dep_time_str} ({e})")
            continue

        if now <= dep_time <= future:
            departures.append({
                "route": route["route_short_name"],
                "destination": trip.get("trip_headsign", ""),
                "time": dep_time.strftime("%H:%M"),
                "live": False,  # majd a siri_live jelzi ha van élő adat
            })

    logger.info(f"{len(departures)} indulás található a(z) {stop_id} megállóhoz.")

    return sorted(departures, key=lambda x: x["time"])


# modul betöltéskor hívjuk
load_data()
