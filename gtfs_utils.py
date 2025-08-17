import os
import csv
from datetime import datetime, date, time, timedelta
from typing import List, Dict, Tuple, Set

GTFS_DIR = os.getenv("GTFS_DIR", "gtfs")  # ide csomagold ki a Bluestar GTFS zip-et

# Segédek
def _parse_hms(hms: str) -> int:
    """HH:MM:SS → nap elejétől számolt percek, 24h feletti időket is kezeli (pl. 25:10:00)."""
    if not hms:
        return -1
    h, m, s = [int(x) for x in hms.split(":")]
    return h * 60 + m + (1 if s >= 30 else 0)

def _today_service_ids(cal_rows: List[Dict], cal_dates: List[Dict]) -> Set[str]:
    """Ma érvényes service_id-k (calendar + calendar_dates alapján)."""
    today = date.today()
    weekday = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][today.weekday()]
    active: Set[str] = set()

    for r in cal_rows:
        start = datetime.strptime(r["start_date"], "%Y%m%d").date()
        end   = datetime.strptime(r["end_date"], "%Y%m%d").date()
        if start <= today <= end and r.get(weekday,"0") == "1":
            active.add(r["service_id"])

    # calendar_dates felülírások
    for r in cal_dates:
        d = datetime.strptime(r["date"], "%Y%m%d").date()
        if d == today:
            if r["exception_type"] == "1":
                active.add(r["service_id"])
            elif r["exception_type"] == "2" and r["service_id"] in active:
                active.remove(r["service_id"])
    return active

class GTFS:
    def __init__(self, base_dir: str = GTFS_DIR):
        self.base = base_dir
        self.stops: List[Dict] = []
        self.stop_times: List[Dict] = []
        self.trips: Dict[str, Dict] = {}
        self.routes: Dict[str, Dict] = {}
        self.calendar: List[Dict] = []
        self.calendar_dates: List[Dict] = []

    def _read_csv(self, name: str) -> List[Dict]:
        path = os.path.join(self.base, name)
        if not os.path.exists(path):
            return []
        with open(path, newline="", encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))

    def load(self):
        self.stops = self._read_csv("stops.txt")
        self.stop_times = self._read_csv("stop_times.txt")
        self.calendar = self._read_csv("calendar.txt")
        self.calendar_dates = self._read_csv("calendar_dates.txt")
        routes = self._read_csv("routes.txt")
        trips = self._read_csv("trips.txt")
        self.routes = {r["route_id"]: r for r in routes}
        self.trips = {t["trip_id"]: t for t in trips}

    def search_stops(self, name_query: str, limit: int = 10) -> List[Dict]:
        q = name_query.strip().lower()
        if not q:
            return []
        items = []
        for s in self.stops:
            name = s.get("stop_name","")
            if q in name.lower():
                items.append({
                    "stop_id": s["stop_id"],
                    "display_name": name
                })
                if len(items) >= limit:
                    break
        return items

    def scheduled_departures(self, stop_id: str, minutes: int = 60, limit: int = 30) -> List[Dict]:
        """Menetrendi indulások adott megállóból a következő X percre."""
        if not self.stop_times or not self.trips:
            return []

        now = datetime.now()
        now_minutes = now.hour*60 + now.minute
        horizon = now_minutes + minutes

        active_services = _today_service_ids(self.calendar, self.calendar_dates)
        out: List[Dict] = []

        for st in self.stop_times:
            if st["stop_id"] != stop_id:
                continue
            dep = _parse_hms(st.get("departure_time") or st.get("arrival_time"))
            if dep < 0:
                continue
            # 24h feletti időket kezeljük: csak az aznapi ablakban tartjuk meg
            if not (now_minutes <= dep <= horizon):
                continue

            trip = self.trips.get(st["trip_id"])
            if not trip or trip.get("service_id") not in active_services:
                continue

            route = self.routes.get(trip["route_id"], {})
            route_short = route.get("route_short_name") or route.get("route_id")
            headsign = trip.get("trip_headsign","")

            # ISO idő a mai napra
            hh = dep // 60
            mm = dep % 60
            dep_iso = now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(minutes=dep)

            out.append({
                "route": str(route_short),
                "destination": headsign,
                "time_iso": dep_iso.isoformat(),
                "is_live": False
            })

        out.sort(key=lambda x: x["time_iso"])
        return out[:limit]

# singleton
_gtfs: GTFS = None

def get_gtfs() -> GTFS:
    global _gtfs
    if _gtfs is None:
        _gtfs = GTFS()
        _gtfs.load()
    return _gtfs
