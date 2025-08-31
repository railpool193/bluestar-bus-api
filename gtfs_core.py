# gtfs_core.py
import csv, io, zipfile, datetime as dt
from typing import Dict, List, Set, Tuple, Optional

# ----- Helper: read GTFS zip from bytes -> dict[file] = list[dict] -----
def read_gtfs_zip(buf: bytes) -> Dict[str, List[Dict[str, str]]]:
    z = zipfile.ZipFile(io.BytesIO(buf))
    out: Dict[str, List[Dict[str, str]]] = {}
    for name in z.namelist():
        if not name.endswith(".txt"): 
            continue
        with z.open(name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
            reader = csv.DictReader(text)
            out[name.split("/")[-1]] = [row for row in reader]
    return out

# ----- Calendar: which service_ids are active on a given date? -----
def active_service_ids(gtfs: Dict[str, List[Dict[str, str]]], date: dt.date) -> Set[str]:
    ymd = int(date.strftime("%Y%m%d"))
    svc: Set[str] = set()
    # calendar.txt
    for r in gtfs.get("calendar.txt", []):
        s = int(r["start_date"]); e = int(r["end_date"])
        if not (s <= ymd <= e): 
            continue
        wd = date.weekday()  # Mon=0
        keys = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"]
        if r[keys[wd]] == "1":
            svc.add(r["service_id"])
    # calendar_dates.txt overrides
    for r in gtfs.get("calendar_dates.txt", []):
        if int(r["date"]) != ymd: 
            continue
        if r["exception_type"] == "1":
            svc.add(r["service_id"])
        elif r["exception_type"] == "2":
            svc.discard(r["service_id"])
    return svc

# ----- Indexek indulásokhoz -----
class GtfsIndex:
    def __init__(self, gtfs: Dict[str, List[Dict[str, str]]]):
        self.gtfs = gtfs
        self.stops = {r["stop_id"]: r for r in gtfs.get("stops.txt", [])}
        self.routes = {r["route_id"]: r for r in gtfs.get("routes.txt", [])}
        self.trips = {r["trip_id"]: r for r in gtfs.get("trips.txt", [])}
        # stop_id -> list[ (trip_id, arr, dep, stop_sequence) ]
        self.stop_times: Dict[str, List[Tuple[str,str,str,int]]] = {}
        for r in gtfs.get("stop_times.txt", []):
            self.stop_times.setdefault(r["stop_id"], []).append((
                r["trip_id"], r.get("arrival_time",""), r.get("departure_time",""),
                int(r.get("stop_sequence","0"))
            ))
        for v in self.stop_times.values():
            v.sort(key=lambda x: (x[1], x[2], x[3]))  # időrend
        # trip -> route_short_name
        self.trip_route_short: Dict[str, str] = {}
        for tid, t in self.trips.items():
            rid = t["route_id"]
            self.trip_route_short[tid] = self.routes.get(rid, {}).get("route_short_name","")

    # case-insensitive contains kereső stop_name + code + atco
    def search_stops(self, q: str, limit: int = 30) -> List[Dict[str, str]]:
        if not q: 
            return []
        qn = q.strip().lower()
        res = []
        for s in self.stops.values():
            name = (s.get("stop_name","") + " " + s.get("stop_code","") + " " + s.get("stop_id","")).lower()
            if qn in name:
                res.append({
                    "stop_id": s["stop_id"],
                    "stop_name": s.get("stop_name",""),
                    "stop_code": s.get("stop_code",""),
                    "lat": s.get("stop_lat",""),
                    "lon": s.get("stop_lon",""),
                })
                if len(res) >= limit: 
                    break
        return res

    # timetable departures for a stop filtered by active calendar
    def departures_for_stop(self, stop_id: str, when: dt.datetime, max_rows: int = 30) -> List[Dict[str, str]]:
        if stop_id not in self.stop_times: 
            return []
        act = active_service_ids(self.gtfs, when.date())
        rows = []
        hhmmss_now = when.strftime("%H:%M:%S")
        for (trip_id, arr, dep, seq) in self.stop_times[stop_id]:
            t = self.trips.get(trip_id)
            if not t or t["service_id"] not in act:
                continue
            time_str = dep or arr
            if not time_str:
                continue
            # csak a mostantól jövő indulások
            if time_str < hhmmss_now:
                continue
            rows.append({
                "time": time_str,
                "trip_id": trip_id,
                "route": self.trip_route_short.get(trip_id,""),
            })
            if len(rows) >= max_rows:
                break
        return rows
