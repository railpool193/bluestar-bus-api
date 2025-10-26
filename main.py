# main.py
import csv
import io
import json
import os
import re
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape

# ---------- Beállítások ----------
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
BODS_FEED_URL = os.getenv(
    "BODS_FEED_URL",
    "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key=9d2f6818e2723996467fedb958ba682aa9860a93",
)
TZ = os.getenv("TZ", "Europe/London")

# ---------- Segéd függvények ----------
def now_uk() -> datetime:
    # egyszerű, tz-naív, de UK időt (GMT/BST) a DfT feedhez elég
    # Railway konténerben nincs mindig zoneinfo, ezért a dst-t nem erőltetjük.
    # A sablonokban csak az óra:perc jelenik meg.
    return datetime.utcnow() + (datetime.now() - datetime.utcnow())

def hm_str(sec: int) -> str:
    # sec -> HH:MM (24h+, GTFS-ben lehet 26:xx is)
    h = (sec // 3600) % 24
    m = (sec % 3600) // 60
    return f"{h:02d}:{m:02d}"

def gtfs_time_to_sec(t: str) -> int:
    # 'HH:MM:SS' -> seconds (enged 24+ órát is)
    try:
        h, m, s = [int(x) for x in t.split(":")]
        return h * 3600 + m * 60 + s
    except Exception:
        return 0

def read_csv(path: Path) -> List[Dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

# ---------- Adat betöltés GTFS ----------
class GTFS:
    def __init__(self, root: str):
        rootp = Path(root)
        if not rootp.exists():
            raise RuntimeError(f"GTFS mappa nem található: {root}")

        self.routes = read_csv(rootp / "routes.txt")
        self.stops = read_csv(rootp / "stops.txt")
        self.trips = read_csv(rootp / "trips.txt")
        self.stop_times = read_csv(rootp / "stop_times.txt")
        self.shapes = []
        if (rootp / "shapes.txt").exists():
            self.shapes = read_csv(rootp / "shapes.txt")

        # indexek
        self.routes_by_id: Dict[str, Dict] = {r["route_id"]: r for r in self.routes}
        self.routes_by_short: Dict[str, List[Dict]] = defaultdict(list)
        for r in self.routes:
            self.routes_by_short[normalize(r.get("route_short_name", ""))].append(r)

        self.stops_by_id: Dict[str, Dict] = {s["stop_id"]: s for s in self.stops}

        self.trips_by_id: Dict[str, Dict] = {t["trip_id"]: t for t in self.trips}
        self.stop_times_by_stop: Dict[str, List[Dict]] = defaultdict(list)
        for st in self.stop_times:
            self.stop_times_by_stop[st["stop_id"]].append(st)
        for k in self.stop_times_by_stop:
            self.stop_times_by_stop[k].sort(key=lambda x: (int(x["stop_sequence"]), gtfs_time_to_sec(x["departure_time"])))

        self.stop_times_by_trip: Dict[str, List[Dict]] = defaultdict(list)
        for st in self.stop_times:
            self.stop_times_by_trip[st["trip_id"]].append(st)
        for k in self.stop_times_by_trip:
            self.stop_times_by_trip[k].sort(key=lambda x: int(x["stop_sequence"]))

        self.shapes_by_id: Dict[str, List[Tuple[float, float, int]]] = defaultdict(list)
        for sh in self.shapes:
            try:
                self.shapes_by_id[sh["shape_id"]].append(
                    (float(sh["shape_pt_lat"]), float(sh["shape_pt_lon"]), int(sh["shape_pt_sequence"]))
                )
            except Exception:
                pass
        for k in self.shapes_by_id:
            self.shapes_by_id[k].sort(key=lambda x: x[2])

        # csak Bluestar / Unilink route-ok (route_id prefix alapján)
        self.allowed_route_ids = {
            r["route_id"] for r in self.routes if r["route_id"].startswith(("BLUS:", "UNIL:"))
        }
        self.allowed_short_names = {
            normalize(r.get("route_short_name", "")) for r in self.routes if r["route_id"] in self.allowed_route_ids
        }

    # kereső
    def search(self, q: str) -> Tuple[List[Dict], List[Dict]]:
        Q = normalize(q).casefold()
        if not Q:
            return [], []
        routes = []
        for r in self.routes:
            if r["route_id"] not in self.allowed_route_ids:
                continue
            short = normalize(r.get("route_short_name", ""))
            longn = normalize(r.get("route_long_name", ""))
            if Q in short.casefold() or Q in longn.casefold():
                routes.append({
                    "route_id": r["route_id"],
                    "short": short,
                    "agency": normalize(r.get("agency_id", "")) or "GoSouthCoast",
                })
        routes = sorted(routes, key=lambda x: (len(x["short"]) if x["short"] else 999, x["short"]))[:100]

        stops = []
        for s in self.stops:
            name = normalize(s.get("stop_name", ""))
            if Q in name.casefold():
                stops.append({"stop_id": s["stop_id"], "name": name})
        stops = stops[:100]
        return routes, stops

    # megálló indulások mostantól
    def departures_from_now(self, stop_id: str, limit: int = 80) -> List[Dict]:
        now = now_uk()
        sec_now = now.hour * 3600 + now.minute * 60 + now.second

        out = []
        for st in self.stop_times_by_stop.get(stop_id, []):
            dep_s = gtfs_time_to_sec(st["departure_time"])
            if dep_s < sec_now:  # csak mostantól
                continue
            trip = self.trips_by_id.get(st["trip_id"])
            if not trip:
                continue
            route = self.routes_by_id.get(trip["route_id"])
            if not route or route["route_id"] not in self.allowed_route_ids:
                continue
            out.append({
                "trip_id": trip["trip_id"],
                "time": hm_str(dep_s),
                "route_id": route["route_id"],
                "route_short_name": normalize(route.get("route_short_name", "")),
                "headsign": normalize(trip.get("trip_headsign") or st.get("stop_headsign") or ""),
            })
            if len(out) >= limit:
                break
        return out

    # trip részletek
    def trip_detail(self, trip_id: str) -> Dict:
        trip = self.trips_by_id.get(trip_id)
        if not trip:
            raise HTTPException(404, "Trip nem található")
        route = self.routes_by_id.get(trip["route_id"])
        if not route or route["route_id"] not in self.allowed_route_ids:
            raise HTTPException(404, "Ez nem Bluestar/Unilink trip")

        sts = self.stop_times_by_trip.get(trip_id, [])
        stop_markers = []
        polyline: List[Tuple[float, float]] = []

        # próbáljuk shape-ből, ha nincs, akkor megállókból rajzolunk
        if trip.get("shape_id") and trip["shape_id"] in self.shapes_by_id:
            polyline = [(lat, lon) for (lat, lon, _seq) in self.shapes_by_id[trip["shape_id"]]]
        else:
            for st in sts:
                stp = self.stops_by_id.get(st["stop_id"])
                if stp:
                    polyline.append((float(stp["stop_lat"]), float(stp["stop_lon"])))

        for st in sts:
            stp = self.stops_by_id.get(st["stop_id"])
            if not stp:
                continue
            stop_markers.append({
                "name": normalize(stp.get("stop_name", "")),
                "lat": float(stp["stop_lat"]),
                "lon": float(stp["stop_lon"]),
                "time": hm_str(gtfs_time_to_sec(st["departure_time"])),
            })

        return {
            "trip": trip,
            "route": route,
            "stop_markers": stop_markers,
            "shape_coords": polyline,
        }

# ---------- Live járművek (BODS feed) ----------
_live_cache = {"ts": 0.0, "vehicles": []}

def fetch_bods_live() -> List[Dict]:
    # egyszerű 15 mp-es cache
    if time.time() - _live_cache["ts"] < 15 and _live_cache["vehicles"]:
        return _live_cache["vehicles"]

    try:
        r = requests.get(BODS_FEED_URL, timeout=20)
        r.raise_for_status()
        content = r.content
        vehicles = []

        def parse_siri_json(obj):
            # VehicleMonitoringDelivery -> VehicleActivity -> MonitoredVehicleJourney
            try:
                deliveries = obj.get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
                if isinstance(deliveries, dict):
                    deliveries = [deliveries]
                for d in deliveries:
                    for va in d.get("VehicleActivity", []) or []:
                        mvj = va.get("MonitoredVehicleJourney", {}) or {}
                        loc = mvj.get("VehicleLocation") or {}
                        line = normalize(str(mvj.get("PublishedLineName") or mvj.get("LineRef") or ""))
                        vehicles.append({
                            "line": line,
                            "lat": float(loc.get("Latitude")) if loc else None,
                            "lon": float(loc.get("Longitude")) if loc else None,
                            "dest": normalize(mvj.get("DestinationName") or ""),
                            "origin": normalize(mvj.get("OriginName") or ""),
                            "operator": normalize(mvj.get("OperatorRef") or ""),
                        })
            except Exception:
                pass

        # ZIP vagy közvetlen JSON
        if r.headers.get("Content-Type", "").startswith("application/zip") or content[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    if not name.lower().endswith(".json"):
                        continue
                    try:
                        data = json.loads(zf.read(name))
                        parse_siri_json(data)
                    except Exception:
                        continue
        else:
            data = r.json()
            parse_siri_json(data)

        _live_cache["ts"] = time.time()
        _live_cache["vehicles"] = vehicles
        return vehicles
    except Exception:
        # halkan hibázunk – üres lista
        return []

def filter_live_for_routes(vehicles: List[Dict], allowed_shorts: set) -> List[Dict]:
    out = []
    for v in vehicles:
        if not v.get("line"):
            continue
        if normalize(v["line"]) in allowed_shorts:
            out.append(v)
    return out

# ---------- FastAPI + Jinja ----------
app = FastAPI()
templates = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape(["html", "xml"])
)
app.mount("/static", StaticFiles(directory="static"), name="static")

GTFS_DB = GTFS(DATA_DIR)

def render(request: Request, name: str, ctx: dict) -> HTMLResponse:
    tmpl = templates.get_template(name)
    ctx2 = {"request": request, "now": now_uk(), "brand": "bluestar"}
    ctx2.update(ctx)
    return HTMLResponse(tmpl.render(**ctx2))

# ---------- Routes ----------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    # összes Bluestar/Unilink route kártyákban
    cards = []
    for r in GTFS_DB.routes:
        if r["route_id"] not in GTFS_DB.allowed_route_ids:
            continue
        cards.append({
            "route_id": r["route_id"],
            "short": normalize(r.get("route_short_name", "")),
            "agency": normalize(r.get("agency_id") or "GoSouthCoast"),
        })
    cards.sort(key=lambda x: (len(x["short"]) if x["short"] else 999, x["short"]))
    return render(request, "index.html", {"routes": cards})

@app.get("/s", response_class=HTMLResponse)
def search(request: Request, q: str = Query("")):
    routes, stops = GTFS_DB.search(q)
    return render(request, "search.html", {"q": q, "routes": routes, "stops": stops})

@app.get("/stop/{stop_id}", response_class=HTMLResponse)
def stop_view(request: Request, stop_id: str):
    stop = GTFS_DB.stops_by_id.get(stop_id)
    if not stop:
        raise HTTPException(404, "Megálló nem található")
    deps = GTFS_DB.departures_from_now(stop_id, limit=150)
    return render(request, "stop.html", {
        "stop": stop,
        "departures": deps,
    })

@app.get("/route/{route_id}", response_class=HTMLResponse)
def route_view(request: Request, route_id: str):
    route = GTFS_DB.routes_by_id.get(route_id)
    if not route or route["route_id"] not in GTFS_DB.allowed_route_ids:
        raise HTTPException(404, "Útvonal nem található")
    # élő járművek ehhez a short name-hez
    live = filter_live_for_routes(fetch_bods_live(), GTFS_DB.allowed_short_names)
    live_on_this = [v for v in live if normalize(v["line"]) == normalize(route.get("route_short_name", ""))]
    return render(request, "route.html", {
        "route": route,
        "live": live_on_this,
    })

@app.get("/trip/{trip_id}", response_class=HTMLResponse)
def trip_view(request: Request, trip_id: str):
    detail = GTFS_DB.trip_detail(trip_id)
    short = normalize(detail["route"].get("route_short_name", ""))
    vehicles = filter_live_for_routes(fetch_bods_live(), {short})
    vehicle = vehicles[0] if vehicles else None
    return render(request, "trip.html", {
        "trip": detail["trip"],
        "route": detail["route"],
        "stop_markers": detail["stop_markers"],
        "shape_coords": detail["shape_coords"],
        "vehicle": vehicle,
    })

# debug / egészségi állapot
@app.get("/cache")
def cache_status():
    return JSONResponse({
        "DATA_DIR": DATA_DIR,
        "routes.txt": bool(GTFS_DB.routes),
        "stops.txt": bool(GTFS_DB.stops),
        "trips.txt": bool(GTFS_DB.trips),
        "stop_times.txt": bool(GTFS_DB.stop_times),
        "routes_count": len(GTFS_DB.routes),
        "stops_count": len(GTFS_DB.stops),
    })

@app.get("/live.json")
def live_json():
    v = filter_live_for_routes(fetch_bods_live(), GTFS_DB.allowed_short_names)
    return JSONResponse({"count": len(v), "vehicles": v})

# ------------ helyi futtatás ------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
