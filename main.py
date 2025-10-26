# main.py
import os
import csv
from typing import Dict, List, Any, Optional
from functools import lru_cache
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
from xml.etree import ElementTree as ET

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# --------------------------
# Beállítások
# --------------------------
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
TZ = ZoneInfo("Europe/London")

# BODS (opcionális)
BODS_DATASET_URL = os.getenv(
    "BODS_DATASET_URL",
    "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={api_key}",
)
BODS_API_KEY = os.getenv("BODS_API_KEY", "").strip()

# --------------------------
# App / templating
# --------------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# --------------------------
# GTFS olvasás
# --------------------------
def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def _parse_hms_to_seconds(hms: str) -> Optional[int]:
    if not hms:
        return None
    parts = hms.split(":")
    if len(parts) < 2:
        return None
    try:
        h = int(parts[0]); m = int(parts[1])
        s = int(parts[2]) if len(parts) > 2 else 0
        return h * 3600 + m * 60 + s
    except Exception:
        return None

def _route_line_code(route_id: str, short_name: str) -> str:
    """Megjelenítési és live-hoz használt 'vonal kód'.
    1) ha van route_short_name -> az
    2) különben route_id utolsó ':' utáni rész
    """
    s = (short_name or "").strip()
    if s:
        return s
    if ":" in route_id:
        tail = route_id.split(":")[-1].strip()
        if tail:
            return tail
    return route_id.strip()

@lru_cache(maxsize=1)
def load_gtfs() -> Dict[str, Any]:
    base = Path(DATA_DIR)
    required = ["routes.txt", "stops.txt", "trips.txt", "stop_times.txt"]
    miss = [fn for fn in required if not (base / fn).exists()]
    if miss:
        raise RuntimeError(f"Hiányzó GTFS fájlok: {', '.join(miss)}")

    routes_rows = _read_csv(base / "routes.txt")
    stops_rows = _read_csv(base / "stops.txt")
    trips_rows = _read_csv(base / "trips.txt")
    st_rows = _read_csv(base / "stop_times.txt")

    routes: Dict[str, Dict[str, Any]] = {}
    for r in routes_rows:
        rid = r.get("route_id") or ""
        if not rid:
            continue
        short = (r.get("route_short_name") or "").strip()
        line_code = _route_line_code(rid, short)
        routes[rid] = {
            "route_id": rid,
            "route_short_name": short,
            "route_long_name": (r.get("route_long_name") or "").strip(),
            "agency_id": (r.get("agency_id") or "").strip(),
            "line_code": line_code,      # <<< fontos
            "display_name": line_code,   # UI-hoz
        }

    stops: Dict[str, Dict[str, Any]] = {}
    for s in stops_rows:
        sid = s.get("stop_id") or ""
        if not sid:
            continue
        stops[sid] = {
            "stop_id": sid,
            "stop_name": (s.get("stop_name") or "").strip(),
            "stop_lat": float(s.get("stop_lat") or 0),
            "stop_lon": float(s.get("stop_lon") or 0),
        }

    trips: Dict[str, Dict[str, Any]] = {}
    for t in trips_rows:
        tid = t.get("trip_id") or ""
        if not tid:
            continue
        trips[tid] = {
            "trip_id": tid,
            "route_id": t.get("route_id"),
            "service_id": t.get("service_id"),
            "trip_headsign": (t.get("trip_headsign") or "").strip(),
            "direction_id": t.get("direction_id"),
        }

    stop_times_by_stop: Dict[str, List[Dict[str, Any]]] = {}
    stop_times_by_trip: Dict[str, List[Dict[str, Any]]] = {}
    for st in st_rows:
        tid = st.get("trip_id") or ""
        sid = st.get("stop_id") or ""
        if not tid or not sid:
            continue
        dep = _parse_hms_to_seconds(st.get("departure_time") or st.get("arrival_time") or "")
        arr = _parse_hms_to_seconds(st.get("arrival_time") or st.get("departure_time") or "")
        seq = int(st.get("stop_sequence") or 0)
        row = {"trip_id": tid, "stop_id": sid, "departure_s": dep, "arrival_s": arr, "stop_sequence": seq}
        stop_times_by_stop.setdefault(sid, []).append(row)
        stop_times_by_trip.setdefault(tid, []).append(row)

    for sid, lst in stop_times_by_stop.items():
        lst.sort(key=lambda x: (x["departure_s"] if x["departure_s"] is not None else 10**9, x["stop_sequence"]))
    for tid, lst in stop_times_by_trip.items():
        lst.sort(key=lambda x: x["stop_sequence"])

    return {
        "routes": routes,
        "stops": stops,
        "trips": trips,
        "stop_times_by_stop": stop_times_by_stop,
        "stop_times_by_trip": stop_times_by_trip,
    }

# --------------------------
# Idő segédek
# --------------------------
def now_uk() -> datetime:
    return datetime.now(tz=TZ)

def seconds_now_uk() -> int:
    n = now_uk()
    return n.hour * 3600 + n.minute * 60 + n.second

def hhmm(s: Optional[int]) -> str:
    if s is None:
        return "--:--"
    s = s % 86400
    h = s // 3600
    m = (s % 3600) // 60
    return f"{h:02d}:{m:02d}"

def minutes_from_now(s: Optional[int]) -> Optional[int]:
    if s is None:
        return None
    delta = s - seconds_now_uk()
    if delta < 0:
        return None
    return delta // 60

# --------------------------
# Live (BODS) – SIRI-VM
# --------------------------
_live_cache: Dict[str, Any] = {"ts": datetime.fromtimestamp(0, tz=TZ), "data": [], "ok": False, "err": ""}

def _norm_line(s: str) -> str:
    # 'U1 C' -> 'U1C', ' 1 ' -> '1'
    return "".join(ch for ch in (s or "").upper() if ch.isalnum())

def fetch_live_positions() -> List[Dict[str, Any]]:
    if not BODS_API_KEY:
        _live_cache.update({"ok": False, "err": "no_api_key"})
        return []
    # 20 mp cache
    if (now_uk() - _live_cache["ts"]).total_seconds() < 20:
        return _live_cache["data"]

    url = BODS_DATASET_URL.format(api_key=BODS_API_KEY)
    try:
        try:
            import requests
        except Exception:
            _live_cache.update({"ok": False, "err": "no_requests"})
            return []

        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            _live_cache.update({"ok": False, "err": f"http_{r.status_code}"})
            return []

        text = r.text
        # próbáljuk XML-ként
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            _live_cache.update({"ok": False, "err": "parse"})
            return []

        ns = {"s": root.tag.split('}')[0].strip('{')}
        items: List[Dict[str, Any]] = []
        for va in root.findall(".//s:VehicleActivity", ns):
            mvj = va.find(".//s:MonitoredVehicleJourney", ns)
            if mvj is None:
                continue
            line = (mvj.findtext("s:PublishedLineName", default="", namespaces=ns)
                    or mvj.findtext("s:LineRef", default="", namespaces=ns) or "").strip()
            loc = mvj.find(".//s:VehicleLocation", ns)
            if not line or loc is None:
                continue
            try:
                lat = float(loc.findtext("s:Latitude", default="", namespaces=ns))
                lon = float(loc.findtext("s:Longitude", default="", namespaces=ns))
            except Exception:
                continue
            bearing_txt = mvj.findtext("s:Bearing", default="", namespaces=ns)
            try:
                bearing = int(float(bearing_txt)) if bearing_txt else None
            except Exception:
                bearing = None
            veh = (mvj.findtext("s:VehicleRef", default="", namespaces=ns) or "").strip()

            items.append({
                "line": line, "line_norm": _norm_line(line),
                "lat": lat, "lon": lon, "bearing": bearing, "vehicle": veh
            })

        _live_cache.update({"ts": now_uk(), "data": items, "ok": True, "err": ""})
        return items
    except Exception as e:
        _live_cache.update({"ok": False, "err": "exception"})
        return []

# --------------------------
# UI segédek
# --------------------------
def routes_for_home(routes: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for r in routes.values():
        items.append({
            "route_id": r["route_id"],
            "route_short_name": r.get("route_short_name") or "",
            "display": r["display_name"],     # ez kerül ki a csempére
            "agency": r.get("agency_id") or "GoSouthCoast",
        })
    def k(x):
        d = x["display"]
        return (0, int(d)) if d.isdigit() else (1, d)
    items.sort(key=k)
    return items

# --------------------------
# Endpontok
# --------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    data = load_gtfs()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "routes": routes_for_home(data["routes"]), "now": now_uk()},
    )

@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: str = ""):
    q = (q or "").strip()
    data = load_gtfs()
    routes = data["routes"]
    stops = data["stops"]

    route_hits: List[Dict[str, Any]] = []
    stop_hits: List[Dict[str, Any]] = []

    if q:
        ql = q.lower()
        for r in routes.values():
            if (ql in (r.get("route_short_name") or "").lower()
                or ql in (r.get("line_code") or "").lower()
                or ql in (r.get("route_id") or "").lower()):
                route_hits.append({
                    "route_id": r["route_id"],
                    "route_short_name": r.get("route_short_name") or "",
                    "display": r["display_name"],
                    "agency": r.get("agency_id") or "GoSouthCoast",
                })

        for s in stops.values():
            name = s.get("stop_name") or ""
            if ql in name.lower():
                stop_hits.append({"stop_id": s["stop_id"], "stop_name": name})

    def rkey(x):
        d = x["display"]
        return (0, int(d)) if d.isdigit() else (1, d)
    route_hits.sort(key=rkey)
    stop_hits.sort(key=lambda x: x["stop_name"])

    return templates.TemplateResponse(
        "search.html",
        {"request": request, "q": q, "routes": route_hits, "stops": stop_hits, "now": now_uk()},
    )

@app.get("/stop/{stop_id}", response_class=HTMLResponse)
def stop_view(request: Request, stop_id: str):
    data = load_gtfs()
    stop = data["stops"].get(stop_id)
    if not stop:
        raise HTTPException(404, "Stop not found")

    stbs = data["stop_times_by_stop"].get(stop_id, [])
    trips = data["trips"]; routes = data["routes"]

    upcoming = []
    for row in stbs:
        dep = row["departure_s"]
        mins = minutes_from_now(dep)
        if mins is None:
            continue
        trip = trips.get(row["trip_id"]); 
        if not trip:
            continue
        route = routes.get(trip["route_id"], {})
        upcoming.append({
            "time_str": hhmm(dep),
            "minutes": mins,
            "trip_id": row["trip_id"],
            "headsign": trip.get("trip_headsign") or "",
            "route_display": route.get("display_name") or (route.get("route_id") or ""),
        })

    upcoming.sort(key=lambda x: (x["minutes"], x["time_str"]))
    upcoming = upcoming[:60]

    return templates.TemplateResponse(
        "stop.html",
        {"request": request, "stop": stop, "departures": upcoming, "now": now_uk()},
    )

@app.get("/r/{route_id}", response_class=HTMLResponse)
def route_view(request: Request, route_id: str):
    data = load_gtfs()
    r = data["routes"].get(route_id)
    if not r:
        raise HTTPException(404, "Route not found")

    live = fetch_live_positions()
    target = _norm_line(r.get("line_code") or r.get("route_short_name") or r["route_id"])
    live_for_route = [v for v in live if v.get("line_norm") == target]

    return templates.TemplateResponse(
        "route.html",
        {"request": request, "route": r, "live": live_for_route, "now": now_uk()},
    )

@app.get("/t/{trip_id}", response_class=HTMLResponse)
def trip_view(request: Request, trip_id: str):
    data = load_gtfs()
    trip = data["trips"].get(trip_id)
    if not trip:
        raise HTTPException(404, "Trip not found")

    stops = data["stops"]
    rows = data["stop_times_by_trip"].get(trip_id, [])

    legs = []
    points = []
    for row in rows:
        sid = row["stop_id"]
        st = stops.get(sid)
        if not st:
            continue
        dep = row["departure_s"]
        legs.append({
            "stop_id": sid,
            "stop_name": st["stop_name"],
            "time_str": hhmm(dep),
            "minutes": minutes_from_now(dep),
        })
        points.append({"lat": st["stop_lat"], "lon": st["stop_lon"]})

    # live a route line_code alapján
    route = data["routes"].get(trip.get("route_id") or "", {})
    target = _norm_line(route.get("line_code") or route.get("route_short_name") or route.get("route_id") or "")
    live = []
    if target:
        live_all = fetch_live_positions()
        live = [v for v in live_all if v.get("line_norm") == target]

    return templates.TemplateResponse(
        "trip.html",
        {"request": request, "trip": trip, "legs": legs, "points": points, "live": live, "now": now_uk()},
    )

# --------------------------
# Diagnosztika
# --------------------------
@app.get("/c")
def check():
    base = Path(DATA_DIR)
    data = load_gtfs()

    # requests teszt
    try:
        import requests  # noqa
        has_requests = True
    except Exception:
        has_requests = False

    return {
        "DATA_DIR": DATA_DIR,
        "routes.txt": (base / "routes.txt").exists(),
        "stops.txt": (base / "stops.txt").exists(),
        "trips.txt": (base / "trips.txt").exists(),
        "stop_times.txt": (base / "stop_times.txt").exists(),
        "routes_count": len(data["routes"]),
        "stops_count": len(data["stops"]),
        "live_enabled": bool(BODS_API_KEY),
        "requests_available": has_requests,
        "live_cache_ok": _live_cache.get("ok"),
        "live_cache_err": _live_cache.get("err"),
    }

# --------------------------
# 404 -> kezdőlap (böngésző)
# --------------------------
@app.exception_handler(404)
async def nf_handler(request: Request, exc):
    if request.headers.get("accept", "").startswith("application/json"):
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    return RedirectResponse(url="/")

# --------------------------
# Lokális futtatás
# --------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
