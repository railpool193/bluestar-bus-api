# main.py
import os
import csv
from typing import Dict, List, Any, Optional
from functools import lru_cache
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path
from xml.etree import ElementTree as ET

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates


# ------------------------------------------------------------
# Beállítások
# ------------------------------------------------------------
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
TZ = ZoneInfo("Europe/London")

# BODS (opcionális) – ha nincs beállítva, a live rész csendben kihagyásra kerül
BODS_DATASET_URL = os.getenv(
    "BODS_DATASET_URL",
    # Ha csak API kulcs van: a 7721-es feed Bluestar/Unilink (a példában így kaptad)
    # A kulcsot a környezeti változóban add meg: BODS_API_KEY
    "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={api_key}",
)
BODS_API_KEY = os.getenv("BODS_API_KEY", "").strip()

# ------------------------------------------------------------
# FastAPI, statikus és templét motor
# ------------------------------------------------------------
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


# ------------------------------------------------------------
# GTFS betöltés
# ------------------------------------------------------------
def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _parse_hms_to_seconds(hms: str) -> Optional[int]:
    # Kezeli a 24+ órát is (pl. 25:10:00)
    if not hms:
        return None
    parts = hms.split(":")
    if len(parts) < 2:
        return None
    try:
        h = int(parts[0])
        m = int(parts[1])
        s = int(parts[2]) if len(parts) > 2 else 0
        return h * 3600 + m * 60 + s
    except Exception:
        return None


@lru_cache(maxsize=1)
def load_gtfs() -> Dict[str, Any]:
    base = Path(DATA_DIR)

    required = ["routes.txt", "stops.txt", "trips.txt", "stop_times.txt"]
    missing = [fn for fn in required if not (base / fn).exists()]
    if missing:
        raise RuntimeError(f"Hiányzó GTFS fájlok: {', '.join(missing)}")

    routes_rows = _read_csv(base / "routes.txt")
    stops_rows = _read_csv(base / "stops.txt")
    trips_rows = _read_csv(base / "trips.txt")
    st_rows = _read_csv(base / "stop_times.txt")

    routes: Dict[str, Dict[str, str]] = {}
    for r in routes_rows:
        rid = r.get("route_id")
        if not rid:
            continue
        routes[rid] = {
            "route_id": rid,
            "route_short_name": (r.get("route_short_name") or "").strip(),
            "route_long_name": (r.get("route_long_name") or "").strip(),
            "agency_id": (r.get("agency_id") or "").strip(),
        }

    stops: Dict[str, Dict[str, Any]] = {}
    for s in stops_rows:
        sid = s.get("stop_id")
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
        tid = t.get("trip_id")
        if not tid:
            continue
        trips[tid] = {
            "trip_id": tid,
            "route_id": t.get("route_id"),
            "service_id": t.get("service_id"),
            "trip_headsign": (t.get("trip_headsign") or "").strip(),
            "direction_id": t.get("direction_id"),
        }

    # stop_times indexek
    stop_times_by_stop: Dict[str, List[Dict[str, Any]]] = {}
    stop_times_by_trip: Dict[str, List[Dict[str, Any]]] = {}

    for st in st_rows:
        tid = st.get("trip_id")
        sid = st.get("stop_id")
        if not tid or not sid:
            continue

        dep = _parse_hms_to_seconds(st.get("departure_time") or st.get("arrival_time") or "")
        arr = _parse_hms_to_seconds(st.get("arrival_time") or st.get("departure_time") or "")
        seq = int(st.get("stop_sequence") or 0)

        row = {
            "trip_id": tid,
            "stop_id": sid,
            "departure_s": dep,
            "arrival_s": arr,
            "stop_sequence": seq,
        }

        stop_times_by_stop.setdefault(sid, []).append(row)
        stop_times_by_trip.setdefault(tid, []).append(row)

    # sorrend biztosítása
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


# ------------------------------------------------------------
# Segédfüggvények
# ------------------------------------------------------------
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
    # GTFS-ben előfordulhat 24+ óra – ezt a mai naphoz képest kezeljük
    now_s = seconds_now_uk()
    delta = (s - now_s)
    if delta < 0:
        return None
    return delta // 60


def sort_routes_for_ui(routes: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    items = []
    for rid, r in routes.items():
        short = (r.get("route_short_name") or "").strip()
        if not short:
            # ha nincs short name, hagyjuk ki a főoldalról
            continue
        items.append({
            "route_id": rid,
            "route_short_name": short,
            "agency": r.get("agency_id") or "GoSouthCoast",
        })
    def keyfun(x):
        sn = x["route_short_name"]
        return (0, int(sn)) if sn.isdigit() else (1, sn)
    items.sort(key=keyfun)
    return items


# ------------------------------------------------------------
# Opcionális: BODS élőadat (SIRI-VM). Biztonságos – sosem dob kivételt kifelé.
# ------------------------------------------------------------
_live_cache: Dict[str, Any] = {"ts": datetime.fromtimestamp(0, tz=TZ), "data": []}

def fetch_live_positions() -> List[Dict[str, Any]]:
    """SIRI-VM VehicleActivity -> egyszerűsített lista.
    Visszaad: {"line": "1", "lat": 50.9, "lon": -1.4, "bearing": 90, "vehicle": "xxx"}...
    """
    # Ha nincs kulcs, nincs live.
    if not BODS_API_KEY:
        return []

    # 20 mp-es cache
    if (now_uk() - _live_cache["ts"]).total_seconds() < 20:
        return _live_cache["data"]

    url = BODS_DATASET_URL.format(api_key=BODS_API_KEY)
    try:
        try:
            import requests  # lazy import, hogy ne dőljön el, ha nincs telepítve
        except Exception:
            return []

        r = requests.get(url, timeout=10)
        if r.status_code != 200 or not r.text.strip():
            return []

        text = r.text
        # A feed gyakran XML SIRI-VM – próbáljuk XML-ként
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            # ha nem XML, nem próbálkozunk tovább
            return []

        ns = {"s": root.tag.split("}")[0].strip("{")}
        items: List[Dict[str, Any]] = []

        # SIRI-VM: .../VehicleMonitoringDelivery/VehicleActivity
        for va in root.findall(".//s:VehicleActivity", ns):
            mvj = va.find(".//s:MonitoredVehicleJourney", ns)
            if mvj is None:
                continue
            line = (mvj.findtext("s:PublishedLineName", default="", namespaces=ns)
                    or mvj.findtext("s:LineRef", default="", namespaces=ns)).strip()
            if not line:
                continue
            loc = mvj.find(".//s:VehicleLocation", ns)
            if loc is None:
                continue
            lat = loc.findtext("s:Latitude", default="", namespaces=ns)
            lon = loc.findtext("s:Longitude", default="", namespaces=ns)
            try:
                lat = float(lat); lon = float(lon)
            except Exception:
                continue

            bearing_txt = mvj.findtext("s:Bearing", default="", namespaces=ns)
            try:
                bearing = int(float(bearing_txt)) if bearing_txt else None
            except Exception:
                bearing = None

            veh = mvj.findtext("s:VehicleRef", default="", namespaces=ns) or ""
            items.append({
                "line": line.strip(), "lat": lat, "lon": lon, "bearing": bearing, "vehicle": veh
            })

        _live_cache["ts"] = now_uk()
        _live_cache["data"] = items
        return items
    except Exception:
        # sose dőljön el
        return []


# ------------------------------------------------------------
# Végpontok
# ------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    data = load_gtfs()
    routes = sort_routes_for_ui(data["routes"])
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "routes": routes,
            "now": now_uk(),
        },
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
        # Járat: route_short_name részleges (szám vagy szöveg)
        q_low = q.lower()
        for rid, r in routes.items():
            short = (r.get("route_short_name") or "")
            if q_low in short.lower():
                route_hits.append({
                    "route_id": rid,
                    "route_short_name": short,
                    "agency": r.get("agency_id") or "GoSouthCoast",
                })

        # Megálló név részleges
        for sid, s in stops.items():
            name = s.get("stop_name") or ""
            if q_low in name.lower():
                stop_hits.append({
                    "stop_id": sid,
                    "stop_name": name,
                })

    # rendezés
    def rkey(x): 
        sn = x["route_short_name"]
        return (0, int(sn)) if sn.isdigit() else (1, sn)
    route_hits.sort(key=rkey)
    stop_hits.sort(key=lambda x: x["stop_name"])

    return templates.TemplateResponse(
        "search.html",
        {
            "request": request,
            "q": q,
            "routes": route_hits,
            "stops": stop_hits,
            "now": now_uk(),
        },
    )


@app.get("/stop/{stop_id}", response_class=HTMLResponse)
def stop_view(request: Request, stop_id: str):
    data = load_gtfs()
    stops = data["stops"]
    if stop_id not in stops:
        raise HTTPException(404, "Stop not found")

    stop = stops[stop_id]
    stbs = data["stop_times_by_stop"].get(stop_id, [])
    trips = data["trips"]
    routes = data["routes"]

    now_s = seconds_now_uk()
    # csak a mostantól következő indulások (max 60 tétel)
    upcoming = []
    for row in stbs:
        dep = row["departure_s"]
        mins = minutes_from_now(dep)
        if mins is None:
            continue
        trip = trips.get(row["trip_id"])
        if not trip:
            continue
        route = routes.get(trip["route_id"], {})
        upcoming.append({
            "time_str": hhmm(dep),
            "minutes": mins,
            "trip_id": row["trip_id"],
            "headsign": trip.get("trip_headsign") or "",
            "route_short_name": (route.get("route_short_name") or route.get("route_id") or ""),
        })

    upcoming.sort(key=lambda x: (x["minutes"], x["time_str"]))
    upcoming = upcoming[:60]

    # kis térképhez (trip oldalon rajzolunk csak részletes útvonalat)
    return templates.TemplateResponse(
        "stop.html",
        {
            "request": request,
            "stop": stop,
            "departures": upcoming,
            "now": now_uk(),
        },
    )


@app.get("/r/{route_id}", response_class=HTMLResponse)
def route_view(request: Request, route_id: str):
    data = load_gtfs()
    r = data["routes"].get(route_id)
    if not r:
        raise HTTPException(404, "Route not found")

    live = fetch_live_positions()
    # A live listában a "line" a PublishedLineName – ezt összevetjük a route_short_name-mel
    short = (r.get("route_short_name") or "").strip()
    live_for_route = [v for v in live if v.get("line") == short]

    return templates.TemplateResponse(
        "route.html",  # ha nincs ilyen sablonod, nyugodtan hagyd így – csak a live üzenet jelenik meg
        {
            "request": request,
            "route": r,
            "live": live_for_route,
            "now": now_uk(),
        },
    )


@app.get("/t/{trip_id}", response_class=HTMLResponse)
def trip_view(request: Request, trip_id: str, start_stop_id: Optional[str] = None):
    data = load_gtfs()
    trips = data["trips"]
    stops = data["stops"]
    stop_times = data["stop_times_by_trip"].get(trip_id, [])

    trip = trips.get(trip_id)
    if not trip:
        raise HTTPException(404, "Trip not found")

    # pontok a térképre (megállók koordinátái sorrendben)
    points: List[Dict[str, float]] = []
    legs: List[Dict[str, Any]] = []
    for row in stop_times:
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

    # élő – route alapján
    live = []
    r = data["routes"].get(trip.get("route_id") or "", {})
    short = (r.get("route_short_name") or "").strip()
    if short:
        live_all = fetch_live_positions()
        live = [v for v in live_all if v.get("line") == short]

    return templates.TemplateResponse(
        "trip.html",
        {
            "request": request,
            "trip": trip,
            "legs": legs,
            "points": points,
            "live": live,  # üres, ha nincs live
            "now": now_uk(),
        },
    )


# ------------------------------------------------------------
# Diagnosztika
# ------------------------------------------------------------
@app.get("/c")
def check():
    base = Path(DATA_DIR)
    data = load_gtfs()
    return {
        "DATA_DIR": DATA_DIR,
        "routes.txt": (base / "routes.txt").exists(),
        "stops.txt": (base / "stops.txt").exists(),
        "trips.txt": (base / "trips.txt").exists(),
        "stop_times.txt": (base / "stop_times.txt").exists(),
        "routes_count": len(data["routes"]),
        "stops_count": len(data["stops"]),
    }


# ------------------------------------------------------------
# Fallback: 404 -> kezdőlapra
# ------------------------------------------------------------
@app.exception_handler(404)
async def nf_handler(request: Request, exc):
    # API hívásoknál maradjon JSON, böngészős GET-nél irány a /
    if request.headers.get("accept", "").startswith("application/json"):
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    return RedirectResponse(url="/")


# ------------------------------------------------------------
# Lokális futtatás
# ------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
