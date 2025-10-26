# main.py
import os, csv
from pathlib import Path
from functools import lru_cache
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
from zoneinfo import ZoneInfo
from xml.etree import ElementTree as ET

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ---------- Beállítások ----------
DATA_DIR = os.getenv("DATA_DIR", "gtfs")
TZ = ZoneInfo("Europe/London")

BODS_API_KEY = (os.getenv("BODS_API_KEY") or "").strip()
BODS_DATASET_URL = os.getenv(
    "BODS_DATASET_URL",
    "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key={api_key}",
)

# ---------- App ----------
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ---------- Segédek ----------
def now_uk() -> datetime:
    return datetime.now(tz=TZ)

def _read_csv(p: Path) -> List[Dict[str, str]]:
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def _parse_hms_to_s(hms: str) -> Optional[int]:
    if not hms:
        return None
    try:
        parts = [int(x) for x in hms.split(":")]
        while len(parts) < 3:
            parts.append(0)
        h, m, s = parts[:3]
        return h * 3600 + m * 60 + s
    except Exception:
        return None

def _route_line_code(route_id: str, short_name: str) -> str:
    short = (short_name or "").strip()
    if short:
        return short
    if ":" in route_id:
        tail = route_id.split(":")[-1].strip()
        if tail:
            return tail
    return route_id.strip()

def _norm(s: str) -> str:
    # alfanumerikus, nagybetűs: "U1 C" -> "U1C", "BLUS:HAA0002:2" -> "BLUSHAA00022"
    return "".join(ch for ch in (s or "").upper() if ch.isalnum())

def _tokens_from_value(val: str) -> Set[str]:
    toks: Set[str] = set()
    if not val:
        return toks
    toks.add(_norm(val))
    if ":" in val:
        toks.add(_norm(val.split(":")[-1]))
    return {t for t in toks if t}

# ---------- GTFS betöltés ----------
@lru_cache(maxsize=1)
def load_gtfs() -> Dict[str, Any]:
    base = Path(DATA_DIR)
    req = ["routes.txt", "stops.txt", "trips.txt", "stop_times.txt"]
    miss = [x for x in req if not (base / x).exists()]
    if miss:
        raise RuntimeError(f"Missing GTFS: {', '.join(miss)}")

    routes_rows = _read_csv(base / "routes.txt")
    stops_rows = _read_csv(base / "stops.txt")
    trips_rows = _read_csv(base / "trips.txt")
    st_rows = _read_csv(base / "stop_times.txt")

    routes: Dict[str, Dict[str, Any]] = {}
    for r in routes_rows:
        rid = r.get("route_id") or ""
        short = (r.get("route_short_name") or "").strip()
        line_code = _route_line_code(rid, short)
        routes[rid] = {
            "route_id": rid,
            "route_short_name": short,
            "route_long_name": (r.get("route_long_name") or "").strip(),
            "agency_id": (r.get("agency_id") or "").strip() or "BLUS",
            "line_code": line_code,
            "display_name": line_code,
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

    st_by_stop: Dict[str, List[Dict[str, Any]]] = {}
    st_by_trip: Dict[str, List[Dict[str, Any]]] = {}
    for st in st_rows:
        tid = st.get("trip_id") or ""
        sid = st.get("stop_id") or ""
        if not tid or not sid:
            continue
        dep = _parse_hms_to_s(st.get("departure_time") or st.get("arrival_time") or "")
        arr = _parse_hms_to_s(st.get("arrival_time") or st.get("departure_time") or "")
        seq = int(st.get("stop_sequence") or 0)
        row = {"trip_id": tid, "stop_id": sid, "departure_s": dep, "arrival_s": arr, "stop_sequence": seq}
        st_by_stop.setdefault(sid, []).append(row)
        st_by_trip.setdefault(tid, []).append(row)

    for lst in st_by_stop.values():
        lst.sort(key=lambda x: (x["departure_s"] if x["departure_s"] is not None else 10**9, x["stop_sequence"]))
    for lst in st_by_trip.values():
        lst.sort(key=lambda x: x["stop_sequence"])

    return {
        "routes": routes,
        "stops": stops,
        "trips": trips,
        "stop_times_by_stop": st_by_stop,
        "stop_times_by_trip": st_by_trip,
    }

# ---------- Idő formázás ----------
def seconds_now() -> int:
    n = now_uk()
    return n.hour * 3600 + n.minute * 60 + n.second

def hhmm(sec: Optional[int]) -> str:
    if sec is None:
        return "--:--"
    sec %= 86400
    h, m = sec // 3600, (sec % 3600) // 60
    return f"{h:02d}:{m:02d}"

def mins_from_now(sec: Optional[int]) -> Optional[int]:
    if sec is None: return None
    d = sec - seconds_now()
    if d < 0: return None
    return d // 60

# ---------- BODS live ----------
_live_cache: Dict[str, Any] = {"ts": datetime.fromtimestamp(0, tz=TZ), "data": [], "ok": False, "err": ""}

def fetch_live() -> List[Dict[str, Any]]:
    if not BODS_API_KEY:
        _live_cache.update({"ok": False, "err": "no_api_key"})
        return []
    if (now_uk() - _live_cache["ts"]).total_seconds() < 20:
        return _live_cache["data"]

    url = BODS_DATASET_URL.format(api_key=BODS_API_KEY)
    try:
        import requests  # type: ignore
    except Exception:
        _live_cache.update({"ok": False, "err": "no_requests"})
        return []

    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            _live_cache.update({"ok": False, "err": f"http_{r.status_code}"})
            return []
        txt = r.text
        try:
            root = ET.fromstring(txt)
        except ET.ParseError:
            _live_cache.update({"ok": False, "err": "parse"})
            return []

        ns = {"s": root.tag.split("}")[0].strip("{")}
        out: List[Dict[str, Any]] = []
        for va in root.findall(".//s:VehicleActivity", ns):
            mvj = va.find(".//s:MonitoredVehicleJourney", ns)
            if mvj is None:
                continue
            pub = (mvj.findtext("s:PublishedLineName", default="", namespaces=ns) or "").strip()
            lrf = (mvj.findtext("s:LineRef", default="", namespaces=ns) or "").strip()
            loc = mvj.find(".//s:VehicleLocation", ns)
            if not loc:
                continue
            try:
                lat = float(loc.findtext("s:Latitude", default="", namespaces=ns))
                lon = float(loc.findtext("s:Longitude", default="", namespaces=ns))
            except Exception:
                continue
            vehicle = (mvj.findtext("s:VehicleRef", default="", namespaces=ns) or "").strip()
            out.append({
                "published": pub,
                "lineref": lrf,
                "published_norm": _norm(pub),
                "lineref_norm": _norm(lrf),
                "lat": lat, "lon": lon, "vehicle": vehicle
            })

        _live_cache.update({"ts": now_uk(), "data": out, "ok": True, "err": ""})
        return out
    except Exception:
        _live_cache.update({"ok": False, "err": "exception"})
        return []

def _route_token_set(route: Dict[str, Any]) -> Set[str]:
    vals = [
        route.get("line_code") or "",
        route.get("route_short_name") or "",
        route.get("route_id") or "",
    ]
    toks: Set[str] = set()
    for v in vals:
        toks |= _tokens_from_value(v)
    return toks

def _veh_token_set(v: Dict[str, Any]) -> Set[str]:
    vals = [v.get("published") or "", v.get("lineref") or ""]
    toks: Set[str] = set()
    for x in vals:
        toks |= _tokens_from_value(x)
    return toks

def live_for_route(route: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = fetch_live()
    if not items:
        return []
    rtoks = _route_token_set(route)
    out = []
    for v in items:
        vtoks = _veh_token_set(v)
        if rtoks & vtoks:     # van metszet -> elfogadjuk
            out.append(v)
    return out

# ---------- Oldalak ----------
def _routes_for_home(routes: Dict[str, Dict[str, Any]]):
    items = [{"route_id": r["route_id"], "display": r["display_name"], "agency": r.get("agency_id") or "BLUS"}
             for r in routes.values()]
    def key(x):
        d = x["display"]
        return (0, int(d)) if d.isdigit() else (1, d)
    items.sort(key=key)
    return items

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    d = load_gtfs()
    return templates.TemplateResponse("index.html",
        {"request": request, "routes": _routes_for_home(d["routes"]), "now": now_uk()}
    )

@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: str = ""):
    d = load_gtfs()
    ql = (q or "").strip().lower()
    route_hits, stop_hits = [], []

    if ql:
        for r in d["routes"].values():
            if (ql in (r.get("route_short_name") or "").lower()
                or ql in (r.get("line_code") or "").lower()
                or ql in (r.get("route_id") or "").lower()):
                route_hits.append({"route_id": r["route_id"], "display": r["display_name"], "agency": r.get("agency_id")})
        for s in d["stops"].values():
            if ql in (s.get("stop_name") or "").lower():
                stop_hits.append({"stop_id": s["stop_id"], "stop_name": s["stop_name"]})

    route_hits.sort(key=lambda x: (0, int(x["display"])) if x["display"].isdigit() else (1, x["display"]))
    stop_hits.sort(key=lambda x: x["stop_name"])
    return templates.TemplateResponse("search.html",
        {"request": request, "q": q, "routes": route_hits, "stops": stop_hits, "now": now_uk()}
    )

@app.get("/stop/{stop_id}", response_class=HTMLResponse)
def stop_view(request: Request, stop_id: str):
    d = load_gtfs()
    stop = d["stops"].get(stop_id)
    if not stop:
        raise HTTPException(404, "Stop not found")
    lst = d["stop_times_by_stop"].get(stop_id, [])
    out = []
    for row in lst:
        dep = row["departure_s"]
        mins = mins_from_now(dep)
        if mins is None:
            continue
        trip = d["trips"].get(row["trip_id"])
        if not trip:
            continue
        route = d["routes"].get(trip["route_id"], {})
        out.append({
            "time_str": hhmm(dep),
            "minutes": mins,
            "trip_id": row["trip_id"],
            "headsign": trip.get("trip_headsign") or "",
            "route_display": route.get("display_name") or (route.get("route_id") or ""),
        })
    out.sort(key=lambda x: (x["minutes"], x["time_str"]))
    out = out[:60]
    return templates.TemplateResponse("stop.html",
        {"request": request, "stop": stop, "departures": out, "now": now_uk()}
    )

@app.get("/r/{route_id}", response_class=HTMLResponse)
def route_view(request: Request, route_id: str):
    d = load_gtfs()
    r = d["routes"].get(route_id)
    if not r:
        raise HTTPException(404, "Route not found")
    live = live_for_route(r)
    return templates.TemplateResponse("route.html",
        {"request": request, "route": r, "live": live, "now": now_uk()}
    )

@app.get("/t/{trip_id}", response_class=HTMLResponse)
def trip_view(request: Request, trip_id: str):
    d = load_gtfs()
    trip = d["trips"].get(trip_id)
    if not trip:
        raise HTTPException(404, "Trip not found")
    rows = d["stop_times_by_trip"].get(trip_id, [])
    stops = d["stops"]

    legs, points = [], []
    for row in rows:
        st = stops.get(row["stop_id"])
        if not st:
            continue
        legs.append({
            "stop_id": st["stop_id"],
            "stop_name": st["stop_name"],
            "time_str": hhmm(row["departure_s"]),
            "minutes": mins_from_now(row["departure_s"]),
        })
        points.append({"lat": st["stop_lat"], "lon": st["stop_lon"]})

    route = d["routes"].get(trip.get("route_id") or "", {})
    live = live_for_route(route) if route else []
    return templates.TemplateResponse("trip.html",
        {"request": request, "trip": trip, "legs": legs, "points": points, "live": live, "now": now_uk()}
    )

# ---------- Diagnosztika ----------
@app.get("/c")
def check():
    base = Path(DATA_DIR)
    d = load_gtfs()
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
        "routes_count": len(d["routes"]),
        "stops_count": len(d["stops"]),
        "live_enabled": bool(BODS_API_KEY),
        "requests_available": has_requests,
        "live_cache_ok": _live_cache.get("ok"),
        "live_cache_err": _live_cache.get("err"),
    }

# BODS nyers minták
@app.get("/live")
def live_debug():
    items = fetch_live()
    return {"count": len(items), "sample": items[:10]}

@app.get("/live/lines")
def live_lines():
    items = fetch_live()
    lines = {}
    for v in items:
        key = v.get("published") or v.get("lineref") or ""
        lines.setdefault(key, 0)
        lines[key] += 1
    # rendezve, hogy lássuk mi jön a feedből
    return [{"line": k, "count": v} for k, v in sorted(lines.items(), key=lambda x: (-x[1], x[0]))]

# ---------- 404 ----------
@app.exception_handler(404)
async def nf(request: Request, exc):
    if request.headers.get("accept", "").startswith("application/json"):
        return JSONResponse({"detail": "Not Found"}, status_code=404)
    return RedirectResponse(url="/")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
