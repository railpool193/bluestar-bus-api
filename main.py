# -- snip: ugyanazok az importok, mint korábban --
import os, io, json, csv, zipfile, time, math
from pathlib import Path
from typing import Dict, Any, List, Optional
from collections import defaultdict
from datetime import datetime, timezone

import httpx
import xmltodict
from fastapi import FastAPI, UploadFile, File, Query, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Bluestar Bus – API", version="3.2.0")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
DATA_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.middleware("http")
async def no_cache_mw(request, call_next):
    resp = await call_next(request)
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp

# ---------------- helpers ----------------
def _read_json(p: Path, default):
    if not p.exists(): return default
    return json.loads(p.read_text(encoding="utf-8"))

def _write_json(p: Path, data: Any):
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")

def gtfs_ok() -> bool:
    return (DATA_DIR / "stops.json").exists() and (DATA_DIR / "schedule.json").exists()

def _find_member(zf: zipfile.ZipFile, name: str) -> Optional[str]:
    ln = name.lower()
    for m in zf.namelist():
        if m.lower() == ln or m.lower().endswith("/"+ln):
            return m
    return None

def _hhmm_to_min(t: str) -> int:
    try:
        h, m, *_ = t.split(":")
        return int(h)*60 + int(m)
    except: return 0

def _parse_dt_z(dt: str) -> Optional[datetime]:
    if not dt: return None
    try:
        if dt.endswith("Z"): dt = dt[:-1] + "+00:00"
        return datetime.fromisoformat(dt)
    except: return None

# --------------- BODS LIVE adapter ---------------
class BODSAdapter:
    def __init__(self, default_url: Optional[str] = None):
        self.url = os.getenv("LIVE_BASE_URL") or default_url
        saved = _read_json(DATA_DIR / "live_source.json", {})
        if saved.get("base_url"): self.url = saved["base_url"]
        self.auth_header = os.getenv("LIVE_AUTH_HEADER") or saved.get("auth_header")

    async def is_available(self) -> bool:
        return bool(self.url)

    async def _fetch_feed(self) -> Dict[str, Any]:
        if not self.url: return {}
        headers = {}
        if self.auth_header: headers["Authorization"] = self.auth_header
        async with httpx.AsyncClient(timeout=20) as cli:
            r = await cli.get(self.url, headers=headers)
            r.raise_for_status()
            if "json" in (r.headers.get("content-type","")): return r.json()
            return xmltodict.parse(r.text)

    def _vehicle_activities(self, feed: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not feed: return []
        siri = feed.get("Siri") or feed.get("siri") or feed
        sd = siri.get("ServiceDelivery") or siri.get("serviceDelivery") or {}
        vmd = sd.get("VehicleMonitoringDelivery") or sd.get("vehicleMonitoringDelivery") or {}
        va = vmd.get("VehicleActivity") or vmd.get("vehicleActivity") or []
        if isinstance(va, dict): va = [va]
        out = []
        for it in va:
            mvj = it.get("MonitoredVehicleJourney") or it.get("monitoredVehicleJourney") or {}
            mc = mvj.get("MonitoredCall") or mvj.get("monitoredCall") or {}
            line = mvj.get("PublishedLineName") or mvj.get("LineRef") or mvj.get("lineRef") or ""
            headsign = mvj.get("DestinationName") or mvj.get("DestinationRef") or ""
            trip = mvj.get("DatedVehicleJourneyRef") or mvj.get("FramedVehicleJourneyRef") or ""
            vehicle = mvj.get("VehicleRef") or it.get("VehicleRef") or ""
            stop_ref = mc.get("StopPointRef") or mc.get("stopPointRef")
            aimed = mc.get("AimedDepartureTime") or mc.get("AimedArrivalTime")
            expected = mc.get("ExpectedDepartureTime") or mc.get("ExpectedArrivalTime")
            loc = mvj.get("VehicleLocation") or {}
            out.append({
                "route": str(line).strip(),
                "headsign": (headsign or "").strip(),
                "trip_id": str(trip or "").strip(),
                "vehicle_reg": (vehicle or "").strip(),
                "stop_ref": (stop_ref or "").strip(),
                "aimed": aimed or "",
                "expected": expected or "",
                "lat": loc.get("Latitude"), "lon": loc.get("Longitude"),
                "bearing": mvj.get("Bearing"),
            })
        return out

    async def stop_next_departures(self, stop_id: str, minutes: int) -> List[Dict[str, Any]]:
        feed = await self._fetch_feed()
        acts = self._vehicle_activities(feed)
        now = datetime.now(timezone.utc)
        items = []
        for a in acts:
            if a["stop_ref"] != stop_id: continue
            aimed = _parse_dt_z(a["aimed"])
            expected = _parse_dt_z(a["expected"]) or aimed
            if not aimed: continue
            eta = None; delay = None
            if expected:
                eta = max(0, math.floor((expected-now).total_seconds()/60))
                delay = math.floor(((expected-aimed).total_seconds())/60)
            if eta is not None and eta > minutes: continue
            sched_local = aimed.astimezone().strftime("%H:%M")
            items.append({
                "route": a["route"], "headsign": a["headsign"],
                "scheduled_time": sched_local,
                "eta_min": eta, "delay_min": delay,
                "vehicle_reg": a["vehicle_reg"], "trip_id": a["trip_id"]
            })
        return items

    async def vehicles_by_route(self, route_no: str) -> List[Dict[str, Any]]:
        feed = await self._fetch_feed()
        acts = self._vehicle_activities(feed)
        out = []
        for a in acts:
            if str(a["route"]) != str(route_no): continue
            if a.get("lat") and a.get("lon"):
                out.append({
                    "lat": float(a["lat"]), "lon": float(a["lon"]),
                    "bearing": a.get("bearing"),
                    "reg": a.get("vehicle_reg"),
                    "trip_id": a.get("trip_id")
                })
        return out

    async def trip_details(self, trip_id: str) -> Dict[str, Any]:
        feed = await self._fetch_feed()
        acts = self._vehicle_activities(feed)
        for a in acts:
            if a.get("trip_id") == trip_id:
                aimed = _parse_dt_z(a["aimed"])
                expected = _parse_dt_z(a["expected"]) or aimed
                now = datetime.now(timezone.utc)
                eta = max(0, math.floor((expected-now).total_seconds()/60)) if expected else None
                delay = math.floor(((expected-(aimed or expected)).total_seconds())/60) if expected else None
                sched_local = aimed.astimezone().strftime("%H:%M") if aimed else ""
                return {
                    "trip_id": trip_id, "route": a["route"], "headsign": a["headsign"],
                    "vehicle": {"reg": a.get("vehicle_reg")},
                    "calls": [{
                        "stop_id": a.get("stop_ref"), "stop_name": a.get("stop_ref"),
                        "time": sched_local, "eta_min": eta, "delay_min": delay
                    }]
                }
        return {}

DEFAULT_BODS = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key=9d2f6818e2723996467fedb958ba682aa9860a93"
siri_live = BODSAdapter(default_url=DEFAULT_BODS)

FLEET_MAP: Dict[str, Dict[str, str]] = _read_json(DATA_DIR / "fleet.json", {})
def enrich_vehicle(v: Dict[str, Any]) -> Dict[str, Any]:
    if not v: return v
    reg = (v.get("reg") or v.get("vehicle_reg") or "").upper()
    if reg in FLEET_MAP:
        meta = FLEET_MAP[reg]
        v.setdefault("type", meta.get("model") or meta.get("type"))
        v.setdefault("fleet_no", meta.get("fleet_no"))
    return v

# --------------- GTFS build ---------------
def _build_from_zip_bytes(zip_bytes: bytes) -> None:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        need = ["stops.txt","trips.txt","stop_times.txt","routes.txt"]
        mem = {n:_find_member(zf,n) for n in need}
        miss = [n for n,m in mem.items() if m is None]
        if miss: raise ValueError("Hiányzó GTFS fájlok: " + ", ".join(miss))

        routes = {}
        with zf.open(mem["routes.txt"]) as f:
            for r in csv.DictReader(io.TextIOWrapper(f,"utf-8-sig")):
                routes[r["route_id"]] = {
                    "short": (r.get("route_short_name") or "").strip(),
                    "long": (r.get("route_long_name") or "").strip()
                }

        trips = {}
        with zf.open(mem["trips.txt"]) as f:
            for r in csv.DictReader(io.TextIOWrapper(f,"utf-8-sig")):
                rr = routes.get(r["route_id"], {"short":"","long":""})
                trips[r["trip_id"]] = {
                    "route": rr["short"] or rr["long"],
                    "headsign": (r.get("trip_headsign") or "").strip()
                }

        stops = []
        with zf.open(mem["stops.txt"]) as f:
            for r in csv.DictReader(io.TextIOWrapper(f,"utf-8-sig")):
                stops.append({"stop_id":r["stop_id"],"stop_name":(r.get("stop_name") or "").strip(),"stop_code":(r.get("stop_code") or "").strip()})
        _write_json(DATA_DIR/"stops.json", stops)

        schedule = defaultdict(list)
        trip_stops = defaultdict(list)
        with zf.open(mem["stop_times.txt"]) as f:
            for r in csv.DictReader(io.TextIOWrapper(f,"utf-8-sig")):
                t = (r.get("departure_time") or "").strip()
                if not t: continue
                tid = r["trip_id"]; tr = trips.get(tid)
                if not tr: continue
                schedule[r["stop_id"]].append({"time":t,"route":tr["route"],"destination":tr["headsign"],"trip_id":tid})
                trip_stops[tid].append({"seq":int(r.get("stop_sequence") or 0),"stop_id":r["stop_id"],"time":t})
        for v in schedule.values(): v.sort(key=lambda x:x["time"])
        for v in trip_stops.values(): v.sort(key=lambda x:x["seq"])

        _write_json(DATA_DIR/"schedule.json", schedule)
        _write_json(DATA_DIR/"trip_stops.json", trip_stops)

# --------------- API ---------------
@app.get("/", include_in_schema=False, response_class=HTMLResponse)
async def root_html():
    return HTMLResponse((BASE_DIR/"index.html").read_text(encoding="utf-8"))

@app.get("/api/status")
async def api_status():
    return {"status":"ok","gtfs":gtfs_ok(),"live":await siri_live.is_available(),"build":str(int(time.time()))}

@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(400, "Kérlek GTFS ZIP fájlt tölts fel.")
    blob = await file.read()
    (DATA_DIR/"last_gtfs.zip").write_bytes(blob)
    try: _build_from_zip_bytes(blob)
    except Exception as e: raise HTTPException(400, f"GTFS feldolgozási hiba: {e}")
    return {"status":"uploaded"}

@app.get("/api/live/config")
async def get_live_config():
    return {"base_url": siri_live.url, "auth_header": bool(siri_live.auth_header)}

@app.post("/api/live/config")
async def set_live_config(payload: Dict[str,str]):
    base = (payload.get("base_url") or "").strip()
    auth = (payload.get("auth_header") or "").strip() or None
    if not base:
        siri_live.url=None; siri_live.auth_header=None; _write_json(DATA_DIR/"live_source.json",{})
        return {"status":"removed"}
    siri_live.url=base; siri_live.auth_header=auth
    _write_json(DATA_DIR/"live_source.json",{"base_url":base,"auth_header":auth})
    return {"status":"saved"}

@app.get("/api/stops/search")
async def api_stops_search(q: str = Query(..., min_length=1), limit: int = 20):
    stops = _read_json(DATA_DIR/"stops.json", [])
    ql = q.strip().lower()
    res = [s for s in stops if ql in (s.get("stop_name") or "").lower() or ql == (s.get("stop_code") or "").lower()]
    res.sort(key=lambda s:(len(s.get("stop_name","")), s.get("stop_name","")))
    return res[:limit]

@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, minutes: int = Query(60, ge=5, le=240), live: bool = True):
    schedule = _read_json(DATA_DIR/"schedule.json", {})
    base = schedule.get(stop_id, [])
    now = time.localtime(); now_min = now.tm_hour*60 + now.tm_min

    upcoming: List[Dict[str,Any]] = []
    for d in base:
        dep_min = _hhmm_to_min(d["time"]) % (24*60)
        in_min = (dep_min - now_min) % (24*60)
        if in_min <= minutes:
            upcoming.append({
                "route": d["route"], "destination": d["destination"], "time": d["time"],
                "trip_id": d.get("trip_id"), "eta_min": None, "delay_min": None,
                "vehicle_reg": None, "live": False
            })

    # LIVE merge – trip_id, exact time, ±2 perces tolerancia
    if live and await siri_live.is_available():
        try:
            live_items = await siri_live.stop_next_departures(stop_id, minutes)
            # indexek
            by_trip = { (L.get("trip_id") or "").strip(): L for L in live_items if L.get("trip_id") }
            by_exact = {}
            for L in live_items:
                key = (str(L.get("route","")), str(L.get("scheduled_time","")))
                by_exact.setdefault(key, []).append(L)

            def attach(it, L):
                it["eta_min"]   = L.get("eta_min")
                it["delay_min"] = L.get("delay_min")
                it["vehicle_reg"]=L.get("vehicle_reg")
                it["trip_id"]   = it["trip_id"] or L.get("trip_id")
                it["live"]      = True

            for it in upcoming:
                done = False
                # 1) trip id
                if it.get("trip_id") and it["trip_id"] in by_trip:
                    attach(it, by_trip[it["trip_id"]]); done = True
                # 2) (route,time) exact
                if not done:
                    Ls = by_exact.get((str(it["route"]), str(it["time"]))) or []
                    if Ls: attach(it, Ls[0]); done = True
                # 3) (route,time ±2p)
                if not done:
                    it_min = _hhmm_to_min(it["time"])
                    best = None; best_diff = 3
                    for L in live_items:
                        if str(L.get("route","")) != str(it["route"]): continue
                        tL = L.get("scheduled_time")
                        if not tL: continue
                        diff = abs(_hhmm_to_min(tL) - it_min)
                        if diff < best_diff:
                            best_diff = diff; best = L
                    if best is not None:
                        attach(it, best)

        except Exception:
            pass

    upcoming.sort(key=lambda x:(not x["live"], x["eta_min"] if x["eta_min"] is not None else 99999, x["time"]))
    return upcoming[:80]

@app.get("/api/trips/{trip_id}")
async def api_trip_details(trip_id: str):
    if await siri_live.is_available():
        try:
            live = await siri_live.trip_details(trip_id)
            if live:
                if "vehicle" in live: live["vehicle"] = enrich_vehicle(live["vehicle"])
                return live
        except Exception:
            pass
    trip_stops = _read_json(DATA_DIR/"trip_stops.json", {})
    stops_idx = { s["stop_id"]: s for s in _read_json(DATA_DIR/"stops.json", []) }
    seq = trip_stops.get(trip_id, [])
    calls = []
    for r in seq:
        st = stops_idx.get(r["stop_id"])
        calls.append({"time":r.get("time"),"stop_id":r.get("stop_id"),
                      "stop_name": (st or {}).get("stop_name") or r.get("stop_id"),
                      "eta_min":None,"delay_min":None})
    return {"trip_id":trip_id,"route":None,"headsign":None,"vehicle":None,"calls":calls}

@app.get("/api/routes/search")
async def api_route_search(q: str = Query("", description="Járatszám/név"), limit: int = 30):
    schedule = _read_json(DATA_DIR/"schedule.json", {})
    routes = set()
    for lst in schedule.values():
        for it in lst:
            if it.get("route"): routes.add(it["route"])
    res = sorted([r for r in routes if q.strip().lower() in str(r).lower()], key=lambda x:(len(str(x)),str(x)))
    return [{"route": r} for r in res[:limit]]

@app.get("/api/routes/{route}/vehicles")
async def api_route_vehicles(route: str):
    if not await siri_live.is_available(): return []
    try:
        vs = await siri_live.vehicles_by_route(route)
        return [enrich_vehicle(v) for v in vs][:100]
    except Exception:
        return []

@app.get("/index.html", include_in_schema=False)
async def index_file():
    return FileResponse(str(BASE_DIR/"index.html"), media_type="text/html")
