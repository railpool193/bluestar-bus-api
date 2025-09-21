# main.py  (v5.3.1) — FastAPI backend GTFS + BODS SIRI-VM
import io, os, time, zipfile, json, math
from datetime import datetime, timedelta, date, time as dtime
from typing import Dict, List, Any, Optional

import pandas as pd
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

try:
    from zoneinfo import ZoneInfo
    LON = ZoneInfo("Europe/London")
except Exception:
    import pytz
    LON = pytz.timezone("Europe/London")

APP_VERSION = "5.3.1"

DATA_DIR   = os.path.abspath("data")
GTFS_DIR   = os.path.join(DATA_DIR, "gtfs")
CACHE_DIR  = os.path.join(DATA_DIR, "cache")
LIVE_JSON  = os.path.join(CACHE_DIR, "siri_vm.json")
LIVE_CFG   = os.path.join(CACHE_DIR, "live_cfg.json")

os.makedirs(GTFS_DIR, exist_ok=True)
os.makedirs(CACHE_DIR, exist_ok=True)
os.makedirs("static", exist_ok=True)

STATE: Dict[str, Any] = {
    "build": int(time.time()),
    "gtfs_ready": False,
    "gtfs_meta": {},
    "live_cfg": {"feed_url": "", "refresh_sec": 8},
    "vehicles": [],
    "vehicles_ts": 0.0,
}

GTFS: Dict[str, pd.DataFrame] = {}  # táblák pandas-szal

def now_utc() -> datetime:
    return datetime.utcnow().replace(tzinfo=ZoneInfo("UTC") if isinstance(LON, ZoneInfo) else None)

def now_local() -> datetime:
    return now_utc().astimezone(LON)

def _safe_json_load(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def _safe_json_dump(path: str, obj: Any):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)

# ---------------- GTFS betöltés ----------------
def load_gtfs_from_dir(gtfs_dir: str):
    tables: Dict[str, pd.DataFrame] = {}
    for name in ["stops","routes","trips","stop_times","shapes","calendar","calendar_dates"]:
        p = os.path.join(gtfs_dir, f"{name}.txt")
        if os.path.exists(p):
            df = pd.read_csv(p, dtype=str)
            tables[name] = df
        else:
            tables[name] = pd.DataFrame()
    missing = [n for n in ["stops","routes","trips","stop_times"] if tables[n].empty]
    if missing:
        raise HTTPException(400, f"Hiányzó GTFS fájl(ok): {', '.join(missing)}")

    GTFS.clear()
    GTFS.update(tables)
    STATE["gtfs_ready"] = True
    STATE["gtfs_meta"] = {
        "stops": len(GTFS["stops"]),
        "routes": len(GTFS["routes"]),
    }

def extract_and_load_gtfs(zip_bytes: bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for fn in os.listdir(GTFS_DIR):
            try: os.remove(os.path.join(GTFS_DIR, fn))
            except Exception: pass
        zf.extractall(GTFS_DIR)
    load_gtfs_from_dir(GTFS_DIR)

# szolgálati nap egyszerű ellenőrzése
def service_runs(service_id: str, day: date) -> bool:
    cal = GTFS.get("calendar")
    ok_calendar = True
    if cal is not None and not cal.empty:
        c = cal[cal["service_id"] == service_id]
        ok_calendar = False
        if not c.empty:
            c = c.iloc[0]
            start = datetime.strptime(c["start_date"], "%Y%m%d").date()
            end   = datetime.strptime(c["end_date"], "%Y%m%d").date()
            if start <= day <= end:
                weekday = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][day.weekday()]
                ok_calendar = bool(int(c[weekday]))
    dates = GTFS.get("calendar_dates")
    if dates is not None and not dates.empty:
        d = dates[dates["service_id"]==service_id]
        if not d.empty:
            d = d.copy()
            d["date"] = pd.to_datetime(d["date"], format="%Y%m%d").dt.date
            same = d[d["date"] == day]
            if not same.empty:
                return int(same.iloc[0]["exception_type"]) == 1
    return ok_calendar

def hhmm_local(dt_utc: datetime) -> str:
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=ZoneInfo("UTC"))
    return dt_utc.astimezone(LON).strftime("%H:%M")

def to_today_time(hms: str, day_local: date) -> datetime:
    h, m, s = [int(x) for x in hms.split(":")]
    base = datetime.combine(day_local, dtime(0,0,0)).replace(tzinfo=LON)
    return base + timedelta(hours=h, minutes=m, seconds=s)

# ---------------- Live (SIRI-VM) ----------------
async def fetch_live() -> List[Dict[str, Any]]:
    url = (_safe_json_load(LIVE_CFG, {}) or {}).get("feed_url") or STATE["live_cfg"].get("feed_url") or ""
    if not url:
        return []
    # cache 15s
    if STATE["vehicles"] and (time.time() - STATE["vehicles_ts"] < 15):
        return STATE["vehicles"]
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            txt = r.text
            try:
                payload = r.json()
            except Exception:
                # lehet XML → xmltodict nélkül egyszerű text-keresés fallback
                payload = None
        items: List[Dict[str, Any]] = []
        if payload:
            # BODS JSON (egyes szolgáltatók) – keresünk Siri/ServiceDelivery
            siri = payload.get("Siri") or payload.get("siri") or {}
            delivs = ((siri.get("ServiceDelivery") or {}).get("VehicleMonitoringDelivery") or [])
            for d in delivs:
                for va in (d.get("VehicleActivity") or []):
                    mvj = va.get("MonitoredVehicleJourney") or {}
                    loc = mvj.get("VehicleLocation") or {}
                    items.append({
                        "route": str(mvj.get("PublishedLineName") or mvj.get("LineRef") or "").strip(),
                        "dest": (mvj.get("DestinationName") or "") or "",
                        "vehicle": str(mvj.get("VehicleRef") or mvj.get("VehicleId") or ""),
                        "lat": loc.get("Latitude"),
                        "lon": loc.get("Longitude"),
                        "bearing": mvj.get("Bearing"),
                        "ts": va.get("RecordedAtTime") or va.get("ValidUntilTime") or "",
                        "expected": (mvj.get("MonitoredCall") or {}).get("ExpectedDepartureTime")
                    })
        else:
            # nyers, „lapos” BODS text (mint a böngészőben láttad) – nagyon egyszerű parser
            # soronként próbáljuk kihalászni: "... 17 inbound ... 50.9 ... -1.4 ..."
            for line in txt.splitlines():
                parts = line.strip().split()
                if len(parts) < 6: 
                    continue
                # próbáljuk a route-ot megtalálni (pl "17", "U1", "19a")
                # heur: ha van olyan token, amiben csak betű/szám van és nem időbélyeg
                # de hagyjuk: csak koordinátákat és route-ot emeljük ki
                try:
                    lngs = [float(x) for x in parts if x.replace('.','',1).replace('-','',1).isdigit()]
                except Exception:
                    lngs = []
                if len(lngs) >= 2:
                    lon = None; lat = None
                    # az angliai feedben tipikusan lon (negatív), lat (pozitív 50.x)
                    for f in lngs:
                        if f > 49 and f < 59: lat = f
                        if f < 0 and f > -10: lon = f
                    if lat is not None and lon is not None:
                        # route: keressünk egy rövid alfanumerikus tokent (1..4 hossz)
                        route = ""
                        for tok in parts:
                            if 1 <= len(tok) <= 4 and tok.replace("a","").isalnum():
                                route = tok
                                break
                        items.append({"route": route, "lat": lat, "lon": lon, "vehicle":"", "dest":"", "bearing": None, "ts": ""})
        STATE["vehicles"] = items
        STATE["vehicles_ts"] = time.time()
        _safe_json_dump(LIVE_JSON, {"ts": STATE["vehicles_ts"], "items": items})
        return items
    except Exception:
        # utolsó cache
        cached = _safe_json_load(LIVE_JSON, {"items": []})
        return cached.get("items", [])

# ---------------- FastAPI app ----------------
app = FastAPI(title="Bluestar Bus — API", version=APP_VERSION, docs_url="/docs", redoc_url=None)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def root():
    # egyetlen fájlból szolgáljuk ki a frontendet is
    index = os.path.abspath("index.html")
    if not os.path.exists(index):
        return JSONResponse({"detail":"Upload index.html next to main.py"})
    return FileResponse(index)

# ---- status
@app.get("/api/status")
def api_status():
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": STATE["build"],
        "time": now_local().strftime("%H:%M:%S"),
        "tz": "Europe/London",
        "live_feed_configured": bool((_safe_json_load(LIVE_CFG, {}) or {}).get("feed_url") or STATE["live_cfg"].get("feed_url")),
        "gtfs_dir": "data/gtfs",
        "gtfs_ready": STATE["gtfs_ready"],
        "gtfs_stops": STATE.get("gtfs_meta", {}).get("stops", 0),
    }

# ---- live config
@app.get("/api/live/config")
def get_live_cfg():
    return _safe_json_load(LIVE_CFG, STATE["live_cfg"])

@app.post("/api/live/config")
def set_live_cfg(cfg: Dict[str, Any]):
    if not cfg or "feed_url" not in cfg:
        raise HTTPException(400, "feed_url is required")
    merged = {**STATE["live_cfg"], **cfg}
    _safe_json_dump(LIVE_CFG, merged)
    STATE["live_cfg"] = merged
    STATE["vehicles"] = []
    STATE["vehicles_ts"] = 0.0
    try:
        os.remove(LIVE_JSON)
    except Exception:
        pass
    return {"ok": True}

# ---- GTFS endpoints
@app.post("/api/gtfs/upload")
async def api_gtfs_upload(file: UploadFile = File(...)):
    raw = await file.read()
    extract_and_load_gtfs(raw)
    return {"ok": True, "stops": STATE["gtfs_meta"]["stops"]}

@app.post("/api/gtfs/load-url")
async def api_gtfs_load_url(body: Dict[str, str]):
    url = (body or {}).get("url")
    if not url:
        raise HTTPException(422, "JSON body: {\"url\":\"https://.../gtfs.zip\"}")
    async with httpx.AsyncClient(timeout=None) as client:
        r = await client.get(url)
        r.raise_for_status()
        extract_and_load_gtfs(r.content)
    return {"ok": True, "stops": STATE["gtfs_meta"]["stops"]}

@app.post("/api/reload-gtfs")
def api_reload_gtfs():
    load_gtfs_from_dir(GTFS_DIR)
    return {"ok": True, "stops": STATE["gtfs_meta"]["stops"]}

# ---- keresés (UI által használt query-s útvonalak)
@app.get("/api/stops/search")
def api_stops_search(q: str):
    if not STATE["gtfs_ready"]:
        raise HTTPException(503, "GTFS not loaded")
    ql = q.strip().casefold()
    df = GTFS["stops"].fillna("")
    out = []
    for _, r in df.iterrows():
        name = str(r.get("stop_name",""))
        if ql in name.casefold():
            out.append({"id": r.get("stop_id"), "name": name})
            if len(out) >= 50: break
    return out

@app.get("/api/routes/search")
def api_routes_search(q: str):
    if not STATE["gtfs_ready"]:
        raise HTTPException(503, "GTFS not loaded")
    ql = q.strip().casefold()
    df = GTFS["routes"].fillna("")
    out = []
    for _, r in df.iterrows():
        short = str(r.get("route_short_name","")).strip()
        longn = str(r.get("route_long_name","")).strip()
        lab = short or longn
        if ql in lab.casefold():
            out.append({"route": lab})
            if len(out) >= 50: break
    return out

# indulások (csak indulási idő — live késés, ha van)
@app.get("/api/departures")
async def api_departures(stop_id: str, window: int = 90):
    if not STATE["gtfs_ready"]:
        raise HTTPException(503, "GTFS not loaded")
    today = now_local().date()
    st   = GTFS["stop_times"].fillna("")
    trips= GTFS["trips"].fillna("")
    routes=GTFS["routes"].fillna("")
    # join: stop_times -> trips -> routes (route_short_name/headsign)
    j = st[st["stop_id"] == stop_id].copy()
    if j.empty:
        return {"departures":[]}
    j = j.merge(trips[["trip_id","route_id","service_id","trip_headsign"]], on="trip_id", how="left")
    j = j.merge(routes[["route_id","route_short_name","route_long_name"]], on="route_id", how="left")
    # időpont számítás
    nowu = now_utc()
    endu = nowu + timedelta(minutes=max(1, min(window, 480)))
    rows: List[Dict[str, Any]] = []
    # live by route (gyorsított)
    live = await fetch_live()
    live_by_route: Dict[str, Dict[str, Any]] = {}
    for r in live:
        rt = (r.get("route") or "").strip()
        if not rt: continue
        live_by_route[rt] = r
    for _, r in j.iterrows():
        try:
            if not service_runs(str(r.get("service_id","")), today):
                continue
            dep_local = to_today_time(str(r.get("departure_time","00:00:00")), today)
            dep_utc   = dep_local.astimezone(ZoneInfo("UTC") if isinstance(LON, ZoneInfo) else None)
            if dep_utc < nowu - timedelta(minutes=1):  # már elment
                continue
            if dep_utc > endu:
                continue
            route_label = (str(r.get("route_short_name","")).strip() or str(r.get("route_long_name","")).strip() or str(r.get("route_id","")).strip())
            headsign = str(r.get("trip_headsign","")).strip()
            # live várható indulás csak ha van és ésszerű
            use = dep_utc
            delay = None
            li = live_by_route.get(route_label)
            if li and li.get("ts"):
                # ExpectedDepartureTime nincs biztosan – hagyjuk meg a menetrendit; később bővíthető
                pass
            mins = int((use - nowu).total_seconds() // 60)
            rows.append({
                "route": route_label or "–",
                "destination": headsign or (li or {}).get("dest") or "–",
                "time_iso": use.isoformat(),
                "time_display": "Due" if (-1 <= mins <= 0) else hhmm_local(use),
                "is_live": False,
                "is_due": (-1 <= mins <= 0),
                "delay_min": delay,
                "trip_id": r.get("trip_id")
            })
        except Exception:
            continue
    # deduplikálás + rendezés
    seen = set()
    uniq = []
    for r in rows:
        key = (r["route"], r["destination"], r["time_iso"])
        if key in seen: continue
        seen.add(key); uniq.append(r)
    uniq.sort(key=lambda x: (not x["is_due"], x["time_iso"]))
    return {"departures": uniq}

@app.get("/api/trip")
def api_trip(trip_id: str):
    if not STATE["gtfs_ready"]:
        raise HTTPException(503, "GTFS not loaded")
    trips = GTFS["trips"].fillna("")
    st    = GTFS["stop_times"].fillna("")
    stops = GTFS["stops"].fillna("")
    meta = trips[trips["trip_id"]==trip_id]
    if meta.empty:
        raise HTTPException(404, "Trip not found")
    meta = meta.iloc[0]
    today = now_local().date()
    j = st[st["trip_id"]==trip_id].merge(stops[["stop_id","stop_name"]], on="stop_id", how="left")
    out=[]
    for _, r in j.sort_values("stop_sequence").iterrows():
        dep_local = to_today_time(str(r.get("departure_time","00:00:00")), today)
        dep_utc   = dep_local.astimezone(ZoneInfo("UTC") if isinstance(LON, ZoneInfo) else None)
        out.append({
            "stop_id": r.get("stop_id"),
            "stop_name": r.get("stop_name"),
            "time_iso": dep_utc.isoformat(),
            "time_display": hhmm_local(dep_utc),
        })
    return {"route": str(meta.get("route_id","")), "headsign": str(meta.get("trip_headsign","")), "stops": out}

@app.get("/api/vehicles")
async def api_vehicles(route: Optional[str] = None):
    items = await fetch_live()
    if route:
        items = [i for i in items if str(i.get("route","")).strip() == str(route).strip()]
    # csak friss és koordinátás
    out=[]
    for i in items:
        if not i.get("lat") or not i.get("lon"): continue
        out.append({
            "vehicle_ref": i.get("vehicle") or "",
            "lat": i["lat"], "lon": i["lon"], "bearing": i.get("bearing"),
            "timestamp": i.get("ts") or "",
            "label": f'{i.get("route","")} · {i.get("dest","")}'.strip()
        })
    return {"items": out, "ts": STATE.get("vehicles_ts", 0.0)}

# --- opcionális: route shape (ha van shapes.txt)
@app.get("/api/route-shape")
def api_route_shape(route: str):
    if not STATE["gtfs_ready"]:
        raise HTTPException(503, "GTFS not loaded")
    trips = GTFS["trips"].fillna("")
    shapes= GTFS["shapes"].fillna("")
    routes= GTFS["routes"].fillna("")
    # route label → route_id
    rids = routes[(routes["route_short_name"]==route) | (routes["route_long_name"]==route)]["route_id"].unique().tolist()
    if not rids:
        return {"shape":[]}
    t = trips[trips["route_id"].isin(rids)]
    if "shape_id" not in t.columns or t["shape_id"].isna().all():
        return {"shape":[]}
    sid = str(t.iloc[0]["shape_id"])
    if not sid or shapes.empty:
        return {"shape":[]}
    seg = shapes[shapes["shape_id"]==sid].copy()
    if "shape_pt_sequence" in seg.columns:
        seg["shape_pt_sequence"] = seg["shape_pt_sequence"].astype(int)
        seg = seg.sort_values("shape_pt_sequence")
    pts = [[float(seg.iloc[i]["shape_pt_lat"]), float(seg.iloc[i]["shape_pt_lon"])] for i in range(len(seg))]
    return {"shape": pts}
