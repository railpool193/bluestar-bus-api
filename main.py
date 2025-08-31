import asyncio
import io
import json
import os
import time
import zipfile
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any

import httpx
import pandas as pd
import pytz
import uvicorn
import xmltodict
from fastapi import FastAPI, File, UploadFile, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

APP_VERSION = "5.3.0"

# ---------- Config / State ----------

DATA_DIR = os.path.abspath("data")
GTFS_DIR = os.path.join(DATA_DIR, "gtfs")
GTFS_ZIP = os.path.join(DATA_DIR, "gtfs.zip")
FLEET_JSON = os.path.join(DATA_DIR, "fleet.json")

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(GTFS_DIR, exist_ok=True)
os.makedirs("static", exist_ok=True)

TZ_NAME = os.environ.get("TZ", "Europe/London")
TZ = pytz.timezone(TZ_NAME)

DEFAULT_LOOKAHEAD_MIN = 60
LIVE_REFRESH_SEC = 8  # mennyi időnként töltjük le a live feedet

class LiveConfig(BaseModel):
    feed_url: Optional[str] = None  # BODS SIRI-VM URL
    refresh_sec: int = LIVE_REFRESH_SEC

STATE = {
    "live_cfg": LiveConfig(),
    "gtfs_ready": False,
    "gtfs_meta": {},  # pl. stops_count
    "build": int(time.time()),
    "vehicles": [],  # legutóbbi letöltés eredménye
    "vehicles_ts": 0.0,
    "fleet": {},  # rendszám -> meta
}

# GTFS-táblák (pandas DataFrame-ként)
GTFS: Dict[str, pd.DataFrame] = {}


# ---------- Helpers ----------

def now_utc() -> datetime:
    return datetime.utcnow().replace(tzinfo=pytz.utc)

def now_local() -> datetime:
    return now_utc().astimezone(TZ)

def parse_hhmmss_to_today(hhmmss: str, local_day: date) -> datetime:
    # "HH:MM:SS" -> a mai nap adott ideje London időzónával
    h, m, s = [int(x) for x in hhmmss.split(":")]
    dt = datetime(local_day.year, local_day.month, local_day.day, 0, 0, 0, tzinfo=TZ)
    dt += timedelta(hours=h, minutes=m, seconds=s)
    return dt

def service_is_running(trip_row: pd.Series, dt_local: datetime) -> bool:
    """
    Eldönti, hogy a trip (calendar + calendar_dates alapján) fut-e a megadott napon.
    Minimalista implementáció: kezeli a calendar.txt napjait és a calendar_dates.txt módosításait.
    """
    service_id = trip_row["service_id"]
    day = dt_local.date()

    cal = GTFS.get("calendar")
    if cal is not None and not cal.empty:
        c = cal[cal["service_id"] == service_id]
        ok_calendar = False
        if not c.empty:
            c = c.iloc[0]
            start = pd.to_datetime(c["start_date"], format="%Y%m%d").date()
            end = pd.to_datetime(c["end_date"], format="%Y%m%d").date()
            if start <= day <= end:
                weekday = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][day.weekday()]
                ok_calendar = bool(int(c[weekday]))
        else:
            ok_calendar = False
    else:
        ok_calendar = True  # ha nincs calendar.txt, akkor engedjük, majd a dates szól bele

    dates = GTFS.get("calendar_dates")
    if dates is not None and not dates.empty:
        d = dates[dates["service_id"] == service_id]
        if not d.empty:
            d["date"] = pd.to_datetime(d["date"], format="%Y%m%d").dt.date
            same = d[d["date"] == day]
            if not same.empty:
                # exception_type: 1 = hozzáadás, 2 = törlés
                ex = int(same.iloc[0]["exception_type"])
                return ex == 1
    return ok_calendar


def ensure_str(v: Any) -> str:
    return "" if v is None else str(v)


# ---------- GTFS betöltés ----------

def load_gtfs_from_dir(gtfs_dir: str):
    required = ["stops", "routes", "trips", "stop_times"]
    tables = {}
    for name in ["stops", "routes", "trips", "stop_times", "shapes", "calendar", "calendar_dates"]:
        path = os.path.join(gtfs_dir, f"{name}.txt")
        if os.path.exists(path):
            df = pd.read_csv(path, dtype=str)
            tables[name] = df
        else:
            tables[name] = pd.DataFrame()

    missing = [t for t in required if tables[t].empty]
    if missing:
        raise RuntimeError(f"Hiányzó GTFS táblák: {', '.join(missing)}")

    GTFS.clear()
    GTFS.update(tables)
    STATE["gtfs_ready"] = True
    STATE["gtfs_meta"] = {
        "gtfs_stops": len(GTFS["stops"]),
        "gtfs_routes": len(GTFS["routes"]),
        "tz": TZ_NAME,
    }


def extract_and_load_gtfs(zip_bytes: bytes):
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # ürítsük a GTFS_DIR-t
        for fname in os.listdir(GTFS_DIR):
            try:
                os.remove(os.path.join(GTFS_DIR, fname))
            except Exception:
                pass
        zf.extractall(GTFS_DIR)
    load_gtfs_from_dir(GTFS_DIR)


# ---------- Fleet betöltés ----------

def load_fleet_json():
    if os.path.exists(FLEET_JSON):
        try:
            with open(FLEET_JSON, "r", encoding="utf-8") as f:
                STATE["fleet"] = json.load(f)
        except Exception:
            STATE["fleet"] = {}
    else:
        STATE["fleet"] = {}


# ---------- Live feed (SIRI-VM) ----------

def parse_siri_vm(xml_text: str) -> List[Dict[str, Any]]:
    """
    BODS SIRI-VM feed feldolgozása minimálisan a következő mezőkkel:
    - vehicle_id / registration
    - line (LineRef)
    - direction
    - lat/lon
    - bearing
    - stop_ref (MonitoredCall -> StopPointRef)
    - aimed_dep, expected_dep (ISO)
    - delay_sec (+ késés/sietés)
    """
    try:
        data = xmltodict.parse(xml_text)
    except Exception as e:
        raise RuntimeError(f"SIRI parse error: {e}")

    def _get(d: dict, path: List[str]):
        cur = d
        for p in path:
            if cur is None:
                return None
            cur = cur.get(p)
        return cur

    vehicles = []
    svc = _get(data, ["Siri", "ServiceDelivery", "VehicleMonitoringDelivery"])
    if svc is None:
        return vehicles

    activities = svc.get("VehicleActivity", [])
    if isinstance(activities, dict):
        activities = [activities]

    for a in activities:
        mvj = _get(a, ["MonitoredVehicleJourney"])
        if not mvj:
            continue
        line = ensure_str(mvj.get("LineRef"))
        direction = ensure_str(mvj.get("DirectionRef"))
        vehicle_id = ensure_str(mvj.get("VehicleRef")) or ensure_str(mvj.get("FramedVehicleJourneyRef",{}).get("DatedVehicleJourneyRef"))
        loc = mvj.get("VehicleLocation") or {}
        lat = loc.get("Latitude")
        lon = loc.get("Longitude")
        bearing = mvj.get("Bearing")

        call = mvj.get("MonitoredCall") or {}
        stop_ref = ensure_str(call.get("StopPointRef"))
        aimed_dep = ensure_str(call.get("AimedDepartureTime") or call.get("AimedArrivalTime"))
        expected_dep = ensure_str(call.get("ExpectedDepartureTime") or call.get("ExpectedArrivalTime"))

        delay_sec = None
        if aimed_dep and expected_dep:
            try:
                aimed = pd.to_datetime(aimed_dep).tz_convert(TZ)
                expected = pd.to_datetime(expected_dep).tz_convert(TZ)
                delay_sec = int((expected - aimed).total_seconds())
            except Exception:
                delay_sec = None

        v = {
            "vehicle_id": vehicle_id,
            "line": line,
            "direction": direction,
            "lat": float(lat) if lat else None,
            "lon": float(lon) if lon else None,
            "bearing": float(bearing) if bearing else None,
            "stop_ref": stop_ref,
            "aimed_dep": aimed_dep,
            "expected_dep": expected_dep,
            "delay_sec": delay_sec,
        }

        # flotta meta csatolása (ha a vehicle_id rendszámként szerepel)
        meta = STATE["fleet"].get(vehicle_id) or STATE["fleet"].get(vehicle_id.replace(" ", ""))
        if meta:
            v["fleet"] = meta

        vehicles.append(v)

    return vehicles


async def maybe_refresh_live():
    cfg: LiveConfig = STATE["live_cfg"]
    now = time.time()
    if not cfg.feed_url:
        return
    if now - STATE["vehicles_ts"] < cfg.refresh_sec:
        return

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(cfg.feed_url)
            r.raise_for_status()
            xml_text = r.text
        vehicles = parse_siri_vm(xml_text)
        STATE["vehicles"] = vehicles
        STATE["vehicles_ts"] = now
    except Exception as e:
        # Nem dobunk hibát a kliens felé, csak logolunk és megtartjuk a régi adatot
        print(f"[live] fetch error: {e}")


# ---------- FastAPI ----------

app = FastAPI(title="Bluestar Bus — API", version=APP_VERSION)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# statikus fájlok (index.html a következő üzenetben)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def root():
    return {"detail": "Open /index.html", "docs": "/docs"}


@app.get("/index.html")
def serve_index():
    path = os.path.join("static", "index.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return JSONResponse({"error": "static/index.html not found"}, status_code=404)


@app.get("/api/status")
def api_status():
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": STATE["build"],
        "time": now_local().strftime("%H:%M:%S"),
        "tz": TZ_NAME,
        "live_feed_configured": bool(STATE["live_cfg"].feed_url),
        "gtfs_dir": "data/gtfs",
        "gtfs_ready": STATE["gtfs_ready"],
        "gtfs_stops": STATE["gtfs_meta"].get("gtfs_stops", 0),
    }


@app.get("/api/live/config")
def get_live_config():
    return STATE["live_cfg"].model_dump()


@app.post("/api/live/config")
def set_live_config(cfg: LiveConfig):
    STATE["live_cfg"] = cfg
    return {"ok": True, "live_cfg": cfg.model_dump()}


# ---------- GTFS upload / load-url / reload ----------

@app.post("/api/gtfs/upload")
async def api_gtfs_upload(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".zip"):
        raise HTTPException(400, "Adj meg egy GTFS zip fájlt.")
    content = await file.read()
    try:
        extract_and_load_gtfs(content)
    except Exception as e:
        raise HTTPException(400, f"GTFS hiba: {e}")
    return {"ok": True, "msg": "GTFS loaded", **STATE["gtfs_meta"]}


class GtfsUrlIn(BaseModel):
    url: str

@app.post("/api/gtfs/load-url")
async def api_gtfs_load_url(body: GtfsUrlIn):
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(body.url)
            r.raise_for_status()
            content = r.content
        with open(GTFS_ZIP, "wb") as f:
            f.write(content)
        extract_and_load_gtfs(content)
    except Exception as e:
        raise HTTPException(400, f"Letöltés/GTFS hiba: {e}")
    return {"ok": True, "msg": "GTFS loaded from URL", **STATE["gtfs_meta"]}


@app.post("/api/reload-gtfs")
def api_reload_gtfs():
    if not os.path.exists(GTFS_DIR):
        raise HTTPException(400, "Nincs GTFS a data/gtfs mappában.")
    try:
        load_gtfs_from_dir(GTFS_DIR)
    except Exception as e:
        raise HTTPException(400, f"GTFS hiba: {e}")
    return {"ok": True, "msg": "GTFS reloaded", **STATE["gtfs_meta"]}


# ---------- Keresők ----------

@app.get("/api/stops/search")
def api_stops_search(q: str):
    if not STATE["gtfs_ready"]:
        raise HTTPException(400, "GTFS nincs betöltve.")
    s = GTFS["stops"]
    ql = q.strip().lower()
    res = s[s["stop_name"].str.lower().str.contains(ql, na=False)].copy()
    res = res.head(25)
    # Minimal szükséges mezők
    out = []
    for _, r in res.iterrows():
        out.append({
            "stop_id": r["stop_id"],
            "name": r["stop_name"],
            "code": r.get("stop_code"),
            "lat": float(r["stop_lat"]) if "stop_lat" in r and pd.notna(r["stop_lat"]) else None,
            "lon": float(r["stop_lon"]) if "stop_lon" in r and pd.notna(r["stop_lon"]) else None,
        })
    return {"items": out}


@app.get("/api/routes/search")
def api_routes_search(q: str):
    if not STATE["gtfs_ready"]:
        raise HTTPException(400, "GTFS nincs betöltve.")
    routes = GTFS["routes"].copy()
    ql = q.strip().lower()
    mask = (
        routes.get("route_short_name","").str.lower().str.contains(ql, na=False) |
        routes.get("route_long_name","").str.lower().str.contains(ql, na=False)
    )
    res = routes[mask].head(20)
    out = []
    for _, r in res.iterrows():
        out.append({
            "route_id": r["route_id"],
            "short_name": r.get("route_short_name"),
            "long_name": r.get("route_long_name"),
            "type": r.get("route_type"),
        })
    return {"items": out}


# ---------- Indulások (csak indulások + Due/On time/Late/Early) ----------

@app.get("/api/departures")
async def api_departures(stop_id: str, lookahead_min: int = DEFAULT_LOOKAHEAD_MIN):
    if not STATE["gtfs_ready"]:
        raise HTTPException(400, "GTFS nincs betöltve.")

    await maybe_refresh_live()
    live = STATE["vehicles"]

    stops = GTFS["stops"]
    stop_times = GTFS["stop_times"]
    trips = GTFS["trips"]
    routes = GTFS["routes"]

    # stop_id vagy stop_code egyezés
    st_mask = (stop_times["stop_id"] == stop_id)
    # ha van stop_code a GTFS-ben, próbáljuk azzal is egyeztetni
    scode = None
    srow = stops[st_mask]
    if srow.empty:
        # keressük ki a stop_code-ot
        s2 = stops[stops.get("stop_id","") == stop_id]
        if not s2.empty:
            scode = s2.iloc[0].get("stop_code")
    if scode:
        st_mask = (stop_times.get("stop_id") == stop_id) | (stop_times.get("stop_id") == scode)

    today = now_local().date()
    now_dt = now_local()
    until = now_dt + timedelta(minutes=lookahead_min)

    # Az aznapi futó trip-ek adott megállói
    merged = stop_times.merge(trips, on="trip_id", how="left", suffixes=("","_t"))
    merged = merged.merge(routes, on="route_id", how="left", suffixes=("","_r"))
    merged = merged[st_mask].copy()

    items = []
    for _, row in merged.iterrows():
        try:
            if not service_is_running(row, now_dt):
                continue

            dep_time = row.get("departure_time") or row.get("arrival_time")
            if not isinstance(dep_time, str) or ":" not in dep_time:
                continue
            sched_dt = parse_hhmmss_to_today(dep_time, today)
            if sched_dt < now_dt - timedelta(minutes=1):
                continue
            if sched_dt > until:
                continue

            route_short = ensure_str(row.get("route_short_name"))
            trip_id = ensure_str(row.get("trip_id"))

            # élő adatok hozzárendelése: azonos vonalszám + köv. megálló egyezés alapján (best effort)
            live_match = None
            for v in live:
                if route_short and v.get("line") == route_short:
                    # Ha StopPointRef van és egyezik a stop_id (vagy stop_code)
                    stop_ref = v.get("stop_ref")
                    if stop_ref and (stop_ref == stop_id or (scode and stop_ref == scode)):
                        live_match = v
                        break

            delay_sec = None
            status = "scheduled"
            is_live = False
            expected_dt = sched_dt

            if live_match:
                is_live = True
                # ha ExpectedDepartureTime van, azt használjuk
                if live_match.get("expected_dep"):
                    try:
                        expected_dt = pd.to_datetime(live_match["expected_dep"]).tz_convert(TZ).to_pydatetime()
                    except Exception:
                        expected_dt = sched_dt
                delay_sec = live_match.get("delay_sec")
                if delay_sec is None and live_match.get("aimed_dep") and live_match.get("expected_dep"):
                    try:
                        aimed = pd.to_datetime(live_match["aimed_dep"]).tz_convert(TZ)
                        exp = pd.to_datetime(live_match["expected_dep"]).tz_convert(TZ)
                        delay_sec = int((exp - aimed).total_seconds())
                    except Exception:
                        delay_sec = 0

                # státusz
                if expected_dt <= now_dt + timedelta(seconds=30):
                    status = "due"
                elif delay_sec and delay_sec > 60:
                    status = "late"
                elif delay_sec and delay_sec < -60:
                    status = "early"
                else:
                    status = "on_time"

            items.append({
                "route": route_short,
                "trip_id": trip_id,
                "scheduled_time": sched_dt.isoformat(),
                "expected_time": expected_dt.isoformat(),
                "in_min": int((expected_dt - now_dt).total_seconds() // 60),
                "is_live": is_live,
                "status": status,         # scheduled / due / on_time / late / early
                "delay_sec": delay_sec,   # + késés, - sietés (másodperc)
            })
        except Exception:
            continue

    # idő szerint rendezzük
    items.sort(key=lambda x: x["expected_time"])
    return {"stop_id": stop_id, "items": items}


# ---------- Trip modul ----------

@app.get("/api/trip")
async def api_trip(trip_id: str):
    if not STATE["gtfs_ready"]:
        raise HTTPException(400, "GTFS nincs betöltve.")

    await maybe_refresh_live()

    trips = GTFS["trips"]
    st = GTFS["stop_times"]
    stops = GTFS["stops"]
    shapes = GTFS.get("shapes", pd.DataFrame())

    t = trips[trips["trip_id"] == trip_id]
    if t.empty:
        raise HTTPException(404, "Ismeretlen trip_id")

    t = t.iloc[0]
    route_short = t.get("route_short_name")
    shape_id = t.get("shape_id")

    # megállók listája időkkel
    tt = st[st["trip_id"] == trip_id].copy()
    tt = tt.sort_values(by="stop_sequence")
    tt = tt.merge(stops, on="stop_id", how="left", suffixes=("","_s"))

    stops_out = []
    for _, r in tt.iterrows():
        stops_out.append({
            "stop_id": r["stop_id"],
            "name": r.get("stop_name"),
            "lat": float(r.get("stop_lat")) if pd.notna(r.get("stop_lat")) else None,
            "lon": float(r.get("stop_lon")) if pd.notna(r.get("stop_lon")) else None,
            "arr": r.get("arrival_time"),
            "dep": r.get("departure_time"),
        })

    # shape poliline
    polyline = []
    if not shapes.empty and pd.notna(shape_id):
        sh = shapes[shapes["shape_id"] == shape_id].copy()
        if not sh.empty:
            sh["seq"] = sh.get("shape_pt_sequence").astype(int)
            sh = sh.sort_values("seq")
            for _, r in sh.iterrows():
                try:
                    lat = float(r["shape_pt_lat"])
                    lon = float(r["shape_pt_lon"])
                    polyline.append([lat, lon])
                except Exception:
                    pass

    # élő jármű hozzárendelése a vonalszám alapján (best effort)
    live_bus = None
    for v in STATE["vehicles"]:
        if route_short and v.get("line") == route_short:
            live_bus = v
            break

    # Késés/sietés félperces pontossággal (ha van live)
    delay_min_05 = None
    if live_bus and live_bus.get("delay_sec") is not None:
        delay_min_05 = round(live_bus["delay_sec"] / 60.0 * 2) / 2.0

    return {
        "trip_id": trip_id,
        "route": route_short,
        "stops": stops_out,
        "shape": polyline,
        "live": {
            "vehicle": live_bus,
            "delay_min": delay_min_05
        }
    }


# ---------- Élő járművek ----------

@app.get("/api/vehicles")
async def api_vehicles(route: Optional[str] = None):
    await maybe_refresh_live()
    items = STATE["vehicles"]
    if route:
        items = [v for v in items if ensure_str(v.get("line")).lower() == route.lower()]
    return {"items": items, "ts": STATE["vehicles_ts"]}


# ---------- Indításkor: flotta + GTFS (ha van) ----------

@app.on_event("startup")
async def on_startup():
    load_fleet_json()
    # ha már van kicsomagolt GTFS, töltsük be
    try:
        load_gtfs_from_dir(GTFS_DIR)
    except Exception:
        pass


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
