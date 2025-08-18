from __future__ import annotations

import io
import json
import csv
import zipfile
import os
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ───────────────────────────────────────────────────────────────────────────────
# Beállítások / könyvtárak
# ───────────────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
INDEX_FILE = BASE_DIR / "index.html"

UK_TZ = ZoneInfo("Europe/London")   # 10) UK időkezelés BST/GMT szerint

# SIRI/RTPI  (1) Élő adatok bekötése környezeti változókkal
# Példa: SIRI_STOP_MONITORING_URL="https://…/StopMonitoring?MonitoringRef={stop_id}&MaximumStopVisits=10"
SIRI_URL_TEMPLATE = os.getenv("SIRI_STOP_MONITORING_URL", "").strip()
# Ha kulcs szükséges query param néven: pl. "api_key"  → hozzáfűzzük &api_key=…
SIRI_KEY_PARAM = os.getenv("SIRI_KEY_PARAM", "").strip()
SIRI_API_KEY   = os.getenv("SIRI_API_KEY", "").strip()
# Ha speciális fejlécek kellenek, add JSON-ként: pl. {"Authorization":"Bearer XXX"}
try:
    SIRI_HEADERS = json.loads(os.getenv("SIRI_HEADERS_JSON", "{}"))
    if not isinstance(SIRI_HEADERS, dict):
        SIRI_HEADERS = {}
except Exception:
    SIRI_HEADERS = {}

app = FastAPI(title="Bluestar Bus – API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ───────────────────────────────────────────────────────────────────────────────
# GTFS helper
# ───────────────────────────────────────────────────────────────────────────────
STOPS_JSON = DATA_DIR / "stops.json"
SCHEDULE_JSON = DATA_DIR / "schedule.json"

def gtfs_files_exist() -> bool:
    return STOPS_JSON.exists() and SCHEDULE_JSON.exists()

def _find_member(zf: zipfile.ZipFile, name: str) -> Optional[str]:
    lname = name.lower()
    for m in zf.namelist():
        ml = m.lower()
        if ml == lname or ml.endswith("/" + lname):
            return m
    return None

def _build_from_zip_bytes(zip_bytes: bytes) -> None:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        req = ["stops.txt", "trips.txt", "stop_times.txt", "routes.txt"]
        members = {n: _find_member(zf, n) for n in req}
        missing = [n for n, m in members.items() if m is None]
        if missing:
            raise ValueError("Hiányzó GTFS fájlok a ZIP-ben: " + ", ".join(missing))

        # stops
        stops: List[Dict[str, str]] = []
        with zf.open(members["stops.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                stops.append({
                    "stop_id": row["stop_id"],
                    "stop_name": (row.get("stop_name") or "").strip()
                })
        STOPS_JSON.write_text(json.dumps(stops, ensure_ascii=False), encoding="utf-8")

        # routes
        routes: Dict[str, str] = {}
        with zf.open(members["routes.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                routes[row["route_id"]] = (row.get("route_short_name") or row.get("route_long_name") or "").strip()

        # trips
        trips: Dict[str, Dict[str, str]] = {}
        with zf.open(members["trips.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                trips[row["trip_id"]] = {
                    "route": routes.get(row["route_id"], ""),
                    "headsign": (row.get("trip_headsign") or "").strip()
                }

        # schedule  stop_id -> list[{time, route, destination}]
        schedule: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        with zf.open(members["stop_times.txt"]) as f:
            reader = csv.DictReader(io.TextIOWrapper(f, "utf-8-sig"))
            for row in reader:
                trip = trips.get(row["trip_id"])
                if not trip:
                    continue
                t = (row.get("departure_time") or row.get("arrival_time") or "").strip()
                if not t:
                    continue
                schedule[row["stop_id"]].append({
                    "time": t,  # HH:MM(:SS), akár 24+ óra is lehet
                    "route": trip["route"],
                    "destination": trip["headsign"]
                })

        # opcionálisan idő szerint sorba
        for sid in schedule:
            schedule[sid].sort(key=lambda x: x["time"])

        SCHEDULE_JSON.write_text(json.dumps(schedule, ensure_ascii=False), encoding="utf-8")

# ───────────────────────────────────────────────────────────────────────────────
# Idő segéd – UK időzóna
# ───────────────────────────────────────────────────────────────────────────────
def now_uk() -> datetime:
    return datetime.now(UK_TZ)

def hms_to_seconds(hms: str) -> int:
    parts = [int(p) for p in hms.split(":")]
    if len(parts) == 2:
        h, m = parts
        s = 0
    else:
        h, m, s = parts[:3]
    return h * 3600 + m * 60 + s

def seconds_to_today_iso_uk(sec_from_midnight: int) -> str:
    d0 = now_uk().replace(hour=0, minute=0, second=0, microsecond=0)
    return (d0 + timedelta(seconds=sec_from_midnight)).isoformat()

# ───────────────────────────────────────────────────────────────────────────────
# SIRI – StopMonitoring (egyszerű kliens + merge)
# ───────────────────────────────────────────────────────────────────────────────
async def siri_is_available() -> bool:
    if not SIRI_URL_TEMPLATE:
        return False
    # csak string ellenőrzés – opcionálisan GET-et is tehetnénk, de felesleges terhelés
    return True

def build_siri_url(stop_id: str) -> Optional[str]:
    if not SIRI_URL_TEMPLATE:
        return None
    url = SIRI_URL_TEMPLATE.replace("{stop_id}", stop_id)
    if SIRI_KEY_PARAM and SIRI_API_KEY:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}{SIRI_KEY_PARAM}={SIRI_API_KEY}"
    return url

async def fetch_siri_stop(stop_id: str) -> List[Dict[str, Any]]:
    """
    Visszaad egy egyszerűsített listát a SIRI StopMonitoring-ból:
      [{ "route": "X", "destination": "Y", "expected_iso": "...", "aimed_iso": "..."}]
    Ha nincs vagy hiba van: üres lista.
    """
    url = build_siri_url(stop_id)
    if not url:
        return []

    try:
        async with httpx.AsyncClient(timeout=8.0, headers=SIRI_HEADERS) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return []
            data = r.json() if "json" in r.headers.get("content-type", "").lower() else None
            if not data:
                # ha XML a feed, itt lehetne xml -> json konverzió; most JSON-t várunk
                return []

            # SIRI JSON struktúrák eltérhetnek. Itt pár gyakori mezőt próbálunk:
            # MonitoredStopVisit[*].MonitoredVehicleJourney.(LineRef, DestinationName, MonitoredCall.(Aimed/ExpectedDepartureTime))
            visits = []
            def _get(d, *keys):
                cur = d
                for k in keys:
                    if isinstance(cur, dict) and k in cur:
                        cur = cur[k]
                    else:
                        return None
                return cur

            container = _get(data, "Siri", "ServiceDelivery", "StopMonitoringDelivery")
            if isinstance(container, list) and container:
                container = container[0]

            msv = []
            if isinstance(container, dict):
                msv = container.get("MonitoredStopVisit") or []

            for v in msv:
                mvj = _get(v, "MonitoredVehicleJourney") or {}
                line = _get(mvj, "LineRef")
                dest = _get(mvj, "DestinationName")
                call = _get(mvj, "MonitoredCall") or {}
                aimed = call.get("AimedDepartureTime") or call.get("AimedArrivalTime")
                exp = call.get("ExpectedDepartureTime") or call.get("ExpectedArrivalTime")

                # egyes feedekben DestinationName lehet list/dict; normalizáljuk
                if isinstance(dest, list) and dest:
                    dest = dest[0]
                if isinstance(dest, dict) and "value" in dest:
                    dest = dest["value"]

                if not (line and (aimed or exp)):
                    continue

                # A feed ISO-ját csak továbbadjuk, a DUE-t majd UK időben számoljuk
                visits.append({
                    "route": str(line),
                    "destination": str(dest or "").strip(),
                    "aimed_iso": str(aimed) if aimed else None,
                    "expected_iso": str(exp) if exp else None,
                })
            return visits
    except Exception:
        return []

def compute_due_minutes_from_iso(iso_ts: str) -> Optional[int]:
    try:
        # Python 3.11: fromisoformat kezeli az offsetet; UK időre konvertáljuk
        t = datetime.fromisoformat(iso_ts)
        if t.tzinfo is None:
            t = t.replace(tzinfo=UK_TZ)
        now = now_uk()
        diff = int(round((t.astimezone(UK_TZ) - now).total_seconds() / 60))
        return diff
    except Exception:
        return None

def merge_schedule_with_siri(
    schedule_rows: List[Dict[str, str]],
    siri_rows: List[Dict[str, Any]],
    window_minutes: int
) -> List[Dict[str, Any]]:
    """
    Összefésüli a menetrendi adatokat és a SIRI-t:
    - A SIRI sorok elsőbbséget kapnak (expected → live=True).
    - A menetrendi sorokat megtartjuk, ha nincs SIRI párjuk.
    - UK idő szerint számoljuk a due_in_min-t.
    """
    now = now_uk()
    horizon = now + timedelta(minutes=window_minutes)

    out: List[Dict[str, Any]] = []

    # 1) SIRI sorok felvétele (live)
    for s in siri_rows:
        ts = s.get("expected_iso") or s.get("aimed_iso")
        if not ts:
            continue
        due = compute_due_minutes_from_iso(ts)
        # Az ablakon kívüli ne
        if due is None:
            continue
        if due < 0 or due > window_minutes:
            continue
        out.append({
            "route": s.get("route") or "",
            "destination": s.get("destination") or "",
            "time_iso": ts,                       # megőrizzük az ISO-t
            "planned_hhmm": None,
            "due_in_min": max(0, due),
            "is_live": True
        })

    # 2) Menetrendi sorok, ha nincs SIRI pár
    # Egyszerű párosítás kulcsa: route + destination ~== (nem tökéletes, de használható)
    def key(r: Dict[str, Any]) -> str:
        return f"{(r.get('route') or '').strip().lower()}|{(r.get('destination') or '').strip().lower()}"

    live_keys = {key(r) for r in out}

    for row in schedule_rows:
        t_str = row.get("time")
        if not t_str:
            continue
        sec = hms_to_seconds(t_str) % (24*3600)
        iso = seconds_to_today_iso_uk(sec)
        # ablak szűrés
        tdt = datetime.fromisoformat(iso).astimezone(UK_TZ)
        if tdt < now or tdt > horizon:
            continue

        k = key(row)
        if k in live_keys:
            # Van élő pár – a SIRI már betette
            continue

        due = int(round((tdt - now).total_seconds() / 60))
        out.append({
            "route": row.get("route") or "",
            "destination": row.get("destination") or "",
            "time_iso": iso,
            "planned_hhmm": t_str[:5],
            "due_in_min": max(0, due),
            "is_live": False
        })

    # rendezés: előbb időben, majd live előre
    out.sort(key=lambda r: (r["due_in_min"], not r["is_live"]))
    return out[:100]

# ───────────────────────────────────────────────────────────────────────────────
# UI – mindig fájlt szolgálunk ki (cache nélkül)
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def ui_root():
    headers = {"Cache-Control": "no-store, no-cache, must-revalidate"}
    return FileResponse(INDEX_FILE, media_type="text/html", headers=headers)

# ───────────────────────────────────────────────────────────────────────────────
# API
# ───────────────────────────────────────────────────────────────────────────────
@app.get("/api/status")
async def api_status():
    return {
        "status": "ok",
        "gtfs": gtfs_files_exist(),
        "live": await siri_is_available(),
        "build": str(int(datetime.now(UK_TZ).timestamp()))
    }

@app.post("/api/upload")
async def api_upload(file: UploadFile = File(...)):
    content = await file.read()
    (DATA_DIR / "last_gtfs.zip").write_bytes(content)
    try:
        _build_from_zip_bytes(content)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"GTFS hiba: {e}") from e
    return {"status": "uploaded"}

@app.get("/api/stops/search")
async def api_stops_search(q: str = Query(..., min_length=2)):
    if not gtfs_files_exist():
        return []
    ql = (q or "").strip().lower()
    stops = json.loads(STOPS_JSON.read_text(encoding="utf-8"))
    res = [s for s in stops if ql in (s.get("stop_name") or "").lower()]
    return res[:30]

@app.get("/api/stops/{stop_id}/next_departures")
async def api_next_departures(stop_id: str, minutes: int = Query(60, ge=5, le=240)):
    """
    Kimenet (UK időre, BST/GMT helyesen):
      [
        {route, destination, time_iso, planned_hhmm, due_in_min, is_live}
      ]
    """
    if not gtfs_files_exist():
        return []

    schedule = json.loads(SCHEDULE_JSON.read_text(encoding="utf-8")).get(stop_id, [])

    # élő adatok behúzása (ha van)
    siri_rows: List[Dict[str, Any]] = await fetch_siri_stop(stop_id)

    merged = merge_schedule_with_siri(schedule, siri_rows, minutes)
    return merged
