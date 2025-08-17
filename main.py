# main.py
from pathlib import Path
from fastapi import FastAPI, File, UploadFile, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import shutil, zipfile, json, os
from datetime import datetime, timezone

app = FastAPI(title="Bluestar Bus – API", version="1.1.0")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
GTFS_DIR = BASE_DIR / "gtfs"
STATIC_DIR = BASE_DIR / "static"

app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")

GTFS_INDEX = DATA_DIR / "stops_index.json"

@app.get("/api/status")
def status():
    return {"status": "ok", "gtfs_loaded": GTFS_INDEX.exists(), "siri_configured": bool(os.getenv("BODS_API_KEY"))}

@app.post("/api/upload")
async def upload_gtfs(file: UploadFile = File(...)):
    tmp_zip = DATA_DIR / "uploaded.zip"
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with tmp_zip.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    # kibontás és index építés (stops.txt -> stops_index.json) …
    # (a korábbi verzióban már megcsináltuk – maradhat ugyanúgy)
    return {"status": "ok", "method": "upload", "message": "GTFS betöltve az adatbázisba."}

@app.get("/api/stops/search")
def search_stops(q: str = Query(min_length=2)):
    if not GTFS_INDEX.exists():
        return []
    data = json.loads(GTFS_INDEX.read_text())
    ql = q.lower()
    return [s for s in data if ql in s["stop_name"].lower()][:20]

@app.get("/api/stops/{stop_id}/next_departures")
def next_departures(stop_id: str, minutes: int = 60):
    # GTFS alapján menetrend (ahogy most működik)
    ...

@app.get("/api/live/{stop_id}")
def live_for_stop(stop_id: str):
    # SIRI/BODS élő adatok meghívása (ha be van állítva BODS_API_KEY, FEED_ID stb.)
    ...

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ------------------------
# Beállítások / könyvtárak
# ------------------------
app = FastAPI(title="Bluestar Bus – API", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ha szeretnéd, itt korlátozhatod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
DATA_DIR = Path(os.getenv("DATA_DIR", BASE_DIR / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Statikus UI
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

# ------------------------
# Egyszerű GTFS "állapot"
# ------------------------
def gtfs_loaded_flag() -> bool:
    candidates = [
        DATA_DIR / "gtfs.sqlite",
        DATA_DIR / "gtfs.db",
        DATA_DIR / "bluestar.db",
        DATA_DIR / "bluestar.sqlite",
    ]
    return any(p.exists() for p in candidates)

def siri_configured_flag() -> bool:
    return bool(os.getenv("BODS_API_KEY"))

@app.get("/api/status", tags=["Status"])
async def api_status():
    return {
        "status": "ok",
        "gtfs_loaded": gtfs_loaded_flag(),
        "siri_configured": siri_configured_flag(),
    }

# ------------------------
# SIRI (BODS) – Live réteg
# ------------------------
BODS_BASE = os.getenv("BODS_BASE", "").rstrip("/")
BODS_FEED_ID = os.getenv("BODS_FEED_ID") or os.getenv("BODS_FEED")  # nálad: 7721
BODS_API_KEY = os.getenv("BODS_API_KEY")

SIRI_CACHE_TTL = 5  # másodperc – ne terheld túl a feedet
_siri_cache: Dict[str, Tuple[float, Any]] = {}  # key: "vm", value: (ts, xml_root)

def _bods_vm_url() -> Optional[str]:
    if not (BODS_BASE and BODS_FEED_ID and BODS_API_KEY):
        return None
    return f"{BODS_BASE}/datafeed/{BODS_FEED_ID}/?api_key={BODS_API_KEY}"

def _fetch_siri_vm_xml() -> Optional[ET.Element]:
    """Letölti és XML-ként visszaadja a SIRI-VM feedet (cache-elve)."""
    now = time.time()
    if "vm" in _siri_cache and now - _siri_cache["vm"][0] < SIRI_CACHE_TTL:
        return _siri_cache["vm"][1]

    url = _bods_vm_url()
    if not url:
        return None

    try:
        req = Request(url, headers={"User-Agent": "BluestarBus/1.0"})
        with urlopen(req, timeout=8) as resp:
            xml_bytes = resp.read()
        root = ET.fromstring(xml_bytes)
        _siri_cache["vm"] = (now, root)
        return root
    except Exception:
        return None

def _ns(tag: str) -> str:
    # SIRI XML namespacet gyakran használ: {namespace}TagName
    # Ha a feed namespace-szel jön, ez a segéd függvénnyel keresünk rá több variációra.
    return tag  # egyszerűsítve: ElementTree-vel sokszor megy prefix nélkül is

def _extract_live_for_stop(stop_id: str) -> List[Dict[str, Any]]:
    """
    Kinyeri a SIRI-VM feedből az adott megállóhoz tartozó élő érkezéseket.
    Visszaad: listát dict-ekkel: { "route": "14", "destination": "...", "expected_time_iso": "2025-08-17T12:34:00" }
    """
    root = _fetch_siri_vm_xml()
    if root is None:
        return []

    results: List[Dict[str, Any]] = []

    # A tipikus struktúra (rövidítve):
    # Siri/ServiceDelivery/VehicleMonitoringDelivery/VehicleActivity/MonitoredVehicleJourney
    #   LineRef, DestinationName, MonitoredCall/StopPointRef, MonitoredCall/ExpectedArrivalTime
    # Végigmegyünk az összes VehicleActivity-n, és amelyeknél a StopPointRef == stop_id, azt felvesszük.
    for vehicle_activity in root.iterfind(".//VehicleActivity"):
        mvj = vehicle_activity.find("MonitoredVehicleJourney")
        if mvj is None:
            continue

        line_ref_el = mvj.find("LineRef")
        line_ref = (line_ref_el.text or "").strip() if line_ref_el is not None else ""

        dest_el = mvj.find("DestinationName")
        destination = (dest_el.text or "").strip() if dest_el is not None else ""

        call = mvj.find("MonitoredCall")
        if call is None:
            continue

        spref_el = call.find("StopPointRef")
        spref = (spref_el.text or "").strip() if spref_el is not None else ""
        if not spref or spref != stop_id:
            continue

        eta_el = call.find("ExpectedArrivalTime") or call.find("AimedArrivalTime")
        eta_iso = (eta_el.text or "").strip() if eta_el is not None else ""

        if line_ref:
            results.append(
                {
                    "route": line_ref,
                    "destination": destination,
                    "expected_time_iso": eta_iso,
                    "is_live": True,
                }
            )

    return results

def _live_routes_at_stop(stop_id: str) -> Set[str]:
    """Az adott megállónál éppen 'élő' útvonalak (LineRef) halmaza."""
    return {item["route"] for item in _extract_live_for_stop(stop_id)}

@app.get("/api/live/{stop_id}", tags=["Live"])
async def live_for_stop(stop_id: str):
    """Nyers live lista az adott megállóra (ellenőrzéshez)."""
    if not siri_configured_flag():
        return {"stop_id": stop_id, "results": []}
    return {"stop_id": stop_id, "results": _extract_live_for_stop(stop_id)}

# -------------------------------------------------------
# Egyszerű GTFS-alapú search + next_departures (példa)
# -------------------------------------------------------
# Megjegyzés: ha nálad ezek már implementálva vannak, hagyd meg a saját
# verziódat, ez csak egy kompatibilis, egyszerű alap.

# GTFS minimál DAO (CSV-kből vagy DB-ből – itt csak jelzésértékű stub)
# A te projektedben valószínűleg már megvan – használd azt!
from collections import defaultdict
import json

STOPS_JSON = DATA_DIR / "stops.json"  # opcionális gyorsítótár teszthez
DEPS_JSON = DATA_DIR / "deps.json"    # opcionális gyorsítótár teszthez

# Ha létezik két mintafájl, betöltjük (különben üres marad)
_stops_idx: List[Dict[str, str]] = []
if STOPS_JSON.exists():
    try:
        _stops_idx = json.loads(STOPS_JSON.read_text())
    except Exception:
        _stops_idx = []

_deps_by_stop: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
if DEPS_JSON.exists():
    try:
        _deps_by_stop = defaultdict(list, json.loads(DEPS_JSON.read_text()))
    except Exception:
        _deps_by_stop = defaultdict(list)

@app.get("/api/stops/search", tags=["Stops"])
async def search_stops(q: str = Query(..., min_length=1)):
    """
    Egyszerű kereső: name/id tartalmazza-e a lekérdezett szöveget.
    A saját (valós) megoldásod itt nyugodtan maradhat.
    """
    ql = q.lower()
    res = [
        s for s in _stops_idx
        if ql in s.get("stop_name", "").lower() or ql in s.get("stop_id", "").lower()
    ][:20]
    return res

@app.get("/api/stops/{stop_id}/next_departures", tags=["Stops"])
async def next_departures(
    stop_id: str,
    minutes: int = Query(60, ge=5, le=240),
):
    """
    Következő indulások a megállóból (GTFS alapján), kiegészítve live jelzővel.
    Ezt a végpontot hívja a frontend.
    """
    # 1) GTFS menetrend (itt: demo/minimál – a saját DB-alapú megoldásod maradhat)
    schedule = _deps_by_stop.get(stop_id, [])
    # csak az elkövetkező 'minutes' ablakot hagyjuk meg (ha van 'time_iso' mező)
    filtered = []
    # (A valós szűrés nálad valószínűleg idő szerint SQL-ben történik.)
    for dep in schedule:
        filtered.append(
            {
                "route": dep.get("route", ""),
                "destination": dep.get("destination", ""),
                "time_iso": dep.get("time_iso", ""),
                "is_live": False,  # majd mindjárt felülírjuk
            }
        )

    # 2) Live (SIRI) illesztés
    live_routes = _live_routes_at_stop(stop_id) if siri_configured_flag() else set()
    if live_routes:
        for item in filtered:
            if item.get("route") in live_routes:
                item["is_live"] = True

    return {
        "stop_id": stop_id,
        "minutes": minutes,
        "results": filtered,
    }

# ------------------------
# UI (statikus index.html)
# ------------------------
@app.get("/", include_in_schema=False)
async def root():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return JSONResponse({"detail": "UI not found. Place index.html under /static."}, status_code=404)

@app.get("/api/ui", include_in_schema=False)
async def ui():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return JSONResponse({"detail": "UI not found. Place index.html under /static."}, status_code=404)

# ------------------------
# Uvicorn helyi futtatáshoz
# ------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
