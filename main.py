# main.py
import os
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

import httpx
from fastapi import FastAPI, HTTPException, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# --- külső segéd modulok (a repo-ban lévők) ---
# A modulok meglétét ellenőrizzük, hogy ne dőljön el, ha hiányoznak.
try:
    import gtfs  # elvárt: load(), is_loaded(), search_stops(query)
except Exception:  # pragma: no cover
    gtfs = None

try:
    import siri_live  # elvárt: departures_for_stop(stop_id, feed_url, api_key, minutes)
except Exception:  # pragma: no cover
    siri_live = None


# --------- Alapbeállítások ---------
APP_VERSION = "5.0.0"
TZ = "Europe/London"
BUILD = int(time.time())

DATA_DIR = Path(os.getenv("DATA_DIR", "data"))
CACHE_DIR = Path(os.getenv("CACHE_DIR", "cache"))
STATIC_DIR = Path(os.getenv("STATIC_DIR", "static"))

LIVE_CFG_FILE = CACHE_DIR / "live_config.json"

DATA_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
STATIC_DIR.mkdir(parents=True, exist_ok=True)

# --------- FastAPI példány ---------
app = FastAPI(
    title="Bluestar Bus — API",
    version=APP_VERSION,
    docs_url="/api",
    redoc_url=None,
    openapi_url="/api/openapi.json",
)

# CORS – ha szeretnéd, itt szigorítható
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --------- Hasznos segédek ---------
def uk_time_str() -> str:
    # A Railway konténerben nincs zónaadat, ezért stringet adunk vissza.
    # A frontendnek csak kijelzésre kell.
    return datetime.utcnow().strftime("%H:%M:%S")


def read_live_cfg() -> Dict[str, Any]:
    if LIVE_CFG_FILE.exists():
        try:
            return json.loads(LIVE_CFG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"feed_url": None, "api_key": None}


def write_live_cfg(cfg: Dict[str, Any]) -> None:
    LIVE_CFG_FILE.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


def mask_key(key: Optional[str]) -> Optional[str]:
    if not key:
        return key
    if len(key) <= 6:
        return "*" * len(key)
    return key[:3] + "…" + key[-3:]


def gtfs_loaded() -> bool:
    try:
        if gtfs and hasattr(gtfs, "is_loaded"):
            return bool(gtfs.is_loaded())
        # ha nincs is_loaded(), de van belőle bármilyen jelzés
        return getattr(gtfs, "_loaded", False)
    except Exception:
        return False


# --------- Életciklus: GTFS betöltés ---------
@app.on_event("startup")
async def _startup():
    # GTFS betöltés, ha van modul és adat
    if gtfs and hasattr(gtfs, "load"):
        try:
            gtfs.load(base_dir=str(Path("gtfs")), cache_dir=str(CACHE_DIR))
        except TypeError:
            # régebbi/eltérő szignatúra
            try:
                gtfs.load()
            except Exception:
                pass
    # élő feed config fájlból beolvasva (ha nincs, létrejön később POST-tal)
    read_live_cfg()


# ---------------- API VÉGPONTOK ----------------

@app.get("/api/status")
def api_status():
    cfg = read_live_cfg()
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": str(BUILD),
        "uk_time": uk_time_str(),
        "tz": TZ,
        "live_feed_configured": bool(cfg.get("feed_url")),
        "gtfs_loaded": gtfs_loaded(),
    }


@app.get("/api/live/config")
def get_live_config():
    cfg = read_live_cfg()
    # api_key-et maszkoljuk a válaszban
    return {
        "feed_url": cfg.get("feed_url"),
        "api_key": mask_key(cfg.get("api_key")),
    }


@app.post("/api/live/config")
def set_live_config(payload: Dict[str, Optional[str]] = Body(..., example={
    "feed_url": "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/",
    "api_key": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
})):
    feed_url = payload.get("feed_url")
    api_key = payload.get("api_key")
    if not feed_url:
        raise HTTPException(status_code=400, detail="feed_url is required")
    cfg = {"feed_url": feed_url, "api_key": api_key}
    write_live_cfg(cfg)
    return {"ok": True}


@app.get("/api/live/stop-search")
def stop_search(q: str = Query(..., alias="query", min_length=1, max_length=64)):
    """
    Megállók keresése névrészlet alapján.
    Visszaad: [{id, name}]
    """
    if not gtfs:
        raise HTTPException(status_code=503, detail="GTFS module not available")

    try:
        # elvárt: gtfs.search_stops(query: str) -> List[Dict[id, name]]
        results = gtfs.search_stops(q)
    except AttributeError:
        # kompatibilitási fallback (ha más a név)
        if hasattr(gtfs, "find_stops"):
            results = gtfs.find_stops(q)
        else:
            raise HTTPException(status_code=500, detail="search_stops() not implemented in gtfs.py")

    # kis takarítás a formátumra
    cleaned: List[Dict[str, str]] = []
    for r in results or []:
        sid = r.get("id") or r.get("stop_id") or r.get("code")
        name = r.get("name") or r.get("stop_name")
        if sid and name:
            cleaned.append({"id": str(sid), "name": str(name)})

    return cleaned


@app.get("/api/live/departures")
async def live_departures(
    stop_id: str = Query(..., min_length=1),
    minutes: int = Query(60, ge=1, le=240),
    limit: Optional[int] = Query(None, ge=1, le=200),
):
    """
    Élő indulások egy megállóból.
    Visszaad: [{line, headsign, time_str, due_seconds, destination, platform}]
    """
    cfg = read_live_cfg()
    feed_url = cfg.get("feed_url")
    api_key = cfg.get("api_key")

    if not feed_url:
        raise HTTPException(status_code=400, detail="Live feed not configured. POST /api/live/config first.")

    # ha van saját siri_live modul
    if siri_live and hasattr(siri_live, "departures_for_stop"):
        try:
            rows = await siri_live.departures_for_stop(
                stop_id=stop_id, feed_url=feed_url, api_key=api_key, minutes=minutes
            )
        except TypeError:
            # sync implementáció
            rows = siri_live.departures_for_stop(stop_id, feed_url, api_key, minutes)
    else:
        # egyszerű (generikus) SIRI-VM lehívás, ha nincs siri_live modul
        # Feltételezzük, hogy a feed_url végére ?api_key=… kell; ha nem, a siri_live modul a biztos megoldás.
        params = {}
        if api_key:
            params["api_key"] = api_key

        try:
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(feed_url, params=params)
                r.raise_for_status()
                data = r.json()
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"Live feed error: {e}")

        # Itt a konkrét SIRI/producer formátum eltérhet; próbálunk kezelhető, egységes listát csinálni.
        rows = []
        # Példa normalizálás – ha más a szerkezet, a siri_live modullal dolgozz!
        visits = (
            data.get("Siri", {})
            .get("ServiceDelivery", {})
            .get("StopMonitoringDelivery", [{}])[0]
            .get("MonitoredStopVisit", [])
        )
        for v in visits:
            mv = v.get("MonitoredVehicleJourney", {})
            line = mv.get("LineRef") or mv.get("PublishedLineName")
            dest = (mv.get("DestinationName") or mv.get("DirectionName") or "").strip()
            call = mv.get("MonitoredCall", {})
            aimed = call.get("AimedDepartureTime") or call.get("AimedArrivalTime")
            expected = call.get("ExpectedDepartureTime") or call.get("ExpectedArrivalTime") or aimed
            time_str = expected or aimed
            rows.append(
                {
                    "line": str(line) if line is not None else "",
                    "headsign": dest,
                    "time_str": time_str,
                    "destination": dest,
                    "platform": call.get("DeparturePlatformName"),
                }
            )

    # limitálás, egyszerű rendezés (ha van due_seconds mező, arra; különben time_str)
    def sort_key(it: Dict[str, Any]):
        return (
            it.get("due_seconds", 999999),
            it.get("time_str", ""),
        )

    rows = sorted(rows, key=sort_key)
    if limit:
        rows = rows[:limit]
    return rows


# ---------------- FRONTEND ----------------
# A / alatt a static/index.html töltődik be (PWA/kliens).
app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")
