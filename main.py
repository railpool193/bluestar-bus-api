from __future__ import annotations

import csv
import json
import os
import zipfile
from io import BytesIO, TextIOWrapper
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime, timezone

import httpx
import xml.etree.ElementTree as ET

from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

# ---------- App meta ----------
APP_VERSION = "5.0.0"
BUILD = os.getenv("BUILD", str(int(datetime.now().timestamp())))
TZ_LABEL = "Europe/Budapest"  # tetszőlegesen átírható

# ---------- Paths ----------
DATA_DIR = Path("data"); DATA_DIR.mkdir(exist_ok=True)
GTFS_DIR = Path("gtfs"); GTFS_DIR.mkdir(exist_ok=True)
STOPS_TXT = GTFS_DIR / "stops.txt"
LIVE_CFG_FILE = DATA_DIR / "live_config.json"

# ---------- Helpers ----------
def now_local_str() -> str:
    try:
        import pytz
        tz = pytz.timezone(TZ_LABEL)
        return datetime.now(tz).strftime("%H:%M:%S")
    except Exception:
        return datetime.now(timezone.utc).strftime("%H:%M:%S")

def mask_url_key(url: str) -> str:
    from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
    try:
        pr = urlparse(url)
        qs = dict(parse_qsl(pr.query, keep_blank_values=True))
        if "api_key" in qs:
            v = qs["api_key"]
            qs["api_key"] = (v[:3] + "…" + v[-3:]) if len(v) > 6 else "•••"
        return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, urlencode(qs), pr.fragment))
    except Exception:
        return url

def parse_iso_dt(text: str) -> Optional[datetime]:
    try:
        if text.endswith("Z"): text = text.replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except Exception:
        return None

def minutes_until(dt: datetime) -> Optional[int]:
    try:
        now = datetime.now(timezone.utc)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return max(int((dt - now).total_seconds() // 60), 0)
    except Exception:
        return None

# ---------- Models ----------
class LiveConfig(BaseModel):
    feed_url: str = Field(..., description="SIRI-SM feed URL (api_key-del együtt)")

class StopOut(BaseModel):
    id: str
    name: str

class DepartureOut(BaseModel):
    line: str
    destination: str
    expected: str
    due_in_min: Optional[int] = None

# ---------- App ----------
app = FastAPI(
    title="Bluestar Bus — API",
    version=APP_VERSION,
    docs_url="/api",
    redoc_url=None,
    openapi_url="/api/openapi.json",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)

# ---------- Live config store ----------
def read_live_config() -> Optional[LiveConfig]:
    if LIVE_CFG_FILE.exists():
        try: return LiveConfig(**json.loads(LIVE_CFG_FILE.read_text("utf-8")))
        except Exception: return None
    return None

def write_live_config(cfg: LiveConfig) -> None:
    LIVE_CFG_FILE.write_text(cfg.model_dump_json(indent=2), "utf-8")

# ---------- GTFS stops loader ----------
_stops: List[Dict[str, str]] = []
def load_stops_from_filelike(f) -> int:
    global _stops
    reader = csv.DictReader(f)
    out = []
    for row in reader:
        sid = row.get("stop_id") or row.get("stopId") or ""
        snm = row.get("stop_name") or row.get("stopName") or ""
        if sid and snm: out.append({"id": sid, "name": snm})
    _stops = out
    return len(_stops)

def load_stops_from_disk() -> int:
    if STOPS_TXT.exists():
        with STOPS_TXT.open("r", encoding="utf-8-sig", newline="") as f:
            return load_stops_from_filelike(f)
    return 0

# initial load
load_stops_from_disk()

# ---------- API: status ----------
@app.get("/api/status")
async def api_status():
    cfg = read_live_config()
    return {
        "ok": True,
        "version": APP_VERSION,
        "build": BUILD,
        "time": now_local_str(),
        "tz": TZ_LABEL,
        "live_feed_configured": bool(cfg and cfg.feed_url),
        "gtfs_stops": len(_stops),
    }

# ---------- API: live config ----------
@app.get("/api/live/config")
async def get_live_config():
    cfg = read_live_config()
    return {"feed_url": mask_url_key(cfg.feed_url)} if cfg else {"feed_url": None}

@app.post("/api/live/config")
async def set_live_config(cfg: LiveConfig):
    if not (cfg.feed_url.startswith("http://") or cfg.feed_url.startswith("https://")):
        raise HTTPException(400, "feed_url must start with http(s)://")
    write_live_config(cfg)
    return {"ok": True}

# ---------- API: GTFS upload ----------
@app.post("/api/upload-gtfs")
async def upload_gtfs(file: UploadFile = File(...)):
    """
    Fogad: 
      - .zip (benne stops.txt)
      - sima stops.txt
    Mentés: gtfs/stops.txt
    """
    content = await file.read()
    name = (file.filename or "").lower()

    try:
        if name.endswith(".zip"):
            with zipfile.ZipFile(BytesIO(content)) as z:
                # keressük a stops.txt-t
                pick = None
                for info in z.infolist():
                    if info.filename.lower().endswith("stops.txt"):
                        pick = info; break
                if not pick:
                    raise HTTPException(400, "No stops.txt found in zip")
                with z.open(pick, "r") as fz:
                    text = TextIOWrapper(fz, encoding="utf-8-sig", newline="")
                    count = load_stops_from_filelike(text)
                    # mentsük is le:
                    z.extract(pick, GTFS_DIR)
                    (GTFS_DIR / pick.filename).rename(STOPS_TXT)
        else:
            # feltételezzük, hogy stops.txt
            with open(STOPS_TXT, "wb") as w:
                w.write(content)
            with open(STOPS_TXT, "r", encoding="utf-8-sig", newline="") as f:
                count = load_stops_from_filelike(f)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"GTFS upload parse error: {e}")

    return {"ok": True, "stops_loaded": count}

# ---------- API: stop search (FRONTEND NEVEZET = /api/live/stop-search) ----------
@app.get("/api/live/stop-search", response_model=List[StopOut])
async def stop_search(q: str = Query(..., min_length=1)):
    term = q.strip().lower()
    if not _stops: load_stops_from_disk()
    hits = []
    for s in _stops:
        if term in s["name"].lower():
            hits.append(StopOut(id=s["id"], name=s["name"]))
            if len(hits) >= 50: break
    return hits

# ---------- API: live departures (FRONTEND NEVEZET = /api/live/departures) ----------
async def fetch_siri_departures(feed_url: str, stop_id: str, window_min: int) -> List[DepartureOut]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(feed_url); r.raise_for_status()
        xml = r.text

    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return []

    ns = {"s": "http://www.siri.org.uk/siri"}
    out: List[DepartureOut] = []
    for msv in root.findall(".//s:MonitoredStopVisit", ns):
        sp = msv.findtext(".//s:StopPointRef", default="", namespaces=ns) or msv.findtext(".//StopPointRef", default="")
        if str(sp).strip() != str(stop_id).strip():
            continue
        line = msv.findtext(".//s:LineRef", default="", namespaces=ns) or msv.findtext(".//LineRef", default="")
        dest = msv.findtext(".//s:DestinationName", default="", namespaces=ns) or msv.findtext(".//DestinationName", default="")
        t = (
            msv.findtext(".//s:ExpectedDepartureTime", default="", namespaces=ns)
            or msv.findtext(".//ExpectedDepartureTime", default="")
            or msv.findtext(".//s:ExpectedArrivalTime", default="", namespaces=ns)
            or msv.findtext(".//ExpectedArrivalTime", default="")
            or msv.findtext(".//s:AimedDepartureTime", default="", namespaces=ns)
            or msv.findtext(".//AimedDepartureTime", default="")
        )
        dt = parse_iso_dt(t) if t else None
        hhmm = dt.astimezone(timezone.utc).strftime("%H:%M") if dt else "--:--"
        due = minutes_until(dt) if dt else None
        out.append(DepartureOut(line=line, destination=dest, expected=hhmm, due_in_min=due))

    if window_min is not None:
        out = [d for d in out if d.due_in_min is None or d.due_in_min <= window_min]
    out.sort(key=lambda d: d.due_in_min if d.due_in_min is not None else 10_000)
    return out

@app.get("/api/live/departures", response_model=List[DepartureOut])
async def live_departures(stopId: str = Query(...), window: int = Query(60, ge=1, le=240)):
    cfg = read_live_config()
    if not cfg: raise HTTPException(503, "Live feed is not configured")
    try:
        return await fetch_siri_departures(cfg.feed_url, stopId, window)
    except httpx.HTTPError as e:
        raise HTTPException(502, f"SIRI fetch failed: {e}")

# ---------- Static frontend on / ----------
app.mount("/", StaticFiles(directory="static", html=True), name="static")

# ---------- Local run ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8000")), reload=True)
