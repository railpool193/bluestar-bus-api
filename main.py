import io
import os
import re
import zipfile
from datetime import datetime, date, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# --- Optional dependency: requests. If not available, fall back to urllib.
try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

import csv
import xml.etree.ElementTree as ET
from zoneinfo import ZoneInfo


APP_TZ = ZoneInfo(os.getenv("APP_TZ", "Europe/London"))

BODS_API_KEY = os.getenv("BODS_API_KEY") or os.getenv("DFT_API_KEY") or os.getenv("API_KEY")
BODS_BASE_URL = os.getenv("BODS_BASE_URL", "https://data.bus-data.dft.gov.uk/api/v1/datafeed/")

GTFS_PATH = os.getenv("GTFS_ZIP_PATH") or os.getenv("GTFS_PATH") or "gtfs.zip"

DEFAULT_LIMIT = 25


def _now_local() -> datetime:
    return datetime.now(tz=APP_TZ)


def _parse_int_safely(v: Any, default: int, min_v: Optional[int] = None, max_v: Optional[int] = None) -> int:
    if v is None:
        out = default
    else:
        s = str(v).strip()
        s = s.rstrip("/")  # <- fix: "5/" should still parse
        s = re.sub(r"[^\d\-]+", "", s)
        try:
            out = int(s)
        except Exception:
            out = default
    if min_v is not None:
        out = max(min_v, out)
    if max_v is not None:
        out = min(max_v, out)
    return out


def _parse_bbox(bbox: str) -> Optional[str]:
    # Accept "minLon,minLat,maxLon,maxLat" (any spaces), return normalized string
    if not bbox:
        return None
    parts = [p.strip() for p in bbox.split(",")]
    if len(parts) != 4:
        return None
    try:
        _ = [float(p) for p in parts]
    except Exception:
        return None
    return ",".join(parts)


def _read_gtfs_file_from_zip(zf: zipfile.ZipFile, name: str) -> List[Dict[str, str]]:
    with zf.open(name) as f:
        # BODS GTFS tends to be UTF-8; handle BOM
        text = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
        reader = csv.DictReader(text)
        return [row for row in reader]


def _read_gtfs_file_from_dir(dir_path: Path, name: str) -> List[Dict[str, str]]:
    p = dir_path / name
    if not p.exists():
        return []
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def _gtfs_time_to_seconds(t: str) -> Optional[int]:
    if not t:
        return None
    m = re.match(r"^\s*(\d{1,3}):(\d{2}):(\d{2})\s*$", t)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2))
    ss = int(m.group(3))
    return hh * 3600 + mm * 60 + ss


def _seconds_to_gtfs_time(sec: int) -> str:
    if sec < 0:
        sec = 0
    hh = sec // 3600
    mm = (sec % 3600) // 60
    ss = sec % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


class GTFSIndex:
    def __init__(self) -> None:
        self.loaded_at = _now_local()
        self.stops: Dict[str, Dict[str, str]] = {}
        self.routes: Dict[str, Dict[str, str]] = {}
        self.trips: Dict[str, Dict[str, str]] = {}
        self.stop_times_by_stop: Dict[str, List[Tuple[int, str, int, str, str]]] = {}
        self.stop_times_by_trip: Dict[str, List[Tuple[int, str, str, int]]] = {}
        self.shapes: Dict[str, List[Tuple[int, float, float]]] = {}
        self.calendar: Dict[str, Dict[str, str]] = {}
        self.calendar_dates: Dict[Tuple[str, str], int] = {}  # (service_id, yyyymmdd) -> exception_type

    def service_active(self, service_id: str, on_date: date) -> bool:
        key = (service_id, _yyyymmdd(on_date))
        if key in self.calendar_dates:
            return self.calendar_dates[key] == 1  # 1 add, 2 remove
        cal = self.calendar.get(service_id)
        if not cal:
            # If no calendar.txt, some feeds rely purely on calendar_dates; default to True
            return True
        start = cal.get("start_date")
        end = cal.get("end_date")
        if start and _yyyymmdd(on_date) < start:
            return False
        if end and _yyyymmdd(on_date) > end:
            return False
        weekday = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][on_date.weekday()]
        return cal.get(weekday, "0") == "1"


@lru_cache(maxsize=1)
def load_gtfs() -> GTFSIndex:
    idx = GTFSIndex()
    path = Path(GTFS_PATH)
    is_zip = path.exists() and path.is_file() and path.suffix.lower() == ".zip"
    is_dir = path.exists() and path.is_dir()

    def read_file(name: str) -> List[Dict[str, str]]:
        if is_zip:
            with zipfile.ZipFile(path, "r") as zf:
                if name not in zf.namelist():
                    return []
                return _read_gtfs_file_from_zip(zf, name)
        if is_dir:
            return _read_gtfs_file_from_dir(path, name)
        # Try default folder "gtfs/"
        alt_dir = Path("gtfs")
        if alt_dir.exists() and alt_dir.is_dir():
            return _read_gtfs_file_from_dir(alt_dir, name)
        raise FileNotFoundError(
            f"GTFS not found. Set GTFS_ZIP_PATH/GTFS_PATH to a GTFS zip or folder. Tried: {path.resolve()}"
        )

    stops = read_file("stops.txt")
    for s in stops:
        if s.get("stop_id"):
            idx.stops[s["stop_id"]] = s

    routes = read_file("routes.txt")
    for r in routes:
        if r.get("route_id"):
            idx.routes[r["route_id"]] = r

    trips = read_file("trips.txt")
    for t in trips:
        if t.get("trip_id"):
            idx.trips[t["trip_id"]] = t

    calendar = read_file("calendar.txt")
    for c in calendar:
        if c.get("service_id"):
            idx.calendar[c["service_id"]] = c

    calendar_dates = read_file("calendar_dates.txt")
    for cd in calendar_dates:
        sid = cd.get("service_id")
        d = cd.get("date")
        et = cd.get("exception_type")
        if sid and d and et:
            try:
                idx.calendar_dates[(sid, d)] = int(et)
            except Exception:
                pass

    stop_times = read_file("stop_times.txt")
    for st in stop_times:
        trip_id = st.get("trip_id")
        stop_id = st.get("stop_id")
        seq = st.get("stop_sequence")
        dep = st.get("departure_time") or ""
        arr = st.get("arrival_time") or dep
        if not (trip_id and stop_id and seq):
            continue
        dep_sec = _gtfs_time_to_seconds(dep)
        if dep_sec is None:
            continue
        try:
            seq_i = int(seq)
        except Exception:
            continue

        idx.stop_times_by_stop.setdefault(stop_id, []).append((dep_sec, trip_id, seq_i, arr, dep))
        idx.stop_times_by_trip.setdefault(trip_id, []).append((seq_i, stop_id, dep, dep_sec))

    for stop_id, lst in idx.stop_times_by_stop.items():
        lst.sort(key=lambda x: x[0])

    for trip_id, lst in idx.stop_times_by_trip.items():
        lst.sort(key=lambda x: x[0])

    shapes = []
    try:
        shapes = read_file("shapes.txt")
    except Exception:
        shapes = []
    for sh in shapes:
        sid = sh.get("shape_id")
        lat = sh.get("shape_pt_lat")
        lon = sh.get("shape_pt_lon")
        seq = sh.get("shape_pt_sequence")
        if not (sid and lat and lon and seq):
            continue
        try:
            idx.shapes.setdefault(sid, []).append((int(seq), float(lat), float(lon)))
        except Exception:
            continue
    for sid, pts in idx.shapes.items():
        pts.sort(key=lambda x: x[0])

    return idx


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()


def _http_get(url: str, params: Dict[str, str], timeout: int = 20) -> Tuple[int, bytes, Dict[str, str]]:
    headers = {
        "User-Agent": "Bluestar-Unilink-App/1.0 (+https://example.local)",
        "Accept": "*/*",
    }
    if requests is None:
        import urllib.parse, urllib.request

        full = url + ("?" + urllib.parse.urlencode(params) if params else "")
        req = urllib.request.Request(full, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    else:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        return resp.status_code, resp.content, dict(resp.headers)


def _maybe_unzip_payload(payload: bytes) -> bytes:
    # Some endpoints may return a zip container (PK..). If so, extract first file.
    if payload[:4] == b"PK\x03\x04":
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as zf:
                names = [n for n in zf.namelist() if not n.endswith("/")]
                if not names:
                    return payload
                with zf.open(names[0]) as f:
                    return f.read()
        except Exception:
            return payload
    return payload


def _parse_siri_vm(payload: bytes) -> List[Dict[str, Any]]:
    payload = _maybe_unzip_payload(payload)

    # Sometimes APIs return JSON error bodies; detect early
    if payload[:1] in (b"{", b"["):
        # Not SIRI; return empty with metadata? We'll handle upstream.
        return []

    try:
        root = ET.fromstring(payload)
    except Exception:
        return []

    def text_of(elem: Optional[ET.Element]) -> Optional[str]:
        if elem is None or elem.text is None:
            return None
        return elem.text.strip()

    def find_desc(parent: ET.Element, tag_endswith: str) -> Optional[ET.Element]:
        for e in parent.iter():
            if e.tag.endswith(tag_endswith):
                return e
        return None

    out: List[Dict[str, Any]] = []

    for va in root.iter():
        if not va.tag.endswith("VehicleActivity"):
            continue
        mvj = find_desc(va, "MonitoredVehicleJourney")
        if mvj is None:
            continue

        vehicle_loc = find_desc(mvj, "VehicleLocation")
        lat = text_of(find_desc(vehicle_loc, "Latitude")) if vehicle_loc is not None else None
        lon = text_of(find_desc(vehicle_loc, "Longitude")) if vehicle_loc is not None else None

        rec = text_of(find_desc(va, "RecordedAtTime")) or text_of(find_desc(mvj, "RecordedAtTime"))

        item = {
            "vehicleRef": text_of(find_desc(mvj, "VehicleRef")),
            "vehicleJourneyRef": text_of(find_desc(mvj, "VehicleJourneyRef")),
            "operatorRef": text_of(find_desc(mvj, "OperatorRef")),
            "lineRef": text_of(find_desc(mvj, "LineRef")),
            "publishedLineName": text_of(find_desc(mvj, "PublishedLineName")),
            "directionRef": text_of(find_desc(mvj, "DirectionRef")),
            "originRef": text_of(find_desc(mvj, "OriginRef")),
            "originName": text_of(find_desc(mvj, "OriginName")),
            "destinationRef": text_of(find_desc(mvj, "DestinationRef")),
            "destinationName": text_of(find_desc(mvj, "DestinationName")),
            "bearing": text_of(find_desc(mvj, "Bearing")),
            "speed": text_of(find_desc(mvj, "Speed")),
            "blockRef": text_of(find_desc(mvj, "BlockRef")),
            "recordedAtTime": rec,
            "lat": float(lat) if lat is not None else None,
            "lon": float(lon) if lon is not None else None,
        }
        # only keep usable points
        if item["lat"] is None or item["lon"] is None:
            continue
        out.append(item)

    return out


app = FastAPI(title="Bluestar & Unilink API", version="2.0")

# CORS (useful if you ever host frontend separately)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.get("/api/health")
def health():
    return {"ok": True, "time": _now_local().isoformat()}


@app.get("/", response_class=HTMLResponse)
def index():
    # index.html is expected in ./static/index.html
    p = Path("static/index.html")
    if not p.exists():
        p = Path("index.html")
    if not p.exists():
        return HTMLResponse(
            "<h1>Missing static/index.html</h1><p>Upload index.html into a <code>static/</code> folder.</p>",
            status_code=500,
        )
    return HTMLResponse(p.read_text(encoding="utf-8"))


# Serve static assets (index.html, css, js, icons)
if Path("static").exists():
    app.mount("/static", StaticFiles(directory="static"), name="static")

# If someone visits /index.html directly
@app.get("/index.html", response_class=HTMLResponse)
def index_alias():
    return index()



@app.get("/api/stops/search")
def stops_search(q: str = Query("", min_length=1), limit: int = Query(20, ge=1, le=50)):
    idx = load_gtfs()
    nq = _norm(q)
    res = []
    for stop_id, s in idx.stops.items():
        name = s.get("stop_name", "")
        if nq in _norm(name) or nq in _norm(stop_id):
            res.append(
                {
                    "stop_id": stop_id,
                    "stop_name": name,
                    "lat": float(s["stop_lat"]) if s.get("stop_lat") else None,
                    "lon": float(s["stop_lon"]) if s.get("stop_lon") else None,
                }
            )
        if len(res) >= limit:
            break
    return {"results": res}


@app.get("/api/routes")
def routes_list(q: str = "", limit: int = Query(200, ge=1, le=1000)):
    idx = load_gtfs()
    nq = _norm(q)
    out = []
    for r in idx.routes.values():
        short = r.get("route_short_name", "") or ""
        longn = r.get("route_long_name", "") or ""
        if not nq or nq in _norm(short) or nq in _norm(longn):
            out.append(
                {
                    "route_id": r.get("route_id"),
                    "route_short_name": short,
                    "route_long_name": longn,
                    "route_color": r.get("route_color"),
                    "route_text_color": r.get("route_text_color"),
                }
            )
        if len(out) >= limit:
            break
    out.sort(key=lambda x: (_norm(x["route_short_name"] or ""), _norm(x["route_long_name"] or "")))
    return {"routes": out}


@app.get("/api/stop/{stop_id}")
def stop_detail(stop_id: str):
    idx = load_gtfs()
    s = idx.stops.get(stop_id)
    if not s:
        raise HTTPException(404, "Stop not found")
    return {
        "stop_id": stop_id,
        "stop_name": s.get("stop_name"),
        "lat": float(s["stop_lat"]) if s.get("stop_lat") else None,
        "lon": float(s["stop_lon"]) if s.get("stop_lon") else None,
    }


@app.get("/api/stop/{stop_id}/departures")
def stop_departures(
    stop_id: str,
    date_: Optional[str] = Query(None, alias="date"),
    time_: Optional[str] = Query(None, alias="time"),
    limit: Optional[str] = Query(None),
):
    idx = load_gtfs()
    if stop_id not in idx.stops:
        raise HTTPException(404, "Stop not found")

    lim = _parse_int_safely(limit, DEFAULT_LIMIT, min_v=1, max_v=200)

    now = _now_local()
    if date_:
        try:
            d = datetime.strptime(date_, "%Y-%m-%d").date()
        except Exception:
            raise HTTPException(400, "date must be YYYY-MM-DD")
    else:
        d = now.date()

    if time_:
        m = re.match(r"^\s*(\d{1,2}):(\d{2})(?::(\d{2}))?\s*$", time_)
        if not m:
            raise HTTPException(400, "time must be HH:MM or HH:MM:SS")
        hh = int(m.group(1))
        mm = int(m.group(2))
        ss = int(m.group(3) or 0)
        now_sec = hh * 3600 + mm * 60 + ss
    else:
        # Use current time if same date; otherwise start of day
        if d == now.date():
            now_sec = now.hour * 3600 + now.minute * 60 + now.second
        else:
            now_sec = 0

    st_list = idx.stop_times_by_stop.get(stop_id, [])
    if not st_list:
        return {"stop_id": stop_id, "date": d.isoformat(), "departures": []}

    out = []
    for dep_sec, trip_id, seq_i, arr_str, dep_str in st_list:
        if dep_sec < now_sec:
            continue
        trip = idx.trips.get(trip_id)
        if not trip:
            continue
        service_id = trip.get("service_id") or ""
        if service_id and not idx.service_active(service_id, d):
            continue
        route_id = trip.get("route_id")
        route = idx.routes.get(route_id or "", {})
        headsign = trip.get("trip_headsign") or ""
        out.append(
            {
                "trip_id": trip_id,
                "route_id": route_id,
                "route_short_name": route.get("route_short_name") or "",
                "route_long_name": route.get("route_long_name") or "",
                "headsign": headsign,
                "departure_time": dep_str,
                "departure_seconds": dep_sec,
            }
        )
        if len(out) >= lim:
            break

    # Add relative minutes
    if d == now.date():
        base = now.hour * 3600 + now.minute * 60 + now.second
        for item in out:
            item["in_minutes"] = max(0, int((item["departure_seconds"] - base + 59) // 60))
    else:
        for item in out:
            item["in_minutes"] = None

    return {"stop_id": stop_id, "date": d.isoformat(), "departures": out}


@app.get("/api/trip/{trip_id}")
def trip_detail(trip_id: str):
    idx = load_gtfs()
    trip = idx.trips.get(trip_id)
    if not trip:
        raise HTTPException(404, "Trip not found")

    route = idx.routes.get(trip.get("route_id") or "", {})
    st = idx.stop_times_by_trip.get(trip_id, [])
    stops = []
    for seq_i, stop_id, dep_str, dep_sec in st:
        s = idx.stops.get(stop_id, {})
        stops.append(
            {
                "stop_sequence": seq_i,
                "stop_id": stop_id,
                "stop_name": s.get("stop_name"),
                "lat": float(s["stop_lat"]) if s.get("stop_lat") else None,
                "lon": float(s["stop_lon"]) if s.get("stop_lon") else None,
                "departure_time": dep_str,
                "departure_seconds": dep_sec,
            }
        )

    shape_id = trip.get("shape_id") or ""
    shape_pts = []
    if shape_id and shape_id in idx.shapes:
        shape_pts = [{"lat": lat, "lon": lon, "seq": seq} for seq, lat, lon in idx.shapes[shape_id]]

    return {
        "trip_id": trip_id,
        "route_id": trip.get("route_id"),
        "route_short_name": route.get("route_short_name") or "",
        "route_long_name": route.get("route_long_name") or "",
        "route_color": route.get("route_color"),
        "trip_headsign": trip.get("trip_headsign"),
        "direction_id": trip.get("direction_id"),
        "shape_id": shape_id or None,
        "stops": stops,
        "shape": shape_pts,
    }


@app.get("/api/live/vehicles")
def live_vehicles(
    operatorRef: str = Query("", description="NOC codes, comma separated. e.g. BLUS,UNIL"),
    lineRef: str = Query("", description="Optional lineRef filter"),
    producerRef: str = Query("", description="Optional producerRef filter"),
    boundingBox: str = Query("", description="minLon,minLat,maxLon,maxLat"),
    routeId: str = Query("", description="GTFS-RT routeId filter (optional)"),
    startTimeAfter: str = Query("", description="GTFS-RT startTimeAfter filter (optional)"),
    startTimeBefore: str = Query("", description="GTFS-RT startTimeBefore filter (optional)"),
    max_results: Optional[str] = Query(None, description="Max vehicles to return (accepts '50' or '50/')"),
):
    if not BODS_API_KEY:
        raise HTTPException(503, "BODS_API_KEY is missing on the server (Railway Variables).")

    lim = _parse_int_safely(max_results, 250, min_v=1, max_v=1000)

    params: Dict[str, str] = {"api_key": BODS_API_KEY}

    if operatorRef.strip():
        # normalize: allow spaces and lowercase
        op = ",".join([p.strip().upper() for p in operatorRef.split(",") if p.strip()])
        if op:
            params["operatorRef"] = op
    if lineRef.strip():
        params["lineRef"] = lineRef.strip()
    if producerRef.strip():
        params["producerRef"] = producerRef.strip()

    bb = _parse_bbox(boundingBox)
    if bb:
        params["boundingBox"] = bb

    # Pass through GTFS-RT specific filters (harmless for SIRI; API will ignore if unsupported)
    if routeId.strip():
        params["routeId"] = routeId.strip()
    if startTimeAfter.strip():
        params["startTimeAfter"] = startTimeAfter.strip()
    if startTimeBefore.strip():
        params["startTimeBefore"] = startTimeBefore.strip()

    status, content, headers = _http_get(BODS_BASE_URL, params=params, timeout=25)

    if status != 200:
        # Provide readable error body for debugging
        body_preview = content[:4000].decode("utf-8", errors="replace")
        raise HTTPException(status, f"DfT BODS returned HTTP {status}. Body: {body_preview}")

    vehicles = _parse_siri_vm(content)

    # If parsing failed, still return debug info
    if not vehicles:
        body_preview = content[:2000].decode("utf-8", errors="replace")
        return JSONResponse(
            {
                "vehicles": [],
                "warning": "No vehicles parsed (feed may be empty, GTFS-RT, or schema changed).",
                "content_type": headers.get("Content-Type"),
                "body_preview": body_preview,
            }
        )

    return {"vehicles": vehicles[:lim], "count": len(vehicles)}


