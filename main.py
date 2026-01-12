import os
import csv
import time
import asyncio
from dataclasses import dataclass
from datetime import datetime, date
from typing import Optional, Dict, List, Tuple, Any

from zoneinfo import ZoneInfo
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles

import httpx
from xml.etree import ElementTree as ET


# =========================
# Config
# =========================
UK_TZ = ZoneInfo("Europe/London")

DFT_API_KEY = os.getenv("DFT_API_KEY", "").strip()
DFT_BASE_URL = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/"

DEFAULT_OPERATOR_REFS = os.getenv("DEFAULT_OPERATOR_REFS", "BLUS,UNIL").strip()
GTFS_DIR = os.getenv("GTFS_DIR", "gtfs").strip()

LIVE_CACHE_TTL_SECONDS = int(os.getenv("LIVE_CACHE_TTL_SECONDS", "10"))  # csökkenti 403 kockázatot
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))

APP_NAME_UA = os.getenv("APP_USER_AGENT", "Bluestar-Unilink-App/1.0 (FastAPI; contact: dev)")


# =========================
# Helpers
# =========================
def _now_uk() -> datetime:
    return datetime.now(UK_TZ)


def _parse_int_loose(value: Optional[str], default: int, min_v: int = 1, max_v: int = 200) -> int:
    """
    Kibékíti az olyan inputot mint: "5/" vagy " 25 " vagy "25abc".
    """
    if value is None:
        return default
    s = str(value).strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    if not digits:
        return default
    try:
        n = int(digits)
    except ValueError:
        return default
    n = max(min_v, min(max_v, n))
    return n


def _split_csv_param(value: str) -> List[str]:
    items = []
    for part in (value or "").split(","):
        p = part.strip()
        if p:
            items.append(p)
    return items


def _safe_excerpt(text: str, limit: int = 600) -> str:
    text = (text or "").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "…"


# =========================
# GTFS (minimal, de használható)
# =========================
@dataclass
class Stop:
    stop_id: str
    stop_name: str
    stop_lat: Optional[float]
    stop_lon: Optional[float]


@dataclass
class Route:
    route_id: str
    route_short_name: str
    route_long_name: str


@dataclass
class Trip:
    trip_id: str
    route_id: str
    service_id: str
    trip_headsign: str
    direction_id: Optional[str]
    shape_id: Optional[str]


@dataclass
class StopTime:
    trip_id: str
    arrival_time: str
    departure_time: str
    stop_id: str
    stop_sequence: int


@dataclass
class ShapePoint:
    shape_id: str
    shape_pt_lat: float
    shape_pt_lon: float
    shape_pt_sequence: int


@dataclass
class CalendarService:
    service_id: str
    start_date: date
    end_date: date
    mon: bool
    tue: bool
    wed: bool
    thu: bool
    fri: bool
    sat: bool
    sun: bool


# In-memory store
GTFS: Dict[str, Any] = {
    "loaded": False,
    "stops": {},             # stop_id -> Stop
    "routes": {},            # route_id -> Route
    "trips": {},             # trip_id -> Trip
    "stop_times_by_stop": {},# stop_id -> List[StopTime]
    "stop_times_by_trip": {},# trip_id -> List[StopTime]
    "shapes": {},            # shape_id -> List[ShapePoint]
    "calendar": {},          # service_id -> CalendarService
    "calendar_dates": {},    # (service_id, date)-> exception_type (1 add, 2 remove)
}


def _read_csv(path: str) -> List[Dict[str, str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def _parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def _gtfs_time_to_minutes(t: str) -> Optional[int]:
    """
    GTFS idő lehet 24:10:00 is.
    """
    if not t:
        return None
    parts = t.split(":")
    if len(parts) < 2:
        return None
    try:
        hh = int(parts[0])
        mm = int(parts[1])
        ss = int(parts[2]) if len(parts) > 2 else 0
    except ValueError:
        return None
    return hh * 60 + mm + (1 if ss >= 30 else 0)


def _service_runs_on(service_id: str, d: date) -> bool:
    cal: Optional[CalendarService] = GTFS["calendar"].get(service_id)
    # exceptions:
    ex = GTFS["calendar_dates"].get((service_id, d))
    if ex == 2:
        return False
    if ex == 1:
        return True

    if not cal:
        # ha nincs calendar, inkább engedjük (külön feedek néha csak calendar_dates-et használnak)
        return True

    if d < cal.start_date or d > cal.end_date:
        return False

    wd = d.weekday()  # mon=0
    flags = [cal.mon, cal.tue, cal.wed, cal.thu, cal.fri, cal.sat, cal.sun]
    return bool(flags[wd])


def load_gtfs() -> None:
    if GTFS["loaded"]:
        return

    base = GTFS_DIR
    if not os.path.isdir(base):
        # nincs GTFS mappa – a live ettől még működik
        GTFS["loaded"] = True
        return

    # Stops
    stops_path = os.path.join(base, "stops.txt")
    if os.path.exists(stops_path):
        for row in _read_csv(stops_path):
            sid = row.get("stop_id", "").strip()
            if not sid:
                continue
            lat = row.get("stop_lat", "").strip()
            lon = row.get("stop_lon", "").strip()
            GTFS["stops"][sid] = Stop(
                stop_id=sid,
                stop_name=(row.get("stop_name", "") or "").strip(),
                stop_lat=float(lat) if lat else None,
                stop_lon=float(lon) if lon else None,
            )

    # Routes
    routes_path = os.path.join(base, "routes.txt")
    if os.path.exists(routes_path):
        for row in _read_csv(routes_path):
            rid = row.get("route_id", "").strip()
            if not rid:
                continue
            GTFS["routes"][rid] = Route(
                route_id=rid,
                route_short_name=(row.get("route_short_name", "") or "").strip(),
                route_long_name=(row.get("route_long_name", "") or "").strip(),
            )

    # Trips
    trips_path = os.path.join(base, "trips.txt")
    if os.path.exists(trips_path):
        for row in _read_csv(trips_path):
            tid = row.get("trip_id", "").strip()
            if not tid:
                continue
            GTFS["trips"][tid] = Trip(
                trip_id=tid,
                route_id=(row.get("route_id", "") or "").strip(),
                service_id=(row.get("service_id", "") or "").strip(),
                trip_headsign=(row.get("trip_headsign", "") or "").strip(),
                direction_id=(row.get("direction_id", "") or "").strip() or None,
                shape_id=(row.get("shape_id", "") or "").strip() or None,
            )

    # Calendar
    cal_path = os.path.join(base, "calendar.txt")
    if os.path.exists(cal_path):
        for row in _read_csv(cal_path):
            sid = (row.get("service_id", "") or "").strip()
            if not sid:
                continue
            GTFS["calendar"][sid] = CalendarService(
                service_id=sid,
                start_date=_parse_yyyymmdd((row.get("start_date") or "").strip()),
                end_date=_parse_yyyymmdd((row.get("end_date") or "").strip()),
                mon=(row.get("monday") == "1"),
                tue=(row.get("tuesday") == "1"),
                wed=(row.get("wednesday") == "1"),
                thu=(row.get("thursday") == "1"),
                fri=(row.get("friday") == "1"),
                sat=(row.get("saturday") == "1"),
                sun=(row.get("sunday") == "1"),
            )

    # Calendar dates
    cald_path = os.path.join(base, "calendar_dates.txt")
    if os.path.exists(cald_path):
        for row in _read_csv(cald_path):
            sid = (row.get("service_id", "") or "").strip()
            ds = (row.get("date", "") or "").strip()
            et = (row.get("exception_type", "") or "").strip()
            if not (sid and ds and et):
                continue
            GTFS["calendar_dates"][(sid, _parse_yyyymmdd(ds))] = int(et)

    # Stop times
    st_path = os.path.join(base, "stop_times.txt")
    if os.path.exists(st_path):
        by_stop: Dict[str, List[StopTime]] = {}
        by_trip: Dict[str, List[StopTime]] = {}
        for row in _read_csv(st_path):
            tid = (row.get("trip_id", "") or "").strip()
            sid = (row.get("stop_id", "") or "").strip()
            if not (tid and sid):
                continue
            seq_s = (row.get("stop_sequence", "") or "0").strip()
            try:
                seq = int(seq_s)
            except ValueError:
                seq = 0
            st = StopTime(
                trip_id=tid,
                arrival_time=(row.get("arrival_time", "") or "").strip(),
                departure_time=(row.get("departure_time", "") or "").strip(),
                stop_id=sid,
                stop_sequence=seq,
            )
            by_stop.setdefault(sid, []).append(st)
            by_trip.setdefault(tid, []).append(st)

        # sort
        for sid, lst in by_stop.items():
            lst.sort(key=lambda x: (_gtfs_time_to_minutes(x.departure_time) or 10**9, x.stop_sequence))
        for tid, lst in by_trip.items():
            lst.sort(key=lambda x: x.stop_sequence)

        GTFS["stop_times_by_stop"] = by_stop
        GTFS["stop_times_by_trip"] = by_trip

    # Shapes
    shapes_path = os.path.join(base, "shapes.txt")
    if os.path.exists(shapes_path):
        shapes: Dict[str, List[ShapePoint]] = {}
        for row in _read_csv(shapes_path):
            shid = (row.get("shape_id", "") or "").strip()
            if not shid:
                continue
            try:
                lat = float((row.get("shape_pt_lat") or "").strip())
                lon = float((row.get("shape_pt_lon") or "").strip())
                seq = int((row.get("shape_pt_sequence") or "0").strip())
            except ValueError:
                continue
            shapes.setdefault(shid, []).append(ShapePoint(shid, lat, lon, seq))
        for shid, lst in shapes.items():
            lst.sort(key=lambda x: x.shape_pt_sequence)
        GTFS["shapes"] = shapes

    GTFS["loaded"] = True


# =========================
# LIVE (DfT) cache + fetch
# =========================
_live_cache: Dict[Tuple[Tuple[str, str], ...], Tuple[float, bytes, int, str]] = {}
_live_lock = asyncio.Lock()


def _cache_key(params: Dict[str, str]) -> Tuple[Tuple[str, str], ...]:
    return tuple(sorted((k, v) for k, v in params.items() if v is not None and v != ""))


async def fetch_dft_xml(params: Dict[str, str]) -> Tuple[bytes, int, str]:
    if not DFT_API_KEY:
        raise HTTPException(status_code=500, detail="DFT_API_KEY nincs beállítva (Railway env var).")

    full_params = dict(params)
    full_params["api_key"] = DFT_API_KEY

    headers = {
        "User-Agent": APP_NAME_UA,
        "Accept": "application/xml,text/xml,*/*",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

    async with httpx.AsyncClient(
        timeout=HTTP_TIMEOUT_SECONDS,
        headers=headers,
        http2=True,
        follow_redirects=True,
    ) as client:
        r = await client.get(DFT_BASE_URL, params=full_params)

    content_type = (r.headers.get("content-type") or "").lower()
    text_excerpt = ""
    if "text" in content_type or "html" in content_type:
        try:
            text_excerpt = _safe_excerpt(r.text, 900)
        except Exception:
            text_excerpt = ""
    return (r.content, r.status_code, text_excerpt)


def parse_siri_vm(xml_bytes: bytes) -> List[Dict[str, Any]]:
    """
    SIRI-VM: VehicleMonitoringDelivery / VehicleActivity / MonitoredVehicleJourney
    """
    if not xml_bytes:
        return []

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return []

    ns = {"siri": "http://www.siri.org.uk/siri"}

    vehicles: List[Dict[str, Any]] = []
    for va in root.findall(".//siri:VehicleActivity", ns):
        recorded_at = va.findtext("./siri:RecordedAtTime", default="", namespaces=ns) or ""
        mvj = va.find("./siri:MonitoredVehicleJourney", ns)
        if mvj is None:
            continue

        line_ref = mvj.findtext("./siri:LineRef", default="", namespaces=ns) or ""
        published_line = mvj.findtext("./siri:PublishedLineName", default="", namespaces=ns) or ""
        operator_ref = mvj.findtext("./siri:OperatorRef", default="", namespaces=ns) or ""
        direction_ref = mvj.findtext("./siri:DirectionRef", default="", namespaces=ns) or ""
        vehicle_ref = mvj.findtext("./siri:VehicleRef", default="", namespaces=ns) or ""

        origin_ref = mvj.findtext("./siri:OriginRef", default="", namespaces=ns) or ""
        origin_name = mvj.findtext("./siri:OriginName", default="", namespaces=ns) or ""
        dest_ref = mvj.findtext("./siri:DestinationRef", default="", namespaces=ns) or ""
        dest_name = mvj.findtext("./siri:DestinationName", default="", namespaces=ns) or ""

        lon = mvj.findtext("./siri:VehicleLocation/siri:Longitude", default="", namespaces=ns) or ""
        lat = mvj.findtext("./siri:VehicleLocation/siri:Latitude", default="", namespaces=ns) or ""
        bearing = mvj.findtext("./siri:Bearing", default="", namespaces=ns) or ""
        block_ref = mvj.findtext("./siri:BlockRef", default="", namespaces=ns) or ""

        # FramedVehicleJourneyRef bits (nem mindig kell, de hasznos)
        dated_vjr = mvj.findtext("./siri:FramedVehicleJourneyRef/siri:DatedVehicleJourneyRef", default="", namespaces=ns) or ""
        data_frame = mvj.findtext("./siri:FramedVehicleJourneyRef/siri:DataFrameRef", default="", namespaces=ns) or ""

        # Extensions / JourneyCode (nem minden szolgáltatónál)
        journey_code = ""
        ext = va.find("./siri:Extensions", ns)
        if ext is not None:
            # lazán keresünk bármely JourneyCode tagre
            jc = ext.find(".//JourneyCode")
            if jc is not None and (jc.text or "").strip():
                journey_code = (jc.text or "").strip()

        try:
            lat_f = float(lat) if lat else None
        except ValueError:
            lat_f = None
        try:
            lon_f = float(lon) if lon else None
        except ValueError:
            lon_f = None
        try:
            bearing_f = float(bearing) if bearing else None
        except ValueError:
            bearing_f = None

        vehicles.append({
            "recorded_at": recorded_at,
            "operatorRef": operator_ref,
            "lineRef": line_ref,
            "publishedLineName": published_line,
            "directionRef": direction_ref,
            "vehicleRef": vehicle_ref,
            "originRef": origin_ref,
            "originName": origin_name,
            "destinationRef": dest_ref,
            "destinationName": dest_name,
            "latitude": lat_f,
            "longitude": lon_f,
            "bearing": bearing_f,
            "blockRef": block_ref,
            "datedVehicleJourneyRef": dated_vjr,
            "dataFrameRef": data_frame,
            "journeyCode": journey_code,
        })

    return vehicles


async def get_live_cached(params: Dict[str, str]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    key = _cache_key(params)
    now = time.time()

    async with _live_lock:
        cached = _live_cache.get(key)
        if cached:
            ts, xml_bytes, status, excerpt = cached
            if now - ts <= LIVE_CACHE_TTL_SECONDS and status == 200 and xml_bytes:
                return parse_siri_vm(xml_bytes), {"cached": True, "status": status, "excerpt": excerpt}

    xml_bytes, status, excerpt = await fetch_dft_xml(params)

    async with _live_lock:
        _live_cache[key] = (now, xml_bytes, status, excerpt)

    if status != 200:
        # adjunk vissza értelmes hibát
        raise HTTPException(
            status_code=status,
            detail={
                "message": "DfT BODS hiba",
                "status": status,
                "excerpt": excerpt or "No excerpt",
                "hint": "Ellenőrizd az API key-t, és hogy nem túl sűrűn hívod. A backend cache-eli 10s-ig.",
            },
        )

    return parse_siri_vm(xml_bytes), {"cached": False, "status": status, "excerpt": excerpt}


# =========================
# FastAPI app
# =========================
app = FastAPI(title="Bluestar & Unilink API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # saját domainre szűkítheted
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _startup():
    load_gtfs()


# Static front-end
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", response_class=HTMLResponse)
async def root():
    # serve static/index.html if exists
    idx = os.path.join("static", "index.html")
    if os.path.exists(idx):
        return FileResponse(idx)
    return HTMLResponse("<h1>API OK</h1><p>Hiányzik a static/index.html</p>")


@app.get("/health")
async def health():
    return {
        "ok": True,
        "time_uk": _now_uk().isoformat(),
        "gtfs_loaded": bool(GTFS["loaded"]),
        "has_dft_key": bool(DFT_API_KEY),
        "default_operator_refs": DEFAULT_OPERATOR_REFS,
    }


# =========================
# LIVE endpoints
# =========================
@app.get("/api/live/vehicles")
async def live_vehicles(
    operatorRef: str = Query(DEFAULT_OPERATOR_REFS, description="pl: BLUS,UNIL"),
    lineRef: Optional[str] = Query(None),
    vehicleRef: Optional[str] = Query(None),
    boundingBox: Optional[str] = Query(None, description="minLon,minLat,maxLon,maxLat"),
    originRef: Optional[str] = Query(None),
    destinationRef: Optional[str] = Query(None),
    routeId: Optional[str] = Query(None),
    startTimeAfter: Optional[str] = Query(None),
    startTimeBefore: Optional[str] = Query(None),
):
    """
    Proxy + parse: SIRI-VM -> JSON
    """
    params: Dict[str, str] = {}

    # DfT API a docs szerint operatorRef-et vár (NOC)
    # Többet úgy küldünk: BLUS,UNIL
    params["operatorRef"] = operatorRef

    if lineRef:
        params["lineRef"] = lineRef
    if vehicleRef:
        params["vehicleRef"] = vehicleRef
    if boundingBox:
        params["boundingBox"] = boundingBox
    if originRef:
        params["originRef"] = originRef
    if destinationRef:
        params["destinationRef"] = destinationRef
    if routeId:
        params["routeId"] = routeId
    if startTimeAfter:
        params["startTimeAfter"] = startTimeAfter
    if startTimeBefore:
        params["startTimeBefore"] = startTimeBefore

    vehicles, meta = await get_live_cached(params)

    # opcionális gyors filter PublishedLineName alapján a frontendnek
    return {
        "meta": meta,
        "count": len(vehicles),
        "vehicles": vehicles,
    }


@app.get("/api/live/raw")
async def live_raw(
    operatorRef: str = Query(DEFAULT_OPERATOR_REFS),
):
    params = {"operatorRef": operatorRef}
    xml_bytes, status, excerpt = await fetch_dft_xml(params)
    if status != 200:
        return JSONResponse(
            status_code=status,
            content={"status": status, "excerpt": excerpt},
        )
    return Response(content=xml_bytes, media_type="application/xml")


@app.get("/api/live/test")
async def live_test(operatorRef: str = Query(DEFAULT_OPERATOR_REFS)):
    """
    Debug: Railway-ről látod, hogy 200 vagy 403, és kapsz excerptet.
    """
    params = {"operatorRef": operatorRef}
    xml_bytes, status, excerpt = await fetch_dft_xml(params)
    return {
        "status": status,
        "content_bytes": len(xml_bytes or b""),
        "excerpt": excerpt,
        "hint": "Ha 403: ellenőrizd az API key-t + cache TTL-t; és ne hívd másodpercenként.",
    }


# =========================
# GTFS endpoints
# =========================
@app.get("/api/stops/search")
async def stops_search(
    q: str = Query("", description="név vagy stop_id részlet"),
    limit: str = Query("20", description="int, de elfogadunk '20/'-t is"),
):
    load_gtfs()
    lim = _parse_int_loose(limit, 20, 1, 100)

    qn = (q or "").strip().lower()
    results: List[Dict[str, Any]] = []

    if not qn:
        return {"count": 0, "stops": []}

    for s in GTFS["stops"].values():
        if qn in s.stop_id.lower() or qn in s.stop_name.lower():
            results.append({
                "stop_id": s.stop_id,
                "stop_name": s.stop_name,
                "stop_lat": s.stop_lat,
                "stop_lon": s.stop_lon,
            })
            if len(results) >= lim:
                break

    return {"count": len(results), "stops": results}


@app.get("/api/stop/{stop_id}")
async def stop_info(stop_id: str):
    load_gtfs()
    s: Optional[Stop] = GTFS["stops"].get(stop_id)
    if not s:
        raise HTTPException(status_code=404, detail="Stop not found")
    return {
        "stop_id": s.stop_id,
        "stop_name": s.stop_name,
        "stop_lat": s.stop_lat,
        "stop_lon": s.stop_lon,
    }


@app.get("/api/stop/{stop_id}/departures")
async def stop_departures(
    stop_id: str,
    service_date: str = Query("", description="YYYY-MM-DD"),
    time_hhmm: str = Query("", alias="time", description="HH:MM (opcionális)"),
    max_results: str = Query("20", description="int, de elfogadunk '20/'-t is"),
):
    load_gtfs()

    lim = _parse_int_loose(max_results, 20, 1, 200)

    if not service_date:
        d = _now_uk().date()
    else:
        try:
            d = datetime.strptime(service_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="service_date format: YYYY-MM-DD")

    if time_hhmm:
        try:
            hh, mm = time_hhmm.split(":")
            now_min = int(hh) * 60 + int(mm)
        except Exception:
            raise HTTPException(status_code=400, detail="time format: HH:MM")
    else:
        # ha ma: most, ha nem ma: 00:00
        now_min = (_now_uk().hour * 60 + _now_uk().minute) if d == _now_uk().date() else 0

    st_list: List[StopTime] = GTFS["stop_times_by_stop"].get(stop_id, [])
    out: List[Dict[str, Any]] = []

    for st in st_list:
        trip: Optional[Trip] = GTFS["trips"].get(st.trip_id)
        if not trip:
            continue

        # calendar filter
        if trip.service_id and not _service_runs_on(trip.service_id, d):
            continue

        dep_min = _gtfs_time_to_minutes(st.departure_time)
        if dep_min is None:
            continue

        if dep_min < now_min:
            continue

        route = GTFS["routes"].get(trip.route_id)
        out.append({
            "trip_id": st.trip_id,
            "route_id": trip.route_id,
            "route_short_name": route.route_short_name if route else "",
            "route_long_name": route.route_long_name if route else "",
            "headsign": trip.trip_headsign,
            "direction_id": trip.direction_id,
            "departure_time": st.departure_time,
        })

        if len(out) >= lim:
            break

    stop_obj = GTFS["stops"].get(stop_id)
    return {
        "stop": {
            "stop_id": stop_id,
            "stop_name": stop_obj.stop_name if stop_obj else stop_id,
        },
        "service_date": d.isoformat(),
        "time_from": f"{now_min//60:02d}:{now_min%60:02d}",
        "count": len(out),
        "departures": out,
    }


@app.get("/api/trip")
async def trip_detail(
    trip_id: str = Query("", description="GTFS trip_id"),
    service_date: str = Query("", description="YYYY-MM-DD (opcionális)"),
):
    load_gtfs()
    if not trip_id:
        raise HTTPException(status_code=400, detail="trip_id required")

    trip: Optional[Trip] = GTFS["trips"].get(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")

    d = _now_uk().date()
    if service_date:
        try:
            d = datetime.strptime(service_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="service_date format: YYYY-MM-DD")

    route = GTFS["routes"].get(trip.route_id)

    sts: List[StopTime] = GTFS["stop_times_by_trip"].get(trip_id, [])
    stops_out: List[Dict[str, Any]] = []
    for st in sts:
        s = GTFS["stops"].get(st.stop_id)
        stops_out.append({
            "stop_id": st.stop_id,
            "stop_name": s.stop_name if s else st.stop_id,
            "stop_lat": s.stop_lat if s else None,
            "stop_lon": s.stop_lon if s else None,
            "arrival_time": st.arrival_time,
            "departure_time": st.departure_time,
            "stop_sequence": st.stop_sequence,
        })

    shape_points: List[Dict[str, float]] = []
    if trip.shape_id and trip.shape_id in GTFS["shapes"]:
        for p in GTFS["shapes"][trip.shape_id]:
            shape_points.append({"lat": p.shape_pt_lat, "lon": p.shape_pt_lon})

    return {
        "trip_id": trip.trip_id,
        "service_date": d.isoformat(),
        "route": {
            "route_id": trip.route_id,
            "route_short_name": route.route_short_name if route else "",
            "route_long_name": route.route_long_name if route else "",
        },
        "headsign": trip.trip_headsign,
        "direction_id": trip.direction_id,
        "shape_id": trip.shape_id,
        "shape": shape_points,
        "stops": stops_out,
    }
