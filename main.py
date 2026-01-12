import os
import io
import csv
import time
import gzip
import zipfile
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional, Tuple

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import FileResponse, JSONResponse

# -------------------------
# Config
# -------------------------
TZ = ZoneInfo(os.getenv("APP_TZ", "Europe/London"))

DFT_API_KEY = os.getenv("DFT_API_KEY", "").strip()
DFT_FEED_ID = os.getenv("DFT_FEED_ID", "7721").strip()
DFT_FEED_URL = os.getenv(
    "DFT_FEED_URL",
    f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{DFT_FEED_ID}/",
).strip()

GTFS_ZIP_PATH = os.getenv("GTFS_ZIP_PATH", "gtfs.zip")

# “Csak épp bejelentkezett/aktuális” definíció:
# - recordedAtTime friss (max ennyi másodperc)
# - validUntilTime még érvényes
# - DataFrameRef (ha dátum) egyezzen a mai nappal (Europe/London)
DEFAULT_MAX_AGE_SECONDS = int(os.getenv("LIVE_MAX_AGE_SECONDS", "240"))  # 4 perc

# -------------------------
# App
# -------------------------
app = FastAPI(title="Bluestar / Unilink – GTFS + LIVE")

# -------------------------
# Simple in-memory caches
# -------------------------
LIVE_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "ttl": 10.0,   # 10s cache a DfT felé
    "vehicles_all": [],
    "raw_count": 0,
    "ok": False,
    "error": None,
}

GTFS: Dict[str, Any] = {
    "ok": False,
    "error": None,
    "stops": {},
    "routes": {},
    "trips": {},
    "stop_times_by_stop": {},
    "stop_times_by_trip": {},
    "calendar": {},
    "calendar_dates": {},
}


# -------------------------
# Helpers
# -------------------------
def now_local() -> datetime:
    return datetime.now(TZ)

def parse_gtfs_date(s: str) -> date:
    # YYYYMMDD
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))

def parse_hms_to_seconds(s: str) -> int:
    # GTFS time can exceed 24:00:00
    h, m, sec = s.split(":")
    return int(h) * 3600 + int(m) * 60 + int(sec)

def safe_text(el: Optional[ET.Element]) -> Optional[str]:
    if el is None:
        return None
    t = el.text
    return t.strip() if t else None

def parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        # Accept both Z and +00:00
        s2 = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(TZ)
    except Exception:
        return None

def looks_like_date(s: Optional[str]) -> bool:
    if not s:
        return False
    # "YYYY-MM-DD"
    if len(s) != 10:
        return False
    return s[4] == "-" and s[7] == "-"

def decode_feed_bytes(content: bytes) -> bytes:
    # ZIP?
    if content[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(content), "r") as zf:
            # first file
            names = zf.namelist()
            if not names:
                return b""
            return zf.read(names[0])

    # GZIP?
    if content[:2] == b"\x1f\x8b":
        return gzip.decompress(content)

    return content


# -------------------------
# GTFS loader
# -------------------------
def load_gtfs() -> None:
    if not os.path.exists(GTFS_ZIP_PATH):
        GTFS["ok"] = False
        GTFS["error"] = f"GTFS zip not found: {GTFS_ZIP_PATH}"
        return

    try:
        with zipfile.ZipFile(GTFS_ZIP_PATH, "r") as zf:
            def read_csv(name: str) -> List[Dict[str, str]]:
                with zf.open(name) as f:
                    text = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
                    return list(csv.DictReader(text))

            stops_rows = read_csv("stops.txt")
            routes_rows = read_csv("routes.txt")
            trips_rows = read_csv("trips.txt")
            stop_times_rows = read_csv("stop_times.txt")

            calendar = {}
            calendar_dates = {}

            if "calendar.txt" in zf.namelist():
                for r in read_csv("calendar.txt"):
                    calendar[r["service_id"]] = {
                        "start_date": parse_gtfs_date(r["start_date"]),
                        "end_date": parse_gtfs_date(r["end_date"]),
                        "monday": r.get("monday") == "1",
                        "tuesday": r.get("tuesday") == "1",
                        "wednesday": r.get("wednesday") == "1",
                        "thursday": r.get("thursday") == "1",
                        "friday": r.get("friday") == "1",
                        "saturday": r.get("saturday") == "1",
                        "sunday": r.get("sunday") == "1",
                    }

            if "calendar_dates.txt" in zf.namelist():
                for r in read_csv("calendar_dates.txt"):
                    sid = r["service_id"]
                    d = parse_gtfs_date(r["date"])
                    ex = int(r["exception_type"])  # 1=added, 2=removed
                    calendar_dates.setdefault(sid, {})[d] = ex

            stops = {}
            for r in stops_rows:
                sid = r["stop_id"]
                stops[sid] = {
                    "stop_id": sid,
                    "stop_name": r.get("stop_name") or sid,
                    "stop_lat": float(r["stop_lat"]) if r.get("stop_lat") else None,
                    "stop_lon": float(r["stop_lon"]) if r.get("stop_lon") else None,
                }

            routes = {}
            for r in routes_rows:
                rid = r["route_id"]
                routes[rid] = {
                    "route_id": rid,
                    "short_name": (r.get("route_short_name") or "").strip(),
                    "long_name": (r.get("route_long_name") or "").strip(),
                }

            trips = {}
            for r in trips_rows:
                tid = r["trip_id"]
                trips[tid] = {
                    "trip_id": tid,
                    "route_id": r["route_id"],
                    "service_id": r["service_id"],
                    "headsign": (r.get("trip_headsign") or "").strip(),
                    "direction_id": r.get("direction_id"),
                }

            stop_times_by_stop: Dict[str, List[Tuple[int, str, int]]] = {}
            stop_times_by_trip: Dict[str, List[Dict[str, Any]]] = {}

            # store: by stop -> list of (dep_seconds, trip_id, stop_sequence)
            for r in stop_times_rows:
                trip_id = r["trip_id"]
                stop_id = r["stop_id"]
                dep = r.get("departure_time") or r.get("arrival_time")
                arr = r.get("arrival_time") or r.get("departure_time")
                if not dep:
                    continue
                dep_s = parse_hms_to_seconds(dep)
                arr_s = parse_hms_to_seconds(arr) if arr else dep_s
                seq = int(r.get("stop_sequence") or 0)

                stop_times_by_stop.setdefault(stop_id, []).append((dep_s, trip_id, seq))

                stop_times_by_trip.setdefault(trip_id, []).append({
                    "stop_id": stop_id,
                    "stop_sequence": seq,
                    "arrival_s": arr_s,
                    "departure_s": dep_s,
                })

            # sort
            for sid in stop_times_by_stop:
                stop_times_by_stop[sid].sort(key=lambda x: x[0])
            for tid in stop_times_by_trip:
                stop_times_by_trip[tid].sort(key=lambda x: x["stop_sequence"])

            GTFS.update({
                "ok": True,
                "error": None,
                "stops": stops,
                "routes": routes,
                "trips": trips,
                "stop_times_by_stop": stop_times_by_stop,
                "stop_times_by_trip": stop_times_by_trip,
                "calendar": calendar,
                "calendar_dates": calendar_dates,
            })

    except Exception as e:
        GTFS["ok"] = False
        GTFS["error"] = f"GTFS load error: {e}"


def service_active(service_id: str, service_date: date) -> bool:
    # calendar_dates override
    ex = GTFS["calendar_dates"].get(service_id, {}).get(service_date)
    if ex == 1:
        return True
    if ex == 2:
        return False

    cal = GTFS["calendar"].get(service_id)
    if not cal:
        # If no calendar.txt, assume active unless explicitly removed
        return True

    if service_date < cal["start_date"] or service_date > cal["end_date"]:
        return False

    wd = service_date.weekday()  # Mon=0..Sun=6
    if wd == 0:
        return cal["monday"]
    if wd == 1:
        return cal["tuesday"]
    if wd == 2:
        return cal["wednesday"]
    if wd == 3:
        return cal["thursday"]
    if wd == 4:
        return cal["friday"]
    if wd == 5:
        return cal["saturday"]
    return cal["sunday"]


def departure_datetime(service_date: date, dep_seconds: int) -> datetime:
    # Allow dep_seconds > 24h (next day)
    base = datetime(service_date.year, service_date.month, service_date.day, tzinfo=TZ)
    return base + timedelta(seconds=dep_seconds)


# -------------------------
# LIVE fetch + parse (SIRI-VM XML)
# -------------------------
def fetch_live_xml() -> bytes:
    if not DFT_API_KEY:
        raise RuntimeError("Missing DFT_API_KEY environment variable")

    resp = requests.get(DFT_FEED_URL, params={"api_key": DFT_API_KEY}, timeout=25)
    resp.raise_for_status()
    return decode_feed_bytes(resp.content)


def parse_siri_vm(xml_bytes: bytes) -> List[Dict[str, Any]]:
    """
    Return a list of vehicles:
    {
      recordedAtTime, validUntilTime, dataFrameRef,
      lineRef, publishedLineName, operatorRef, directionRef,
      originName, destinationName,
      longitude, latitude, bearing,
      vehicleRef,
      calls: [ {stopRef, stopName, aimedDep, expDep, vehicleAtStop} ... ],
      currentStopRef, vehicleAtStop,
    }
    """
    vehicles: List[Dict[str, Any]] = []
    if not xml_bytes:
        return vehicles

    root = ET.fromstring(xml_bytes)

    # There can be multiple deliveries
    deliveries = root.findall(".//{*}VehicleMonitoringDelivery")
    for d in deliveries:
        dataFrameRef = safe_text(d.find(".//{*}FramedVehicleJourneyRef/{*}DataFrameRef")) or safe_text(d.find(".//{*}DataFrameRef"))
        validUntil = safe_text(d.find(".//{*}ValidUntilTime"))
        validUntil_dt = parse_iso_dt(validUntil)

        for va in d.findall(".//{*}VehicleActivity"):
            recorded = safe_text(va.find(".//{*}RecordedAtTime"))
            recorded_dt = parse_iso_dt(recorded)

            mvj = va.find(".//{*}MonitoredVehicleJourney")
            if mvj is None:
                continue

            def pick(tag: str) -> Optional[str]:
                return safe_text(mvj.find(f".//{{*}}{tag}"))

            lineRef = pick("LineRef")
            publishedLineName = pick("PublishedLineName") or lineRef
            operatorRef = pick("OperatorRef")
            directionRef = pick("DirectionRef")
            originName = pick("OriginName") or pick("OriginRef")
            destinationName = pick("DestinationName") or pick("DestinationRef")
            vehicleRef = pick("VehicleRef")

            # location
            lon = safe_text(mvj.find(".//{*}VehicleLocation/{*}Longitude"))
            lat = safe_text(mvj.find(".//{*}VehicleLocation/{*}Latitude"))
            bearing = pick("Bearing")

            # calls
            calls: List[Dict[str, Any]] = []

            monitoredCall = mvj.find(".//{*}MonitoredCall")
            currentStopRef = None
            vehicleAtStop = False

            def parse_call(call_el: ET.Element, is_monitored: bool) -> Optional[Dict[str, Any]]:
                stopRef = safe_text(call_el.find(".//{*}StopPointRef"))
                stopName = safe_text(call_el.find(".//{*}StopPointName"))
                aimedDep = safe_text(call_el.find(".//{*}AimedDepartureTime"))
                expDep = safe_text(call_el.find(".//{*}ExpectedDepartureTime")) or safe_text(call_el.find(".//{*}ExpectedArrivalTime"))
                vAtStop = safe_text(call_el.find(".//{*}VehicleAtStop"))
                vAtStopBool = (vAtStop or "").lower() == "true"
                return {
                    "stopRef": stopRef,
                    "stopName": stopName,
                    "aimedDep": aimedDep,
                    "expDep": expDep,
                    "vehicleAtStop": vAtStopBool if is_monitored else False,
                    "isMonitored": is_monitored,
                } if stopRef else None

            if monitoredCall is not None:
                c = parse_call(monitoredCall, True)
                if c:
                    calls.append(c)
                    currentStopRef = c["stopRef"]
                    vehicleAtStop = c["vehicleAtStop"]

            onward = mvj.find(".//{*}OnwardCalls")
            if onward is not None:
                for oc in onward.findall(".//{*}OnwardCall"):
                    c = parse_call(oc, False)
                    if c:
                        calls.append(c)

            v = {
                "recordedAtTime": recorded,
                "recordedAtTimeLocal": recorded_dt.isoformat() if recorded_dt else None,
                "validUntilTime": validUntil,
                "dataFrameRef": dataFrameRef,
                "lineRef": lineRef,
                "publishedLineName": publishedLineName,
                "operatorRef": operatorRef,
                "directionRef": directionRef,
                "originName": originName,
                "destinationName": destinationName,
                "longitude": float(lon) if lon else None,
                "latitude": float(lat) if lat else None,
                "bearing": float(bearing) if bearing else None,
                "vehicleRef": vehicleRef,
                "calls": calls,
                "currentStopRef": currentStopRef,
                "vehicleAtStop": bool(vehicleAtStop),
            }
            vehicles.append(v)

    return vehicles


def get_live_all_cached() -> Tuple[List[Dict[str, Any]], int]:
    # Cache to avoid hammering DfT
    now_ts = time.time()
    if (now_ts - LIVE_CACHE["ts"]) < LIVE_CACHE["ttl"] and LIVE_CACHE["vehicles_all"] is not None:
        return LIVE_CACHE["vehicles_all"], LIVE_CACHE["raw_count"]

    try:
        xml_bytes = fetch_live_xml()
        vehicles_all = parse_siri_vm(xml_bytes)
        LIVE_CACHE.update({
            "ts": now_ts,
            "vehicles_all": vehicles_all,
            "raw_count": len(vehicles_all),
            "ok": True,
            "error": None,
        })
        return vehicles_all, len(vehicles_all)
    except Exception as e:
        LIVE_CACHE.update({
            "ts": now_ts,
            "vehicles_all": [],
            "raw_count": 0,
            "ok": False,
            "error": str(e),
        })
        return [], 0


def filter_live_vehicles(
    vehicles_all: List[Dict[str, Any]],
    line: Optional[str],
    operator: Optional[str],
    max_age_seconds: int,
    fresh_only: bool,
) -> List[Dict[str, Any]]:
    nl = now_local()
    today_str = nl.date().isoformat()

    out = []
    for v in vehicles_all:
        # DataFrameRef date filter (IMPORTANT: kiszedi a korábbi napok meneteit)
        df = v.get("dataFrameRef")
        if looks_like_date(df) and df != today_str:
            continue

        # Age filter (IMPORTANT: “csak épp bejelentkezett/aktuális”)
        rdt = parse_iso_dt(v.get("recordedAtTime"))
        if rdt is None:
            if fresh_only:
                continue
            age_s = None
        else:
            age_s = (nl - rdt).total_seconds()

        if fresh_only and (age_s is None or age_s > max_age_seconds):
            continue

        # validUntilTime filter (ha van)
        vut = parse_iso_dt(v.get("validUntilTime"))
        if vut is not None and nl > (vut + timedelta(seconds=5)):
            continue

        # line/operator filters
        if line:
            # match both lineRef and publishedLineName
            if (v.get("lineRef") or "").strip().lower() != line.strip().lower() and (v.get("publishedLineName") or "").strip().lower() != line.strip().lower():
                continue

        if operator:
            if (v.get("operatorRef") or "").strip().lower() != operator.strip().lower():
                continue

        vv = dict(v)
        vv["ageSeconds"] = int(age_s) if age_s is not None else None
        out.append(vv)

    return out


# -------------------------
# Startup
# -------------------------
@app.on_event("startup")
def _startup():
    load_gtfs()


# -------------------------
# Static
# -------------------------
@app.get("/", response_class=FileResponse)
def index():
    return FileResponse("index.html")


# -------------------------
# API: status
# -------------------------
@app.get("/api/status")
def api_status():
    vehicles_all, raw_count = get_live_all_cached()
    # apply default filter to show what we consider "active now"
    active = filter_live_vehicles(
        vehicles_all,
        line=None,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    return {
        "live": {
            "ok": bool(LIVE_CACHE["ok"]),
            "activeCount": len(active),
            "rawCount": raw_count,
            "maxAgeSeconds": DEFAULT_MAX_AGE_SECONDS,
            "error": LIVE_CACHE["error"],
        },
        "gtfs": {
            "ok": bool(GTFS["ok"]),
            "error": GTFS["error"],
            "zip": GTFS_ZIP_PATH,
        },
        "serverTime": now_local().isoformat(),
        "timezone": str(TZ),
    }

# Backward-compat alias (ha nálad már /status volt)
@app.get("/status")
def status_alias():
    return api_status()


# -------------------------
# API: search
# -------------------------
@app.get("/api/search")
def api_search(q: str = Query("", min_length=0, max_length=64)):
    qn = (q or "").strip().lower()
    stops = []
    routes = []

    if qn and GTFS["ok"]:
        # stops
        for s in GTFS["stops"].values():
            name = (s.get("stop_name") or "").lower()
            sid = (s.get("stop_id") or "").lower()
            if qn in name or qn in sid:
                stops.append({
                    "stop_id": s["stop_id"],
                    "stop_name": s["stop_name"],
                })
                if len(stops) >= 25:
                    break

        # routes
        for r in GTFS["routes"].values():
            sn = (r.get("short_name") or "").lower()
            ln = (r.get("long_name") or "").lower()
            if qn in sn or qn in ln:
                routes.append({
                    "route_id": r["route_id"],
                    "short_name": r.get("short_name") or "",
                    "long_name": r.get("long_name") or "",
                })
                if len(routes) >= 25:
                    break

    return {"stops": stops, "routes": routes}


# -------------------------
# API: vehicles (MAP)
# -------------------------
@app.get("/api/vehicles")
def api_vehicles(
    line: Optional[str] = None,
    operator: Optional[str] = None,
    max_age_seconds: int = Query(DEFAULT_MAX_AGE_SECONDS, ge=30, le=3600),
    fresh_only: bool = True,
):
    vehicles_all, _ = get_live_all_cached()
    active = filter_live_vehicles(
        vehicles_all,
        line=line,
        operator=operator,
        max_age_seconds=max_age_seconds,
        fresh_only=fresh_only,
    )
    return {"vehicles": active, "count": len(active), "maxAgeSeconds": max_age_seconds}

# Backward-compat alias (a screenshotodon /vehicles-t néztél)
@app.get("/vehicles")
def vehicles_alias(
    line: Optional[str] = None,
    operator: Optional[str] = None,
    max_age_seconds: int = Query(DEFAULT_MAX_AGE_SECONDS, ge=30, le=3600),
    fresh_only: bool = True,
):
    return api_vehicles(line=line, operator=operator, max_age_seconds=max_age_seconds, fresh_only=fresh_only)


# -------------------------
# API: stop info + departures (GTFS + LIVE)
# -------------------------
@app.get("/api/stop/{stop_id}")
def api_stop(stop_id: str):
    if not GTFS["ok"]:
        raise HTTPException(status_code=503, detail=GTFS["error"] or "GTFS not loaded")
    s = GTFS["stops"].get(stop_id)
    if not s:
        raise HTTPException(status_code=404, detail="Stop not found")
    return s

@app.get("/api/stop/{stop_id}/departures")
def api_stop_departures(
    stop_id: str,
    minutes: int = Query(120, ge=10, le=360),
    limit: int = Query(30, ge=1, le=100),
):
    if not GTFS["ok"]:
        raise HTTPException(status_code=503, detail=GTFS["error"] or "GTFS not loaded")
    if stop_id not in GTFS["stops"]:
        raise HTTPException(status_code=404, detail="Stop not found")

    nl = now_local()
    service_date = nl.date()
    window_end = nl + timedelta(minutes=minutes)

    # --- scheduled from GTFS
    scheduled = []
    st_list = GTFS["stop_times_by_stop"].get(stop_id, [])
    for dep_s, trip_id, seq in st_list:
        trip = GTFS["trips"].get(trip_id)
        if not trip:
            continue
        if not service_active(trip["service_id"], service_date):
            continue

        dep_dt = departure_datetime(service_date, dep_s)
        if dep_dt < nl or dep_dt > window_end:
            continue

        route = GTFS["routes"].get(trip["route_id"], {})
        line = (route.get("short_name") or "").strip() or (route.get("long_name") or "").strip()
        headsign = trip.get("headsign") or ""

        scheduled.append({
            "trip_id": trip_id,
            "line": line,
            "destination": headsign,
            "scheduledTime": dep_dt.isoformat(),
        })

    scheduled.sort(key=lambda x: x["scheduledTime"])
    scheduled = scheduled[:limit]

    # --- live predicted for this stop (from SIRI calls)
    vehicles_all, _ = get_live_all_cached()
    active = filter_live_vehicles(
        vehicles_all, line=None, operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS, fresh_only=True
    )

    live_candidates = []
    for v in active:
        calls = v.get("calls") or []
        for c in calls:
            if c.get("stopRef") != stop_id:
                continue
            exp = parse_iso_dt(c.get("expDep"))
            if not exp:
                continue
            if exp < nl - timedelta(minutes=2) or exp > window_end:
                continue

            live_candidates.append({
                "vehicleRef": v.get("vehicleRef"),
                "line": (v.get("publishedLineName") or v.get("lineRef") or "").strip(),
                "destination": (v.get("destinationName") or "").strip(),
                "expectedTime": exp.isoformat(),
                "aimedTime": c.get("aimedDep"),
                "vehicleAtStop": bool(v.get("vehicleAtStop")) and (v.get("currentStopRef") == stop_id),
            })

    # match live to scheduled (same line, close time)
    # result rows: schedule (white) OR live (green)
    results = []
    used_live = set()

    def to_dt_iso(iso: str) -> datetime:
        return datetime.fromisoformat(iso).astimezone(TZ)

    for sch in scheduled:
        sch_dt = to_dt_iso(sch["scheduledTime"])
        best_i = None
        best_diff = None

        for i, lv in enumerate(live_candidates):
            if i in used_live:
                continue
            if (lv["line"] or "").lower() != (sch["line"] or "").lower():
                continue
            lv_dt = to_dt_iso(lv["expectedTime"])
            diff = abs((lv_dt - sch_dt).total_seconds())
            if diff <= 20 * 60:  # 20 perc ablak
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best_i = i

        if best_i is not None:
            lv = live_candidates[best_i]
            used_live.add(best_i)

            lv_dt = to_dt_iso(lv["expectedTime"])
            delay_min = int(round((lv_dt - sch_dt).total_seconds() / 60.0))

            results.append({
                "trip_id": sch["trip_id"],
                "line": sch["line"],
                "destination": sch["destination"] or lv["destination"],
                "scheduledTime": sch["scheduledTime"],
                "expectedTime": lv["expectedTime"],
                "isLive": True,
                "delayMin": delay_min,
                "vehicleRef": lv.get("vehicleRef"),
                "vehicleAtStop": lv.get("vehicleAtStop", False),
            })
        else:
            results.append({
                "trip_id": sch["trip_id"],
                "line": sch["line"],
                "destination": sch["destination"],
                "scheduledTime": sch["scheduledTime"],
                "expectedTime": None,
                "isLive": False,
                "delayMin": None,
                "vehicleRef": None,
                "vehicleAtStop": False,
            })

    # add live-only if no GTFS match
    for i, lv in enumerate(live_candidates):
        if i in used_live:
            continue
        results.append({
            "trip_id": None,
            "line": lv["line"],
            "destination": lv["destination"],
            "scheduledTime": None,
            "expectedTime": lv["expectedTime"],
            "isLive": True,
            "delayMin": None,
            "vehicleRef": lv.get("vehicleRef"),
            "vehicleAtStop": lv.get("vehicleAtStop", False),
        })

    # sort by "best time"
    def sort_key(x):
        t = x["expectedTime"] or x["scheduledTime"] or "9999-12-31T00:00:00+00:00"
        return t

    results.sort(key=sort_key)
    results = results[:limit]

    # enrich: countdown minutes
    for r in results:
        t_iso = r["expectedTime"] or r["scheduledTime"]
        if t_iso:
            dt = to_dt_iso(t_iso)
            r["inMin"] = int(round((dt - nl).total_seconds() / 60.0))
        else:
            r["inMin"] = None

    return {
        "stop": GTFS["stops"][stop_id],
        "now": nl.isoformat(),
        "departures": results,
    }


# Backward-compat alias (ha nálad már /stop/... volt)
@app.get("/stop/{stop_id}/departures")
def stop_departures_alias(stop_id: str, minutes: int = 120, limit: int = 30):
    return api_stop_departures(stop_id=stop_id, minutes=minutes, limit=limit)


# -------------------------
# API: trip (GTFS list) + vehicle (LIVE per vehicleRef)
# -------------------------
@app.get("/api/trip/{trip_id}")
def api_trip(trip_id: str):
    if not GTFS["ok"]:
        raise HTTPException(status_code=503, detail=GTFS["error"] or "GTFS not loaded")
    trip = GTFS["trips"].get(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")

    route = GTFS["routes"].get(trip["route_id"], {})
    stops_seq = GTFS["stop_times_by_trip"].get(trip_id, [])

    # assume today's service day for display
    service_date = now_local().date()

    out_stops = []
    for st in stops_seq:
        stop_id = st["stop_id"]
        stop = GTFS["stops"].get(stop_id, {"stop_name": stop_id})
        dep_dt = departure_datetime(service_date, st["departure_s"])
        out_stops.append({
            "stop_id": stop_id,
            "stop_name": stop.get("stop_name") or stop_id,
            "scheduledTime": dep_dt.isoformat(),
        })

    return {
        "trip_id": trip_id,
        "line": (route.get("short_name") or "").strip() or (route.get("long_name") or "").strip(),
        "destination": trip.get("headsign") or "",
        "stops": out_stops,
    }


@app.get("/api/vehicle/{vehicle_ref}")
def api_vehicle(vehicle_ref: str):
    vehicles_all, _ = get_live_all_cached()
    active = filter_live_vehicles(
        vehicles_all, line=None, operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS, fresh_only=True
    )

    for v in active:
        if (v.get("vehicleRef") or "") == vehicle_ref:
            # build calls map for quick lookup in frontend
            calls_map = {}
            for c in v.get("calls") or []:
                sr = c.get("stopRef")
                exp = c.get("expDep")
                if sr and exp:
                    calls_map[sr] = exp

            return {
                "vehicleRef": v.get("vehicleRef"),
                "line": v.get("publishedLineName") or v.get("lineRef"),
                "destination": v.get("destinationName"),
                "recordedAtTime": v.get("recordedAtTime"),
                "ageSeconds": v.get("ageSeconds"),
                "latitude": v.get("latitude"),
                "longitude": v.get("longitude"),
                "bearing": v.get("bearing"),
                "currentStopRef": v.get("currentStopRef"),
                "vehicleAtStop": bool(v.get("vehicleAtStop")),
                "calls": calls_map,  # stopRef -> expected time (ISO)
            }

    raise HTTPException(status_code=404, detail="Vehicle not found (not active / too old)")


# -------------------------
# Health
# -------------------------
@app.get("/api/health")
def api_health():
    return {"ok": True}
