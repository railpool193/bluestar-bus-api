import os
import io
import csv
import zipfile
import asyncio
from dataclasses import dataclass
from datetime import datetime, date, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple, Any
import xml.etree.ElementTree as ET

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# ----------------------------
# Config
# ----------------------------
TZ_NAME = os.getenv("APP_TZ", "Europe/London")
LOCAL_TZ = ZoneInfo(TZ_NAME)

GTFS_ZIP_PATH = os.getenv("GTFS_ZIP_PATH", "gtfs.zip")

BODS_DATAFEED_ID = os.getenv("BODS_DATAFEED_ID", "7721")
BODS_API_KEY = os.getenv("BODS_API_KEY", "")  # set in Railway env
BODS_BASE = os.getenv("BODS_BASE_URL", "https://data.bus-data.dft.gov.uk/api/v1")

LIVE_CACHE_SECONDS = int(os.getenv("LIVE_CACHE_SECONDS", "10"))
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "12"))

APP_TITLE = os.getenv("APP_TITLE", "Bluestar & Unilink")

# ----------------------------
# Helpers
# ----------------------------
def now_local() -> datetime:
    return datetime.now(tz=LOCAL_TZ)

def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))

def parse_gtfs_time_to_seconds(hhmmss: str) -> int:
    # GTFS can exceed 24:00:00 (e.g. 25:10:00)
    parts = hhmmss.strip().split(":")
    if len(parts) != 3:
        return 0
    h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
    return h * 3600 + m * 60 + s

def seconds_to_hhmm(seconds: int) -> str:
    # keep hour modulo 24 for display, but preserve day rollover in datetime separately
    h = (seconds // 3600) % 24
    m = (seconds % 3600) // 60
    return f"{h:02d}:{m:02d}"

def strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag

def safe_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None

# ----------------------------
# GTFS loader + indexes
# ----------------------------
@dataclass
class Stop:
    stop_id: str
    stop_name: str
    stop_code: str
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
    direction_id: str
    shape_id: str

class GTFS:
    def __init__(self):
        self.loaded = False
        self.counts: Dict[str, int] = {}
        self.calendar_start_min: Optional[str] = None
        self.calendar_end_max: Optional[str] = None

        self.stops: Dict[str, Stop] = {}
        self.routes: Dict[str, Route] = {}
        self.trips: Dict[str, Trip] = {}

        # stop_id -> list of stop_time rows
        self.stop_times_by_stop: Dict[str, List[Dict[str, Any]]] = {}
        # trip_id -> list of stop_time rows
        self.stop_times_by_trip: Dict[str, List[Dict[str, Any]]] = {}
        # service_id -> calendar row
        self.calendar: Dict[str, Dict[str, Any]] = {}
        # date -> (adds set, removes set)
        self.calendar_dates_add: Dict[str, set] = {}
        self.calendar_dates_remove: Dict[str, set] = {}

        # shape_id -> list of (lat, lon, seq)
        self.shapes: Dict[str, List[Tuple[float, float, int]]] = {}

    def _read_csv_from_zip(self, z: zipfile.ZipFile, name: str) -> List[Dict[str, str]]:
        try:
            with z.open(name) as f:
                raw = f.read()
            # try utf-8 with fallback
            for enc in ("utf-8-sig", "utf-8", "latin-1"):
                try:
                    txt = raw.decode(enc)
                    break
                except Exception:
                    txt = None
            if txt is None:
                txt = raw.decode("utf-8", errors="ignore")
            reader = csv.DictReader(io.StringIO(txt))
            return [row for row in reader]
        except KeyError:
            return []

    def load(self, path: str):
        if not os.path.exists(path):
            raise FileNotFoundError(f"GTFS zip not found: {path}")

        with zipfile.ZipFile(path, "r") as z:
            stops_rows = self._read_csv_from_zip(z, "stops.txt")
            routes_rows = self._read_csv_from_zip(z, "routes.txt")
            trips_rows = self._read_csv_from_zip(z, "trips.txt")
            stop_times_rows = self._read_csv_from_zip(z, "stop_times.txt")
            calendar_rows = self._read_csv_from_zip(z, "calendar.txt")
            cal_dates_rows = self._read_csv_from_zip(z, "calendar_dates.txt")
            shapes_rows = self._read_csv_from_zip(z, "shapes.txt")

        # stops
        for r in stops_rows:
            sid = (r.get("stop_id") or "").strip()
            if not sid:
                continue
            self.stops[sid] = Stop(
                stop_id=sid,
                stop_name=(r.get("stop_name") or "").strip(),
                stop_code=(r.get("stop_code") or "").strip(),
                stop_lat=safe_float(r.get("stop_lat")),
                stop_lon=safe_float(r.get("stop_lon")),
            )

        # routes
        for r in routes_rows:
            rid = (r.get("route_id") or "").strip()
            if not rid:
                continue
            self.routes[rid] = Route(
                route_id=rid,
                route_short_name=(r.get("route_short_name") or "").strip(),
                route_long_name=(r.get("route_long_name") or "").strip(),
            )

        # calendar
        self.calendar_start_min = None
        self.calendar_end_max = None
        for r in calendar_rows:
            sid = (r.get("service_id") or "").strip()
            if not sid:
                continue
            self.calendar[sid] = r
            st = (r.get("start_date") or "").strip()
            en = (r.get("end_date") or "").strip()
            if st:
                if self.calendar_start_min is None or st < self.calendar_start_min:
                    self.calendar_start_min = st
            if en:
                if self.calendar_end_max is None or en > self.calendar_end_max:
                    self.calendar_end_max = en

        # calendar_dates
        for r in cal_dates_rows:
            sid = (r.get("service_id") or "").strip()
            d = (r.get("date") or "").strip()
            ex = (r.get("exception_type") or "").strip()
            if not sid or not d or not ex:
                continue
            if ex == "1":
                self.calendar_dates_add.setdefault(d, set()).add(sid)
            elif ex == "2":
                self.calendar_dates_remove.setdefault(d, set()).add(sid)

        # trips
        for r in trips_rows:
            tid = (r.get("trip_id") or "").strip()
            if not tid:
                continue
            self.trips[tid] = Trip(
                trip_id=tid,
                route_id=(r.get("route_id") or "").strip(),
                service_id=(r.get("service_id") or "").strip(),
                trip_headsign=(r.get("trip_headsign") or "").strip(),
                direction_id=(r.get("direction_id") or "").strip(),
                shape_id=(r.get("shape_id") or "").strip(),
            )

        # stop_times indexes
        for r in stop_times_rows:
            sid = (r.get("stop_id") or "").strip()
            tid = (r.get("trip_id") or "").strip()
            if not sid or not tid:
                continue
            seq = int((r.get("stop_sequence") or "0").strip() or "0")
            row = {
                "trip_id": tid,
                "stop_id": sid,
                "arrival_time": (r.get("arrival_time") or "").strip(),
                "departure_time": (r.get("departure_time") or "").strip(),
                "stop_sequence": seq,
                "pickup_type": (r.get("pickup_type") or "").strip(),
                "drop_off_type": (r.get("drop_off_type") or "").strip(),
            }
            self.stop_times_by_stop.setdefault(sid, []).append(row)
            self.stop_times_by_trip.setdefault(tid, []).append(row)

        for sid, lst in self.stop_times_by_stop.items():
            lst.sort(key=lambda x: (x["departure_time"], x["stop_sequence"]))
        for tid, lst in self.stop_times_by_trip.items():
            lst.sort(key=lambda x: x["stop_sequence"])

        # shapes
        for r in shapes_rows:
            sh = (r.get("shape_id") or "").strip()
            if not sh:
                continue
            lat = safe_float(r.get("shape_pt_lat"))
            lon = safe_float(r.get("shape_pt_lon"))
            seq = int((r.get("shape_pt_sequence") or "0").strip() or "0")
            if lat is None or lon is None:
                continue
            self.shapes.setdefault(sh, []).append((lat, lon, seq))
        for sh, pts in self.shapes.items():
            pts.sort(key=lambda x: x[2])

        self.counts = {
            "stops": len(self.stops),
            "routes": len(self.routes),
            "trips": len(self.trips),
            "stop_times_trips": len(self.stop_times_by_trip),
            "stop_departures_index_stops": len(self.stop_times_by_stop),
        }
        self.loaded = True

    def active_service_ids(self, d: date) -> set:
        active = set()
        dstr = yyyymmdd(d)
        # calendar.txt base
        dow = d.weekday()  # Mon=0
        dow_key = ["monday","tuesday","wednesday","thursday","friday","saturday","sunday"][dow]
        for sid, r in self.calendar.items():
            st = (r.get("start_date") or "").strip()
            en = (r.get("end_date") or "").strip()
            if not st or not en:
                continue
            if st <= dstr <= en and (r.get(dow_key) or "0").strip() == "1":
                active.add(sid)
        # exceptions
        for sid in self.calendar_dates_remove.get(dstr, set()):
            active.discard(sid)
        for sid in self.calendar_dates_add.get(dstr, set()):
            active.add(sid)
        return active

    def stop_departures(self, stop_id: str, now_dt: datetime, horizon_min: int = 180, limit: int = 25) -> List[Dict[str, Any]]:
        if stop_id not in self.stops:
            raise KeyError("stop not found")

        service_today = self.active_service_ids(now_dt.date())
        service_yday = self.active_service_ids((now_dt.date() - timedelta(days=1)))

        # we must consider times > 24:00:00 that belong to next-day early morning,
        # but operationally they are still part of yesterday's service day.
        horizon = now_dt + timedelta(minutes=horizon_min)

        candidates = []
        for row in self.stop_times_by_stop.get(stop_id, []):
            tid = row["trip_id"]
            trip = self.trips.get(tid)
            if not trip:
                continue

            dep_s = parse_gtfs_time_to_seconds(row["departure_time"] or "00:00:00")

            # Determine the "service day" base:
            # If dep time >= 24h: it belongs to next calendar day but same service day.
            if dep_s >= 24 * 3600:
                base_service_date = now_dt.date() - timedelta(days=1)  # yesterday service
                base_set = service_yday
            else:
                base_service_date = now_dt.date()
                base_set = service_today

            if trip.service_id not in base_set:
                continue

            # Build datetime for this departure
            dep_seconds_in_day = dep_s % (24 * 3600)
            dep_date = base_service_date if dep_s < 24*3600 else (base_service_date + timedelta(days=1))
            dep_dt = datetime.combine(dep_date, datetime.min.time(), tzinfo=LOCAL_TZ) + timedelta(seconds=dep_seconds_in_day)

            if dep_dt < now_dt - timedelta(minutes=1):
                continue
            if dep_dt > horizon:
                continue

            route = self.routes.get(trip.route_id)
            candidates.append({
                "trip_id": tid,
                "route_id": trip.route_id,
                "route": route.route_short_name if route else "",
                "route_long": route.route_long_name if route else "",
                "destination": trip.trip_headsign or "",
                "stop_id": stop_id,
                "scheduled_dt": dep_dt,
                "scheduled": dep_dt.isoformat(),
            })

        candidates.sort(key=lambda x: x["scheduled_dt"])
        return candidates[:limit]

gtfs = GTFS()

# ----------------------------
# Live (SIRI-VM) cache + parsing
# ----------------------------
class LiveCache:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.last_fetch: Optional[datetime] = None
        self.last_http_status: Optional[int] = None
        self.last_error: str = ""
        self.vehicles: List[Dict[str, Any]] = []
        # stopRef -> list of predicted calls
        self.calls_by_stop: Dict[str, List[Dict[str, Any]]] = {}

    def feed_url(self) -> str:
        # Correct form: .../datafeed/<id>/?api_key=<KEY>
        key = (BODS_API_KEY or "").strip()
        return f"{BODS_BASE}/datafeed/{BODS_DATAFEED_ID}/?api_key={key}"

    def key_preview(self) -> str:
        k = (BODS_API_KEY or "").strip()
        if not k:
            return ""
        return k[:4] + "…" + k[-3:]

    def _fetch_raw(self) -> Tuple[int, bytes, str]:
        if not (BODS_API_KEY or "").strip():
            raise RuntimeError("BODS_API_KEY missing")
        url = self.feed_url()
        r = requests.get(
            url,
            timeout=REQUEST_TIMEOUT,
            headers={"Accept": "application/xml, text/xml;q=0.9, */*;q=0.8"},
        )
        return r.status_code, r.content, r.headers.get("content-type", "")

    def _parse_siri_vm(self, xml_bytes: bytes) -> Tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
        vehicles: List[Dict[str, Any]] = []
        calls_by_stop: Dict[str, List[Dict[str, Any]]] = {}

        root = ET.fromstring(xml_bytes)

        # Iterate VehicleActivity nodes (namespace-agnostic)
        for va in root.iter():
            if strip_ns(va.tag) != "VehicleActivity":
                continue

            def find_text(node, localname) -> str:
                for ch in node.iter():
                    if strip_ns(ch.tag) == localname:
                        if ch.text:
                            return ch.text.strip()
                return ""

            recorded = find_text(va, "RecordedAtTime")
            valid_until = find_text(va, "ValidUntilTime")

            mvj = None
            for ch in va.iter():
                if strip_ns(ch.tag) == "MonitoredVehicleJourney":
                    mvj = ch
                    break
            if mvj is None:
                continue

            line_ref = find_text(mvj, "LineRef")
            pub_line = find_text(mvj, "PublishedLineName") or line_ref
            op_ref = find_text(mvj, "OperatorRef")
            direction_ref = find_text(mvj, "DirectionRef")
            origin_ref = find_text(mvj, "OriginRef")
            origin_name = find_text(mvj, "OriginName")
            dest_ref = find_text(mvj, "DestinationRef")
            dest_name = find_text(mvj, "DestinationName")
            block_ref = find_text(mvj, "BlockRef")
            vehicle_ref = find_text(mvj, "VehicleRef")
            bearing = safe_float(find_text(mvj, "Bearing"))
            lon = safe_float(find_text(mvj, "Longitude"))
            lat = safe_float(find_text(mvj, "Latitude"))

            # Journey refs (optional)
            data_frame_ref = ""
            dated_vj_ref = ""
            framed = None
            for ch in mvj.iter():
                if strip_ns(ch.tag) == "FramedVehicleJourneyRef":
                    framed = ch
                    break
            if framed is not None:
                data_frame_ref = find_text(framed, "DataFrameRef")
                dated_vj_ref = find_text(framed, "DatedVehicleJourneyRef")
            else:
                # some feeds put DatedVehicleJourneyRef directly
                dated_vj_ref = find_text(mvj, "DatedVehicleJourneyRef")

            trip_id_guess = ""
            if op_ref and data_frame_ref and dated_vj_ref:
                trip_id_guess = f"{op_ref}:{data_frame_ref}:{dated_vj_ref}"
            elif dated_vj_ref:
                trip_id_guess = dated_vj_ref

            v = {
                "recordedAtTime": recorded,
                "validUntilTime": valid_until,
                "lineRef": line_ref,
                "publishedLineName": pub_line,
                "lineNorm": pub_line,
                "operatorRef": op_ref,
                "directionRef": direction_ref,
                "originRef": origin_ref,
                "originName": origin_name,
                "destinationRef": dest_ref,
                "destinationName": dest_name,
                "longitude": lon,
                "latitude": lat,
                "bearing": bearing,
                "blockRef": block_ref,
                "vehicleRef": vehicle_ref,
                "dataFrameRef": data_frame_ref,
                "datedVehicleJourneyRef": dated_vj_ref,
                "tripId": trip_id_guess,
            }
            vehicles.append(v)

            # OnwardCalls -> predictions per stop
            # mvj/OnwardCalls/OnwardCall
            onward_calls = []
            for ch in mvj.iter():
                if strip_ns(ch.tag) == "OnwardCall":
                    onward_calls.append(ch)

            for oc in onward_calls:
                stop_ref = find_text(oc, "StopPointRef")
                exp_dep = find_text(oc, "ExpectedDepartureTime") or find_text(oc, "ExpectedArrivalTime")
                aimed_dep = find_text(oc, "AimedDepartureTime") or find_text(oc, "AimedArrivalTime")
                if not stop_ref or not exp_dep:
                    continue

                calls_by_stop.setdefault(stop_ref, []).append({
                    "stop_id": stop_ref,
                    "route": pub_line,
                    "lineRef": line_ref,
                    "destination": dest_name or "",
                    "expected": exp_dep,
                    "aimed": aimed_dep,
                    "vehicleRef": vehicle_ref,
                    "tripId": trip_id_guess,
                    "directionRef": direction_ref,
                })

        # Sort predictions by expected time
        for sid, lst in calls_by_stop.items():
            def parse_iso(x: str) -> datetime:
                try:
                    return datetime.fromisoformat(x.replace("Z", "+00:00")).astimezone(LOCAL_TZ)
                except Exception:
                    return datetime.max.replace(tzinfo=LOCAL_TZ)
            lst.sort(key=lambda c: parse_iso(c["expected"]))
        return vehicles, calls_by_stop

    def _normalize_predictions(self, calls_by_stop: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        # keep only near-future predictions to avoid clutter
        now_dt = now_local()
        horizon = now_dt + timedelta(hours=3)
        out: Dict[str, List[Dict[str, Any]]] = {}
        for sid, lst in calls_by_stop.items():
            keep = []
            for c in lst:
                try:
                    dt = datetime.fromisoformat(c["expected"].replace("Z", "+00:00")).astimezone(LOCAL_TZ)
                except Exception:
                    continue
                if now_dt - timedelta(minutes=3) <= dt <= horizon:
                    c2 = dict(c)
                    c2["expected_dt"] = dt.isoformat()
                    keep.append(c2)
            out[sid] = keep
        return out

    def refresh(self):
        status, content, ctype = self._fetch_raw()
        self.last_http_status = status
        if status != 200:
            raise RuntimeError(f"HTTP {status}")

        # Assume SIRI-VM XML (BODS standard)
        vehicles, calls = self._parse_siri_vm(content)
        self.vehicles = vehicles
        self.calls_by_stop = self._normalize_predictions(calls)
        self.last_error = ""

live = LiveCache()

async def ensure_live_fresh():
    async with live.lock:
        now_dt = now_local()
        if live.last_fetch and (now_dt - live.last_fetch).total_seconds() < LIVE_CACHE_SECONDS:
            return
        # run blocking I/O in thread
        try:
            await asyncio.to_thread(live.refresh)
            live.last_fetch = now_dt
        except Exception as e:
            live.last_fetch = now_dt
            live.last_error = str(e)

def merge_departures_with_live(
    scheduled: List[Dict[str, Any]],
    stop_id: str,
    limit: int
) -> List[Dict[str, Any]]:
    # scheduled: list from gtfs.stop_departures
    # live.calls_by_stop: expected times
    calls = live.calls_by_stop.get(stop_id, [])
    now_dt = now_local()

    # Prepare callable expected datetimes
    parsed_calls = []
    for c in calls:
        try:
            exp_dt = datetime.fromisoformat(c["expected"].replace("Z", "+00:00")).astimezone(LOCAL_TZ)
        except Exception:
            continue
        parsed_calls.append((exp_dt, c))

    used = set()
    out = []

    # match by route + proximity
    for s in scheduled:
        sch_dt: datetime = s["scheduled_dt"]
        best_i = None
        best_delta = None
        for i, (exp_dt, c) in enumerate(parsed_calls):
            if i in used:
                continue
            # route match is important, but sometimes live route names differ.
            route_ok = True
            if s.get("route"):
                route_ok = (str(c.get("route","")).strip().lower() == str(s["route"]).strip().lower())
            if not route_ok:
                continue
            delta = abs((exp_dt - sch_dt).total_seconds())
            if delta <= 30 * 60:  # 30 min window
                if best_delta is None or delta < best_delta:
                    best_delta = delta
                    best_i = i
        if best_i is not None:
            used.add(best_i)
            exp_dt, c = parsed_calls[best_i]
            delay_min = int(round((exp_dt - sch_dt).total_seconds() / 60))
            minutes = int(round((exp_dt - now_dt).total_seconds() / 60))
            out.append({
                "trip_id": s["trip_id"],
                "route_id": s["route_id"],
                "route": s["route"],
                "destination": s["destination"],
                "scheduled": s["scheduled_dt"].isoformat(),
                "expected": exp_dt.isoformat(),
                "minutes": minutes,
                "is_live": True,
                "delay_min": delay_min,
                "platform": stop_id,
            })
        else:
            minutes = int(round((sch_dt - now_dt).total_seconds() / 60))
            out.append({
                "trip_id": s["trip_id"],
                "route_id": s["route_id"],
                "route": s["route"],
                "destination": s["destination"],
                "scheduled": s["scheduled_dt"].isoformat(),
                "expected": None,
                "minutes": minutes,
                "is_live": False,
                "delay_min": None,
                "platform": stop_id,
            })

    # Add unmatched live-only predictions (optional, but nice)
    extras = []
    for i, (exp_dt, c) in enumerate(parsed_calls):
        if i in used:
            continue
        minutes = int(round((exp_dt - now_local()).total_seconds() / 60))
        if minutes < -2:
            continue
        extras.append({
            "trip_id": c.get("tripId") or None,
            "route_id": None,
            "route": c.get("route") or "",
            "destination": c.get("destination") or "",
            "scheduled": None,
            "expected": exp_dt.isoformat(),
            "minutes": minutes,
            "is_live": True,
            "delay_min": None,
            "platform": stop_id,
        })
    extras.sort(key=lambda x: x["expected"] or "")
    out.extend(extras)

    # Sort by expected if exists else scheduled
    def sort_key(d):
        t = d["expected"] or d["scheduled"] or ""
        return t
    out.sort(key=sort_key)

    return out[:limit]

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def _startup():
    # Load GTFS at boot
    try:
        gtfs.load(GTFS_ZIP_PATH)
    except Exception as e:
        # don't crash app; UI will show GTFS error
        gtfs.loaded = False
        gtfs.counts = {}
        print(f"[GTFS] load failed: {e}")

@app.get("/", response_class=HTMLResponse)
def home():
    # Serve index.html from same folder
    if not os.path.exists("index.html"):
        return HTMLResponse("<h1>index.html missing</h1>", status_code=500)
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/health")
def health():
    return {"ok": True, "time": now_local().isoformat()}

@app.get("/api/gtfs/status")
def gtfs_status():
    return {
        "loaded": gtfs.loaded,
        "counts": gtfs.counts,
        "calendarRange": {"calendar_start_min": gtfs.calendar_start_min, "calendar_end_max": gtfs.calendar_end_max},
        "agencyFilter": os.getenv("AGENCY_FILTER", ""),
    }

@app.get("/api/live/status")
async def live_status():
    key_present = bool((BODS_API_KEY or "").strip())
    await ensure_live_fresh()
    return {
        "effectiveFeedUrl": f"{BODS_BASE}/datafeed/{BODS_DATAFEED_ID}/",
        "keyPreview": live.key_preview(),
        "keyPresent": key_present,
        "vehicleCount": len(live.vehicles),
        "lastError": live.last_error,
        "lastHttpStatus": live.last_http_status,
        "lastFetchTime": live.last_fetch.isoformat() if live.last_fetch else None,
        "sample": live.vehicles[:1],
    }

@app.get("/api/search")
def api_search(q: str = Query(default="", min_length=0)):
    if not gtfs.loaded:
        raise HTTPException(status_code=503, detail="GTFS not loaded")

    qn = q.strip().lower()
    stops = []
    routes = []
    if qn:
        for s in gtfs.stops.values():
            if qn in (s.stop_name or "").lower() or qn in (s.stop_id or "").lower():
                stops.append({
                    "stop_id": s.stop_id,
                    "stop_name": s.stop_name,
                    "stop_code": s.stop_code,
                    "stop_lat": s.stop_lat,
                    "stop_lon": s.stop_lon,
                })
        for r in gtfs.routes.values():
            if qn in (r.route_short_name or "").lower() or qn in (r.route_long_name or "").lower():
                routes.append({
                    "route_id": r.route_id,
                    "route_short_name": r.route_short_name,
                    "route_long_name": r.route_long_name,
                })

    # cap results
    stops = stops[:50]
    routes = routes[:50]
    return {"stops": stops, "routes": routes}

@app.get("/api/stop/{stop_id}")
def api_stop(stop_id: str):
    if not gtfs.loaded:
        raise HTTPException(status_code=503, detail="GTFS not loaded")
    s = gtfs.stops.get(stop_id)
    if not s:
        raise HTTPException(status_code=404, detail="stop not found")
    return {
        "stop_id": s.stop_id,
        "stop_name": s.stop_name,
        "stop_code": s.stop_code,
        "stop_lat": s.stop_lat,
        "stop_lon": s.stop_lon,
    }

@app.get("/api/stop/{stop_id}/departures")
async def api_stop_departures(stop_id: str, limit: int = 25):
    if not gtfs.loaded:
        raise HTTPException(status_code=503, detail="GTFS not loaded")
    if stop_id not in gtfs.stops:
        raise HTTPException(status_code=404, detail="stop not found")

    await ensure_live_fresh()
    now_dt = now_local()
    try:
        sched = gtfs.stop_departures(stop_id, now_dt=now_dt, horizon_min=180, limit=limit)
    except KeyError:
        raise HTTPException(status_code=404, detail="stop not found")

    merged = merge_departures_with_live(sched, stop_id=stop_id, limit=limit)

    # Format time labels
    out = []
    for d in merged:
        def fmt_iso(iso):
            if not iso:
                return None
            try:
                dt = datetime.fromisoformat(iso).astimezone(LOCAL_TZ)
                return {"iso": dt.isoformat(), "hhmm": dt.strftime("%H:%M")}
            except Exception:
                return {"iso": iso, "hhmm": None}

        out.append({
            **d,
            "scheduled_time": fmt_iso(d.get("scheduled")),
            "expected_time": fmt_iso(d.get("expected")),
        })

    return {"stop_id": stop_id, "now": now_dt.isoformat(), "departures": out}

@app.get("/api/trip/{trip_id}")
async def api_trip(trip_id: str):
    if not gtfs.loaded:
        raise HTTPException(status_code=503, detail="GTFS not loaded")
    trip = gtfs.trips.get(trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="trip not found")

    route = gtfs.routes.get(trip.route_id)
    st = gtfs.stop_times_by_trip.get(trip_id, [])
    stops = []
    for row in st:
        s = gtfs.stops.get(row["stop_id"])
        if not s:
            continue
        stops.append({
            "stop_id": s.stop_id,
            "stop_name": s.stop_name,
            "stop_lat": s.stop_lat,
            "stop_lon": s.stop_lon,
            "stop_sequence": row["stop_sequence"],
            "arrival_time": row["arrival_time"],
            "departure_time": row["departure_time"],
        })

    shape_pts = []
    if trip.shape_id and trip.shape_id in gtfs.shapes:
        shape_pts = [[lat, lon] for (lat, lon, _seq) in gtfs.shapes[trip.shape_id]]

    await ensure_live_fresh()

    return {
        "trip_id": trip.trip_id,
        "route_id": trip.route_id,
        "route_short_name": route.route_short_name if route else "",
        "route_long_name": route.route_long_name if route else "",
        "headsign": trip.trip_headsign,
        "direction_id": trip.direction_id,
        "shape_id": trip.shape_id,
        "stops": stops,
        "shape": shape_pts,
        "live_vehicle_count": len(live.vehicles),
    }

@app.get("/api/vehicles")
async def api_vehicles(route: str = ""):
    await ensure_live_fresh()
    route_n = route.strip().lower()
    vehicles = live.vehicles
    if route_n:
        vehicles = [v for v in vehicles if str(v.get("publishedLineName","")).strip().lower() == route_n]
    return {"vehicles": vehicles, "count": len(vehicles), "lastFetchTime": live.last_fetch.isoformat() if live.last_fetch else None}

@app.get("/api/app/status")
async def api_app_status():
    await ensure_live_fresh()
    return {
        "title": APP_TITLE,
        "gtfs": {"loaded": gtfs.loaded, "counts": gtfs.counts},
        "live": {
            "on": bool((BODS_API_KEY or "").strip()) and (live.last_http_status == 200) and not live.last_error,
            "vehicleCount": len(live.vehicles),
            "lastHttpStatus": live.last_http_status,
            "lastError": live.last_error,
        },
        "time": now_local().isoformat(),
    }
