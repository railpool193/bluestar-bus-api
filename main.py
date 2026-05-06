import csv
import io
import os
import re
import zipfile
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, date, time, timedelta, timezone
from math import asin, cos, radians, sin, sqrt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from zoneinfo import ZoneInfo

APP_NAME = "Bluestar Unilink Menetrend"
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
GTFS_DIR = BASE_DIR / "gtfs"
GTFS_ZIP = BASE_DIR / "gtfs.zip"
LONDON = ZoneInfo("Europe/London")

LIVE_FEED_URL_DEFAULT = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/"
LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "10"))
LIVE_MAX_AGE_SECONDS = int(os.getenv("LIVE_MAX_AGE_SECONDS", "240"))
LIVE_OPERATOR_FILTER = os.getenv("LIVE_OPERATOR_FILTER", "BLUS").strip().upper()
DEPARTURE_WINDOW_MIN = int(os.getenv("DEPARTURE_WINDOW_MIN", "120"))
DEPARTURE_LIMIT = int(os.getenv("DEPARTURE_LIMIT", "60"))
LIVE_STOP_MATCH_MINUTES = int(os.getenv("LIVE_STOP_MATCH_MINUTES", "18"))

app = FastAPI(title=APP_NAME)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def now_london() -> datetime:
    return datetime.now(LONDON)


def iso_now_london() -> str:
    return now_london().isoformat()


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip().replace("\ufeff", "")
    s = s.replace("_", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def human_name(value: Any) -> str:
    s = clean_text(value)
    s = re.sub(r"\bStand\s+([A-Z])\b", r"[\1]", s, flags=re.I)
    s = re.sub(r"\bStop\s+([A-Z0-9]+)\b", r"[\1]", s, flags=re.I)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def norm(value: Any) -> str:
    s = clean_text(value).lower()
    return re.sub(r"[^a-z0-9]+", "", s)


def line_norm(value: Any) -> str:
    return clean_text(value).upper().replace(" ", "")


def safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value)))
    except Exception:
        return default


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(str(value))
    except Exception:
        return None


def parse_iso_dt(value: Any) -> Optional[datetime]:
    s = clean_text(value)
    if not s:
        return None
    try:
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(LONDON)
    except Exception:
        return None


def gtfs_time_to_datetime(service_day: date, gtfs_time: str) -> Optional[datetime]:
    if not gtfs_time:
        return None
    try:
        parts = gtfs_time.strip().split(":")
        h = int(parts[0])
        m = int(parts[1])
        sec = int(parts[2]) if len(parts) > 2 else 0
        extra_days = h // 24
        h = h % 24
        return datetime.combine(service_day + timedelta(days=extra_days), time(h, m, sec), tzinfo=LONDON)
    except Exception:
        return None


def datetime_to_hhmm(dt: Optional[datetime]) -> str:
    return dt.astimezone(LONDON).strftime("%H:%M") if dt else ""


def minutes_until(dt: Optional[datetime]) -> Optional[int]:
    if not dt:
        return None
    return int(round((dt.astimezone(LONDON) - now_london()).total_seconds() / 60))


def gtfs_time_to_code(t: str) -> str:
    if not t:
        return ""
    parts = t.split(":")
    if len(parts) < 2:
        return ""
    return f"{int(parts[0]) % 24:02d}{int(parts[1]):02d}"


def haversine_m(lat1: Any, lon1: Any, lat2: Any, lon2: Any) -> Optional[float]:
    try:
        lat1, lon1, lat2, lon2 = float(lat1), float(lon1), float(lat2), float(lon2)
        r = 6371000.0
        dlat = radians(lat2 - lat1)
        dlon = radians(lon2 - lon1)
        a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
        return 2 * r * asin(sqrt(a))
    except Exception:
        return None


def stop_public_code_from_name(stop_name: str) -> str:
    m = re.search(r"\[([A-Z0-9]{1,6})\]", clean_text(stop_name), flags=re.I)
    return m.group(1).upper() if m else ""


def safe_platform_code(row: Dict[str, Any], stop_name: str) -> str:
    from_name = stop_public_code_from_name(stop_name)
    if from_name:
        return from_name
    platform = clean_text(row.get("platform_code"))
    if platform and len(platform) <= 6 and re.search(r"[A-Za-z]", platform):
        return platform.upper()
    # Do not derive fake codes from stop_id. That caused wrong numbers like 77 / 71.
    return "BUS"


def short_destination(value: str) -> str:
    s = human_name(value)
    known = [
        ("Winchester Bus Station", "Winchester"),
        ("Hanover Buildings", "Southampton"),
        ("Southampton, Hanover Buildings", "Southampton"),
        ("Southampton, Vincents Walk", "Southampton"),
        ("Bargate", "Southampton"),
        ("City Centre", "City"),
        ("Adanac Park", "Adanac Park"),
        ("Lordshill", "Lordshill"),
        ("Weston", "Weston"),
        ("Millbrook", "Millbrook"),
        ("Sholing", "Sholing"),
        ("Hamble", "Hamble"),
        ("Romsey", "Romsey"),
        ("Eastleigh Bus Station", "Eastleigh"),
        ("Chandlers Ford", "Chandlers Ford"),
        ("North Harbour Tesco", "North Harbour"),
        ("Thornhill", "Thornhill"),
        ("Fair Oak", "Fair Oak"),
        ("Hedge End", "Hedge End"),
        ("Totton", "Totton"),
        ("Calmore", "Calmore"),
    ]
    ns = norm(s)
    for a, b in known:
        if norm(a) and norm(a) in ns:
            return b
    s = s.replace("Southampton, ", "")
    s = re.sub(r"\s*\[[A-Z0-9]+\]\s*$", "", s).strip()
    return s[:24].rstrip() + "…" if len(s) > 25 else s


def same_stop_id(a: str, b: str) -> bool:
    return clean_text(a).upper() == clean_text(b).upper()


def stop_matches(stop_ref: str, stop_name: str, stop: Dict[str, Any]) -> bool:
    if stop_ref and same_stop_id(stop_ref, stop.get("stop_id", "")):
        return True
    if stop_name:
        a = norm(stop_name)
        b = norm(stop.get("stop_name"))
        if a and b and (a in b or b in a):
            return True
    return False


class GTFSStore:
    def __init__(self):
        self.loaded = False
        self.error: Optional[str] = None
        self.source = ""
        self.agency: Dict[str, Dict[str, Any]] = {}
        self.stops: Dict[str, Dict[str, Any]] = {}
        self.routes: Dict[str, Dict[str, Any]] = {}
        self.trips: Dict[str, Dict[str, Any]] = {}
        self.calendar: Dict[str, Dict[str, Any]] = {}
        self.calendar_dates: Dict[str, Dict[str, int]] = defaultdict(dict)
        self.stop_times_by_trip: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.stop_departures_index: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.shapes: Dict[str, List[Dict[str, float]]] = defaultdict(list)
        self.route_by_short: Dict[str, List[str]] = defaultdict(list)

    def reset(self):
        self.__init__()

    def _read_file_from_zip(self, zf: zipfile.ZipFile, name: str) -> List[Dict[str, str]]:
        target = None
        for n in zf.namelist():
            if n.lower() == name.lower() or n.lower().endswith("/" + name.lower()):
                target = n
                break
        if not target:
            return []
        raw = zf.read(target).decode("utf-8-sig", errors="replace")
        return list(csv.DictReader(io.StringIO(raw)))

    def _read_file_from_dir(self, folder: Path, name: str) -> List[Dict[str, str]]:
        p = folder / name
        if not p.exists():
            return []
        raw = p.read_text(encoding="utf-8-sig", errors="replace")
        return list(csv.DictReader(io.StringIO(raw)))

    def _read_table(self, source_type: str, source: Any, name: str) -> List[Dict[str, str]]:
        return self._read_file_from_zip(source, name) if source_type == "zip" else self._read_file_from_dir(source, name)

    def load(self):
        self.reset()
        try:
            env_zip = os.getenv("GTFS_ZIP", "").strip()
            env_dir = os.getenv("GTFS_DIR", "").strip()
            zip_path = Path(env_zip) if env_zip and Path(env_zip).exists() else GTFS_ZIP if GTFS_ZIP.exists() else None
            dir_path = Path(env_dir) if env_dir and Path(env_dir).exists() else GTFS_DIR if GTFS_DIR.exists() else None
            if zip_path:
                self.source = f"zip:{zip_path.name}"
                with zipfile.ZipFile(zip_path, "r") as zf:
                    self._load_from("zip", zf)
            elif dir_path:
                self.source = f"dir:{dir_path.name}"
                self._load_from("dir", dir_path)
            else:
                raise FileNotFoundError("GTFS source not found. Add gtfs.zip or gtfs/ folder.")
            self.loaded = True
            self.error = None
        except Exception as e:
            self.loaded = False
            self.error = str(e)

    def _load_from(self, source_type: str, source: Any):
        agency_rows = self._read_table(source_type, source, "agency.txt")
        stops_rows = self._read_table(source_type, source, "stops.txt")
        routes_rows = self._read_table(source_type, source, "routes.txt")
        trips_rows = self._read_table(source_type, source, "trips.txt")
        stop_times_rows = self._read_table(source_type, source, "stop_times.txt")
        calendar_rows = self._read_table(source_type, source, "calendar.txt")
        calendar_dates_rows = self._read_table(source_type, source, "calendar_dates.txt")
        shapes_rows = self._read_table(source_type, source, "shapes.txt")

        for r in agency_rows:
            aid = clean_text(r.get("agency_id")) or "agency"
            self.agency[aid] = r

        for r in stops_rows:
            sid = clean_text(r.get("stop_id"))
            if not sid:
                continue
            stop_name = human_name(r.get("stop_name"))
            lat = safe_float(r.get("stop_lat"))
            lon = safe_float(r.get("stop_lon"))
            platform = safe_platform_code(r, stop_name)
            self.stops[sid] = {
                "stop_id": sid,
                "stopId": sid,
                "stop_name": stop_name,
                "stopName": stop_name,
                "name": stop_name,
                "stop_code": clean_text(r.get("stop_code")),
                "platform_code": platform,
                "platformCode": platform,
                "stop_lat": lat,
                "stop_lon": lon,
                "lat": lat,
                "lon": lon,
            }

        for r in routes_rows:
            rid = clean_text(r.get("route_id"))
            if not rid:
                continue
            short = clean_text(r.get("route_short_name")) or clean_text(r.get("route_long_name"))
            long_name = human_name(r.get("route_long_name"))
            route = {
                "route_id": rid,
                "routeId": rid,
                "agency_id": clean_text(r.get("agency_id")),
                "short_name": short,
                "shortName": short,
                "route_short_name": short,
                "routeShortName": short,
                "long_name": long_name,
                "longName": long_name,
                "route_long_name": long_name,
                "routeLongName": long_name,
                "route_type": clean_text(r.get("route_type")),
                "route_color": clean_text(r.get("route_color")) or "20AEEA",
                "route_text_color": clean_text(r.get("route_text_color")) or "FFFFFF",
            }
            self.routes[rid] = route
            self.route_by_short[line_norm(short)].append(rid)

        for r in calendar_rows:
            sid = clean_text(r.get("service_id"))
            if not sid:
                continue
            self.calendar[sid] = {
                "service_id": sid,
                "monday": safe_int(r.get("monday")),
                "tuesday": safe_int(r.get("tuesday")),
                "wednesday": safe_int(r.get("wednesday")),
                "thursday": safe_int(r.get("thursday")),
                "friday": safe_int(r.get("friday")),
                "saturday": safe_int(r.get("saturday")),
                "sunday": safe_int(r.get("sunday")),
                "start_date": clean_text(r.get("start_date")),
                "end_date": clean_text(r.get("end_date")),
            }

        for r in calendar_dates_rows:
            sid = clean_text(r.get("service_id"))
            d = clean_text(r.get("date"))
            ex = safe_int(r.get("exception_type"))
            if sid and d:
                self.calendar_dates[sid][d] = ex

        for r in trips_rows:
            tid = clean_text(r.get("trip_id"))
            rid = clean_text(r.get("route_id"))
            if not tid:
                continue
            route = self.routes.get(rid, {})
            line = clean_text(route.get("route_short_name")) or clean_text(route.get("short_name"))
            self.trips[tid] = {
                "trip_id": tid,
                "tripId": tid,
                "route_id": rid,
                "routeId": rid,
                "service_id": clean_text(r.get("service_id")),
                "serviceId": clean_text(r.get("service_id")),
                "trip_headsign": human_name(r.get("trip_headsign")),
                "headsign": human_name(r.get("trip_headsign")),
                "direction_id": clean_text(r.get("direction_id")),
                "directionId": clean_text(r.get("direction_id")),
                "block_id": clean_text(r.get("block_id")),
                "blockId": clean_text(r.get("block_id")),
                "shape_id": clean_text(r.get("shape_id")),
                "shapeId": clean_text(r.get("shape_id")),
                "route_short_name": line,
                "routeShortName": line,
                "line": line,
            }

        for r in stop_times_rows:
            tid = clean_text(r.get("trip_id"))
            sid = clean_text(r.get("stop_id"))
            if not tid or not sid:
                continue
            arr = clean_text(r.get("arrival_time"))
            dep = clean_text(r.get("departure_time")) or arr
            seq = safe_int(r.get("stop_sequence"))
            item = {
                "trip_id": tid,
                "tripId": tid,
                "stop_id": sid,
                "stopId": sid,
                "arrival_time": arr,
                "arrivalTime": arr,
                "departure_time": dep,
                "departureTime": dep,
                "stop_sequence": seq,
                "stopSequence": seq,
                "stop_headsign": human_name(r.get("stop_headsign")),
                "pickup_type": clean_text(r.get("pickup_type")),
                "drop_off_type": clean_text(r.get("drop_off_type")),
            }
            self.stop_times_by_trip[tid].append(item)

        for tid, rows in self.stop_times_by_trip.items():
            rows.sort(key=lambda x: x.get("stop_sequence", 0))

        for tid, rows in self.stop_times_by_trip.items():
            trip = self.trips.get(tid)
            if not trip or not rows:
                continue
            first, last = rows[0], rows[-1]
            trip["first_stop_id"] = first.get("stop_id")
            trip["firstStopId"] = first.get("stop_id")
            trip["last_stop_id"] = last.get("stop_id")
            trip["lastStopId"] = last.get("stop_id")
            trip["first_departure_time"] = first.get("departure_time") or first.get("arrival_time")
            trip["firstDepartureTime"] = trip["first_departure_time"]
            trip["first_departure_code"] = gtfs_time_to_code(trip["first_departure_time"])
            trip["last_arrival_time"] = last.get("arrival_time") or last.get("departure_time")
            trip["lastArrivalTime"] = trip["last_arrival_time"]

        for tid, rows in self.stop_times_by_trip.items():
            trip = self.trips.get(tid)
            if not trip or not rows:
                continue
            route = self.routes.get(trip["route_id"], {})
            line = clean_text(route.get("route_short_name")) or clean_text(trip.get("line"))
            final_stop = self.stops.get(rows[-1]["stop_id"], {})
            destination = clean_text(trip.get("trip_headsign")) or clean_text(rows[-1].get("stop_headsign")) or clean_text(final_stop.get("stop_name"))
            first_seq = rows[0].get("stop_sequence")
            last_seq = rows[-1].get("stop_sequence")
            for st in rows:
                sid = st["stop_id"]
                self.stop_departures_index[sid].append({
                    "trip_id": tid,
                    "tripId": tid,
                    "route_id": trip["route_id"],
                    "routeId": trip["route_id"],
                    "service_id": trip["service_id"],
                    "serviceId": trip["service_id"],
                    "line": line,
                    "route_short_name": line,
                    "routeShortName": line,
                    "direction_id": trip.get("direction_id"),
                    "directionId": trip.get("direction_id"),
                    "headsign": human_name(destination),
                    "destination": short_destination(destination),
                    "destinationFull": human_name(destination),
                    "stop_id": sid,
                    "stopId": sid,
                    "stop_sequence": st["stop_sequence"],
                    "stopSequence": st["stop_sequence"],
                    "arrival_time": st["arrival_time"],
                    "arrivalTime": st["arrival_time"],
                    "departure_time": st["departure_time"],
                    "departureTime": st["departure_time"],
                    "pickup_type": st.get("pickup_type"),
                    "drop_off_type": st.get("drop_off_type"),
                    "is_first_stop": st.get("stop_sequence") == first_seq,
                    "isFirstStop": st.get("stop_sequence") == first_seq,
                    "is_last_stop": st.get("stop_sequence") == last_seq,
                    "isLastStop": st.get("stop_sequence") == last_seq,
                })

        for sid in self.stop_departures_index:
            self.stop_departures_index[sid].sort(key=lambda x: (x.get("departure_time") or "", x.get("line") or "", x.get("trip_id") or ""))

        for r in shapes_rows:
            shape_id = clean_text(r.get("shape_id"))
            lat = safe_float(r.get("shape_pt_lat"))
            lon = safe_float(r.get("shape_pt_lon"))
            seq = safe_int(r.get("shape_pt_sequence"))
            if shape_id and lat is not None and lon is not None:
                self.shapes[shape_id].append({"lat": lat, "lon": lon, "seq": seq})
        for sid in self.shapes:
            self.shapes[sid].sort(key=lambda x: x["seq"])

    def active_service_ids(self, service_day: date) -> set:
        ymd = service_day.strftime("%Y%m%d")
        weekday = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][service_day.weekday()]
        result = set()
        for sid, cal in self.calendar.items():
            start = clean_text(cal.get("start_date"))
            end = clean_text(cal.get("end_date"))
            if start and ymd < start:
                continue
            if end and ymd > end:
                continue
            if safe_int(cal.get(weekday)) == 1:
                result.add(sid)
        for sid, exceptions in self.calendar_dates.items():
            ex = exceptions.get(ymd)
            if ex == 1:
                result.add(sid)
            elif ex == 2:
                result.discard(sid)
        if not self.calendar and self.calendar_dates:
            for sid, exceptions in self.calendar_dates.items():
                if exceptions.get(ymd) == 1:
                    result.add(sid)
        return result

    def counts(self) -> Dict[str, Any]:
        return {
            "agency": len(self.agency),
            "stops": len(self.stops),
            "routes": len(self.routes),
            "trips": len(self.trips),
            "stop_times_trips": len(self.stop_times_by_trip),
            "stop_departures_index_stops": len(self.stop_departures_index),
            "shapes": len(self.shapes),
        }

    def calendar_range(self) -> Dict[str, Any]:
        dates = []
        for cal in self.calendar.values():
            if cal.get("start_date"):
                dates.append(cal["start_date"])
            if cal.get("end_date"):
                dates.append(cal["end_date"])
        for exs in self.calendar_dates.values():
            dates.extend(exs.keys())
        dates = [d for d in dates if d]
        return {"calendar_start_min": min(dates) if dates else "", "calendar_end_max": max(dates) if dates else ""}


gtfs = GTFSStore()


class LiveStore:
    def __init__(self):
        self.cache_time: Optional[datetime] = None
        self.vehicles: List[Dict[str, Any]] = []
        self.raw_count = 0
        self.active_count = 0
        self.last_error = ""
        self.last_http_status = None
        self.last_fetch_time = ""
        self.effective_feed_url = ""
        self.key_present = False
        self.key_preview = ""

    def configured_url_and_params(self) -> Tuple[str, Dict[str, str]]:
        url = os.getenv("LIVE_FEED_URL", "").strip() or LIVE_FEED_URL_DEFAULT
        api_key = (os.getenv("BODS_API_KEY", "").strip() or os.getenv("BODS_KEY", "").strip() or os.getenv("LIVE_API_KEY", "").strip() or os.getenv("API_KEY", "").strip())
        self.key_present = bool(api_key)
        self.key_preview = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) >= 8 else ""
        parsed = urllib.parse.urlparse(url)
        query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        if api_key and "api_key" not in query:
            query["api_key"] = api_key
        clean_url = urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, "", parsed.fragment))
        self.effective_feed_url = clean_url
        return clean_url, query

    def fetch(self, force: bool = False) -> List[Dict[str, Any]]:
        n = now_london()
        if not force and self.cache_time and (n - self.cache_time).total_seconds() < LIVE_CACHE_TTL_SEC:
            return self.vehicles
        url, params = self.configured_url_and_params()
        if not params.get("api_key"):
            self.last_error = "Missing BODS_API_KEY"
            self.last_http_status = None
            self.last_fetch_time = iso_now_london()
            self.cache_time = n
            self.vehicles = []
            self.raw_count = 0
            self.active_count = 0
            return []
        full_url = f"{url}?{urllib.parse.urlencode(params)}" if params else url
        try:
            req = urllib.request.Request(full_url, headers={"User-Agent": "Bluestar-Unilink-Webapp/1.2", "Accept": "application/xml,text/xml,*/*"})
            with urllib.request.urlopen(req, timeout=20) as resp:
                self.last_http_status = resp.status
                body = resp.read()
            vehicles_all = self.parse_xml(body)
            self.raw_count = len(vehicles_all)
            active = []
            for v in vehicles_all:
                op = clean_text(v.get("operatorRef")).upper()
                if LIVE_OPERATOR_FILTER and op and op != LIVE_OPERATOR_FILTER:
                    continue
                age = v.get("ageSeconds")
                if age is not None and age > LIVE_MAX_AGE_SECONDS:
                    continue
                valid_until = parse_iso_dt(v.get("validUntilTime"))
                if valid_until and valid_until < n - timedelta(seconds=30):
                    continue
                if v.get("latitude") is None or v.get("longitude") is None:
                    continue
                active.append(v)
            self.vehicles = self.dedupe_latest(active)
            self.active_count = len(self.vehicles)
            self.last_error = ""
            self.last_fetch_time = iso_now_london()
            self.cache_time = n
            return self.vehicles
        except Exception as e:
            self.last_error = str(e)
            self.last_fetch_time = iso_now_london()
            self.cache_time = n
            self.vehicles = []
            self.active_count = 0
            return []

    def dedupe_latest(self, vehicles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        latest: Dict[str, Dict[str, Any]] = {}
        for v in vehicles:
            key = clean_text(v.get("vehicleUniqueId")) or clean_text(v.get("vehicleRef")) or f"{v.get('lineNorm')}:{v.get('journeyCode')}:{v.get('datedVehicleJourneyRef')}"
            old = latest.get(key)
            if not old:
                latest[key] = v
                continue
            old_dt = parse_iso_dt(old.get("recordedAtTime"))
            new_dt = parse_iso_dt(v.get("recordedAtTime"))
            if new_dt and old_dt and new_dt > old_dt:
                latest[key] = v
        return list(latest.values())

    def parse_xml(self, body: bytes) -> List[Dict[str, Any]]:
        root = ET.fromstring(body)

        def local(tag: str) -> str:
            return tag.split("}", 1)[-1] if "}" in tag else tag

        def first_child(el, wanted: str):
            if el is None:
                return None
            for c in list(el):
                if local(c.tag) == wanted:
                    return c
            return None

        def text_any(el, wanted: str) -> str:
            if el is None:
                return ""
            for c in el.iter():
                if local(c.tag) == wanted:
                    return clean_text(c.text)
            return ""

        def text_child(el, wanted: str) -> str:
            c = first_child(el, wanted)
            return clean_text(c.text) if c is not None else ""

        def all_nodes(wanted: str):
            return [x for x in root.iter() if local(x.tag) == wanted]

        result = []
        for va in all_nodes("VehicleActivity"):
            recorded = text_child(va, "RecordedAtTime")
            valid_until = text_child(va, "ValidUntilTime")
            mvj = first_child(va, "MonitoredVehicleJourney")
            if mvj is None:
                continue
            loc = first_child(mvj, "VehicleLocation")
            lat = safe_float(text_child(loc, "Latitude"))
            lon = safe_float(text_child(loc, "Longitude"))
            call = first_child(mvj, "MonitoredCall")
            recorded_dt = parse_iso_dt(recorded)
            age = int((now_london() - recorded_dt).total_seconds()) if recorded_dt else None
            line = text_any(mvj, "PublishedLineName") or text_any(mvj, "LineRef")
            destination_name = human_name(text_any(mvj, "DestinationName"))
            origin_name = human_name(text_any(mvj, "OriginName"))
            aimed_dep = text_child(call, "AimedDepartureTime") if call is not None else ""
            expected_dep = text_child(call, "ExpectedDepartureTime") if call is not None else ""
            aimed_arr = text_child(call, "AimedArrivalTime") if call is not None else ""
            expected_arr = text_child(call, "ExpectedArrivalTime") if call is not None else ""
            live_dt = parse_iso_dt(expected_dep or expected_arr or aimed_dep or aimed_arr)
            aimed_dt = parse_iso_dt(aimed_dep or aimed_arr)
            delay_min = None
            if live_dt and aimed_dt:
                delay_min = int(round((live_dt - aimed_dt).total_seconds() / 60))
            current_stop_ref = ""
            current_stop_name = ""
            vehicle_at_stop = False
            if call is not None:
                current_stop_ref = text_child(call, "StopPointRef")
                current_stop_name = human_name(text_child(call, "StopPointName"))
                vehicle_at_stop = text_child(call, "VehicleAtStop").lower() == "true"
            vehicle_ref = text_any(mvj, "VehicleRef")
            vehicle_unique = text_any(mvj, "VehicleUniqueId")
            item = {
                "recordedAtTime": recorded,
                "recordedAtTimeLocal": recorded_dt.isoformat() if recorded_dt else "",
                "validUntilTime": valid_until,
                "dataFrameRef": text_any(mvj, "DataFrameRef"),
                "datedVehicleJourneyRef": text_any(mvj, "DatedVehicleJourneyRef"),
                "lineRef": text_any(mvj, "LineRef"),
                "publishedLineName": line,
                "line": line,
                "lineNorm": line_norm(line),
                "operatorRef": text_any(mvj, "OperatorRef"),
                "directionRef": text_any(mvj, "DirectionRef"),
                "originRef": text_any(mvj, "OriginRef"),
                "originName": origin_name,
                "destinationRef": text_any(mvj, "DestinationRef"),
                "destinationName": destination_name,
                "destinationShort": short_destination(destination_name),
                "longitude": lon,
                "latitude": lat,
                "bearing": safe_float(text_any(mvj, "Bearing")),
                "blockRef": text_any(mvj, "BlockRef"),
                "vehicleRef": vehicle_ref,
                "vehicleUniqueId": vehicle_unique or vehicle_ref,
                "fleetNumber": vehicle_ref or vehicle_unique,
                "ticketMachineServiceCode": text_any(mvj, "TicketMachineServiceCode"),
                "journeyCode": text_any(mvj, "JourneyCode"),
                "currentStopRef": current_stop_ref,
                "currentStopName": current_stop_name,
                "vehicleAtStop": vehicle_at_stop,
                "aimedDepartureTime": aimed_dep,
                "expectedDepartureTime": expected_dep,
                "aimedArrivalTime": aimed_arr,
                "expectedArrivalTime": expected_arr,
                "liveDateTime": live_dt.isoformat() if live_dt else "",
                "aimedDateTime": aimed_dt.isoformat() if aimed_dt else "",
                "delayMinutes": delay_min,
                "ageSeconds": age,
            }
            result.append(item)
        return result


live = LiveStore()


def ensure_loaded():
    if not gtfs.loaded:
        gtfs.load()


def route_ids_for_line(line: str) -> List[str]:
    ensure_loaded()
    return gtfs.route_by_short.get(line_norm(line), [])


def get_trip_service_day(trip: Dict[str, Any]) -> date:
    today = now_london().date()
    for d in [today - timedelta(days=1), today, today + timedelta(days=1)]:
        if trip.get("service_id") in gtfs.active_service_ids(d):
            return d
    return today


def live_trip_score(v: Dict[str, Any], trip: Dict[str, Any]) -> int:
    score = 0
    if line_norm(v.get("line")) and line_norm(v.get("line")) == line_norm(trip.get("line")):
        score += 40
    dest = norm(v.get("destinationName"))
    head = norm(trip.get("trip_headsign"))
    if dest and head and (dest in head or head in dest):
        score += 25
    if clean_text(v.get("blockRef")) and clean_text(v.get("blockRef")) == clean_text(trip.get("block_id")):
        score += 35
    code = clean_text(trip.get("first_departure_code"))
    journey = clean_text(v.get("journeyCode")) + clean_text(v.get("datedVehicleJourneyRef"))
    if code and code in journey:
        score += 20
    return score


def find_best_live_for_departure(dep: Dict[str, Any], stop: Dict[str, Any], vehicles: List[Dict[str, Any]], sched_dt: datetime) -> Optional[Dict[str, Any]]:
    """
    Pair a timetable departure with a VehicleMonitoring item only when the live
    MonitoredCall is the SAME stop. This avoids showing arrivals / terminus-only
    buses as departures and avoids using a live time from another stop.
    """
    best = None
    best_score = -9999
    for v in vehicles:
        if line_norm(v.get("line")) != line_norm(dep.get("line")):
            continue

        if not stop_matches(v.get("currentStopRef", ""), v.get("currentStopName", ""), stop):
            continue

        live_dt = parse_iso_dt(v.get("liveDateTime"))
        if not live_dt:
            continue

        min_diff = abs((live_dt - sched_dt).total_seconds()) / 60
        if min_diff > LIVE_STOP_MATCH_MINUTES:
            continue

        score = 80 - int(min_diff)
        dest_v = norm(v.get("destinationName"))
        dest_d = norm(dep.get("destinationFull") or dep.get("destination") or dep.get("headsign"))
        if dest_v and dest_d and (dest_v in dest_d or dest_d in dest_v):
            score += 30

        # Same first-departure/journey code is a very strong signal when present.
        trip = gtfs.trips.get(dep.get("trip_id") or dep.get("tripId") or "", {})
        code = clean_text(trip.get("first_departure_code"))
        journey = clean_text(v.get("journeyCode")) + clean_text(v.get("datedVehicleJourneyRef"))
        if code and code in journey:
            score += 30

        if score > best_score:
            best_score = score
            best = v

    return best if best_score >= 55 else None


def scheduled_departures_for_stop(stop_id: str, window_min: int = DEPARTURE_WINDOW_MIN, limit: int = DEPARTURE_LIMIT) -> List[Dict[str, Any]]:
    ensure_loaded()
    stop = gtfs.stops.get(stop_id)
    if not stop:
        return []
    now = now_london()
    end = now + timedelta(minutes=window_min)
    active_by_day = {d: gtfs.active_service_ids(d) for d in [now.date() - timedelta(days=1), now.date(), now.date() + timedelta(days=1)]}
    out = []
    rows = gtfs.stop_departures_index.get(stop_id, [])
    for row in rows:
        # Only show departures. Final stops / arrivals are not departures.
        if row.get("is_last_stop") or row.get("isLastStop"):
            continue
        if clean_text(row.get("pickup_type")) == "1":
            continue
        service_id = row.get("service_id")
        for service_day, active_services in active_by_day.items():
            if service_id not in active_services:
                continue
            dt = gtfs_time_to_datetime(service_day, row.get("departure_time") or row.get("arrival_time"))
            if not dt or dt < now - timedelta(seconds=30) or dt > end:
                continue
            mins = minutes_until(dt)
            item = dict(row)
            item.update({
                "stop": stop,
                "stopName": stop.get("stop_name"),
                "platformCode": stop.get("platform_code"),
                "scheduledDateTime": dt.isoformat(),
                "scheduledTime": datetime_to_hhmm(dt),
                "time": datetime_to_hhmm(dt),
                "minutes": mins,
                "countdown": "Due" if mins is not None and mins <= 1 else f"{mins} min" if mins is not None else "",
                "isDue": bool(mins is not None and mins <= 1),
                "source": "GTFS",
                "live": False,
                "vehicle": None,
            })
            out.append(item)
    out.sort(key=lambda x: (x.get("scheduledDateTime") or "", x.get("line") or ""))
    return out[:limit]


def merge_live_departures(stop_id: str, scheduled: List[Dict[str, Any]], vehicles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    stop = gtfs.stops.get(stop_id, {})
    now = now_london()
    end = now + timedelta(minutes=DEPARTURE_WINDOW_MIN)
    merged: List[Dict[str, Any]] = []
    used_vehicle_keys = set()

    for dep in scheduled:
        sched_dt = parse_iso_dt(dep.get("scheduledDateTime"))
        if not sched_dt:
            continue
        v = find_best_live_for_departure(dep, stop, vehicles, sched_dt)
        if v:
            live_dt = parse_iso_dt(v.get("liveDateTime")) or sched_dt
            mins = minutes_until(live_dt)
            dep = dict(dep)
            dep.update({
                "time": datetime_to_hhmm(live_dt),
                "liveTime": datetime_to_hhmm(live_dt),
                "liveDateTime": live_dt.isoformat(),
                "minutes": mins,
                "countdown": "Due" if mins is not None and mins <= 1 else f"{mins} min" if mins is not None else "",
                "isDue": bool(mins is not None and mins <= 1),
                "source": "LIVE",
                "live": True,
                "vehicle": v,
                "vehicleRef": v.get("vehicleRef") or v.get("fleetNumber") or v.get("vehicleUniqueId"),
                "delayMinutes": v.get("delayMinutes"),
            })
            used_vehicle_keys.add(v.get("vehicleUniqueId") or v.get("vehicleRef"))
        merged.append(dep)

    # Important: do NOT add unmatched live-only rows here.
    # VehicleMonitoring may report a bus arriving/terminating at a stop, but the
    # stop board must show departures only. A live row is therefore shown only
    # when it can be paired with a valid GTFS departure above.

    merged.sort(key=lambda x: (parse_iso_dt(x.get("liveDateTime") or x.get("scheduledDateTime")) or now + timedelta(days=9), x.get("line") or ""))
    return merged[:DEPARTURE_LIMIT]


def find_vehicle_position_in_trip(vehicle: Optional[Dict[str, Any]], stops: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not vehicle or not stops:
        return None
    current_ref = clean_text(vehicle.get("currentStopRef"))
    current_name = norm(vehicle.get("currentStopName"))
    if current_ref:
        for s in stops:
            if same_stop_id(s.get("stop_id", ""), current_ref):
                return {"stop_id": s.get("stop_id"), "sequence": s.get("sequence"), "distance_m": 0, "method": "currentStopRef", "at_stop": bool(vehicle.get("vehicleAtStop"))}
    if current_name:
        for s in stops:
            sn = norm(s.get("stopName") or s.get("stop_name"))
            if sn and (current_name in sn or sn in current_name):
                return {"stop_id": s.get("stop_id"), "sequence": s.get("sequence"), "distance_m": 0, "method": "currentStopName", "at_stop": bool(vehicle.get("vehicleAtStop"))}
    vlat, vlon = vehicle.get("latitude"), vehicle.get("longitude")
    if vlat is None or vlon is None:
        return None
    best = None
    best_dist = None
    for s in stops:
        if s.get("lat") is None or s.get("lon") is None:
            continue
        d = haversine_m(vlat, vlon, s.get("lat"), s.get("lon"))
        if d is None:
            continue
        if best_dist is None or d < best_dist:
            best_dist = d
            best = s
    if not best:
        return None
    return {"stop_id": best.get("stop_id"), "sequence": best.get("sequence"), "distance_m": round(best_dist or 0), "method": "nearestStop", "at_stop": bool(vehicle.get("vehicleAtStop")) or (best_dist is not None and best_dist <= 120)}


def representative_directions_for_line(line: str) -> List[Dict[str, Any]]:
    ensure_loaded()
    rids = set(route_ids_for_line(line))
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for t in gtfs.trips.values():
        if t.get("route_id") not in rids:
            continue
        rows = gtfs.stop_times_by_trip.get(t.get("trip_id"), [])
        if not rows:
            continue
        key = f"{t.get('direction_id')}:{norm(t.get('trip_headsign'))}:{rows[0].get('stop_id')}:{rows[-1].get('stop_id')}"
        groups[key].append(t)
    directions = []
    for trips in groups.values():
        trip = sorted(trips, key=lambda x: x.get("trip_id", ""))[0]
        rows = gtfs.stop_times_by_trip.get(trip.get("trip_id"), [])
        if not rows:
            continue
        stops = []
        for st in rows:
            s = gtfs.stops.get(st.get("stop_id"), {})
            stops.append({
                "stop_id": s.get("stop_id"),
                "stopId": s.get("stop_id"),
                "stop_name": s.get("stop_name"),
                "stopName": s.get("stop_name"),
                "platformCode": s.get("platform_code"),
                "lat": s.get("lat"),
                "lon": s.get("lon"),
                "stop_sequence": st.get("stop_sequence"),
                "stopSequence": st.get("stop_sequence"),
            })
        first, last = stops[0], stops[-1]
        directions.append({
            "trip_id": trip.get("trip_id"),
            "tripId": trip.get("trip_id"),
            "route_id": trip.get("route_id"),
            "routeId": trip.get("route_id"),
            "direction_id": trip.get("direction_id"),
            "directionId": trip.get("direction_id"),
            "headsign": short_destination(trip.get("trip_headsign") or last.get("stopName")),
            "headsignFull": human_name(trip.get("trip_headsign") or last.get("stopName")),
            "from": first,
            "to": last,
            "stop_count": len(stops),
            "stopCount": len(stops),
            "stops": stops,
        })
    directions.sort(key=lambda x: (x.get("headsign") or "", x.get("from", {}).get("stop_name") or ""))
    return directions[:10]


def shape_for_line(line: str) -> List[List[Dict[str, float]]]:
    ensure_loaded()
    rids = set(route_ids_for_line(line))
    seen = set()
    out = []
    for t in gtfs.trips.values():
        if t.get("route_id") not in rids:
            continue
        shape_id = t.get("shape_id") or t.get("shapeId")
        if not shape_id or shape_id in seen or shape_id not in gtfs.shapes:
            continue
        seen.add(shape_id)
        pts = [{"lat": p["lat"], "lon": p["lon"]} for p in gtfs.shapes[shape_id]]
        if pts:
            out.append(pts)
        if len(out) >= 4:
            break
    return out


@app.on_event("startup")
def startup_load():
    gtfs.load()
    live.fetch(force=True)


@app.get("/")
def index():
    p = TEMPLATES_DIR / "index.html"
    if p.exists():
        return FileResponse(str(p), media_type="text/html")
    p2 = BASE_DIR / "index.html"
    if p2.exists():
        return FileResponse(str(p2), media_type="text/html")
    return HTMLResponse("<h1>Bluestar Unilink</h1><p>index.html missing</p>")


@app.get("/health")
def health():
    return {"ok": True, "serverTime": iso_now_london(), "timezone": "Europe/London"}


@app.get("/api/status")
def api_status():
    ensure_loaded()
    vehicles = live.fetch()
    return {
        "live": {
            "ok": not bool(live.last_error),
            "activeCount": len(vehicles),
            "rawCount": live.raw_count,
            "maxAgeSeconds": LIVE_MAX_AGE_SECONDS,
            "operatorFilter": LIVE_OPERATOR_FILTER,
            "error": live.last_error or None,
            "lastHttpStatus": live.last_http_status,
            "lastFetchTime": live.last_fetch_time,
        },
        "gtfs": {
            "ok": gtfs.loaded,
            "error": gtfs.error,
            "source": gtfs.source,
            "counts": gtfs.counts(),
            "calendarRange": gtfs.calendar_range(),
        },
        "serverTime": iso_now_london(),
        "timezone": "Europe/London",
    }


@app.get("/api/gtfs/status")
def gtfs_status():
    ensure_loaded()
    return {"loaded": gtfs.loaded, "error": gtfs.error, "source": gtfs.source, "counts": gtfs.counts(), "calendarRange": gtfs.calendar_range(), "agencyFilter": ""}


@app.get("/api/live/status")
def live_status():
    vehicles = live.fetch(force=True)
    return {
        "effectiveFeedUrl": live.effective_feed_url,
        "keyPreview": live.key_preview,
        "keyPresent": live.key_present,
        "vehicleCount": len(vehicles),
        "rawCount": live.raw_count,
        "lastError": live.last_error,
        "lastHttpStatus": live.last_http_status,
        "lastFetchTime": live.last_fetch_time,
        "sample": vehicles[:5],
    }


@app.get("/api/search")
def search(q: str = ""):
    ensure_loaded()
    query = norm(q)
    if not query:
        return {"q": q, "stops": [], "routes": []}
    stops = []
    for s in gtfs.stops.values():
        hay = norm(s.get("stop_name")) + norm(s.get("stop_id")) + norm(s.get("platform_code"))
        if query in hay:
            stops.append(s)
    stops.sort(key=lambda s: (0 if norm(s.get("stop_name", "")).startswith(query) else 1, s.get("stop_name", "")))
    routes = []
    for r in gtfs.routes.values():
        hay = norm(r.get("route_short_name")) + norm(r.get("route_long_name"))
        if query in hay:
            routes.append(r)
    # Unique routes by short name.
    uniq = {}
    for r in routes:
        uniq.setdefault(line_norm(r.get("route_short_name")), r)
    routes = list(uniq.values())
    routes.sort(key=lambda r: (len(str(r.get("route_short_name", ""))), str(r.get("route_short_name", ""))))
    return {"q": q, "stops": stops[:40], "routes": routes[:30]}


@app.get("/api/stop/{stop_id}")
def stop_detail(stop_id: str):
    ensure_loaded()
    s = gtfs.stops.get(stop_id)
    if not s:
        return JSONResponse({"error": "Stop not found", "stop_id": stop_id}, status_code=404)
    return s


@app.get("/api/stop/{stop_id}/departures")
def stop_departures(stop_id: str, window: int = DEPARTURE_WINDOW_MIN):
    ensure_loaded()
    stop = gtfs.stops.get(stop_id)
    if not stop:
        return JSONResponse({"error": "Stop not found", "stop_id": stop_id, "departures": []}, status_code=404)
    scheduled = scheduled_departures_for_stop(stop_id, window_min=window)
    vehicles = live.fetch()
    departures = merge_live_departures(stop_id, scheduled, vehicles)
    return {"stop": stop, "stop_id": stop_id, "departures": departures, "count": len(departures), "serverTime": iso_now_london()}


@app.get("/api/departures")
def departures_query(stop_id: str, window: int = DEPARTURE_WINDOW_MIN):
    return stop_departures(stop_id, window)


@app.get("/api/stops/{stop_id}/departures")
def departures_alt(stop_id: str, window: int = DEPARTURE_WINDOW_MIN):
    return stop_departures(stop_id, window)


@app.get("/api/routes")
def list_routes():
    ensure_loaded()
    uniq = {}
    for r in gtfs.routes.values():
        key = line_norm(r.get("route_short_name"))
        uniq.setdefault(key, r)
    return {"routes": sorted(uniq.values(), key=lambda r: (len(str(r.get("route_short_name", ""))), str(r.get("route_short_name", ""))))}


@app.get("/api/route/{line}")
def route_detail(line: str):
    ensure_loaded()
    rids = route_ids_for_line(line)
    routes = [gtfs.routes[rid] for rid in rids if rid in gtfs.routes]
    if not routes:
        return JSONResponse({"error": "Route not found", "line": line, "directions": []}, status_code=404)
    return {"line": line, "routes": routes, "directions": representative_directions_for_line(line)}


@app.get("/api/route")
def route_detail_query(line: str):
    return route_detail(line)


@app.get("/api/vehicles")
def vehicles(line: str = ""):
    items = live.fetch()
    if line:
        items = [v for v in items if line_norm(v.get("line")) == line_norm(line)]
    items.sort(key=lambda v: (line_norm(v.get("line")), clean_text(v.get("destinationShort")), clean_text(v.get("vehicleRef"))))
    return {"line": line, "vehicles": items, "count": len(items), "serverTime": iso_now_london()}


@app.get("/api/route/{line}/vehicles")
def route_vehicles(line: str):
    return vehicles(line)


@app.get("/api/map")
def map_data(line: str = ""):
    ensure_loaded()
    items = live.fetch()
    if line:
        items = [v for v in items if line_norm(v.get("line")) == line_norm(line)]
    shapes = shape_for_line(line) if line else []
    return {"line": line, "vehicles": items, "shapes": shapes, "count": len(items), "serverTime": iso_now_london()}


@app.get("/api/route/{line}/map")
def route_map(line: str):
    return map_data(line)




def find_live_vehicle_by_ref(vehicle_ref: str, line: str = "") -> Optional[Dict[str, Any]]:
    ref = clean_text(vehicle_ref)
    if not ref:
        return None
    for v in live.fetch():
        ids = {clean_text(v.get("vehicleRef")), clean_text(v.get("vehicleUniqueId")), clean_text(v.get("fleetNumber"))}
        if ref in ids and (not line or line_norm(v.get("line")) == line_norm(line)):
            return v
    return None

@app.get("/api/trip/{trip_id}")
def trip_detail_path(trip_id: str, vehicle_ref: str = ""):
    return trip_detail(trip_id, vehicle_ref)


@app.get("/api/trip")
def trip_detail(trip_id: str, vehicle_ref: str = ""):
    ensure_loaded()
    trip = gtfs.trips.get(trip_id)
    if not trip:
        return JSONResponse({"error": "Trip not found", "trip_id": trip_id}, status_code=404)
    route = gtfs.routes.get(trip.get("route_id"), {})
    rows = gtfs.stop_times_by_trip.get(trip_id, [])
    service_day = get_trip_service_day(trip)
    stops = []
    for st in rows:
        stop = gtfs.stops.get(st.get("stop_id"), {})
        dt = gtfs_time_to_datetime(service_day, st.get("departure_time") or st.get("arrival_time"))
        stops.append({
            "stop_id": st.get("stop_id"),
            "stopId": st.get("stop_id"),
            "stop_name": stop.get("stop_name"),
            "stopName": stop.get("stop_name"),
            "platformCode": stop.get("platform_code"),
            "lat": stop.get("lat"),
            "lon": stop.get("lon"),
            "sequence": st.get("stop_sequence"),
            "stop_sequence": st.get("stop_sequence"),
            "scheduledTime": datetime_to_hhmm(dt),
            "time": datetime_to_hhmm(dt),
            "scheduledDateTime": dt.isoformat() if dt else "",
            "minutes": minutes_until(dt) if dt else None,
            "source": "GTFS",
            "live": False,
            "vehicleHere": False,
            "vehicleAtStop": False,
            "distanceFromVehicleM": None,
            "isPast": dt < now_london() if dt else False,
        })
    line = clean_text(route.get("route_short_name")) or clean_text(trip.get("line"))
    destination = clean_text(trip.get("trip_headsign")) or (stops[-1].get("stopName", "") if stops else "")
    vehicles_live = live.fetch()
    matched_vehicle = find_live_vehicle_by_ref(vehicle_ref, line) if vehicle_ref else None
    best_score = 999 if matched_vehicle else -999

    if not matched_vehicle:
        for v in vehicles_live:
            score = live_trip_score(v, trip)
            if score > best_score:
                best_score = score
                matched_vehicle = v
        if best_score < 60:
            matched_vehicle = None

    vehicle_position = find_vehicle_position_in_trip(matched_vehicle, stops)
    if matched_vehicle and vehicle_position:
        pos_seq = safe_int(vehicle_position.get("sequence"), -1)
        for s in stops:
            sseq = safe_int(s.get("sequence"), -1)
            if sseq < pos_seq:
                s["isPast"] = True
            elif sseq == pos_seq:
                s["isPast"] = False
                s["live"] = True
                s["vehicleHere"] = True
                s["vehicleAtStop"] = bool(vehicle_position.get("at_stop"))
                s["distanceFromVehicleM"] = vehicle_position.get("distance_m")
            elif sseq > pos_seq:
                s["isPast"] = False
    return {
        "trip": trip,
        "route": route,
        "line": line,
        "destination": short_destination(destination),
        "destinationFull": human_name(destination),
        "vehicle": matched_vehicle,
        "vehiclePosition": vehicle_position,
        "liveMatched": bool(matched_vehicle and vehicle_position),
        "stops": stops,
        "count": len(stops),
    }


@app.get("/r/{line}")
def route_page(line: str):
    return index()


@app.get("/s/{stop_id}")
def stop_page(stop_id: str):
    return index()


@app.get("/{full_path:path}")
def spa_fallback(full_path: str, request: Request):
    if full_path.startswith("api/"):
        return JSONResponse({"error": "Not found"}, status_code=404)
    return index()
