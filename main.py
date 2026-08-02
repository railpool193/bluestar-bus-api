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
from typing import Any, Dict, List, Optional, Tuple, Set

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from zoneinfo import ZoneInfo

APP_NAME = "Bluestar Unilink Menetrend"
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
GTFS_DIR = Path(os.getenv("GTFS_DIR", str(BASE_DIR / "gtfs")))
GTFS_ZIP = Path(os.getenv("GTFS_ZIP", str(BASE_DIR / "gtfs.zip")))
LONDON = ZoneInfo("Europe/London")

LIVE_FEED_URL_DEFAULT = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/"
LIVE_FEED_URL = os.getenv("LIVE_FEED_URL", LIVE_FEED_URL_DEFAULT).strip()
LIVE_API_KEY = os.getenv("LIVE_API_KEY", os.getenv("BODS_API_KEY", "")).strip()
LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "8"))
LIVE_MAX_AGE_SECONDS = int(os.getenv("LIVE_MAX_AGE_SECONDS", "360"))
LIVE_OPERATOR_FILTER = os.getenv("LIVE_OPERATOR_FILTER", "BLUS").strip().upper()
DEPARTURE_WINDOW_MIN = int(os.getenv("DEPARTURE_WINDOW_MIN", "120"))
DEPARTURE_LIMIT = int(os.getenv("DEPARTURE_LIMIT", "80"))
LIVE_MATCH_MINUTES = int(os.getenv("LIVE_MATCH_MINUTES", "38"))

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


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\ufeff", "").replace("_", " ").strip())


def human_name(value: Any) -> str:
    s = clean_text(value)
    s = re.sub(r"\bStand\s+([A-Z0-9]+)\b", r"[\1]", s, flags=re.I)
    s = re.sub(r"\bStop\s+([A-Z0-9]+)\b", r"[\1]", s, flags=re.I)
    return re.sub(r"\s+", " ", s).strip()


def norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean_text(value).lower())


def line_norm(value: Any) -> str:
    return re.sub(r"\s+", "", clean_text(value).upper())


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == "":
            return default
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
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(LONDON)
    except Exception:
        return None


def gtfs_time_to_datetime(service_day: date, gtfs_time: str) -> Optional[datetime]:
    gtfs_time = clean_text(gtfs_time)
    if not gtfs_time:
        return None
    try:
        hh, mm, *rest = gtfs_time.split(":")
        h = int(hh)
        m = int(mm)
        sec = int(rest[0]) if rest else 0
        extra = h // 24
        h = h % 24
        return datetime.combine(service_day + timedelta(days=extra), time(h, m, sec), tzinfo=LONDON)
    except Exception:
        return None


def hhmm(dt: Optional[datetime]) -> str:
    return dt.astimezone(LONDON).strftime("%H:%M") if dt else ""


def minutes_until(dt: Optional[datetime]) -> Optional[int]:
    if not dt:
        return None
    return int(round((dt.astimezone(LONDON) - now_london()).total_seconds() / 60))


def stop_code_from_name(stop_name: str) -> str:
    m = re.search(r"\[([A-Z0-9]{1,6})\]", clean_text(stop_name), flags=re.I)
    return m.group(1).upper() if m else ""


def public_stop_code(row: Dict[str, Any]) -> str:
    by_name = stop_code_from_name(row.get("stop_name", ""))
    if by_name:
        return by_name
    pc = clean_text(row.get("platform_code"))
    if pc and len(pc) <= 6 and re.search(r"[A-Za-z]", pc):
        return pc.upper()
    return "BUS"


def short_destination(value: str) -> str:
    s = human_name(value).replace("Southampton, ", "")
    known = [
        ("Winchester Bus Station", "Winchester"),
        ("Hanover Buildings", "Southampton"),
        ("Vincents Walk", "Southampton"),
        ("Bargate", "Southampton"),
        ("City Centre", "City"),
        ("Adanac Park", "Adanac Park"),
        ("Lordshill", "Lordshill"),
        ("Weston", "Weston"),
        ("Millbrook", "Millbrook"),
        ("Sholing", "Sholing"),
        ("Hamble", "Hamble"),
        ("Romsey", "Romsey"),
        ("Eastleigh", "Eastleigh"),
        ("Chandlers Ford", "Chandlers Ford"),
        ("North Harbour", "North Harbour"),
        ("Thornhill", "Thornhill"),
        ("Fair Oak", "Fair Oak"),
        ("Hedge End", "Hedge End"),
        ("Totton", "Totton"),
        ("Calmore", "Calmore"),
        ("Bevois Valley", "Bevois Valley"),
    ]
    ns = norm(s)
    for a, b in known:
        if norm(a) in ns:
            return b
    s = re.sub(r"\s*\[[A-Z0-9]+\]\s*$", "", s).strip()
    return s[:27].rstrip() + "…" if len(s) > 28 else s


def destination_match(a: str, b: str) -> bool:
    na, nb = norm(a), norm(b)
    if not na or not nb:
        return False
    if na == nb or na in nb or nb in na:
        return True
    sa, sb = norm(short_destination(a)), norm(short_destination(b))
    return bool(sa and sb and (sa == sb or sa in sb or sb in sa))


def extract_codes(*values: Any) -> Set[str]:
    out: Set[str] = set()
    for value in values:
        s = clean_text(value)
        if not s:
            continue
        out.add(norm(s))
        for m in re.finditer(r"\d{3,6}", s):
            out.add(m.group(0))
            out.add(m.group(0)[-4:])
        for part in re.split(r"[^A-Za-z0-9]+", s):
            p = part.strip()
            if len(p) >= 3:
                out.add(p.upper())
    return {x for x in out if x}


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


def stop_same(stop: Dict[str, Any], stop_ref: str = "", stop_name: str = "") -> bool:
    if stop_ref and clean_text(stop_ref).upper() == clean_text(stop.get("stop_id")).upper():
        return True
    if stop_name:
        a, b = norm(stop_name), norm(stop.get("stop_name"))
        if a and b and (a in b or b in a):
            return True
    return False


class GTFSStore:
    def __init__(self):
        self.loaded = False
        self.error = ""
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

    def _read_zip(self, zf: zipfile.ZipFile, name: str) -> List[Dict[str, str]]:
        target = None
        for n in zf.namelist():
            if n.lower() == name.lower() or n.lower().endswith("/" + name.lower()):
                target = n
                break
        if not target:
            return []
        return list(csv.DictReader(io.StringIO(zf.read(target).decode("utf-8-sig", errors="replace"))))

    def _read_dir(self, folder: Path, name: str) -> List[Dict[str, str]]:
        p = folder / name
        if not p.exists():
            return []
        return list(csv.DictReader(io.StringIO(p.read_text(encoding="utf-8-sig", errors="replace"))))

    def load(self):
        self.__init__()
        try:
            if GTFS_ZIP.exists():
                self.load_from_path(GTFS_ZIP)
                return
            elif GTFS_DIR.exists():
                self.source = f"dir:{GTFS_DIR.name}"
                self._load_tables(lambda n: self._read_dir(GTFS_DIR, n))
            else:
                raise FileNotFoundError("GTFS not found: upload gtfs.zip or add gtfs/ folder")
            self.loaded = bool(self.stops and self.routes and self.trips and self.stop_times_by_trip)
            if not self.loaded:
                raise RuntimeError("GTFS loaded but required tables are empty")
        except Exception as exc:
            self.loaded = False
            self.error = str(exc)

    def load_from_path(self, path: Path):
        self.__init__()
        candidate = Path(path)
        try:
            if not candidate.is_file():
                raise FileNotFoundError(f"GTFS ZIP not found: {candidate}")
            self.source = f"zip:{candidate.name}"
            with zipfile.ZipFile(candidate, "r") as zf:
                self._load_tables(lambda n: self._read_zip(zf, n))
            self.loaded = bool(self.stops and self.routes and self.trips and self.stop_times_by_trip)
            if not self.loaded:
                raise RuntimeError("GTFS loaded but required tables are empty")
        except Exception as exc:
            self.loaded = False
            self.error = str(exc)
        return self

    def _load_tables(self, reader):
        for r in reader("agency.txt"):
            aid = clean_text(r.get("agency_id")) or "agency"
            self.agency[aid] = dict(r)

        for r in reader("stops.txt"):
            sid = clean_text(r.get("stop_id"))
            if not sid:
                continue
            name = human_name(r.get("stop_name")) or sid
            self.stops[sid] = {
                **dict(r),
                "stop_id": sid,
                "stop_name": name,
                "name": name,
                "code": public_stop_code({**dict(r), "stop_name": name}),
                "lat": safe_float(r.get("stop_lat")),
                "lon": safe_float(r.get("stop_lon")),
            }

        for r in reader("routes.txt"):
            rid = clean_text(r.get("route_id"))
            if not rid:
                continue
            short = clean_text(r.get("route_short_name")) or clean_text(r.get("route_long_name")) or rid
            row = {**dict(r), "route_id": rid, "route_short_name": short, "line": short}
            self.routes[rid] = row
            self.route_by_short[line_norm(short)].append(rid)

        for r in reader("trips.txt"):
            tid = clean_text(r.get("trip_id"))
            if not tid:
                continue
            rid = clean_text(r.get("route_id"))
            line = self.routes.get(rid, {}).get("route_short_name", "")
            self.trips[tid] = {**dict(r), "trip_id": tid, "route_id": rid, "line": line, "destination": short_destination(r.get("trip_headsign"))}

        for r in reader("calendar.txt"):
            sid = clean_text(r.get("service_id"))
            if sid:
                self.calendar[sid] = dict(r)

        for r in reader("calendar_dates.txt"):
            sid = clean_text(r.get("service_id"))
            d = clean_text(r.get("date"))
            if sid and d:
                self.calendar_dates[sid][d] = safe_int(r.get("exception_type"), 0)

        for r in reader("stop_times.txt"):
            tid = clean_text(r.get("trip_id"))
            sid = clean_text(r.get("stop_id"))
            if not tid or not sid:
                continue
            row = {**dict(r), "trip_id": tid, "stop_id": sid, "stop_sequence": safe_int(r.get("stop_sequence"), 0)}
            self.stop_times_by_trip[tid].append(row)

        for tid, rows in self.stop_times_by_trip.items():
            rows.sort(key=lambda x: safe_int(x.get("stop_sequence"), 0))
            trip = self.trips.get(tid, {})
            if not trip:
                continue
            for i, row in enumerate(rows):
                sid = row.get("stop_id")
                st = self.stops.get(sid, {})
                dep = {
                    **row,
                    "line": trip.get("line", ""),
                    "route_id": trip.get("route_id", ""),
                    "service_id": trip.get("service_id", ""),
                    "direction_id": trip.get("direction_id", ""),
                    "headsign": short_destination(row.get("stop_headsign") or trip.get("trip_headsign") or trip.get("destination")),
                    "headsign_full": human_name(row.get("stop_headsign") or trip.get("trip_headsign") or trip.get("destination")),
                    "stop_name": st.get("stop_name", sid),
                    "stop_code": st.get("code", "BUS"),
                    "is_last_stop": i == len(rows) - 1,
                }
                self.stop_departures_index[sid].append(dep)

        shape_rows = reader("shapes.txt")
        for r in shape_rows:
            sid = clean_text(r.get("shape_id"))
            lat = safe_float(r.get("shape_pt_lat"))
            lon = safe_float(r.get("shape_pt_lon"))
            if sid and lat is not None and lon is not None:
                self.shapes[sid].append({"lat": lat, "lon": lon, "seq": safe_int(r.get("shape_pt_sequence"), 0)})
        for sid, pts in self.shapes.items():
            pts.sort(key=lambda p: p.get("seq", 0))

    def active_service_ids(self, service_day: date) -> Set[str]:
        ymd = service_day.strftime("%Y%m%d")
        weekday = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"][service_day.weekday()]
        active: Set[str] = set()
        for sid, r in self.calendar.items():
            start = clean_text(r.get("start_date"))
            end = clean_text(r.get("end_date"))
            if start and ymd < start:
                continue
            if end and ymd > end:
                continue
            if clean_text(r.get(weekday)) == "1":
                active.add(sid)
        for sid, changes in self.calendar_dates.items():
            et = changes.get(ymd)
            if et == 1:
                active.add(sid)
            elif et == 2 and sid in active:
                active.remove(sid)
        if not self.calendar and self.calendar_dates:
            for sid, changes in self.calendar_dates.items():
                if changes.get(ymd) == 1:
                    active.add(sid)
        return active

    def trip_first_stop(self, trip_id: str) -> Dict[str, Any]:
        rows = self.stop_times_by_trip.get(trip_id, [])
        return rows[0] if rows else {}

    def trip_last_stop(self, trip_id: str) -> Dict[str, Any]:
        rows = self.stop_times_by_trip.get(trip_id, [])
        return rows[-1] if rows else {}


gtfs = GTFSStore()


def xml_text(node: Optional[ET.Element], path: str = "") -> str:
    if node is None:
        return ""
    target = node.find(path) if path else node
    if target is not None and target.text:
        return clean_text(target.text)
    # Namespace-free fallback.
    name = path.split("/")[-1]
    if not name:
        return ""
    for e in node.iter():
        if e.tag.split("}")[-1] == name and e.text:
            return clean_text(e.text)
    return ""


def children_by_local(root: ET.Element, name: str) -> List[ET.Element]:
    return [e for e in root.iter() if e.tag.split("}")[-1] == name]


def fleet_from_vehicle_ref(vehicle_ref: str) -> str:
    s = clean_text(vehicle_ref)
    if not s:
        return ""
    nums = re.findall(r"\d{2,6}", s)
    return nums[-1][-4:] if nums else s[-6:]


class LiveStore:
    def __init__(self):
        self.vehicles: List[Dict[str, Any]] = []
        self.ok = False
        self.error = ""
        self.last_fetch: Optional[datetime] = None
        self.raw_count = 0

    def fetch(self, force: bool = False) -> List[Dict[str, Any]]:
        n = now_london()
        if not force and self.last_fetch and (n - self.last_fetch).total_seconds() < LIVE_CACHE_TTL_SEC:
            return self.vehicles
        try:
            url = LIVE_FEED_URL
            headers = {"User-Agent": "Bluestar-Unilink-App/1.0"}
            if LIVE_API_KEY:
                sep = "&" if "?" in url else "?"
                if "api_key=" not in url.lower() and "key=" not in url.lower():
                    url = f"{url}{sep}api_key={urllib.parse.quote(LIVE_API_KEY)}"
                headers["x-api-key"] = LIVE_API_KEY
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as resp:
                xml = resp.read()
            root = ET.fromstring(xml)
            vehicles: List[Dict[str, Any]] = []
            raw = 0
            for mvj in children_by_local(root, "MonitoredVehicleJourney"):
                raw += 1
                operator = xml_text(mvj, "OperatorRef").upper()
                if LIVE_OPERATOR_FILTER and operator and operator != LIVE_OPERATOR_FILTER:
                    continue
                line = xml_text(mvj, "PublishedLineName") or xml_text(mvj, "LineRef")
                if not line:
                    continue
                dest = xml_text(mvj, "DestinationName") or xml_text(mvj, "DestinationRef")
                vehicle_ref = xml_text(mvj, "VehicleRef") or xml_text(mvj, "VehicleMonitoringRef")
                dated_ref = xml_text(mvj, "FramedVehicleJourneyRef/DatedVehicleJourneyRef") or xml_text(mvj, "DatedVehicleJourneyRef")
                block_ref = xml_text(mvj, "BlockRef")
                lat = safe_float(xml_text(mvj, "VehicleLocation/Latitude"))
                lon = safe_float(xml_text(mvj, "VehicleLocation/Longitude"))
                bearing = safe_float(xml_text(mvj, "Bearing"))
                recorded = parse_iso_dt(xml_text(mvj, "RecordedAtTime") or xml_text(mvj, "ValidUntilTime"))
                call = None
                for e in mvj.iter():
                    if e.tag.split("}")[-1] == "MonitoredCall":
                        call = e
                        break
                stop_ref = xml_text(call, "StopPointRef") if call is not None else ""
                stop_name = human_name(xml_text(call, "StopPointName")) if call is not None else ""
                aimed = parse_iso_dt(xml_text(call, "AimedDepartureTime") or xml_text(call, "AimedArrivalTime")) if call is not None else None
                expected = parse_iso_dt(xml_text(call, "ExpectedDepartureTime") or xml_text(call, "ExpectedArrivalTime")) if call is not None else None
                live_time = expected or aimed
                delay = None
                if aimed and expected:
                    delay = int(round((expected - aimed).total_seconds() / 60))
                vehicle_at_stop = (xml_text(call, "VehicleAtStop").lower() == "true") if call is not None else False
                age = int((n - recorded).total_seconds()) if recorded else 0
                if recorded and age > LIVE_MAX_AGE_SECONDS:
                    continue
                vehicles.append({
                    "line": clean_text(line),
                    "lineNorm": line_norm(line),
                    "destination": short_destination(dest),
                    "destinationFull": human_name(dest),
                    "operator": operator,
                    "vehicleRef": vehicle_ref,
                    "fleet": fleet_from_vehicle_ref(vehicle_ref),
                    "datedVehicleJourneyRef": dated_ref,
                    "blockRef": block_ref,
                    "codes": list(extract_codes(vehicle_ref, fleet_from_vehicle_ref(vehicle_ref), dated_ref, block_ref)),
                    "latitude": lat,
                    "longitude": lon,
                    "bearing": bearing or 0,
                    "recordedAt": recorded.isoformat() if recorded else "",
                    "currentStopRef": stop_ref,
                    "currentStopName": stop_name,
                    "vehicleAtStop": vehicle_at_stop,
                    "aimedTime": aimed.isoformat() if aimed else "",
                    "expectedTime": expected.isoformat() if expected else "",
                    "liveTime": live_time.isoformat() if live_time else "",
                    "delayMinutes": delay,
                    "status": "At stop" if vehicle_at_stop else "Moving",
                })
            self.raw_count = raw
            self.vehicles = vehicles
            self.ok = True
            self.error = ""
            self.last_fetch = n
        except Exception as exc:
            self.ok = False
            self.error = str(exc)
            self.last_fetch = n
        return self.vehicles


live_store = LiveStore()


def service_days_for_departures() -> List[date]:
    n = now_london()
    return [(n - timedelta(days=1)).date(), n.date(), (n + timedelta(days=1)).date()]


def trip_headsign(trip: Dict[str, Any]) -> str:
    return human_name(trip.get("trip_headsign") or trip.get("destination") or "")


def shape_for_trip(trip: Dict[str, Any]) -> List[Dict[str, float]]:
    sid = clean_text(trip.get("shape_id"))
    if sid and gtfs.shapes.get(sid):
        return gtfs.shapes[sid][:3000]
    out = []
    for r in gtfs.stop_times_by_trip.get(trip.get("trip_id"), []):
        st = gtfs.stops.get(r.get("stop_id"), {})
        if st.get("lat") is not None and st.get("lon") is not None:
            out.append({"lat": st["lat"], "lon": st["lon"]})
    return out


def match_live_to_departure(dep: Dict[str, Any], sched_dt: datetime, vehicles: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    best = None
    best_score = -9999
    for v in vehicles:
        if line_norm(v.get("line")) != line_norm(dep.get("line")):
            continue
        if not destination_match(v.get("destinationFull") or v.get("destination"), dep.get("headsign_full") or dep.get("headsign")):
            continue
        live_dt = parse_iso_dt(v.get("liveTime"))
        score = 0
        if live_dt:
            diff = abs((live_dt - sched_dt).total_seconds()) / 60
            if diff > LIVE_MATCH_MINUTES:
                continue
            score += max(0, 100 - int(diff * 3))
        stop = gtfs.stops.get(dep.get("stop_id"), {})
        if stop_same(stop, v.get("currentStopRef", ""), v.get("currentStopName", "")):
            score += 80
        if v.get("vehicleAtStop"):
            score += 20
        if v.get("fleet"):
            score += 5
        if score > best_score:
            best_score = score
            best = v
    return best


def enrich_departure(dep: Dict[str, Any], service_day: date, sched_dt: datetime, vehicles: List[Dict[str, Any]]) -> Dict[str, Any]:
    live = match_live_to_departure(dep, sched_dt, vehicles)
    live_dt = parse_iso_dt(live.get("liveTime")) if live else None
    display_dt = live_dt or sched_dt
    mins = minutes_until(display_dt)
    if mins is not None and mins < 0:
        mins = 0 if mins >= -2 else None
    is_due = bool(mins is not None and mins <= 1)
    return {
        "tripId": dep.get("trip_id"),
        "trip_id": dep.get("trip_id"),
        "serviceDate": service_day.isoformat(),
        "line": dep.get("line", ""),
        "routeId": dep.get("route_id", ""),
        "stopId": dep.get("stop_id", ""),
        "stopName": dep.get("stop_name", ""),
        "stopSequence": dep.get("stop_sequence", 0),
        "destination": short_destination(dep.get("headsign_full") or dep.get("headsign")),
        "destinationFull": dep.get("headsign_full") or dep.get("headsign"),
        "scheduledTime": hhmm(sched_dt),
        "scheduledTimeIso": sched_dt.isoformat(),
        "displayTime": hhmm(display_dt),
        "displayTimeIso": display_dt.isoformat(),
        "minutes": mins,
        "minutesText": "Due" if is_due else (f"{mins} min" if mins is not None else ""),
        "live": bool(live),
        "isDue": is_due,
        "vehicleRef": live.get("vehicleRef") if live else "",
        "fleet": live.get("fleet") if live else "",
        "delayMinutes": live.get("delayMinutes") if live else None,
    }


def find_live_for_trip(trip: Dict[str, Any], service_day: date, vehicles: List[Dict[str, Any]], vehicle_hint: str = "") -> Tuple[Optional[Dict[str, Any]], Optional[int]]:
    line = trip.get("line", "")
    dest = trip_headsign(trip)
    trip_id = trip.get("trip_id", "")
    hint_codes = extract_codes(vehicle_hint)
    rows = gtfs.stop_times_by_trip.get(trip_id, [])
    first = gtfs.trip_first_stop(trip_id)
    first_dt = gtfs_time_to_datetime(service_day, first.get("departure_time") or first.get("arrival_time") or "")
    best = None
    best_seq = None
    best_score = -9999
    for v in vehicles:
        if line_norm(v.get("line")) != line_norm(line):
            continue
        score = 0
        if destination_match(v.get("destinationFull") or v.get("destination"), dest):
            score += 30
        else:
            continue
        if hint_codes and hint_codes.intersection(set(v.get("codes", []))):
            score += 400
        if trip_id and (trip_id in clean_text(v.get("datedVehicleJourneyRef")) or norm(trip_id) in norm(v.get("datedVehicleJourneyRef"))):
            score += 300
        seq_found = None
        for r in rows:
            st = gtfs.stops.get(r.get("stop_id"), {})
            if stop_same(st, v.get("currentStopRef", ""), v.get("currentStopName", "")):
                seq_found = safe_int(r.get("stop_sequence"), 0)
                score += 160
                break
        live_dt = parse_iso_dt(v.get("liveTime"))
        if first_dt and live_dt:
            diff = abs((live_dt - first_dt).total_seconds()) / 60
            if diff < 90:
                score += max(0, 60 - int(diff))
        if score > best_score:
            best_score = score
            best = v
            best_seq = seq_found
    return best, best_seq


def api_error(text: str, status: int = 400):
    return JSONResponse({"ok": False, "error": text}, status_code=status)


def require_gtfs():
    if not gtfs.loaded:
        gtfs.load()
    if not gtfs.loaded:
        return api_error(f"GTFS data unavailable: {gtfs.error or 'no usable dataset'}", 503)
    return None


def initialize_legacy_stores():
    gtfs.load()
    live_store.fetch(force=True)


@app.get("/health")
def health():
    return {"ok": True, "app": APP_NAME, "time": now_london().isoformat()}


@app.get("/api/status")
def status():
    if not gtfs.loaded:
        gtfs.load()
    vehicles = live_store.fetch()
    refresh_service = getattr(app.state, "gtfs_refresh", None)
    refresh = refresh_service.snapshot() if refresh_service else {}
    return {
        "live": {
            "ok": live_store.ok,
            "activeCount": len(vehicles),
            "rawCount": live_store.raw_count,
            "maxAgeSeconds": LIVE_MAX_AGE_SECONDS,
            "operatorFilter": LIVE_OPERATOR_FILTER,
            "error": live_store.error or None,
            "lastFetchTime": live_store.last_fetch.isoformat() if live_store.last_fetch else None,
        },
        "gtfs": {
            "ok": gtfs.loaded,
            "loaded": gtfs.loaded,
            "error": gtfs.error or None,
            "source": refresh.get("source") or gtfs.source,
            "activeDataSource": gtfs.source,
            "refreshEnabled": refresh.get("enabled", False),
            "refreshRunning": refresh.get("running", False),
            "lastCheckedAt": refresh.get("lastCheckedAt"),
            "lastUpdatedAt": refresh.get("lastUpdatedAt"),
            "lastSuccessfulLoadAt": refresh.get("lastSuccessfulLoadAt"),
            "sha256": refresh.get("sha256"),
            "etag": refresh.get("etag"),
            "lastModified": refresh.get("lastModified"),
            "usingCachedData": refresh.get("usingCachedData", bool(gtfs.loaded)),
            "refreshIntervalSeconds": refresh.get("refreshIntervalSeconds"),
            "lastError": refresh.get("lastError") or (gtfs.error or None),
            "counts": {
                "agency": len(gtfs.agency),
                "stops": len(gtfs.stops),
                "routes": len(gtfs.routes),
                "trips": len(gtfs.trips),
                "stop_times_trips": len(gtfs.stop_times_by_trip),
                "stop_departures_index_stops": len(gtfs.stop_departures_index),
                "shapes": len(gtfs.shapes),
            },
        },
        "serverTime": now_london().isoformat(),
        "timezone": "Europe/London",
    }


@app.get("/")
def index():
    p = TEMPLATES_DIR / "index.html"
    if p.exists():
        return FileResponse(str(p))
    return HTMLResponse("Bluestar Unilink")


@app.get("/api/search")
def api_search(q: str = ""):
    unavailable = require_gtfs()
    if unavailable:
        return unavailable
    query = clean_text(q)
    nq = norm(query)
    stops = []
    routes = []
    if nq:
        for sid, st in gtfs.stops.items():
            hay = norm(st.get("stop_name", "") + " " + sid + " " + st.get("code", ""))
            if nq in hay:
                stops.append({
                    "id": sid,
                    "stop_id": sid,
                    "name": st.get("stop_name", sid),
                    "code": st.get("code", "BUS"),
                    "lat": st.get("lat"),
                    "lon": st.get("lon"),
                })
                if len(stops) >= 50:
                    break
        for rid, rt in gtfs.routes.items():
            line = rt.get("route_short_name", "")
            hay = norm(line + " " + rt.get("route_long_name", "") + " " + rid)
            if nq in hay or nq == norm(line):
                routes.append({
                    "id": line,
                    "routeId": rid,
                    "line": line,
                    "name": line,
                    "subtitle": rt.get("route_long_name", ""),
                })
        routes.sort(key=lambda r: (0 if line_norm(r.get("line")) == line_norm(query) else 1, r.get("line")))
    return {"ok": True, "query": query, "stops": stops[:50], "routes": routes[:40]}


@app.get("/api/stops/{stop_id}/departures")
def api_stop_departures(stop_id: str, minutes: int = DEPARTURE_WINDOW_MIN):
    unavailable = require_gtfs()
    if unavailable:
        return unavailable
    stop = gtfs.stops.get(stop_id)
    if not stop:
        return api_error("Stop not found", 404)
    n = now_london()
    end = n + timedelta(minutes=max(10, min(minutes, 360)))
    vehicles = live_store.fetch()
    result = []
    for service_day in service_days_for_departures():
        active = gtfs.active_service_ids(service_day)
        for dep in gtfs.stop_departures_index.get(stop_id, []):
            if active and dep.get("service_id") not in active:
                continue
            if clean_text(dep.get("pickup_type")) == "1" or dep.get("is_last_stop"):
                continue
            sched_dt = gtfs_time_to_datetime(service_day, dep.get("departure_time") or dep.get("arrival_time"))
            if not sched_dt or sched_dt < n - timedelta(minutes=2) or sched_dt > end:
                continue
            result.append(enrich_departure(dep, service_day, sched_dt, vehicles))
    result.sort(key=lambda x: x.get("displayTimeIso") or x.get("scheduledTimeIso") or "")
    dedup = []
    seen = set()
    for x in result:
        key = (x.get("tripId"), x.get("serviceDate"), x.get("stopSequence"))
        if key in seen:
            continue
        seen.add(key)
        dedup.append(x)
    return {"ok": True, "stop": stop, "departures": dedup[:DEPARTURE_LIMIT], "now": now_london().isoformat()}


@app.get("/api/trips/{trip_id}")
def api_trip(trip_id: str, service_date: str = "", vehicle: str = ""):
    unavailable = require_gtfs()
    if unavailable:
        return unavailable
    trip = gtfs.trips.get(trip_id)
    if not trip:
        return api_error("Trip not found", 404)
    try:
        service_day = date.fromisoformat(service_date) if service_date else now_london().date()
    except Exception:
        service_day = now_london().date()
    route = gtfs.routes.get(trip.get("route_id"), {})
    vehicles = live_store.fetch()
    live, current_seq = find_live_for_trip(trip, service_day, vehicles, vehicle)
    rows = gtfs.stop_times_by_trip.get(trip_id, [])
    n = now_london()
    delay = live.get("delayMinutes") if live else None
    out = []
    for r in rows:
        st = gtfs.stops.get(r.get("stop_id"), {})
        sched_dt = gtfs_time_to_datetime(service_day, r.get("departure_time") or r.get("arrival_time"))
        live_dt = None
        is_current = False
        live_future = False
        seq = safe_int(r.get("stop_sequence"), 0)
        if live:
            if stop_same(st, live.get("currentStopRef", ""), live.get("currentStopName", "")):
                is_current = True
                live_dt = parse_iso_dt(live.get("liveTime")) or sched_dt
            elif current_seq and seq > current_seq and isinstance(delay, int) and sched_dt:
                live_future = True
                live_dt = sched_dt + timedelta(minutes=delay)
        display_dt = live_dt or sched_dt
        mins = minutes_until(display_dt)
        if mins is not None and mins < 0:
            mins = None
        past = bool(display_dt and display_dt < n - timedelta(seconds=30) and not is_current)
        if current_seq and seq < current_seq:
            past = True
        if is_current:
            right = "LIVE" if live.get("vehicleAtStop") else "Due"
        elif mins is not None:
            right = "Due" if mins <= 1 and (live_future or live) else f"{mins}'"
        else:
            right = ""
        out.append({
            "stopId": r.get("stop_id"),
            "name": st.get("stop_name", r.get("stop_id")),
            "sequence": seq,
            "lat": st.get("lat"),
            "lon": st.get("lon"),
            "scheduledTime": hhmm(sched_dt),
            "scheduledTimeIso": sched_dt.isoformat() if sched_dt else "",
            "displayTime": hhmm(display_dt),
            "displayTimeIso": display_dt.isoformat() if display_dt else "",
            "minutes": mins,
            "rightLabel": right,
            "live": bool(is_current or live_future),
            "current": is_current,
            "past": past,
        })
    if isinstance(delay, int):
        delay_label = f"{delay:+d}"
    elif live:
        delay_label = "LIVE"
    else:
        delay_label = "--"
    return {
        "ok": True,
        "trip": {**trip, "destination": short_destination(trip_headsign(trip)), "destinationFull": trip_headsign(trip)},
        "route": route,
        "serviceDate": service_day.isoformat(),
        "stops": out,
        "live": live,
        "delayLabel": delay_label,
        "currentSequence": current_seq,
        "shape": shape_for_trip(trip),
        "now": now_london().isoformat(),
    }


@app.get("/api/routes/{line}")
def api_route(line: str):
    unavailable = require_gtfs()
    if unavailable:
        return unavailable
    ln = line_norm(line)
    route_ids = gtfs.route_by_short.get(ln, [])
    if not route_ids:
        route_ids = [rid for rid, r in gtfs.routes.items() if line_norm(r.get("route_id")) == ln]
    if not route_ids:
        return api_error("Route not found", 404)
    vehicles = [v for v in live_store.fetch() if line_norm(v.get("line")) == ln]
    active = gtfs.active_service_ids(now_london().date()) | gtfs.active_service_ids((now_london() - timedelta(days=1)).date())
    directions_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for trip in gtfs.trips.values():
        if trip.get("route_id") not in route_ids:
            continue
        if active and trip.get("service_id") not in active:
            continue
        dest = short_destination(trip_headsign(trip))
        key = (trip.get("direction_id", ""), dest)
        if key in directions_map:
            continue
        stops = []
        for r in gtfs.stop_times_by_trip.get(trip.get("trip_id"), []):
            st = gtfs.stops.get(r.get("stop_id"), {})
            stops.append({"id": r.get("stop_id"), "name": st.get("stop_name", r.get("stop_id")), "code": st.get("code", "BUS"), "lat": st.get("lat"), "lon": st.get("lon"), "sequence": r.get("stop_sequence")})
        directions_map[key] = {"directionId": key[0], "destination": dest, "tripId": trip.get("trip_id"), "stops": stops}
        if len(directions_map) >= 6:
            break
    return {"ok": True, "line": clean_text(line), "routes": [gtfs.routes[rid] for rid in route_ids], "vehicles": vehicles, "directions": list(directions_map.values())}


@app.get("/api/vehicles")
def api_vehicles(line: str = ""):
    vehicles = live_store.fetch()
    if line:
        vehicles = [v for v in vehicles if line_norm(v.get("line")) == line_norm(line)]
    vehicles.sort(key=lambda v: (line_norm(v.get("line")), v.get("destination", ""), v.get("fleet", "")))
    return {"ok": True, "line": clean_text(line), "vehicles": vehicles, "now": now_london().isoformat()}


@app.get("/api/map")
def api_map(line: str = ""):
    unavailable = require_gtfs()
    if unavailable:
        return unavailable
    ln = line_norm(line)
    vehicles = live_store.fetch()
    if ln:
        vehicles = [v for v in vehicles if line_norm(v.get("line")) == ln]
    shapes = []
    if ln:
        route_ids = gtfs.route_by_short.get(ln, [])
        active = gtfs.active_service_ids(now_london().date()) | gtfs.active_service_ids((now_london() - timedelta(days=1)).date())
        seen = set()
        for trip in gtfs.trips.values():
            sid = clean_text(trip.get("shape_id"))
            if trip.get("route_id") in route_ids and sid and sid not in seen:
                if active and trip.get("service_id") not in active:
                    continue
                pts = gtfs.shapes.get(sid, [])
                if pts:
                    shapes.append({"shapeId": sid, "points": pts[:3000]})
                    seen.add(sid)
            if len(shapes) >= 8:
                break
        if not shapes:
            for trip in gtfs.trips.values():
                if trip.get("route_id") in route_ids:
                    shapes.append({"shapeId": trip.get("trip_id"), "points": shape_for_trip(trip)})
                    break
    pts = [(v.get("latitude"), v.get("longitude")) for v in vehicles if v.get("latitude") is not None and v.get("longitude") is not None]
    if pts:
        center = {"lat": sum(p[0] for p in pts) / len(pts), "lon": sum(p[1] for p in pts) / len(pts)}
    else:
        center = {"lat": 50.9097, "lon": -1.4044}
    return {"ok": True, "line": clean_text(line), "vehicles": vehicles, "shapes": shapes, "center": center, "now": now_london().isoformat()}


@app.get("/{path:path}", response_class=HTMLResponse)
def spa_fallback(path: str):
    p = TEMPLATES_DIR / "index.html"
    if p.exists():
        return FileResponse(str(p))
    return HTMLResponse("Bluestar Unilink")
