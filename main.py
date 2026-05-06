import csv
import io
import json
import math
import os
import re
import zipfile
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime, date, time, timedelta, timezone
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

DEPARTURE_WINDOW_MIN = int(os.getenv("DEPARTURE_WINDOW_MIN", "180"))
DEPARTURE_LIMIT = int(os.getenv("DEPARTURE_LIMIT", "80"))


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

        return datetime.combine(
            service_day + timedelta(days=extra_days),
            time(h, m, sec),
            tzinfo=LONDON,
        )
    except Exception:
        return None


def datetime_to_hhmm(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(LONDON).strftime("%H:%M")


def minutes_until(dt: Optional[datetime]) -> Optional[int]:
    if not dt:
        return None
    return int(round((dt.astimezone(LONDON) - now_london()).total_seconds() / 60))


def stop_public_code_from_name(stop_name: str) -> str:
    m = re.search(r"\[([A-Z0-9]{1,6})\]", clean_text(stop_name))
    if m:
        return m.group(1).upper()
    return ""


def safe_platform_code(row: Dict[str, Any], stop_name: str) -> str:
    """
    Fontos:
    Nem használjuk a stop_id végét, mert abból sokszor rossz kód lesz.
    Csak akkor írunk megálló kódot, ha:
    - a névben van [CK] / [SG] / stb.
    - vagy van rövid platform_code mező.
    Egyébként BUS.
    """
    from_name = stop_public_code_from_name(stop_name)
    if from_name:
        return from_name

    platform = clean_text(row.get("platform_code"))
    if platform and len(platform) <= 6:
        return platform.upper()

    return "BUS"


def short_destination(value: str) -> str:
    s = human_name(value)

    known = [
        ("Winchester Bus Station", "Winchester"),
        ("Hanover Buildings", "Southampton"),
        ("Southampton, Hanover Buildings", "Southampton"),
        ("Southampton, Vincents Walk", "Southampton"),
        ("Adanac Park", "Adanac Park"),
        ("Lordshill", "Lordshill"),
        ("Weston", "Weston"),
        ("Millbrook", "Millbrook"),
        ("City Centre", "City"),
        ("Sholing", "Sholing"),
        ("Hamble", "Hamble"),
        ("Romsey", "Romsey"),
        ("Eastleigh Bus Station", "Eastleigh"),
        ("North Harbour Tesco", "North Harbour"),
        ("Thornhill", "Thornhill"),
        ("Fair Oak", "Fair Oak"),
    ]

    ns = norm(s)

    for a, b in known:
        if norm(a) in ns:
            return b

    s = s.replace("Southampton, ", "")
    s = re.sub(r"\s*\[[A-Z0-9]+\]\s*$", "", s).strip()

    if len(s) > 22:
        s = s[:20].rstrip() + "…"

    return s or ""


def minutes_between_hhmm(a: str, b: str) -> Optional[int]:
    try:
        if not a or not b:
            return None

        def parse(x: str) -> int:
            nums = re.sub(r"[^0-9]", "", x)
            if len(nums) == 3:
                nums = "0" + nums
            if len(nums) < 4:
                return -999999
            return int(nums[:2]) * 60 + int(nums[2:4])

        ma = parse(a)
        mb = parse(b)
        if ma < 0 or mb < 0:
            return None

        diff = abs(ma - mb)
        diff = min(diff, 1440 - diff)
        return diff
    except Exception:
        return None


def gtfs_time_to_code(t: str) -> str:
    if not t:
        return ""
    parts = t.split(":")
    if len(parts) < 2:
        return ""
    h = int(parts[0]) % 24
    m = int(parts[1])
    return f"{h:02d}{m:02d}"


class GTFSStore:
    def __init__(self):
        self.loaded = False
        self.error = None
        self.source = ""

        self.agency = {}
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
        if source_type == "zip":
            return self._read_file_from_zip(source, name)
        return self._read_file_from_dir(source, name)

    def load(self):
        self.reset()

        try:
            env_zip = os.getenv("GTFS_ZIP", "").strip()
            env_dir = os.getenv("GTFS_DIR", "").strip()

            zip_path = None
            dir_path = None

            if env_zip and Path(env_zip).exists():
                zip_path = Path(env_zip)
            elif GTFS_ZIP.exists():
                zip_path = GTFS_ZIP

            if env_dir and Path(env_dir).exists():
                dir_path = Path(env_dir)
            elif GTFS_DIR.exists():
                dir_path = GTFS_DIR

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

            stop_name = clean_text(r.get("stop_name"))
            lat = safe_float(r.get("stop_lat"))
            lon = safe_float(r.get("stop_lon"))
            platform_code = safe_platform_code(r, stop_name)

            self.stops[sid] = {
                "stop_id": sid,
                "stopId": sid,
                "stop_name": stop_name,
                "stopName": stop_name,
                "name": stop_name,
                "stop_code": clean_text(r.get("stop_code")),
                "platform_code": platform_code,
                "platformCode": platform_code,
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
            long_name = clean_text(r.get("route_long_name"))

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
                "trip_headsign": clean_text(r.get("trip_headsign")),
                "headsign": clean_text(r.get("trip_headsign")),
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
                "stop_headsign": clean_text(r.get("stop_headsign")),
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

            first = rows[0]
            last = rows[-1]

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

            final_stop_id = rows[-1]["stop_id"]
            final_stop = self.stops.get(final_stop_id, {})
            destination = (
                clean_text(trip.get("trip_headsign"))
                or clean_text(rows[-1].get("stop_headsign"))
                or clean_text(final_stop.get("stop_name"))
            )

            for st in rows:
                sid = st["stop_id"]

                self.stop_departures_index[sid].append(
                    {
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
                    }
                )

        for sid in self.stop_departures_index:
            self.stop_departures_index[sid].sort(
                key=lambda x: (
                    x.get("departure_time") or "",
                    x.get("line") or "",
                    x.get("trip_id") or "",
                )
            )

        for r in shapes_rows:
            shape_id = clean_text(r.get("shape_id"))
            lat = safe_float(r.get("shape_pt_lat"))
            lon = safe_float(r.get("shape_pt_lon"))
            seq = safe_int(r.get("shape_pt_sequence"))

            if shape_id and lat is not None and lon is not None:
                self.shapes[shape_id].append(
                    {
                        "lat": lat,
                        "lon": lon,
                        "seq": seq,
                    }
                )

        for sid in self.shapes:
            self.shapes[sid].sort(key=lambda x: x["seq"])

    def active_service_ids(self, service_day: date) -> set:
        ymd = service_day.strftime("%Y%m%d")
        weekday = [
            "monday",
            "tuesday",
            "wednesday",
            "thursday",
            "friday",
            "saturday",
            "sunday",
        ][service_day.weekday()]

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
            elif ex == 2 and sid in result:
                result.remove(sid)

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

        return {
            "calendar_start_min": min(dates) if dates else "",
            "calendar_end_max": max(dates) if dates else "",
        }


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

        api_key = (
            os.getenv("BODS_API_KEY", "").strip()
            or os.getenv("BODS_KEY", "").strip()
            or os.getenv("LIVE_API_KEY", "").strip()
            or os.getenv("API_KEY", "").strip()
        )

        self.key_present = bool(api_key)
        self.key_preview = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) >= 8 else ""

        parsed = urllib.parse.urlparse(url)
        query = dict(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))

        if api_key and "api_key" not in query:
            query["api_key"] = api_key

        clean_url = urllib.parse.urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                "",
                parsed.fragment,
            )
        )

        self.effective_feed_url = clean_url

        return clean_url, query

    def fetch(self, force: bool = False) -> List[Dict[str, Any]]:
        n = now_london()

        if (
            not force
            and self.cache_time
            and (n - self.cache_time).total_seconds() < LIVE_CACHE_TTL_SEC
        ):
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

        full_url = url
        if params:
            full_url = f"{url}?{urllib.parse.urlencode(params)}"

        try:
            req = urllib.request.Request(
                full_url,
                headers={
                    "User-Agent": "Bluestar-Unilink-Webapp/1.0",
                    "Accept": "application/xml,text/xml,*/*",
                },
            )

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

            active = self.dedupe_latest(active)

            self.vehicles = active
            self.active_count = len(active)
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
        latest = {}

        for v in vehicles:
            key = (
                clean_text(v.get("vehicleUniqueId"))
                or clean_text(v.get("vehicleRef"))
                or f"{v.get('lineNorm')}:{v.get('journeyCode')}:{v.get('datedVehicleJourneyRef')}"
            )

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
            age = None

            if recorded_dt:
                age = int((now_london() - recorded_dt).total_seconds())

            line = text_any(mvj, "PublishedLineName") or text_any(mvj, "LineRef")
            destination_name = human_name(text_any(mvj, "DestinationName"))
            origin_name = human_name(text_any(mvj, "OriginName"))

            vehicle_ref = text_any(mvj, "VehicleRef")
            vehicle_unique = text_any(mvj, "VehicleUniqueId")

            current_stop_ref = ""
            current_stop_name = ""
            vehicle_at_stop = False

            if call is not None:
                current_stop_ref = text_child(call, "StopPointRef")
                current_stop_name = human_name(text_child(call, "StopPointName"))
                vehicle_at_stop = text_child(call, "VehicleAtStop").lower() == "true"

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
                "lon": lon,
                "lat": lat,
                "bearing": safe_float(text_any(mvj, "Bearing")),
                "blockRef": text_any(mvj, "BlockRef"),
                "vehicleRef": vehicle_ref,
                "vehicleUniqueId": vehicle_unique or vehicle_ref,
                "ticketMachineServiceCode": text_any(mvj, "TicketMachineServiceCode"),
                "journeyCode": text_any(mvj, "JourneyCode"),
                "currentStopRef": current_stop_ref,
                "currentStopName": current_stop_name,
                "vehicleAtStop": vehicle_at_stop,
                "ageSeconds": age,
            }

            item["label"] = item["publishedLineName"]
            item["mapLabel"] = f"{item['publishedLineName']} {item['destinationShort']}".strip()

            result.append(item)

        return result

    def status(self) -> Dict[str, Any]:
        return {
            "ok": not bool(self.last_error),
            "activeCount": self.active_count,
            "rawCount": self.raw_count,
            "maxAgeSeconds": LIVE_MAX_AGE_SECONDS,
            "operatorFilter": LIVE_OPERATOR_FILTER,
            "error": self.last_error or None,
            "lastHttpStatus": self.last_http_status,
            "lastFetchTime": self.last_fetch_time,
        }


live = LiveStore()


def live_trip_score(vehicle: Dict[str, Any], trip: Dict[str, Any]) -> int:
    score = 0

    if line_norm(vehicle.get("publishedLineName")) != line_norm(trip.get("line")):
        return -999

    origin_ref = clean_text(vehicle.get("originRef"))
    dest_ref = clean_text(vehicle.get("destinationRef"))

    if origin_ref and origin_ref == clean_text(trip.get("first_stop_id")):
        score += 35

    if dest_ref and dest_ref == clean_text(trip.get("last_stop_id")):
        score += 35

    vdest = norm(vehicle.get("destinationName") or vehicle.get("destinationShort"))
    tdest = norm(trip.get("trip_headsign") or trip.get("headsign"))

    if vdest and tdest:
        if vdest in tdest or tdest in vdest:
            score += 20

    first_code = clean_text(trip.get("first_departure_code"))

    live_codes = [
        clean_text(vehicle.get("journeyCode")),
        clean_text(vehicle.get("datedVehicleJourneyRef")),
    ]

    for lc in live_codes:
        diff = minutes_between_hhmm(lc, first_code)
        if diff is None:
            continue

        if diff <= 3:
            score += 80
            break
        elif diff <= 10:
            score += 60
            break
        elif diff <= 20:
            score += 35
            break

    return score


def match_live_to_departures(departures: List[Dict[str, Any]], stop_id: str) -> List[Dict[str, Any]]:
    vehicles = live.fetch()

    if not departures or not vehicles:
        return departures

    used_vehicles = set()

    for dep in departures:
        trip = gtfs.trips.get(dep.get("trip_id"))
        if not trip:
            continue

        best = None
        best_score = -999

        for v in vehicles:
            vkey = clean_text(v.get("vehicleUniqueId")) or clean_text(v.get("vehicleRef"))
            if vkey in used_vehicles:
                continue

            score = live_trip_score(v, trip)

            if clean_text(v.get("currentStopRef")) == stop_id:
                score += 25

            if v.get("vehicleAtStop"):
                score += 15

            if score > best_score:
                best = v
                best_score = score

        if best and best_score >= 80:
            vkey = clean_text(best.get("vehicleUniqueId")) or clean_text(best.get("vehicleRef"))
            used_vehicles.add(vkey)

            dep["live"] = True
            dep["isLive"] = True
            dep["source"] = "LIVE"
            dep["sourceLabel"] = "Élő adat"
            dep["vehicleRef"] = best.get("vehicleRef") or best.get("vehicleUniqueId") or ""
            dep["vehicleUniqueId"] = best.get("vehicleUniqueId") or ""
            dep["recordedAtTime"] = best.get("recordedAtTime")
            dep["ageSeconds"] = best.get("ageSeconds")
            dep["vehicleAtStop"] = bool(best.get("vehicleAtStop"))
            dep["liveDestination"] = best.get("destinationShort") or best.get("destinationName") or ""

            if best.get("vehicleAtStop") or clean_text(best.get("currentStopRef")) == stop_id:
                dep["due"] = True
                dep["minutes"] = 0
                dep["displayTime"] = "Due"
                dep["minutesText"] = "Due"

    return departures


@app.on_event("startup")
def startup_event():
    gtfs.load()
    live.fetch(force=True)


@app.get("/health")
def health():
    return {
        "ok": True,
        "app": APP_NAME,
        "time": iso_now_london(),
    }


@app.get("/api/status")
def api_status():
    if not gtfs.loaded:
        gtfs.load()

    live.fetch()

    return {
        "live": live.status(),
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
    if not gtfs.loaded:
        gtfs.load()

    return {
        "loaded": gtfs.loaded,
        "ok": gtfs.loaded,
        "error": gtfs.error,
        "source": gtfs.source,
        "counts": gtfs.counts(),
        "calendarRange": gtfs.calendar_range(),
        "agencyFilter": "",
    }


@app.get("/api/live/status")
def live_status():
    live.fetch()
    return live.status()


@app.get("/api/live/debug")
def live_debug():
    vehicles = live.fetch(force=True)

    return {
        "effectiveFeedUrl": live.effective_feed_url,
        "keyPreview": live.key_preview,
        "keyPresent": live.key_present,
        "vehicleCount": len(vehicles),
        "activeCount": live.active_count,
        "rawCount": live.raw_count,
        "lastError": live.last_error,
        "lastHttpStatus": live.last_http_status,
        "lastFetchTime": live.last_fetch_time,
        "sample": vehicles[:8],
    }


@app.get("/api/live/vehicles")
def live_vehicles(line: str = "", force: int = 0):
    vehicles = live.fetch(force=bool(force))

    if line:
        ln = line_norm(line)
        vehicles = [v for v in vehicles if v.get("lineNorm") == ln]

    return {
        "vehicles": vehicles,
        "count": len(vehicles),
        "activeCount": len(vehicles),
        "rawCount": live.raw_count,
        "operatorFilter": LIVE_OPERATOR_FILTER,
        "maxAgeSeconds": LIVE_MAX_AGE_SECONDS,
    }


@app.get("/api/vehicles")
def vehicles_alias(line: str = "", force: int = 0):
    return live_vehicles(line=line, force=force)


@app.get("/api/map/vehicles")
def map_vehicles(line: str = "", force: int = 0):
    vehicles = live.fetch(force=bool(force))

    if line:
        ln = line_norm(line)
        vehicles = [v for v in vehicles if v.get("lineNorm") == ln]

    return {
        "vehicles": vehicles,
        "count": len(vehicles),
        "line": line,
    }


@app.post("/api/upload/gtfs")
async def upload_gtfs(request: Request):
    body = await request.body()

    if not body:
        return JSONResponse(
            {
                "ok": False,
                "error": "Empty upload body",
            },
            status_code=400,
        )

    GTFS_ZIP.write_bytes(body)
    gtfs.load()

    return {
        "ok": gtfs.loaded,
        "error": gtfs.error,
        "source": gtfs.source,
        "counts": gtfs.counts(),
        "calendarRange": gtfs.calendar_range(),
    }


@app.get("/api/reload")
def reload_all():
    gtfs.load()
    live.fetch(force=True)
    return api_status()


@app.get("/api/stops")
def all_stops():
    if not gtfs.loaded:
        gtfs.load()

    return {
        "stops": list(gtfs.stops.values()),
        "count": len(gtfs.stops),
    }


@app.get("/api/routes")
def all_routes():
    if not gtfs.loaded:
        gtfs.load()

    routes = sorted(
        gtfs.routes.values(),
        key=lambda r: line_norm(r.get("route_short_name")),
    )

    return {
        "routes": routes,
        "count": len(routes),
    }


@app.get("/api/search")
def search(q: str = "", query: str = "", limit: int = 80):
    if not gtfs.loaded:
        gtfs.load()

    term = q or query
    term_norm = norm(term)

    stops = []
    routes = []

    if not term_norm:
        return {
            "query": term,
            "stops": [],
            "routes": [],
            "items": [],
        }

    for stop in gtfs.stops.values():
        hay = norm(
            f"{stop.get('stop_name')} {stop.get('stop_id')} {stop.get('platform_code')}"
        )

        if term_norm in hay:
            stops.append(
                {
                    **stop,
                    "type": "stop",
                    "title": stop.get("stop_name"),
                    "subtitle": stop.get("stop_id"),
                    "badge": stop.get("platform_code") or "BUS",
                }
            )

        if len(stops) >= limit:
            break

    for route in gtfs.routes.values():
        line = clean_text(route.get("route_short_name"))
        hay = norm(
            f"{line} {route.get('route_long_name')} {route.get('route_id')}"
        )

        if term_norm in hay or term_norm == norm(line):
            routes.append(
                {
                    **route,
                    "type": "route",
                    "title": line,
                    "subtitle": clean_text(route.get("route_long_name")) or "Viszonylat",
                    "badge": line,
                    "line": line,
                }
            )

    routes = sorted(routes, key=lambda r: line_norm(r.get("line")))[:limit]

    return {
        "query": term,
        "stops": stops,
        "routes": routes,
        "items": stops + routes,
    }


@app.get("/api/stop/{stop_id}")
def stop_detail(stop_id: str):
    if not gtfs.loaded:
        gtfs.load()

    stop = gtfs.stops.get(stop_id)

    if not stop:
        return JSONResponse(
            {
                "error": "Stop not found",
                "stop_id": stop_id,
            },
            status_code=404,
        )

    return stop


@app.get("/api/stop/{stop_id}/departures")
def stop_departures(
    stop_id: str,
    minutes: int = DEPARTURE_WINDOW_MIN,
    limit: int = DEPARTURE_LIMIT,
):
    if not gtfs.loaded:
        gtfs.load()

    stop = gtfs.stops.get(stop_id)

    if not stop:
        return JSONResponse(
            {
                "error": "Stop not found",
                "stop_id": stop_id,
                "departures": [],
            },
            status_code=404,
        )

    n = now_london()
    end = n + timedelta(minutes=minutes)

    departures = []
    seen = set()

    candidate_service_days = [
        n.date() - timedelta(days=1),
        n.date(),
        n.date() + timedelta(days=1),
    ]

    active_by_day = {
        d: gtfs.active_service_ids(d)
        for d in candidate_service_days
    }

    rows = gtfs.stop_departures_index.get(stop_id, [])

    for row in rows:
        if clean_text(row.get("pickup_type")) == "1":
            continue

        trip = gtfs.trips.get(row["trip_id"], {})
        service_id = trip.get("service_id")

        for service_day in candidate_service_days:
            if service_id not in active_by_day.get(service_day, set()):
                continue

            dt = gtfs_time_to_datetime(
                service_day,
                row.get("departure_time") or row.get("arrival_time"),
            )

            if not dt:
                continue

            if dt < n - timedelta(minutes=1):
                continue

            if dt > end:
                continue

            key = f"{row.get('trip_id')}|{row.get('stop_id')}|{dt.isoformat()}"

            if key in seen:
                continue

            seen.add(key)

            m = minutes_until(dt)

            item = {
                "tripId": row.get("trip_id"),
                "trip_id": row.get("trip_id"),
                "routeId": row.get("route_id"),
                "route_id": row.get("route_id"),
                "serviceId": row.get("service_id"),
                "service_id": row.get("service_id"),
                "line": row.get("line"),
                "route": row.get("line"),
                "routeShortName": row.get("line"),
                "destination": row.get("destination"),
                "destinationFull": row.get("destinationFull"),
                "headsign": row.get("headsign"),
                "directionId": row.get("direction_id"),
                "direction_id": row.get("direction_id"),
                "stopId": stop_id,
                "stop_id": stop_id,
                "platform": stop_id,
                "platformCode": stop.get("platform_code"),
                "stopName": stop.get("stop_name"),
                "scheduledTime": datetime_to_hhmm(dt),
                "scheduledDateTime": dt.isoformat(),
                "displayTime": datetime_to_hhmm(dt),
                "minutes": m,
                "minutesText": "Due" if m is not None and m <= 1 else f"{m} min" if m is not None else "",
                "source": "GTFS",
                "sourceLabel": "Menetrendi adat",
                "live": False,
                "isLive": False,
                "due": m is not None and m <= 1,
            }

            departures.append(item)

    departures.sort(key=lambda x: x.get("scheduledDateTime", ""))
    departures = departures[:limit]
    departures = match_live_to_departures(departures, stop_id)

    return {
        "stop": stop,
        "stop_id": stop_id,
        "stop_name": stop.get("stop_name"),
        "departures": departures,
        "count": len(departures),
        "windowMinutes": minutes,
        "note": "Fehér: menetrend (GTFS) · Zöld: élő (LIVE)",
    }


@app.get("/api/departures/{stop_id}")
def departures_alias(
    stop_id: str,
    minutes: int = DEPARTURE_WINDOW_MIN,
    limit: int = DEPARTURE_LIMIT,
):
    return stop_departures(stop_id=stop_id, minutes=minutes, limit=limit)


@app.get("/api/trip/{trip_id}")
def trip_detail(trip_id: str):
    if not gtfs.loaded:
        gtfs.load()

    trip = gtfs.trips.get(trip_id)

    if not trip:
        return JSONResponse(
            {
                "error": "Trip not found",
                "trip_id": trip_id,
            },
            status_code=404,
        )

    route = gtfs.routes.get(trip.get("route_id"), {})
    rows = gtfs.stop_times_by_trip.get(trip_id, [])

    today = now_london().date()

    service_days = [
        today - timedelta(days=1),
        today,
        today + timedelta(days=1),
    ]

    best_day = today

    for d in service_days:
        if trip.get("service_id") in gtfs.active_service_ids(d):
            best_day = d
            break

    stops = []

    for st in rows:
        stop = gtfs.stops.get(st.get("stop_id"), {})
        dt = gtfs_time_to_datetime(
            best_day,
            st.get("departure_time") or st.get("arrival_time"),
        )

        stops.append(
            {
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
                "isPast": dt < now_london() if dt else False,
                "vehicleAtStop": False,
            }
        )

    line = clean_text(route.get("route_short_name")) or clean_text(trip.get("line"))
    destination = clean_text(trip.get("trip_headsign"))

    if not destination and stops:
        destination = stops[-1].get("stopName", "")

    vehicles = live.fetch()
    matched_vehicle = None
    best_score = -999

    for v in vehicles:
        score = live_trip_score(v, trip)
        if score > best_score:
            best_score = score
            matched_vehicle = v

    if best_score < 80:
        matched_vehicle = None

    if matched_vehicle:
        current_ref = clean_text(matched_vehicle.get("currentStopRef"))

        for s in stops:
            if current_ref and s.get("stop_id") == current_ref:
                s["live"] = True
                s["vehicleAtStop"] = bool(matched_vehicle.get("vehicleAtStop"))

    return {
        "trip": trip,
        "route": route,
        "line": line,
        "destination": short_destination(destination),
        "destinationFull": human_name(destination),
        "vehicle": matched_vehicle,
        "liveMatched": bool(matched_vehicle),
        "stops": stops,
        "count": len(stops),
    }


@app.get("/api/route/{line}")
def route_detail(line: str):
    if not gtfs.loaded:
        gtfs.load()

    ln = line_norm(line)
    route_ids = gtfs.route_by_short.get(ln, [])

    if not route_ids:
        return JSONResponse(
            {
                "error": "Route not found",
                "line": line,
            },
            status_code=404,
        )

    routes = [gtfs.routes[rid] for rid in route_ids if rid in gtfs.routes]

    directions = []
    seen = set()

    for trip in gtfs.trips.values():
        if trip.get("route_id") not in route_ids:
            continue

        rows = gtfs.stop_times_by_trip.get(trip["trip_id"], [])

        if not rows:
            continue

        first = gtfs.stops.get(rows[0]["stop_id"], {})
        last = gtfs.stops.get(rows[-1]["stop_id"], {})

        headsign = trip.get("trip_headsign") or last.get("stop_name")
        key = f"{trip.get('direction_id')}|{first.get('stop_id')}|{last.get('stop_id')}|{headsign}"

        if key in seen:
            continue

        seen.add(key)

        stops = []

        for st in rows:
            stop = gtfs.stops.get(st["stop_id"], {})
            stops.append(
                {
                    "stop_id": st["stop_id"],
                    "stopId": st["stop_id"],
                    "stop_name": stop.get("stop_name"),
                    "stopName": stop.get("stop_name"),
                    "lat": stop.get("lat"),
                    "lon": stop.get("lon"),
                    "platformCode": stop.get("platform_code"),
                    "stop_sequence": st.get("stop_sequence"),
                    "sequence": st.get("stop_sequence"),
                }
            )

        directions.append(
            {
                "trip_id": trip["trip_id"],
                "tripId": trip["trip_id"],
                "route_id": trip["route_id"],
                "direction_id": trip.get("direction_id"),
                "directionId": trip.get("direction_id"),
                "headsign": human_name(headsign),
                "destination": short_destination(headsign),
                "from": first,
                "to": last,
                "stop_count": len(stops),
                "stopCount": len(stops),
                "stops": stops,
                "shape_id": trip.get("shape_id"),
                "shapeId": trip.get("shape_id"),
            }
        )

    directions.sort(key=lambda d: d.get("destination") or "")

    return {
        "line": line,
        "routes": routes,
        "directions": directions,
        "vehicles": live_vehicles(line=line).get("vehicles", []),
    }


@app.get("/api/route/{line}/directions")
def route_directions(line: str):
    return route_detail(line)


@app.get("/api/route/{line}/vehicles")
def route_vehicles(line: str):
    return live_vehicles(line=line, force=0)


@app.get("/api/route/{line}/shape")
def route_shape(line: str):
    if not gtfs.loaded:
        gtfs.load()

    ln = line_norm(line)
    route_ids = gtfs.route_by_short.get(ln, [])

    shapes = []
    seen = set()

    for trip in gtfs.trips.values():
        if trip.get("route_id") not in route_ids:
            continue

        shape_id = trip.get("shape_id")

        if not shape_id or shape_id in seen:
            continue

        seen.add(shape_id)

        pts = gtfs.shapes.get(shape_id, [])

        if pts:
            shapes.append(
                {
                    "shape_id": shape_id,
                    "direction_id": trip.get("direction_id"),
                    "headsign": trip.get("trip_headsign"),
                    "points": pts,
                }
            )

    return {
        "line": line,
        "shapes": shapes,
        "count": len(shapes),
    }


@app.get("/", response_class=HTMLResponse)
async def home():
    index_in_templates = TEMPLATES_DIR / "index.html"
    index_in_root = BASE_DIR / "index.html"

    if index_in_templates.exists():
        return FileResponse(str(index_in_templates), media_type="text/html")

    if index_in_root.exists():
        return FileResponse(str(index_in_root), media_type="text/html")

    return HTMLResponse(
        """
        <!doctype html>
        <html lang="hu">
        <head>
          <meta charset="utf-8">
          <meta name="viewport" content="width=device-width, initial-scale=1">
          <title>Bluestar Unilink</title>
        </head>
        <body style="background:#303030;color:white;font-family:Arial;padding:20px">
          <h1>Bluestar Unilink</h1>
          <p>index.html nem található.</p>
          <p>Tedd ide: templates/index.html</p>
        </body>
        </html>
        """,
        status_code=200,
    )


@app.get("/{full_path:path}", response_class=HTMLResponse)
async def spa_fallback(full_path: str):
    if full_path.startswith("api/"):
        return JSONResponse(
            {
                "error": "API endpoint not found",
                "path": full_path,
            },
            status_code=404,
        )

    index_in_templates = TEMPLATES_DIR / "index.html"
    index_in_root = BASE_DIR / "index.html"

    if index_in_templates.exists():
        return FileResponse(str(index_in_templates), media_type="text/html")

    if index_in_root.exists():
        return FileResponse(str(index_in_root), media_type="text/html")

    return HTMLResponse("index.html nem található", status_code=404)
