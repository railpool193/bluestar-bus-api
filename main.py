import csv
import io
import os
import re
import time
import zipfile
import math
import json
from pathlib import Path
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode

import httpx
from fastapi import FastAPI, UploadFile, File
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from starlette.requests import Request
import xml.etree.ElementTree as ET


# ============================================================
# CONFIG
# ============================================================

APP_TITLE = "Bluestar Unilink Menetrend"

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"

DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "gtfs")))
GTFS_ZIP_PATH = Path(os.getenv("GTFS_ZIP_PATH", str(BASE_DIR / "gtfs.zip")))

LIVE_FEED_ID = os.getenv("LIVE_FEED_ID", "7721")
LIVE_API_KEY = (
    os.getenv("BODS_API_KEY")
    or os.getenv("BUS_DATA_API_KEY")
    or os.getenv("API_KEY")
    or os.getenv("LIVE_API_KEY")
    or ""
).strip()

LIVE_FEED_URL = os.getenv(
    "LIVE_FEED_URL",
    f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{LIVE_FEED_ID}/",
).strip()

LIVE_ENABLED = os.getenv("LIVE_ENABLED", "1").lower() not in ("0", "false", "no", "off")
LIVE_CACHE_TTL_SEC = int(os.getenv("LIVE_CACHE_TTL_SEC", "15"))
LIVE_ACTIVE_MAX_AGE_SECONDS = int(os.getenv("LIVE_ACTIVE_MAX_AGE_SECONDS", "240"))
LIVE_OPERATOR_FILTER = os.getenv("LIVE_OPERATOR_FILTER", "BLUS").strip()

LOCAL_TZ_NAME = os.getenv("TZ", "Europe/London")


# ============================================================
# APP
# ============================================================

app = FastAPI(title=APP_TITLE)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))


# ============================================================
# GLOBAL STATE
# ============================================================

GTFS: Dict[str, Any] = {
    "loaded": False,
    "error": None,
    "source": None,

    "agency": [],
    "stops": {},
    "routes": {},
    "trips": {},

    "stop_times_by_trip": {},
    "departures_by_stop": {},

    "calendar": [],
    "calendar_dates": {},

    "routes_by_line": {},
    "trip_meta": {},
    "line_directions": {},
    "shapes": {},
}

LIVE_CACHE: Dict[str, Any] = {
    "fetched_at_monotonic": 0.0,
    "vehicles": [],
    "raw_count": 0,
    "active_count": 0,
    "last_error": "",
    "last_http_status": None,
    "last_fetch_time": None,
    "effective_url": "",
}


# ============================================================
# TIME HELPERS
# ============================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_local() -> datetime:
    return utc_now().astimezone()


def today_local() -> date:
    return now_local().date()


def parse_iso_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None

    s = str(value).strip()
    if not s:
        return None

    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"

        dt = datetime.fromisoformat(s)

        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt.astimezone()
    except Exception:
        return None


def parse_gtfs_time_to_seconds(value: Any) -> Optional[int]:
    if value is None:
        return None

    s = str(value).strip()
    if not s:
        return None

    parts = s.split(":")
    if len(parts) < 2:
        return None

    try:
        h = int(parts[0])
        m = int(parts[1])
        sec = int(parts[2]) if len(parts) > 2 else 0
        return h * 3600 + m * 60 + sec
    except Exception:
        return None


def departure_datetime(service_date: date, seconds: int) -> datetime:
    base = datetime(service_date.year, service_date.month, service_date.day)
    base = base.replace(tzinfo=now_local().tzinfo)
    return base + timedelta(seconds=int(seconds))


def seconds_from_service_date(dt: datetime, service_date: date) -> int:
    base = datetime(service_date.year, service_date.month, service_date.day)
    base = base.replace(tzinfo=dt.tzinfo)
    return int((dt - base).total_seconds())


def hhmm(dt: Optional[datetime]) -> str:
    if not dt:
        return ""

    return dt.astimezone().strftime("%H:%M")


def date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


# ============================================================
# TEXT HELPERS
# ============================================================

def clean_text(s: Any) -> str:
    if s is None:
        return ""

    x = str(s)
    x = x.replace("_", " ")
    x = re.sub(r"\s+", " ", x)
    return x.strip()


def norm_text(s: Any) -> str:
    x = clean_text(s).lower()
    x = x.replace("&", "and")
    x = re.sub(r"[^a-z0-9]+", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x


def norm_line(s: Any) -> str:
    x = clean_text(s).upper()
    x = x.replace(" ", "")
    return x


def names_match(a: Any, b: Any) -> bool:
    aa = norm_text(a)
    bb = norm_text(b)

    if not aa or not bb:
        return False

    if aa == bb:
        return True

    if aa in bb or bb in aa:
        return True

    return False


def extract_stop_code(stop_name: str, stop_id: str = "") -> str:
    m = re.search(r"\[([A-Za-z0-9]+)\]", stop_name or "")

    if m:
        return m.group(1).upper()

    sid = str(stop_id or "")

    if len(sid) >= 2:
        return sid[-2:].upper()

    return "BUS"


def pretty_place_name(s: Optional[str]) -> Optional[str]:
    if not s:
        return s

    x = str(s)

    x = x.replace("_", " ")
    x = x.replace(" stop ", " ")
    x = x.replace(" stop", "")
    x = x.replace(" Stand G", " [G]")
    x = x.replace(" Stand", "")
    x = x.replace(" CU", " [CU]")
    x = x.replace(" CM", " [CM]")
    x = x.replace(" CK", " [CK]")
    x = x.replace(" SG", " [SG]")
    x = x.replace(" SI", " [SI]")

    x = re.sub(r"\s+", " ", x).strip()

    return x


def short_destination_name(s: Any) -> str:
    x = pretty_place_name(clean_text(s)) or ""

    replacements = {
        "Winchester Bus Station": "Winchester",
        "Southampton Hanover Buildings": "Southampton",
        "Hanover Buildings": "Southampton",
        "Southampton City Centre": "City Centre",
        "Weston Barnfield Road": "Weston",
        "Barton Peveril": "Barton Peveril",
        "North Harbour Tesco": "North Harbour",
        "Eastleigh Bus Station": "Eastleigh",
        "Romsey Bus Station": "Romsey",
        "Lordshill Sainsbury's": "Lordshill",
        "Adanac Park": "Adanac Park",
    }

    for k, v in replacements.items():
        if k.lower() in x.lower():
            return v

    if len(x) > 22:
        return x[:22].rstrip() + "…"

    return x


# ============================================================
# CSV / GTFS LOADING
# ============================================================

def read_csv_from_bytes(data: bytes) -> List[Dict[str, str]]:
    text = data.decode("utf-8-sig", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    return [dict(row) for row in reader]


def load_gtfs_file_map() -> Tuple[Dict[str, bytes], str]:
    files: Dict[str, bytes] = {}

    if GTFS_ZIP_PATH.exists():
        with zipfile.ZipFile(GTFS_ZIP_PATH, "r") as z:
            for name in z.namelist():
                base = Path(name).name
                if base.endswith(".txt"):
                    files[base] = z.read(name)

        return files, f"zip:{GTFS_ZIP_PATH.name}"

    if DATA_DIR.exists() and DATA_DIR.is_dir():
        for p in DATA_DIR.glob("*.txt"):
            files[p.name] = p.read_bytes()

        return files, f"folder:{DATA_DIR}"

    raise FileNotFoundError("Nem található gtfs.zip vagy gtfs mappa.")


def get_required_gtfs_file(files: Dict[str, bytes], filename: str) -> List[Dict[str, str]]:
    if filename not in files:
        raise FileNotFoundError(f"Hiányzó GTFS fájl: {filename}")

    return read_csv_from_bytes(files[filename])


def get_optional_gtfs_file(files: Dict[str, bytes], filename: str) -> List[Dict[str, str]]:
    if filename not in files:
        return []

    return read_csv_from_bytes(files[filename])


def active_service_ids_for_date(d: date) -> set:
    ymd = date_to_yyyymmdd(d)
    weekday = d.weekday()

    weekday_field = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ][weekday]

    active = set()

    for row in GTFS["calendar"]:
        try:
            start = row.get("start_date", "")
            end = row.get("end_date", "")

            if start <= ymd <= end and str(row.get(weekday_field, "0")) == "1":
                active.add(row.get("service_id", ""))
        except Exception:
            continue

    exceptions = GTFS["calendar_dates"].get(ymd, [])

    for service_id, exception_type in exceptions:
        if exception_type == "1":
            active.add(service_id)
        elif exception_type == "2":
            active.discard(service_id)

    return active


def load_gtfs() -> None:
    global GTFS

    try:
        files, source = load_gtfs_file_map()

        agency_rows = get_optional_gtfs_file(files, "agency.txt")
        stops_rows = get_required_gtfs_file(files, "stops.txt")
        routes_rows = get_required_gtfs_file(files, "routes.txt")
        trips_rows = get_required_gtfs_file(files, "trips.txt")
        stop_times_rows = get_required_gtfs_file(files, "stop_times.txt")
        calendar_rows = get_optional_gtfs_file(files, "calendar.txt")
        calendar_dates_rows = get_optional_gtfs_file(files, "calendar_dates.txt")
        shapes_rows = get_optional_gtfs_file(files, "shapes.txt")

        stops: Dict[str, Dict[str, Any]] = {}
        routes: Dict[str, Dict[str, Any]] = {}
        trips: Dict[str, Dict[str, Any]] = {}
        stop_times_by_trip: Dict[str, List[Dict[str, Any]]] = {}
        departures_by_stop: Dict[str, List[Dict[str, Any]]] = {}
        routes_by_line: Dict[str, List[Dict[str, Any]]] = {}
        trip_meta: Dict[str, Dict[str, Any]] = {}
        line_directions: Dict[str, List[Dict[str, Any]]] = {}
        calendar_dates: Dict[str, List[Tuple[str, str]]] = {}
        shapes: Dict[str, List[List[float]]] = {}

        for row in stops_rows:
            stop_id = row.get("stop_id", "").strip()
            if not stop_id:
                continue

            name = clean_text(row.get("stop_name", ""))

            try:
                lat = float(row.get("stop_lat", "") or 0)
                lon = float(row.get("stop_lon", "") or 0)
            except Exception:
                lat = 0.0
                lon = 0.0

            stops[stop_id] = {
                "stop_id": stop_id,
                "stop_name": name,
                "stop_code": row.get("stop_code", ""),
                "stop_lat": lat,
                "stop_lon": lon,
                "indicator": extract_stop_code(name, stop_id),
            }

        for row in routes_rows:
            route_id = row.get("route_id", "").strip()
            if not route_id:
                continue

            short_name = clean_text(row.get("route_short_name", "") or row.get("short_name", ""))
            long_name = clean_text(row.get("route_long_name", "") or row.get("long_name", ""))

            route = {
                "route_id": route_id,
                "agency_id": row.get("agency_id", ""),
                "short_name": short_name,
                "long_name": long_name,
                "route_type": row.get("route_type", ""),
                "route_color": row.get("route_color", ""),
                "route_text_color": row.get("route_text_color", ""),
            }

            routes[route_id] = route

            line = norm_line(short_name)

            if line:
                routes_by_line.setdefault(line, []).append(route)

        for row in trips_rows:
            trip_id = row.get("trip_id", "").strip()
            route_id = row.get("route_id", "").strip()

            if not trip_id or not route_id:
                continue

            route = routes.get(route_id, {})
            line = route.get("short_name", "")

            trips[trip_id] = {
                "trip_id": trip_id,
                "route_id": route_id,
                "service_id": row.get("service_id", ""),
                "trip_headsign": clean_text(row.get("trip_headsign", "")),
                "direction_id": row.get("direction_id", ""),
                "block_id": row.get("block_id", ""),
                "shape_id": row.get("shape_id", ""),
                "line": line,
            }

        for row in stop_times_rows:
            trip_id = row.get("trip_id", "").strip()
            stop_id = row.get("stop_id", "").strip()

            if not trip_id or not stop_id:
                continue

            arr_s = parse_gtfs_time_to_seconds(row.get("arrival_time"))
            dep_s = parse_gtfs_time_to_seconds(row.get("departure_time"))

            if dep_s is None:
                dep_s = arr_s

            if arr_s is None:
                arr_s = dep_s

            if dep_s is None:
                continue

            try:
                seq = int(row.get("stop_sequence", "0") or 0)
            except Exception:
                seq = 0

            st = {
                "trip_id": trip_id,
                "stop_id": stop_id,
                "arrival_time": row.get("arrival_time", ""),
                "departure_time": row.get("departure_time", ""),
                "arrival_s": arr_s,
                "departure_s": dep_s,
                "stop_sequence": seq,
                "pickup_type": row.get("pickup_type", ""),
                "drop_off_type": row.get("drop_off_type", ""),
            }

            stop_times_by_trip.setdefault(trip_id, []).append(st)

        for trip_id, arr in stop_times_by_trip.items():
            arr.sort(key=lambda x: x["stop_sequence"])

        for row in calendar_dates_rows:
            ymd = row.get("date", "").strip()
            sid = row.get("service_id", "").strip()
            et = row.get("exception_type", "").strip()

            if ymd and sid and et:
                calendar_dates.setdefault(ymd, []).append((sid, et))

        for row in shapes_rows:
            sid = row.get("shape_id", "").strip()

            if not sid:
                continue

            try:
                lat = float(row.get("shape_pt_lat", "") or 0)
                lon = float(row.get("shape_pt_lon", "") or 0)
                seq = int(row.get("shape_pt_sequence", "0") or 0)
            except Exception:
                continue

            shapes.setdefault(sid, []).append([seq, lat, lon])

        for sid in list(shapes.keys()):
            shapes[sid].sort(key=lambda x: x[0])
            shapes[sid] = [[x[1], x[2]] for x in shapes[sid]]

        for trip_id, times in stop_times_by_trip.items():
            if not times:
                continue

            trip = trips.get(trip_id)

            if not trip:
                continue

            route = routes.get(trip["route_id"], {})
            line = trip.get("line", "")

            first_st = times[0]
            last_st = times[-1]

            first_stop = stops.get(first_st["stop_id"], {})
            last_stop = stops.get(last_st["stop_id"], {})

            headsign = clean_text(trip.get("trip_headsign", "")) or last_stop.get("stop_name", "")

            meta = {
                "trip_id": trip_id,
                "route_id": trip["route_id"],
                "service_id": trip["service_id"],
                "line": line,
                "lineNorm": norm_line(line),
                "direction_id": trip.get("direction_id", ""),
                "headsign": headsign,
                "block_id": trip.get("block_id", ""),
                "shape_id": trip.get("shape_id", ""),
                "first_stop_id": first_st["stop_id"],
                "first_stop_name": first_stop.get("stop_name", ""),
                "last_stop_id": last_st["stop_id"],
                "last_stop_name": last_stop.get("stop_name", ""),
                "start_s": first_st["departure_s"],
                "end_s": last_st["arrival_s"] or last_st["departure_s"],
                "stop_count": len(times),
            }

            trip_meta[trip_id] = meta

            for st in times:
                stop_id = st["stop_id"]
                stop = stops.get(stop_id, {})

                departures_by_stop.setdefault(stop_id, []).append({
                    "trip_id": trip_id,
                    "route_id": trip["route_id"],
                    "service_id": trip["service_id"],
                    "line": line,
                    "direction_id": trip.get("direction_id", ""),
                    "headsign": headsign,
                    "destination": headsign,
                    "stop_id": stop_id,
                    "stop_name": stop.get("stop_name", ""),
                    "stop_sequence": st["stop_sequence"],
                    "arrival_s": st["arrival_s"],
                    "departure_s": st["departure_s"],
                    "pickup_type": st.get("pickup_type", ""),
                    "is_last_stop": st["stop_sequence"] == last_st["stop_sequence"],
                })

        for stop_id, arr in departures_by_stop.items():
            arr.sort(key=lambda x: x["departure_s"])

        direction_seen = set()

        for trip_id, meta in trip_meta.items():
            line_norm = meta["lineNorm"]
            direction_id = str(meta.get("direction_id", ""))
            key = (line_norm, direction_id, meta["first_stop_id"], meta["last_stop_id"])

            if key in direction_seen:
                continue

            direction_seen.add(key)

            stops_for_trip = []

            for st in stop_times_by_trip.get(trip_id, []):
                stop = stops.get(st["stop_id"], {})

                stops_for_trip.append({
                    "stop_id": st["stop_id"],
                    "stop_name": stop.get("stop_name", ""),
                    "stop_lat": stop.get("stop_lat"),
                    "stop_lon": stop.get("stop_lon"),
                    "stop_sequence": st["stop_sequence"],
                    "indicator": stop.get("indicator", ""),
                })

            line_directions.setdefault(line_norm, []).append({
                "trip_id": trip_id,
                "route_id": meta["route_id"],
                "direction_id": direction_id,
                "headsign": meta["headsign"],
                "from": {
                    "stop_id": meta["first_stop_id"],
                    "stop_name": meta["first_stop_name"],
                },
                "to": {
                    "stop_id": meta["last_stop_id"],
                    "stop_name": meta["last_stop_name"],
                },
                "stop_count": meta["stop_count"],
                "shape_id": meta.get("shape_id"),
                "stops": stops_for_trip,
            })

        GTFS = {
            "loaded": True,
            "error": None,
            "source": source,

            "agency": agency_rows,
            "stops": stops,
            "routes": routes,
            "trips": trips,

            "stop_times_by_trip": stop_times_by_trip,
            "departures_by_stop": departures_by_stop,

            "calendar": calendar_rows,
            "calendar_dates": calendar_dates,

            "routes_by_line": routes_by_line,
            "trip_meta": trip_meta,
            "line_directions": line_directions,
            "shapes": shapes,
        }

    except Exception as e:
        GTFS["loaded"] = False
        GTFS["error"] = str(e)


@app.on_event("startup")
def startup_load_gtfs():
    load_gtfs()


# ============================================================
# LIVE FETCH + PARSE
# ============================================================

def build_live_url() -> str:
    url = LIVE_FEED_URL

    if not url.endswith("/"):
        url += "/"

    if LIVE_API_KEY:
        sep = "&" if "?" in url else "?"
        url = url + sep + urlencode({"api_key": LIVE_API_KEY})

    return url


def xml_text(node: Optional[ET.Element]) -> str:
    if node is None or node.text is None:
        return ""

    return node.text.strip()


def find_child_text(parent: ET.Element, name: str) -> str:
    for child in parent.iter():
        if child.tag.split("}")[-1] == name:
            return xml_text(child)

    return ""


def first_direct_or_deep(parent: ET.Element, name: str) -> Optional[ET.Element]:
    for child in parent.iter():
        if child.tag.split("}")[-1] == name:
            return child

    return None


def parse_bool(v: Any) -> bool:
    return str(v).strip().lower() in ("true", "1", "yes")


def local_name(tag: str) -> str:
    return tag.split("}")[-1]


def parse_siri_vehicle_monitoring(xml_bytes: bytes) -> List[Dict[str, Any]]:
    root = ET.fromstring(xml_bytes)

    vehicles: List[Dict[str, Any]] = []

    for va in root.iter():
        if local_name(va.tag) != "VehicleActivity":
            continue

        mvj = None

        for node in va.iter():
            if local_name(node.tag) == "MonitoredVehicleJourney":
                mvj = node
                break

        if mvj is None:
            continue

        recorded = find_child_text(va, "RecordedAtTime")
        valid_until = find_child_text(va, "ValidUntilTime")

        line_ref = find_child_text(mvj, "LineRef")
        published_line = find_child_text(mvj, "PublishedLineName") or line_ref

        operator_ref = find_child_text(mvj, "OperatorRef")
        direction_ref = find_child_text(mvj, "DirectionRef")

        origin_ref = find_child_text(mvj, "OriginRef")
        origin_name = find_child_text(mvj, "OriginName")

        destination_ref = find_child_text(mvj, "DestinationRef")
        destination_name = find_child_text(mvj, "DestinationName")

        block_ref = find_child_text(mvj, "BlockRef")
        vehicle_ref = find_child_text(mvj, "VehicleRef")
        vehicle_unique_id = find_child_text(mvj, "VehicleUniqueId")
        ticket_machine_service_code = find_child_text(mvj, "TicketMachineServiceCode")
        journey_code = find_child_text(mvj, "JourneyCode")

        data_frame_ref = find_child_text(mvj, "DataFrameRef")
        dated_vehicle_journey_ref = find_child_text(mvj, "DatedVehicleJourneyRef")

        bearing_text = find_child_text(mvj, "Bearing")

        try:
            bearing = float(bearing_text) if bearing_text else None
        except Exception:
            bearing = None

        lon = None
        lat = None

        loc = first_direct_or_deep(mvj, "VehicleLocation")

        if loc is not None:
            try:
                lon_s = find_child_text(loc, "Longitude")
                lat_s = find_child_text(loc, "Latitude")
                lon = float(lon_s) if lon_s else None
                lat = float(lat_s) if lat_s else None
            except Exception:
                lon = None
                lat = None

        monitored_call = first_direct_or_deep(mvj, "MonitoredCall")

        current_stop_ref = ""
        current_stop_name = ""
        vehicle_at_stop = False

        calls: List[Dict[str, Any]] = []

        if monitored_call is not None:
            current_stop_ref = find_child_text(monitored_call, "StopPointRef")
            current_stop_name = find_child_text(monitored_call, "StopPointName")
            vehicle_at_stop = parse_bool(find_child_text(monitored_call, "VehicleAtStop"))

            calls.append({
                "stopRef": current_stop_ref,
                "stopName": pretty_place_name(current_stop_name),
                "aimedArr": find_child_text(monitored_call, "AimedArrivalTime"),
                "expArr": find_child_text(monitored_call, "ExpectedArrivalTime"),
                "aimedDep": find_child_text(monitored_call, "AimedDepartureTime"),
                "expDep": find_child_text(monitored_call, "ExpectedDepartureTime"),
                "vehicleAtStop": vehicle_at_stop,
                "order": 0,
            })

        order = 1

        for node in mvj.iter():
            if local_name(node.tag) != "OnwardCall":
                continue

            stop_ref = find_child_text(node, "StopPointRef")

            if not stop_ref:
                continue

            calls.append({
                "stopRef": stop_ref,
                "stopName": pretty_place_name(find_child_text(node, "StopPointName")),
                "aimedArr": find_child_text(node, "AimedArrivalTime"),
                "expArr": find_child_text(node, "ExpectedArrivalTime"),
                "aimedDep": find_child_text(node, "AimedDepartureTime"),
                "expDep": find_child_text(node, "ExpectedDepartureTime"),
                "vehicleAtStop": parse_bool(find_child_text(node, "VehicleAtStop")),
                "order": order,
            })

            order += 1

        recorded_dt = parse_iso_dt(recorded)
        age_seconds = None

        if recorded_dt:
            age_seconds = int((now_local() - recorded_dt).total_seconds())

        vehicles.append({
            "recordedAtTime": recorded,
            "recordedAtTimeLocal": recorded_dt.isoformat() if recorded_dt else "",
            "validUntilTime": valid_until,

            "lineRef": line_ref,
            "publishedLineName": published_line,
            "lineNorm": norm_line(published_line or line_ref),

            "operatorRef": operator_ref,
            "directionRef": direction_ref,

            "originRef": origin_ref,
            "originName": pretty_place_name(origin_name),
            "destinationRef": destination_ref,
            "destinationName": pretty_place_name(destination_name),

            "originAimedDepartureTime": find_child_text(mvj, "OriginAimedDepartureTime"),
            "destinationAimedArrivalTime": find_child_text(mvj, "DestinationAimedArrivalTime"),

            "longitude": lon,
            "latitude": lat,
            "bearing": bearing,

            "blockRef": block_ref,
            "vehicleRef": vehicle_ref,
            "vehicleUniqueId": vehicle_unique_id,
            "ticketMachineServiceCode": ticket_machine_service_code,
            "journeyCode": journey_code,

            "dataFrameRef": data_frame_ref,
            "datedVehicleJourneyRef": dated_vehicle_journey_ref,
            "tripId": dated_vehicle_journey_ref or journey_code,

            "currentStopRef": current_stop_ref,
            "currentStopName": pretty_place_name(current_stop_name),
            "vehicleAtStop": vehicle_at_stop,

            "ageSeconds": age_seconds,
            "calls": calls,
        })

    return vehicles


async def fetch_live_vehicles(force: bool = False) -> List[Dict[str, Any]]:
    if not LIVE_ENABLED:
        LIVE_CACHE["last_error"] = "LIVE_DISABLED"
        return []

    if not LIVE_API_KEY:
        LIVE_CACHE["last_error"] = "Missing API key"
        return []

    age = time.monotonic() - float(LIVE_CACHE.get("fetched_at_monotonic") or 0)

    if not force and age < LIVE_CACHE_TTL_SEC:
        return LIVE_CACHE.get("vehicles", [])

    url = build_live_url()

    LIVE_CACHE["effective_url"] = LIVE_FEED_URL

    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            r = await client.get(url)

        LIVE_CACHE["last_http_status"] = r.status_code
        LIVE_CACHE["last_fetch_time"] = now_local().isoformat()

        r.raise_for_status()

        vehicles = parse_siri_vehicle_monitoring(r.content)

        raw_count = len(vehicles)

        active: List[Dict[str, Any]] = []

        for v in vehicles:
            op = str(v.get("operatorRef") or "")

            if LIVE_OPERATOR_FILTER and LIVE_OPERATOR_FILTER.upper() not in op.upper():
                continue

            if v.get("latitude") is None or v.get("longitude") is None:
                continue

            age_s = v.get("ageSeconds")

            if age_s is None:
                continue

            # FONTOS:
            # Itt szűrjük ki a régi, korábbi menetekből megmaradt járműveket.
            if age_s < -60 or age_s > LIVE_ACTIVE_MAX_AGE_SECONDS:
                continue

            active.append(v)

        LIVE_CACHE["fetched_at_monotonic"] = time.monotonic()
        LIVE_CACHE["vehicles"] = active
        LIVE_CACHE["raw_count"] = raw_count
        LIVE_CACHE["active_count"] = len(active)
        LIVE_CACHE["last_error"] = ""

        return active

    except Exception as e:
        LIVE_CACHE["last_error"] = str(e)
        LIVE_CACHE["fetched_at_monotonic"] = time.monotonic()
        LIVE_CACHE["vehicles"] = []
        LIVE_CACHE["raw_count"] = 0
        LIVE_CACHE["active_count"] = 0
        return []


def public_vehicle(v: Dict[str, Any]) -> Dict[str, Any]:
    dest = pretty_place_name(v.get("destinationName"))
    origin = pretty_place_name(v.get("originName"))

    return {
        "recordedAtTime": v.get("recordedAtTime"),
        "recordedAtTimeLocal": v.get("recordedAtTimeLocal"),
        "validUntilTime": v.get("validUntilTime"),

        "dataFrameRef": v.get("dataFrameRef"),
        "datedVehicleJourneyRef": v.get("datedVehicleJourneyRef"),

        "lineRef": v.get("lineRef"),
        "publishedLineName": v.get("publishedLineName"),
        "lineNorm": v.get("publishedLineName") or v.get("lineRef"),

        "operatorRef": v.get("operatorRef"),
        "directionRef": v.get("directionRef"),

        "originRef": v.get("originRef"),
        "originName": origin,
        "destinationRef": v.get("destinationRef"),
        "destinationName": dest,
        "destinationShort": short_destination_name(dest),

        "originAimedDepartureTime": v.get("originAimedDepartureTime"),
        "destinationAimedArrivalTime": v.get("destinationAimedArrivalTime"),

        "longitude": v.get("longitude"),
        "latitude": v.get("latitude"),
        "bearing": v.get("bearing"),

        "blockRef": v.get("blockRef"),
        "vehicleRef": v.get("vehicleRef"),
        "vehicleUniqueId": v.get("vehicleUniqueId"),
        "ticketMachineServiceCode": v.get("ticketMachineServiceCode"),
        "journeyCode": v.get("journeyCode"),

        "currentStopRef": v.get("currentStopRef"),
        "currentStopName": pretty_place_name(v.get("currentStopName")),
        "vehicleAtStop": bool(v.get("vehicleAtStop")),

        "ageSeconds": v.get("ageSeconds"),
        "tripId": v.get("tripId"),
    }


# ============================================================
# LIVE + GTFS MATCHING
# ============================================================

def live_call_for_stop(v: Dict[str, Any], stop_id: str) -> Optional[Dict[str, Any]]:
    for c in v.get("calls") or []:
        if c.get("stopRef") == stop_id:
            return c

    return None


def live_vehicle_has_any_trip_stop(v: Dict[str, Any], trip_id: str) -> bool:
    stop_ids = {
        x["stop_id"]
        for x in GTFS["stop_times_by_trip"].get(trip_id, [])
    }

    if not stop_ids:
        return False

    if v.get("currentStopRef") in stop_ids:
        return True

    for c in v.get("calls") or []:
        if c.get("stopRef") in stop_ids:
            return True

    return False


def live_matches_destination(v: Dict[str, Any], meta: Dict[str, Any]) -> bool:
    live_dest_ref = v.get("destinationRef")
    live_dest_name = v.get("destinationName")

    if live_dest_ref and meta.get("last_stop_id") and live_dest_ref == meta.get("last_stop_id"):
        return True

    if names_match(live_dest_name, meta.get("headsign")):
        return True

    if names_match(live_dest_name, meta.get("last_stop_name")):
        return True

    return False


def live_matches_origin(v: Dict[str, Any], meta: Dict[str, Any]) -> bool:
    live_origin_ref = v.get("originRef")
    live_origin_name = v.get("originName")

    if live_origin_ref and meta.get("first_stop_id") and live_origin_ref == meta.get("first_stop_id"):
        return True

    if names_match(live_origin_name, meta.get("first_stop_name")):
        return True

    return False


def live_origin_aimed_seconds(v: Dict[str, Any], service_date: date) -> Optional[int]:
    dt = parse_iso_dt(v.get("originAimedDepartureTime"))

    if not dt:
        return None

    return seconds_from_service_date(dt, service_date)


def score_live_for_trip(v: Dict[str, Any], trip_id: str, service_date: date) -> Tuple[float, str]:
    meta = GTFS["trip_meta"].get(trip_id)

    if not meta:
        return 0.0, "no_meta"

    live_line = norm_line(v.get("publishedLineName") or v.get("lineRef"))
    trip_line = norm_line(meta.get("line"))

    if live_line and trip_line and live_line != trip_line:
        return 0.0, "line_mismatch"

    score = 0.0
    reasons = []

    refs = [
        v.get("tripId"),
        v.get("datedVehicleJourneyRef"),
        v.get("journeyCode"),
        v.get("blockRef"),
    ]

    exact_ref = False

    for ref in refs:
        if not ref:
            continue

        ref = str(ref)
        trip_text = str(trip_id)

        if ref == trip_text or trip_text.endswith(":" + ref) or ref in trip_text:
            exact_ref = True
            score += 85
            reasons.append("exact_ref")
            break

    has_trip_stop = live_vehicle_has_any_trip_stop(v, trip_id)

    if has_trip_stop:
        score += 35
        reasons.append("stop_on_trip")

    if live_matches_destination(v, meta):
        score += 18
        reasons.append("dest")

    if live_matches_origin(v, meta):
        score += 10
        reasons.append("origin")

    live_origin_s = live_origin_aimed_seconds(v, service_date)
    gtfs_start_s = meta.get("start_s")

    if live_origin_s is not None and gtfs_start_s is not None:
        diff = abs(live_origin_s - gtfs_start_s)

        if diff <= 120:
            score += 45
            reasons.append("origin_time_exact")
        elif diff <= 8 * 60:
            score += 25
            reasons.append("origin_time_near")
        else:
            score -= 40
            reasons.append("origin_time_far")

    # FONTOS:
    # Csak járatszám + végállomás alapján nem jelölünk élőnek.
    # Ettől voltak az ellentmondásos "Élő adat" sorok.
    if not exact_ref and not has_trip_stop and live_origin_s is None:
        return 0.0, "not_enough_evidence"

    return score, ",".join(reasons)


def match_live_for_trip(
    trip_id: str,
    service_date: date,
    live_vehicles: List[Dict[str, Any]],
    cache: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    key = f"{service_date.isoformat()}::{trip_id}"

    if key in cache:
        return cache[key]

    best = None
    best_score = 0.0
    best_reason = ""

    for v in live_vehicles:
        score, reason = score_live_for_trip(v, trip_id, service_date)

        if score > best_score:
            best = v
            best_score = score
            best_reason = reason

    # Magasabb küszöb, hogy ne legyen hamis live adat.
    if best is not None and best_score >= 70:
        matched = dict(best)
        matched["_matchScore"] = round(best_score, 1)
        matched["_matchReason"] = best_reason
        cache[key] = matched
        return matched

    cache[key] = None
    return None


def live_info_for_trip_stop(
    trip_id: str,
    service_date: date,
    stop_time: Dict[str, Any],
    live_vehicle: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    scheduled_dt = departure_datetime(service_date, stop_time["departure_s"])

    empty = {
        "isLive": False,
        "expected_dt": None,
        "delayMinutes": None,
        "vehicleAtStop": False,
        "passed": False,
        "currentStopName": None,
        "nextStopName": None,
        "vehicleRef": None,
        "vehicleUniqueId": None,
        "matchScore": None,
        "matchReason": None,
    }

    if not live_vehicle:
        return empty

    stop_id = stop_time["stop_id"]

    c = live_call_for_stop(live_vehicle, stop_id)

    if c:
        exp_dt = (
            parse_iso_dt(c.get("expDep"))
            or parse_iso_dt(c.get("expArr"))
        )

        aimed_dt = (
            parse_iso_dt(c.get("aimedDep"))
            or parse_iso_dt(c.get("aimedArr"))
            or scheduled_dt
        )

        # Ha nincs konkrét expected idő, nem írjuk rá hamisan, hogy élő adat.
        if not exp_dt:
            if c.get("vehicleAtStop"):
                exp_dt = now_local()
            else:
                return empty

        delay = int(round((exp_dt - aimed_dt).total_seconds() / 60))

        return {
            "isLive": True,
            "expected_dt": exp_dt,
            "delayMinutes": delay,
            "vehicleAtStop": bool(c.get("vehicleAtStop")),
            "passed": False,
            "currentStopName": pretty_place_name(live_vehicle.get("currentStopName")),
            "nextStopName": pretty_place_name(c.get("stopName")),
            "vehicleRef": live_vehicle.get("vehicleRef"),
            "vehicleUniqueId": live_vehicle.get("vehicleUniqueId"),
            "matchScore": live_vehicle.get("_matchScore"),
            "matchReason": live_vehicle.get("_matchReason"),
        }

    if live_vehicle.get("currentStopRef") == stop_id:
        return {
            "isLive": True,
            "expected_dt": now_local(),
            "delayMinutes": int(round((now_local() - scheduled_dt).total_seconds() / 60)),
            "vehicleAtStop": True,
            "passed": False,
            "currentStopName": pretty_place_name(live_vehicle.get("currentStopName")),
            "nextStopName": pretty_place_name(live_vehicle.get("currentStopName")),
            "vehicleRef": live_vehicle.get("vehicleRef"),
            "vehicleUniqueId": live_vehicle.get("vehicleUniqueId"),
            "matchScore": live_vehicle.get("_matchScore"),
            "matchReason": live_vehicle.get("_matchReason"),
        }

    # FONTOS:
    # Nincs GPS-alapú becslés.
    # Ha nincs konkrét SIRI call ehhez a megállóhoz, akkor marad menetrendi adat.
    return empty


# ============================================================
# API HELPERS
# ============================================================

def status_payload() -> Dict[str, Any]:
    counts = {}

    if GTFS.get("loaded"):
        all_calendar_dates = []

        for row in GTFS.get("calendar", []):
            if row.get("start_date"):
                all_calendar_dates.append(row.get("start_date"))

            if row.get("end_date"):
                all_calendar_dates.append(row.get("end_date"))

        counts = {
            "agency": len(GTFS.get("agency", [])),
            "stops": len(GTFS.get("stops", {})),
            "routes": len(GTFS.get("routes", {})),
            "trips": len(GTFS.get("trips", {})),
            "stop_times_trips": len(GTFS.get("stop_times_by_trip", {})),
            "stop_departures_index_stops": len(GTFS.get("departures_by_stop", {})),
            "shapes": len(GTFS.get("shapes", {})),
        }

        if all_calendar_dates:
            counts["calendarRange"] = {
                "calendar_start_min": min(all_calendar_dates),
                "calendar_end_max": max(all_calendar_dates),
            }

    return {
        "live": {
            "ok": bool(LIVE_CACHE.get("last_error") == "" and LIVE_CACHE.get("last_http_status") == 200),
            "activeCount": LIVE_CACHE.get("active_count", 0),
            "rawCount": LIVE_CACHE.get("raw_count", 0),
            "maxAgeSeconds": LIVE_ACTIVE_MAX_AGE_SECONDS,
            "operatorFilter": LIVE_OPERATOR_FILTER,
            "error": LIVE_CACHE.get("last_error") or None,
            "lastHttpStatus": LIVE_CACHE.get("last_http_status"),
            "lastFetchTime": LIVE_CACHE.get("last_fetch_time"),
        },
        "gtfs": {
            "ok": bool(GTFS.get("loaded")),
            "error": GTFS.get("error"),
            "source": GTFS.get("source"),
            "counts": counts,
        },
        "serverTime": now_local().isoformat(),
        "timezone": LOCAL_TZ_NAME,
    }


def stop_public(stop_id: str) -> Optional[Dict[str, Any]]:
    s = GTFS.get("stops", {}).get(stop_id)

    if not s:
        return None

    return {
        "stop_id": s["stop_id"],
        "stop_name": s["stop_name"],
        "stop_code": s.get("stop_code", ""),
        "indicator": s.get("indicator") or extract_stop_code(s["stop_name"], s["stop_id"]),
        "stop_lat": s.get("stop_lat"),
        "stop_lon": s.get("stop_lon"),
    }


def route_public(route: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "route_id": route.get("route_id"),
        "agency_id": route.get("agency_id"),
        "short_name": route.get("short_name"),
        "long_name": route.get("long_name"),
        "route_type": route.get("route_type"),
        "route_color": route.get("route_color"),
        "route_text_color": route.get("route_text_color"),
    }


# ============================================================
# ROUTES - HTML
# ============================================================

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    index_template = TEMPLATES_DIR / "index.html"

    if index_template.exists():
        return templates.TemplateResponse("index.html", {"request": request})

    fallback = """
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8">
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Bluestar Unilink</title>
    </head>
    <body>
      <h1>Bluestar Unilink API</h1>
      <p>templates/index.html nincs feltöltve.</p>
    </body>
    </html>
    """

    return HTMLResponse(fallback)


# ============================================================
# ROUTES - STATUS
# ============================================================

@app.get("/api/status")
async def api_status():
    await fetch_live_vehicles()
    return status_payload()


@app.get("/api/gtfs/status")
def api_gtfs_status():
    return {
        "loaded": GTFS.get("loaded"),
        "error": GTFS.get("error"),
        "source": GTFS.get("source"),
        "counts": {
            "agency": len(GTFS.get("agency", [])),
            "stops": len(GTFS.get("stops", {})),
            "routes": len(GTFS.get("routes", {})),
            "trips": len(GTFS.get("trips", {})),
            "stop_times_trips": len(GTFS.get("stop_times_by_trip", {})),
            "stop_departures_index_stops": len(GTFS.get("departures_by_stop", {})),
            "shapes": len(GTFS.get("shapes", {})),
        },
        "agencyFilter": "",
    }


@app.get("/api/live/status")
async def api_live_status():
    vehicles = await fetch_live_vehicles()

    sample = [public_vehicle(v) for v in vehicles[:5]]

    return {
        "effectiveFeedUrl": LIVE_FEED_URL,
        "keyPreview": (LIVE_API_KEY[:4] + "..." + LIVE_API_KEY[-4:]) if LIVE_API_KEY else "",
        "keyPresent": bool(LIVE_API_KEY),
        "vehicleCount": len(vehicles),
        "rawCount": LIVE_CACHE.get("raw_count", 0),
        "lastError": LIVE_CACHE.get("last_error"),
        "lastHttpStatus": LIVE_CACHE.get("last_http_status"),
        "lastFetchTime": LIVE_CACHE.get("last_fetch_time"),
        "sample": sample,
    }


@app.get("/health")
async def health():
    await fetch_live_vehicles()
    return {"ok": True, **status_payload()}


# ============================================================
# ROUTES - SEARCH
# ============================================================

@app.get("/api/search")
def api_search(q: str = "", limit: int = 30):
    q = clean_text(q)

    if not GTFS.get("loaded"):
        return {"q": q, "stops": [], "routes": [], "error": GTFS.get("error")}

    nq = norm_text(q)
    nl = norm_line(q)

    stops_result = []
    routes_result = []

    if nq:
        scored_stops = []

        for s in GTFS["stops"].values():
            name = s.get("stop_name", "")
            sid = s.get("stop_id", "")
            indicator = s.get("indicator", "")

            hay = norm_text(f"{name} {sid} {indicator}")

            if nq in hay:
                score = 0

                if hay.startswith(nq):
                    score += 100

                if norm_text(name).startswith(nq):
                    score += 80

                if nq == norm_text(indicator):
                    score += 60

                score += max(0, 40 - len(name))

                scored_stops.append((score, s))

        scored_stops.sort(key=lambda x: (-x[0], x[1]["stop_name"]))

        for _, s in scored_stops[:limit]:
            stops_result.append(stop_public(s["stop_id"]))

        seen_lines = set()

        for route in GTFS["routes"].values():
            short = route.get("short_name", "")
            long = route.get("long_name", "")

            if not short:
                continue

            if nl and (nl in norm_line(short) or nq in norm_text(long)):
                key = norm_line(short)

                if key in seen_lines:
                    continue

                seen_lines.add(key)
                routes_result.append(route_public(route))

        routes_result.sort(key=lambda r: (len(norm_line(r.get("short_name"))), norm_line(r.get("short_name"))))
        routes_result = routes_result[:limit]

    return {
        "q": q,
        "stops": stops_result,
        "routes": routes_result,
    }


@app.get("/search")
def legacy_search(q: str = "", limit: int = 30):
    return api_search(q=q, limit=limit)


# ============================================================
# ROUTES - STOPS / DEPARTURES
# ============================================================

@app.get("/api/stop/{stop_id}")
def api_stop(stop_id: str):
    s = stop_public(stop_id)

    if not s:
        return JSONResponse({"error": "Stop not found", "stop_id": stop_id}, status_code=404)

    return {"stop": s}


@app.get("/api/stops/{stop_id}/departures")
async def api_stop_departures(stop_id: str, minutes: int = 90):
    if not GTFS.get("loaded"):
        return JSONResponse({"error": GTFS.get("error") or "GTFS not loaded"}, status_code=500)

    stop = stop_public(stop_id)

    if not stop:
        return JSONResponse({"error": "Stop not found", "stop_id": stop_id}, status_code=404)

    vehicles = await fetch_live_vehicles()
    service_date = today_local()
    now = now_local()

    active_services = active_service_ids_for_date(service_date)

    result = []
    match_cache: Dict[str, Any] = {}

    for dep in GTFS["departures_by_stop"].get(stop_id, []):
        if dep.get("service_id") not in active_services:
            continue

        # Ha ez a trip utolsó megállója, akkor ez érkezés, nem indulás.
        if dep.get("is_last_stop"):
            continue

        scheduled_dt = departure_datetime(service_date, dep["departure_s"])

        if scheduled_dt < now - timedelta(minutes=2):
            continue

        if scheduled_dt > now + timedelta(minutes=minutes):
            continue

        live_vehicle = match_live_for_trip(dep["trip_id"], service_date, vehicles, match_cache)

        info = live_info_for_trip_stop(
            dep["trip_id"],
            service_date,
            {
                "stop_id": dep["stop_id"],
                "departure_s": dep["departure_s"],
                "arrival_s": dep.get("arrival_s"),
            },
            live_vehicle,
        )

        is_live = bool(info.get("isLive"))
        expected_dt = info.get("expected_dt") if is_live else None
        display_dt = expected_dt or scheduled_dt
        mins = max(0, int(round((display_dt - now).total_seconds() / 60)))

        destination = dep.get("destination") or dep.get("headsign") or ""

        row = {
            "tripId": dep["trip_id"],
            "routeId": dep["route_id"],
            "line": dep["line"],
            "destination": pretty_place_name(destination),
            "destinationShort": short_destination_name(destination),
            "stopId": stop_id,
            "stopName": stop["stop_name"],
            "stopIndicator": stop.get("indicator"),
            "scheduledTime": hhmm(scheduled_dt),
            "expectedTime": hhmm(expected_dt) if expected_dt else None,
            "displayTime": hhmm(display_dt),
            "minutes": mins,
            "source": "live" if is_live else "gtfs",
            "isLive": is_live,
            "delayMinutes": info.get("delayMinutes"),
            "vehicleAtStop": bool(info.get("vehicleAtStop")),
            "vehicleRef": info.get("vehicleRef"),
            "vehicleUniqueId": info.get("vehicleUniqueId"),
            "matchScore": info.get("matchScore"),
            "matchReason": info.get("matchReason"),
        }

        result.append(row)

    # Duplikáció csökkentés.
    dedup = {}

    for r in result:
        key = (
            r["line"],
            r["destination"],
            r["scheduledTime"],
            r["tripId"],
        )

        if key not in dedup:
            dedup[key] = r
        else:
            if r["isLive"] and not dedup[key]["isLive"]:
                dedup[key] = r

    result = list(dedup.values())
    result.sort(key=lambda x: (x["minutes"], x["displayTime"], norm_line(x["line"])))

    return {
        "stop": stop,
        "departures": result[:80],
        "legend": "Fehér: menetrend (GTFS) · Zöld: élő (LIVE)",
        "serverTime": now.isoformat(),
    }


@app.get("/api/stop/{stop_id}/departures")
async def api_stop_departures_alias(stop_id: str, minutes: int = 90):
    return await api_stop_departures(stop_id, minutes)


# ============================================================
# ROUTES - TRIP
# ============================================================

@app.get("/api/trip/{trip_id}")
async def api_trip(trip_id: str, service_date: Optional[str] = None):
    if not GTFS.get("loaded"):
        return JSONResponse({"error": GTFS.get("error") or "GTFS not loaded"}, status_code=500)

    times = GTFS["stop_times_by_trip"].get(trip_id)

    if not times:
        return JSONResponse({"error": "Trip not found", "trip_id": trip_id}, status_code=404)

    meta = GTFS["trip_meta"].get(trip_id, {})
    d = today_local()

    if service_date:
        try:
            d = datetime.strptime(service_date, "%Y-%m-%d").date()
        except Exception:
            d = today_local()

    vehicles = await fetch_live_vehicles()
    match_cache: Dict[str, Any] = {}
    live_vehicle = match_live_for_trip(trip_id, d, vehicles, match_cache)

    now = now_local()

    stops_out = []

    delay_values = []

    current_stop_name = pretty_place_name(live_vehicle.get("currentStopName")) if live_vehicle else None
    vehicle_at_stop = bool(live_vehicle.get("vehicleAtStop")) if live_vehicle else False

    for st in times:
        stop = GTFS["stops"].get(st["stop_id"], {})
        scheduled_dt = departure_datetime(d, st["departure_s"])

        info = live_info_for_trip_stop(trip_id, d, st, live_vehicle)
        is_live = bool(info.get("isLive"))
        expected_dt = info.get("expected_dt") if is_live else None
        display_dt = expected_dt or scheduled_dt

        if info.get("delayMinutes") is not None:
            delay_values.append(info.get("delayMinutes"))

        mins = int(round((display_dt - now).total_seconds() / 60))

        stop_name = pretty_place_name(stop.get("stop_name", ""))

        at_this_stop = False

        if current_stop_name and names_match(current_stop_name, stop_name):
            at_this_stop = True

        if live_vehicle and live_vehicle.get("currentStopRef") == st["stop_id"]:
            at_this_stop = True

        stops_out.append({
            "stopId": st["stop_id"],
            "stopName": stop_name,
            "indicator": stop.get("indicator"),
            "lat": stop.get("stop_lat"),
            "lon": stop.get("stop_lon"),
            "sequence": st["stop_sequence"],
            "scheduledTime": hhmm(scheduled_dt),
            "expectedTime": hhmm(expected_dt) if expected_dt else None,
            "displayTime": hhmm(display_dt),
            "minutes": mins,
            "source": "live" if is_live else "gtfs",
            "isLive": is_live,
            "delayMinutes": info.get("delayMinutes"),
            "vehicleAtStop": bool(info.get("vehicleAtStop")) or at_this_stop,
            "isCurrentStop": at_this_stop,
            "passed": display_dt < now - timedelta(minutes=1),
        })

    header_delay = None

    if delay_values:
        header_delay = delay_values[0]

    shape = []

    shape_id = meta.get("shape_id")

    if shape_id and shape_id in GTFS["shapes"]:
        shape = GTFS["shapes"][shape_id]

    return {
        "trip": {
            "tripId": trip_id,
            "line": meta.get("line"),
            "destination": pretty_place_name(meta.get("headsign") or meta.get("last_stop_name")),
            "destinationShort": short_destination_name(meta.get("headsign") or meta.get("last_stop_name")),
            "origin": pretty_place_name(meta.get("first_stop_name")),
            "lastStop": pretty_place_name(meta.get("last_stop_name")),
            "directionId": meta.get("direction_id"),
            "serviceDate": d.isoformat(),
            "source": "live" if live_vehicle else "gtfs",
            "delayMinutes": header_delay,
            "vehicleRef": live_vehicle.get("vehicleRef") if live_vehicle else None,
            "vehicleUniqueId": live_vehicle.get("vehicleUniqueId") if live_vehicle else None,
            "currentStopName": current_stop_name,
            "vehicleAtStop": vehicle_at_stop,
            "matchScore": live_vehicle.get("_matchScore") if live_vehicle else None,
            "matchReason": live_vehicle.get("_matchReason") if live_vehicle else None,
        },
        "stops": stops_out,
        "shape": shape,
        "serverTime": now.isoformat(),
    }


# ============================================================
# ROUTES - ROUTE / LINE
# ============================================================

@app.get("/api/route/{line}")
def api_route_directions(line: str):
    if not GTFS.get("loaded"):
        return JSONResponse({"error": GTFS.get("error") or "GTFS not loaded"}, status_code=500)

    ln = norm_line(line)

    routes = GTFS["routes_by_line"].get(ln, [])
    directions = GTFS["line_directions"].get(ln, [])

    if not routes and not directions:
        return JSONResponse({"error": "Route not found", "line": line}, status_code=404)

    return {
        "line": ln,
        "routes": [route_public(r) for r in routes],
        "directions": directions,
    }


@app.get("/api/line/{line}")
def api_line_main(line: str):
    return api_route_directions(line)


@app.get("/api/routes/{line}")
def api_routes_line_alias(line: str):
    return api_route_directions(line)


@app.get("/api/route/{line}/vehicles")
async def api_route_vehicles(line: str):
    vehicles = await fetch_live_vehicles()
    ln = norm_line(line)

    out = [
        public_vehicle(v)
        for v in vehicles
        if norm_line(v.get("publishedLineName") or v.get("lineRef")) == ln
    ]

    out.sort(key=lambda x: (
        x.get("destinationShort") or "",
        str(x.get("vehicleRef") or ""),
    ))

    return {
        "line": ln,
        "vehicles": out,
        "count": len(out),
        "note": "Csak aktuális, bejelentkezett live járművek.",
    }


@app.get("/api/route/{line}/stops")
def api_route_stops(line: str, direction_id: Optional[str] = None):
    data = api_route_directions(line)

    if isinstance(data, JSONResponse):
        return data

    directions = data.get("directions", [])

    if direction_id is not None:
        directions = [
            d for d in directions
            if str(d.get("direction_id")) == str(direction_id)
        ]

    return {
        "line": norm_line(line),
        "directions": directions,
    }


@app.get("/api/route/{line}/map")
async def api_route_map(line: str, direction_id: Optional[str] = None):
    ln = norm_line(line)

    directions = GTFS["line_directions"].get(ln, [])

    if direction_id is not None:
        directions = [
            d for d in directions
            if str(d.get("direction_id")) == str(direction_id)
        ]

    shapes = []

    for d in directions:
        sid = d.get("shape_id")
        pts = GTFS["shapes"].get(sid, [])

        if pts:
            shapes.append({
                "directionId": d.get("direction_id"),
                "headsign": d.get("headsign"),
                "shapeId": sid,
                "points": pts,
            })

    vehicles = await fetch_live_vehicles()

    route_vehicles = [
        public_vehicle(v)
        for v in vehicles
        if norm_line(v.get("publishedLineName") or v.get("lineRef")) == ln
    ]

    return {
        "line": ln,
        "directions": directions,
        "shapes": shapes,
        "vehicles": route_vehicles,
    }


# ============================================================
# ROUTES - LIVE VEHICLES MAP
# ============================================================

@app.get("/api/live/vehicles")
async def api_live_vehicles(line: Optional[str] = None, activeOnly: bool = True):
    vehicles = await fetch_live_vehicles()
    ln = norm_line(line) if line else ""

    out = []

    for v in vehicles:
        if ln and norm_line(v.get("publishedLineName") or v.get("lineRef")) != ln:
            continue

        out.append(public_vehicle(v))

    return {
        "vehicles": out,
        "count": len(out),
        "rawCount": LIVE_CACHE.get("raw_count", 0),
        "activeCount": LIVE_CACHE.get("active_count", 0),
        "filter": ln or None,
        "note": "Csak aktuális live járművek, régi menetek kiszűrve.",
    }


@app.get("/api/vehicles")
async def api_vehicles(line: Optional[str] = None):
    return await api_live_vehicles(line=line)


# ============================================================
# ADMIN / UPLOAD
# ============================================================

@app.post("/api/gtfs/reload")
def api_gtfs_reload():
    load_gtfs()
    return {
        "ok": bool(GTFS.get("loaded")),
        "error": GTFS.get("error"),
        "source": GTFS.get("source"),
    }


@app.post("/api/gtfs/upload")
async def api_gtfs_upload(file: UploadFile = File(...)):
    content = await file.read()

    if not content:
        return JSONResponse({"ok": False, "error": "Üres fájl."}, status_code=400)

    try:
        with zipfile.ZipFile(io.BytesIO(content), "r") as z:
            names = z.namelist()

            required = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt"]
            base_names = {Path(n).name for n in names}

            missing = [x for x in required if x not in base_names]

            if missing:
                return JSONResponse(
                    {"ok": False, "error": f"Hiányzó GTFS fájlok: {', '.join(missing)}"},
                    status_code=400,
                )

        GTFS_ZIP_PATH.write_bytes(content)
        load_gtfs()

        return {
            "ok": bool(GTFS.get("loaded")),
            "error": GTFS.get("error"),
            "source": GTFS.get("source"),
        }

    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=400)


# ============================================================
# LEGACY SHORT ENDPOINTS
# ============================================================

@app.get("/api/departures/{stop_id}")
async def api_departures_short(stop_id: str, minutes: int = 90):
    return await api_stop_departures(stop_id, minutes)


@app.get("/api/live")
async def api_live_short(line: Optional[str] = None):
    return await api_live_vehicles(line=line)


@app.get("/api/debug/live")
async def api_debug_live():
    vehicles = await fetch_live_vehicles(force=True)

    return {
        "effectiveFeedUrl": LIVE_FEED_URL,
        "keyPreview": (LIVE_API_KEY[:4] + "..." + LIVE_API_KEY[-4:]) if LIVE_API_KEY else "",
        "keyPresent": bool(LIVE_API_KEY),
        "activeCount": len(vehicles),
        "rawCount": LIVE_CACHE.get("raw_count"),
        "lastError": LIVE_CACHE.get("last_error"),
        "lastHttpStatus": LIVE_CACHE.get("last_http_status"),
        "lastFetchTime": LIVE_CACHE.get("last_fetch_time"),
        "sample": [public_vehicle(v) for v in vehicles[:10]],
    }


@app.get("/api/debug/gtfs")
def api_debug_gtfs():
    return {
        "loaded": GTFS.get("loaded"),
        "error": GTFS.get("error"),
        "source": GTFS.get("source"),
        "counts": {
            "agency": len(GTFS.get("agency", [])),
            "stops": len(GTFS.get("stops", {})),
            "routes": len(GTFS.get("routes", {})),
            "trips": len(GTFS.get("trips", {})),
            "stop_times_trips": len(GTFS.get("stop_times_by_trip", {})),
            "stop_departures_index_stops": len(GTFS.get("departures_by_stop", {})),
            "shapes": len(GTFS.get("shapes", {})),
        },
        "today": today_local().isoformat(),
        "activeServicesToday": len(active_service_ids_for_date(today_local())) if GTFS.get("loaded") else 0,
    }
