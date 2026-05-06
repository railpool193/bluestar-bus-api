import os
import io
import csv
import re
import time
import gzip
import math
import zipfile
import requests
import xml.etree.ElementTree as ET

from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional, Tuple, Set

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import FileResponse, JSONResponse


# ============================================================
# CONFIG
# ============================================================

TZ = ZoneInfo(os.getenv("APP_TZ", "Europe/London"))

DFT_API_KEY = os.getenv("DFT_API_KEY", "").strip()
DFT_FEED_ID = os.getenv("DFT_FEED_ID", "7721").strip()

DFT_FEED_URL = os.getenv(
    "DFT_FEED_URL",
    f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{DFT_FEED_ID}/",
).strip()

GTFS_ZIP_PATH = os.getenv("GTFS_ZIP_PATH", "gtfs.zip")
GTFS_DIR = os.getenv("GTFS_DIR", "gtfs")

LIVE_CACHE_TTL_SEC = float(os.getenv("LIVE_CACHE_TTL_SEC", "10"))
DEFAULT_MAX_AGE_SECONDS = int(os.getenv("LIVE_MAX_AGE_SECONDS", "240"))

DEFAULT_OPERATOR_REFS = [
    x.strip().upper()
    for x in os.getenv("DEFAULT_OPERATOR_REFS", "BLUS").split(",")
    if x.strip()
]

DEFAULT_DEPARTURE_WINDOW_MINUTES = int(os.getenv("DEPARTURE_WINDOW_MINUTES", "90"))


# ============================================================
# APP
# ============================================================

app = FastAPI(title="Bluestar / Unilink Menetrend")


# ============================================================
# GLOBAL CACHES
# ============================================================

LIVE_CACHE: Dict[str, Any] = {
    "ts": 0.0,
    "ttl": LIVE_CACHE_TTL_SEC,
    "vehicles_all": [],
    "raw_count": 0,
    "ok": False,
    "error": None,
    "last_http_status": None,
    "last_fetch_time": None,
}

GTFS: Dict[str, Any] = {
    "ok": False,
    "error": None,
    "source": None,

    "agency": {},
    "stops": {},
    "routes": {},
    "trips": {},

    "stop_times_by_stop": {},
    "stop_times_by_trip": {},

    "calendar": {},
    "calendar_dates": {},

    "shapes": {},

    "trip_meta": {},
    "routes_by_line": {},
}


# ============================================================
# HELPERS
# ============================================================

def now_local() -> datetime:
    return datetime.now(TZ)


def today_local() -> date:
    return now_local().date()


def parse_gtfs_date(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def parse_hms_to_seconds(s: str) -> int:
    h, m, sec = s.strip().split(":")
    return int(h) * 3600 + int(m) * 60 + int(sec)


def seconds_to_hhmm(seconds: int) -> str:
    seconds = seconds % (24 * 3600)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h:02d}:{m:02d}"


def departure_datetime(service_date: date, dep_seconds: int) -> datetime:
    base = datetime(
        service_date.year,
        service_date.month,
        service_date.day,
        0,
        0,
        0,
        tzinfo=TZ,
    )
    return base + timedelta(seconds=dep_seconds)


def safe_text(el: Optional[ET.Element]) -> Optional[str]:
    if el is None:
        return None
    if el.text is None:
        return None
    t = el.text.strip()
    return t if t else None


def parse_iso_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None

    try:
        s2 = str(s).replace("Z", "+00:00")
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(TZ)
    except Exception:
        return None


def iso_or_none(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def hhmm_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    return dt.astimezone(TZ).strftime("%H:%M")


def looks_like_date(s: Optional[str]) -> bool:
    if not s:
        return False
    s = str(s)
    return len(s) == 10 and s[4] == "-" and s[7] == "-"


def norm_line(s: Optional[str]) -> str:
    return (s or "").strip().lower().replace(" ", "")


def norm_operator(s: Optional[str]) -> str:
    return (s or "").strip().upper()


def clean_name(s: Optional[str]) -> str:
    if not s:
        return ""

    s = str(s)
    s = s.replace("_", " ")
    s = re.sub(r"\[[^\]]+\]", " ", s)
    s = re.sub(r"\([^)]*\)", " ", s)
    s = s.lower()

    words_to_remove = [
        "southampton",
        "winchester",
        "eastleigh",
        "romsey",
        "bus station",
        "bus stn",
        "station",
        "stand",
        "stop",
        "platform",
        "the",
    ]

    for w in words_to_remove:
        s = s.replace(w, " ")

    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s


def names_match(a: Optional[str], b: Optional[str]) -> bool:
    aa = clean_name(a)
    bb = clean_name(b)

    if not aa or not bb:
        return False

    if aa == bb:
        return True

    if aa in bb or bb in aa:
        return True

    aw = set(aa.split())
    bw = set(bb.split())

    if not aw or not bw:
        return False

    common = aw & bw
    return len(common) >= 1 and len(common) / max(len(aw), len(bw)) >= 0.5


def stop_short_code(stop: Dict[str, Any]) -> str:
    name = stop.get("stop_name") or ""
    sid = stop.get("stop_id") or ""

    m = re.search(r"\[([A-Za-z0-9]+)\]", name)
    if m:
        return m.group(1).upper()

    if len(sid) >= 2:
        return sid[-2:].upper()

    return "BUS"


def decode_feed_bytes(content: bytes) -> bytes:
    if not content:
        return b""

    if content[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(content), "r") as zf:
            names = zf.namelist()
            if not names:
                return b""
            return zf.read(names[0])

    if content[:2] == b"\x1f\x8b":
        return gzip.decompress(content)

    return content


def distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)

    a = (
        math.sin(dp / 2) * math.sin(dp / 2)
        + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) * math.sin(dl / 2)
    )

    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def seconds_from_service_date(dt: datetime, service_date: date) -> int:
    base = datetime(
        service_date.year,
        service_date.month,
        service_date.day,
        0,
        0,
        0,
        tzinfo=TZ,
    )
    return int((dt.astimezone(TZ) - base).total_seconds())


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# ============================================================
# GTFS LOADER
# ============================================================

def _resolve_zip_name(zf: zipfile.ZipFile, wanted: str) -> Optional[str]:
    names = zf.namelist()

    if wanted in names:
        return wanted

    wanted_lower = wanted.lower()

    for n in names:
        if n.lower().endswith("/" + wanted_lower):
            return n

    for n in names:
        if os.path.basename(n).lower() == wanted_lower:
            return n

    return None


def _read_gtfs_csv_from_zip(zf: zipfile.ZipFile, name: str) -> List[Dict[str, str]]:
    real_name = _resolve_zip_name(zf, name)

    if not real_name:
        raise FileNotFoundError(name)

    with zf.open(real_name) as f:
        text = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
        return list(csv.DictReader(text))


def _read_gtfs_csv_from_dir(base_dir: str, name: str) -> List[Dict[str, str]]:
    path = os.path.join(base_dir, name)

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_gtfs() -> None:
    source_kind = None
    source_value = None

    if os.path.exists(GTFS_ZIP_PATH):
        source_kind = "zip"
        source_value = GTFS_ZIP_PATH
    elif os.path.isdir(GTFS_DIR):
        source_kind = "dir"
        source_value = GTFS_DIR
    else:
        GTFS["ok"] = False
        GTFS["error"] = f"GTFS not found. Tried {GTFS_ZIP_PATH} and {GTFS_DIR}"
        return

    try:
        if source_kind == "zip":
            zf_obj = zipfile.ZipFile(source_value, "r")

            def has_file(name: str) -> bool:
                return _resolve_zip_name(zf_obj, name) is not None

            def read_csv(name: str) -> List[Dict[str, str]]:
                return _read_gtfs_csv_from_zip(zf_obj, name)

            close_after = zf_obj

        else:
            def has_file(name: str) -> bool:
                return os.path.exists(os.path.join(source_value, name))

            def read_csv(name: str) -> List[Dict[str, str]]:
                return _read_gtfs_csv_from_dir(source_value, name)

            close_after = None

        try:
            agency_rows = read_csv("agency.txt") if has_file("agency.txt") else []
            stops_rows = read_csv("stops.txt")
            routes_rows = read_csv("routes.txt")
            trips_rows = read_csv("trips.txt")
            stop_times_rows = read_csv("stop_times.txt")

            agency: Dict[str, Dict[str, Any]] = {}
            stops: Dict[str, Dict[str, Any]] = {}
            routes: Dict[str, Dict[str, Any]] = {}
            trips: Dict[str, Dict[str, Any]] = {}
            stop_times_by_stop: Dict[str, List[Dict[str, Any]]] = {}
            stop_times_by_trip: Dict[str, List[Dict[str, Any]]] = {}
            calendar: Dict[str, Any] = {}
            calendar_dates: Dict[str, Dict[date, int]] = {}
            shapes: Dict[str, List[Dict[str, Any]]] = {}

            for r in agency_rows:
                agency_id = r.get("agency_id") or "default"
                agency[agency_id] = {
                    "agency_id": agency_id,
                    "agency_name": r.get("agency_name") or "",
                    "agency_url": r.get("agency_url") or "",
                    "agency_timezone": r.get("agency_timezone") or "",
                }

            if has_file("calendar.txt"):
                for r in read_csv("calendar.txt"):
                    sid = r.get("service_id")
                    if not sid:
                        continue

                    calendar[sid] = {
                        "service_id": sid,
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

            if has_file("calendar_dates.txt"):
                for r in read_csv("calendar_dates.txt"):
                    sid = r.get("service_id")
                    d = r.get("date")
                    ex = r.get("exception_type")

                    if not sid or not d or not ex:
                        continue

                    calendar_dates.setdefault(sid, {})[parse_gtfs_date(d)] = int(ex)

            for r in stops_rows:
                sid = r.get("stop_id")
                if not sid:
                    continue

                try:
                    lat = float(r["stop_lat"]) if r.get("stop_lat") else None
                    lon = float(r["stop_lon"]) if r.get("stop_lon") else None
                except Exception:
                    lat = None
                    lon = None

                stop = {
                    "stop_id": sid,
                    "stop_name": r.get("stop_name") or sid,
                    "stop_code": r.get("stop_code") or "",
                    "stop_lat": lat,
                    "stop_lon": lon,
                }

                stop["short_code"] = stop_short_code(stop)
                stops[sid] = stop

            routes_by_line: Dict[str, List[str]] = {}

            for r in routes_rows:
                rid = r.get("route_id")
                if not rid:
                    continue

                short_name = (r.get("route_short_name") or "").strip()

                route = {
                    "route_id": rid,
                    "agency_id": r.get("agency_id") or "",
                    "short_name": short_name,
                    "long_name": (r.get("route_long_name") or "").strip(),
                    "route_type": r.get("route_type") or "",
                    "route_color": r.get("route_color") or "",
                    "route_text_color": r.get("route_text_color") or "",
                }

                routes[rid] = route

                if short_name:
                    routes_by_line.setdefault(norm_line(short_name), []).append(rid)

            for r in trips_rows:
                tid = r.get("trip_id")
                rid = r.get("route_id")
                sid = r.get("service_id")

                if not tid or not rid or not sid:
                    continue

                trips[tid] = {
                    "trip_id": tid,
                    "route_id": rid,
                    "service_id": sid,
                    "headsign": (r.get("trip_headsign") or "").strip(),
                    "direction_id": r.get("direction_id"),
                    "block_id": r.get("block_id") or "",
                    "shape_id": r.get("shape_id") or "",
                }

            for r in stop_times_rows:
                trip_id = r.get("trip_id")
                stop_id = r.get("stop_id")

                if not trip_id or not stop_id:
                    continue

                dep = r.get("departure_time") or r.get("arrival_time")
                arr = r.get("arrival_time") or r.get("departure_time")

                if not dep:
                    continue

                try:
                    dep_s = parse_hms_to_seconds(dep)
                    arr_s = parse_hms_to_seconds(arr) if arr else dep_s
                    seq = int(float(r.get("stop_sequence") or 0))
                except Exception:
                    continue

                row = {
                    "trip_id": trip_id,
                    "stop_id": stop_id,
                    "stop_sequence": seq,
                    "arrival_s": arr_s,
                    "departure_s": dep_s,
                    "pickup_type": r.get("pickup_type") or "",
                    "drop_off_type": r.get("drop_off_type") or "",
                }

                stop_times_by_stop.setdefault(stop_id, []).append(row)
                stop_times_by_trip.setdefault(trip_id, []).append(row)

            for sid in stop_times_by_stop:
                stop_times_by_stop[sid].sort(key=lambda x: (x["departure_s"], x["trip_id"]))

            for tid in stop_times_by_trip:
                stop_times_by_trip[tid].sort(key=lambda x: x["stop_sequence"])

            if has_file("shapes.txt"):
                for r in read_csv("shapes.txt"):
                    shape_id = r.get("shape_id")
                    if not shape_id:
                        continue

                    try:
                        lat = float(r["shape_pt_lat"])
                        lon = float(r["shape_pt_lon"])
                        seq = int(float(r.get("shape_pt_sequence") or 0))
                    except Exception:
                        continue

                    shapes.setdefault(shape_id, []).append({
                        "lat": lat,
                        "lon": lon,
                        "seq": seq,
                    })

                for shape_id in shapes:
                    shapes[shape_id].sort(key=lambda x: x["seq"])

            trip_meta: Dict[str, Dict[str, Any]] = {}

            for tid, trip in trips.items():
                times = stop_times_by_trip.get(tid, [])
                route = routes.get(trip["route_id"], {})
                line = route.get("short_name") or trip["route_id"]

                if times:
                    first = times[0]
                    last = times[-1]
                    first_stop = stops.get(first["stop_id"], {})
                    last_stop = stops.get(last["stop_id"], {})
                    start_s = first["departure_s"]
                    end_s = last["arrival_s"]
                else:
                    first = {}
                    last = {}
                    first_stop = {}
                    last_stop = {}
                    start_s = None
                    end_s = None

                trip_meta[tid] = {
                    "trip_id": tid,
                    "route_id": trip["route_id"],
                    "line": line,
                    "service_id": trip["service_id"],
                    "headsign": trip.get("headsign") or last_stop.get("stop_name") or "",
                    "direction_id": trip.get("direction_id"),
                    "block_id": trip.get("block_id") or "",
                    "shape_id": trip.get("shape_id") or "",
                    "start_s": start_s,
                    "end_s": end_s,
                    "first_stop_id": first.get("stop_id"),
                    "last_stop_id": last.get("stop_id"),
                    "first_stop_name": first_stop.get("stop_name") or "",
                    "last_stop_name": last_stop.get("stop_name") or "",
                    "stop_count": len(times),
                }

            GTFS.update({
                "ok": True,
                "error": None,
                "source": f"{source_kind}:{source_value}",

                "agency": agency,
                "stops": stops,
                "routes": routes,
                "trips": trips,

                "stop_times_by_stop": stop_times_by_stop,
                "stop_times_by_trip": stop_times_by_trip,

                "calendar": calendar,
                "calendar_dates": calendar_dates,

                "shapes": shapes,

                "trip_meta": trip_meta,
                "routes_by_line": routes_by_line,
            })

        finally:
            if close_after is not None:
                close_after.close()

    except Exception as e:
        GTFS["ok"] = False
        GTFS["error"] = f"GTFS load error: {e}"


def service_active(service_id: str, service_date: date) -> bool:
    ex = GTFS["calendar_dates"].get(service_id, {}).get(service_date)

    if ex == 1:
        return True

    if ex == 2:
        return False

    cal = GTFS["calendar"].get(service_id)

    if not cal:
        return True

    if service_date < cal["start_date"] or service_date > cal["end_date"]:
        return False

    wd = service_date.weekday()

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


def route_for_trip(trip_id: str) -> Dict[str, Any]:
    trip = GTFS["trips"].get(trip_id, {})
    return GTFS["routes"].get(trip.get("route_id"), {})


def line_for_trip(trip_id: str) -> str:
    meta = GTFS["trip_meta"].get(trip_id)
    if meta:
        return meta.get("line") or ""
    route = route_for_trip(trip_id)
    return route.get("short_name") or ""


def routes_for_line(line: str) -> List[Dict[str, Any]]:
    wanted = norm_line(line)
    out = []

    for r in GTFS["routes"].values():
        short_name = r.get("short_name") or ""
        long_name = r.get("long_name") or ""
        route_id = r.get("route_id") or ""

        if (
            norm_line(short_name) == wanted
            or norm_line(route_id) == wanted
            or norm_line(long_name) == wanted
        ):
            out.append(r)

    return out


# ============================================================
# LIVE SIRI-VM
# ============================================================

def fetch_live_xml() -> bytes:
    if not DFT_API_KEY and "api_key=" not in DFT_FEED_URL:
        raise RuntimeError("Missing DFT_API_KEY environment variable")

    if "api_key=" in DFT_FEED_URL:
        resp = requests.get(DFT_FEED_URL, timeout=25)
    else:
        resp = requests.get(DFT_FEED_URL, params={"api_key": DFT_API_KEY}, timeout=25)

    LIVE_CACHE["last_http_status"] = resp.status_code
    resp.raise_for_status()

    return decode_feed_bytes(resp.content)


def parse_siri_vm(xml_bytes: bytes) -> List[Dict[str, Any]]:
    vehicles: List[Dict[str, Any]] = []

    if not xml_bytes:
        return vehicles

    root = ET.fromstring(xml_bytes)

    for delivery in root.findall(".//{*}VehicleMonitoringDelivery"):
        delivery_valid_until = (
            safe_text(delivery.find("./{*}ValidUntilTime"))
            or safe_text(delivery.find(".//{*}ValidUntilTime"))
        )

        for va in delivery.findall(".//{*}VehicleActivity"):
            recorded = (
                safe_text(va.find("./{*}RecordedAtTime"))
                or safe_text(va.find(".//{*}RecordedAtTime"))
            )

            recorded_dt = parse_iso_dt(recorded)

            item_identifier = (
                safe_text(va.find("./{*}ItemIdentifier"))
                or safe_text(va.find(".//{*}ItemIdentifier"))
            )

            valid_until = (
                safe_text(va.find("./{*}ValidUntilTime"))
                or delivery_valid_until
            )

            mvj = va.find(".//{*}MonitoredVehicleJourney")

            if mvj is None:
                continue

            def pick_from_mvj(tag: str) -> Optional[str]:
                return safe_text(mvj.find(f".//{{*}}{tag}"))

            def pick_from_va(tag: str) -> Optional[str]:
                return safe_text(va.find(f".//{{*}}{tag}"))

            data_frame_ref = safe_text(
                mvj.find(".//{*}FramedVehicleJourneyRef/{*}DataFrameRef")
            )

            dated_vehicle_journey_ref = safe_text(
                mvj.find(".//{*}FramedVehicleJourneyRef/{*}DatedVehicleJourneyRef")
            )

            line_ref = pick_from_mvj("LineRef")
            published_line_name = pick_from_mvj("PublishedLineName") or line_ref

            operator_ref = pick_from_mvj("OperatorRef")
            direction_ref = pick_from_mvj("DirectionRef")

            origin_ref = pick_from_mvj("OriginRef")
            origin_name = pick_from_mvj("OriginName") or origin_ref
            destination_ref = pick_from_mvj("DestinationRef")
            destination_name = pick_from_mvj("DestinationName") or destination_ref

            origin_aimed_departure_time = pick_from_mvj("OriginAimedDepartureTime")
            destination_aimed_arrival_time = pick_from_mvj("DestinationAimedArrivalTime")

            block_ref = pick_from_mvj("BlockRef")
            vehicle_ref = pick_from_mvj("VehicleRef")

            vehicle_unique_id = (
                pick_from_va("VehicleUniqueId")
                or pick_from_mvj("VehicleUniqueId")
                or vehicle_ref
            )

            ticket_machine_service_code = (
                pick_from_va("TicketMachineServiceCode")
                or pick_from_mvj("TicketMachineServiceCode")
            )

            journey_code = (
                pick_from_va("JourneyCode")
                or pick_from_mvj("JourneyCode")
                or dated_vehicle_journey_ref
            )

            lon = safe_text(mvj.find(".//{*}VehicleLocation/{*}Longitude"))
            lat = safe_text(mvj.find(".//{*}VehicleLocation/{*}Latitude"))
            bearing = pick_from_mvj("Bearing")

            calls: List[Dict[str, Any]] = []
            current_stop_ref = None
            current_stop_name = None
            vehicle_at_stop = False

            def parse_call(call_el: ET.Element, is_monitored: bool) -> Optional[Dict[str, Any]]:
                stop_ref = safe_text(call_el.find(".//{*}StopPointRef"))
                stop_name = safe_text(call_el.find(".//{*}StopPointName"))

                aimed_arr = safe_text(call_el.find(".//{*}AimedArrivalTime"))
                aimed_dep = safe_text(call_el.find(".//{*}AimedDepartureTime"))
                exp_arr = safe_text(call_el.find(".//{*}ExpectedArrivalTime"))
                exp_dep = safe_text(call_el.find(".//{*}ExpectedDepartureTime"))

                v_at_stop = safe_text(call_el.find(".//{*}VehicleAtStop"))
                v_at_stop_bool = (v_at_stop or "").lower() == "true"

                if not stop_ref:
                    return None

                return {
                    "stopRef": stop_ref,
                    "stopName": stop_name,
                    "aimedArr": aimed_arr,
                    "aimedDep": aimed_dep,
                    "expArr": exp_arr,
                    "expDep": exp_dep or exp_arr,
                    "vehicleAtStop": v_at_stop_bool if is_monitored else False,
                    "isMonitored": is_monitored,
                }

            monitored_call = mvj.find(".//{*}MonitoredCall")

            if monitored_call is not None:
                c = parse_call(monitored_call, True)
                if c:
                    calls.append(c)
                    current_stop_ref = c["stopRef"]
                    current_stop_name = c.get("stopName")
                    vehicle_at_stop = bool(c.get("vehicleAtStop"))

            onward = mvj.find(".//{*}OnwardCalls")

            if onward is not None:
                for oc in onward.findall(".//{*}OnwardCall"):
                    c = parse_call(oc, False)
                    if c:
                        calls.append(c)

            try:
                longitude = float(lon) if lon else None
                latitude = float(lat) if lat else None
            except Exception:
                longitude = None
                latitude = None

            try:
                bearing_f = float(bearing) if bearing else None
            except Exception:
                bearing_f = None

            v: Dict[str, Any] = {
                "itemIdentifier": item_identifier,

                "recordedAtTime": recorded,
                "recordedAtTimeLocal": recorded_dt.isoformat() if recorded_dt else None,
                "validUntilTime": valid_until,

                "dataFrameRef": data_frame_ref,
                "datedVehicleJourneyRef": dated_vehicle_journey_ref,

                "lineRef": line_ref,
                "publishedLineName": published_line_name,
                "lineNorm": norm_line(published_line_name or line_ref),

                "operatorRef": operator_ref,
                "directionRef": direction_ref,

                "originRef": origin_ref,
                "originName": origin_name,
                "destinationRef": destination_ref,
                "destinationName": destination_name,

                "originAimedDepartureTime": origin_aimed_departure_time,
                "destinationAimedArrivalTime": destination_aimed_arrival_time,

                "longitude": longitude,
                "latitude": latitude,
                "bearing": bearing_f,

                "blockRef": block_ref,
                "vehicleRef": vehicle_ref,
                "vehicleUniqueId": vehicle_unique_id,

                "ticketMachineServiceCode": ticket_machine_service_code,
                "journeyCode": journey_code,

                "calls": calls,
                "currentStopRef": current_stop_ref,
                "currentStopName": current_stop_name,
                "vehicleAtStop": bool(vehicle_at_stop),
            }

            v["tripId"] = (
                v.get("datedVehicleJourneyRef")
                or v.get("journeyCode")
                or v.get("blockRef")
            )

            vehicles.append(v)

    return vehicles


def get_live_all_cached() -> Tuple[List[Dict[str, Any]], int]:
    now_ts = time.time()

    if (
        (now_ts - LIVE_CACHE["ts"]) < LIVE_CACHE["ttl"]
        and LIVE_CACHE["vehicles_all"] is not None
    ):
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
            "last_fetch_time": now_local().isoformat(),
        })

        return vehicles_all, len(vehicles_all)

    except Exception as e:
        LIVE_CACHE.update({
            "ts": now_ts,
            "vehicles_all": [],
            "raw_count": 0,
            "ok": False,
            "error": str(e),
            "last_fetch_time": now_local().isoformat(),
        })

        return [], 0


def filter_live_vehicles(
    vehicles_all: List[Dict[str, Any]],
    line: Optional[str] = None,
    operator: Optional[str] = None,
    max_age_seconds: int = DEFAULT_MAX_AGE_SECONDS,
    fresh_only: bool = True,
) -> List[Dict[str, Any]]:
    nl = now_local()
    today_str = nl.date().isoformat()

    if operator:
        allowed_ops: Optional[Set[str]] = {norm_operator(operator)}
    elif DEFAULT_OPERATOR_REFS:
        allowed_ops = set(DEFAULT_OPERATOR_REFS)
    else:
        allowed_ops = None

    out: List[Dict[str, Any]] = []

    for v in vehicles_all:
        df = v.get("dataFrameRef")

        if looks_like_date(df) and df != today_str:
            continue

        rdt = parse_iso_dt(v.get("recordedAtTime"))

        if rdt is None:
            if fresh_only:
                continue
            age_s = None
        else:
            age_s = (nl - rdt).total_seconds()

        if fresh_only:
            if age_s is None:
                continue
            if age_s > max_age_seconds:
                continue
            if age_s < -60:
                continue

        vut = parse_iso_dt(v.get("validUntilTime"))

        if vut is not None and nl > (vut + timedelta(seconds=5)):
            continue

        if line:
            wanted = norm_line(line)
            if (
                norm_line(v.get("lineRef")) != wanted
                and norm_line(v.get("publishedLineName")) != wanted
            ):
                continue

        if allowed_ops is not None:
            op = norm_operator(v.get("operatorRef"))
            if op not in allowed_ops:
                continue

        vv = dict(v)
        vv["ageSeconds"] = int(age_s) if age_s is not None else None
        out.append(vv)

    latest: Dict[str, Dict[str, Any]] = {}
    no_ref: List[Dict[str, Any]] = []

    for v in out:
        ref = v.get("vehicleRef") or v.get("vehicleUniqueId") or v.get("itemIdentifier")

        if not ref:
            no_ref.append(v)
            continue

        old = latest.get(str(ref))

        if old is None:
            latest[str(ref)] = v
        else:
            old_dt = parse_iso_dt(old.get("recordedAtTime")) or datetime.min.replace(tzinfo=TZ)
            new_dt = parse_iso_dt(v.get("recordedAtTime")) or datetime.min.replace(tzinfo=TZ)

            if new_dt > old_dt:
                latest[str(ref)] = v

    result = list(latest.values()) + no_ref

    result.sort(
        key=lambda x: (
            norm_line(x.get("publishedLineName") or x.get("lineRef")),
            x.get("vehicleRef") or x.get("vehicleUniqueId") or "",
        )
    )

    return result


def public_vehicle(v: Dict[str, Any]) -> Dict[str, Any]:
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
        "originName": v.get("originName"),
        "destinationRef": v.get("destinationRef"),
        "destinationName": v.get("destinationName"),

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
        "currentStopName": v.get("currentStopName"),
        "vehicleAtStop": bool(v.get("vehicleAtStop")),

        "ageSeconds": v.get("ageSeconds"),
        "tripId": v.get("tripId"),
    }


# ============================================================
# LIVE + GTFS MATCHING
# ============================================================

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

    live_trip_refs = [
        v.get("tripId"),
        v.get("datedVehicleJourneyRef"),
        v.get("journeyCode"),
        v.get("blockRef"),
    ]

    for ref in live_trip_refs:
        if ref and str(ref) == str(trip_id):
            score += 80
            reasons.append("exact_trip")
            break

    trip_text = " ".join([
        str(trip_id),
        str(meta.get("block_id") or ""),
        str(meta.get("route_id") or ""),
    ])

    for ref in live_trip_refs:
        if ref and len(str(ref)) >= 3 and str(ref) in trip_text:
            score += 12
            reasons.append("ref_in_trip")
            break

    if live_matches_destination(v, meta):
        score += 18
        reasons.append("dest")

    if live_matches_origin(v, meta):
        score += 8
        reasons.append("origin")

    live_origin_s = live_origin_aimed_seconds(v, service_date)
    gtfs_start_s = meta.get("start_s")

    if live_origin_s is not None and gtfs_start_s is not None:
        diff = abs(live_origin_s - gtfs_start_s)

        if diff <= 90:
            score += 45
            reasons.append("origin_time_exact")
        elif diff <= 5 * 60:
            score += 35
            reasons.append("origin_time_near")
        elif diff <= 15 * 60:
            score += 15
            reasons.append("origin_time_loose")
        else:
            score -= 25
            reasons.append("origin_time_far")

    nl = now_local()
    start_s = meta.get("start_s")
    end_s = meta.get("end_s")

    if start_s is not None and end_s is not None:
        trip_start = departure_datetime(service_date, start_s)
        trip_end = departure_datetime(service_date, end_s)

        if trip_start - timedelta(minutes=20) <= nl <= trip_end + timedelta(minutes=35):
            score += 10
            reasons.append("time_active")
        elif nl < trip_start - timedelta(minutes=60) or nl > trip_end + timedelta(minutes=90):
            score -= 20
            reasons.append("not_time_active")

    if v.get("currentStopRef"):
        current_ref = v.get("currentStopRef")
        times = GTFS["stop_times_by_trip"].get(trip_id, [])
        if any(t.get("stop_id") == current_ref for t in times):
            score += 20
            reasons.append("current_stop_on_trip")

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

    if best is not None and best_score >= 35:
        matched = dict(best)
        matched["_matchScore"] = round(best_score, 1)
        matched["_matchReason"] = best_reason
        cache[key] = matched
        return matched

    cache[key] = None
    return None


def nearest_stop_on_trip(v: Dict[str, Any], trip_id: str) -> Optional[Dict[str, Any]]:
    lat = v.get("latitude")
    lon = v.get("longitude")

    if lat is None or lon is None:
        return None

    best = None
    best_dist = None

    for st in GTFS["stop_times_by_trip"].get(trip_id, []):
        stop = GTFS["stops"].get(st["stop_id"])

        if not stop:
            continue

        slat = stop.get("stop_lat")
        slon = stop.get("stop_lon")

        if slat is None or slon is None:
            continue

        d = distance_m(float(lat), float(lon), float(slat), float(slon))

        if best is None or d < best_dist:
            best = {
                **st,
                "stop_name": stop.get("stop_name"),
                "distance_m": d,
            }
            best_dist = d

    return best


def live_info_for_trip_stop(
    trip_id: str,
    service_date: date,
    stop_time: Dict[str, Any],
    live_vehicle: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    scheduled_dt = departure_datetime(service_date, stop_time["departure_s"])

    if not live_vehicle:
        return {
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

    stop_id = stop_time["stop_id"]
    target_seq = stop_time["stop_sequence"]

    for c in live_vehicle.get("calls") or []:
        if c.get("stopRef") == stop_id:
            exp_dt = (
                parse_iso_dt(c.get("expDep"))
                or parse_iso_dt(c.get("expArr"))
                or scheduled_dt
            )

            aimed_dt = (
                parse_iso_dt(c.get("aimedDep"))
                or parse_iso_dt(c.get("aimedArr"))
                or scheduled_dt
            )

            delay = int(round((exp_dt - aimed_dt).total_seconds() / 60))

            return {
                "isLive": True,
                "expected_dt": exp_dt,
                "delayMinutes": delay,
                "vehicleAtStop": bool(c.get("vehicleAtStop")),
                "passed": False,
                "currentStopName": live_vehicle.get("currentStopName"),
                "nextStopName": c.get("stopName"),
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
            "vehicleAtStop": bool(live_vehicle.get("vehicleAtStop")) or True,
            "passed": False,
            "currentStopName": live_vehicle.get("currentStopName"),
            "nextStopName": live_vehicle.get("currentStopName"),
            "vehicleRef": live_vehicle.get("vehicleRef"),
            "vehicleUniqueId": live_vehicle.get("vehicleUniqueId"),
            "matchScore": live_vehicle.get("_matchScore"),
            "matchReason": live_vehicle.get("_matchReason"),
        }

    nearest = nearest_stop_on_trip(live_vehicle, trip_id)

    if nearest:
        current_seq = nearest["stop_sequence"]
        current_stop_name = nearest.get("stop_name")
        current_scheduled_dt = departure_datetime(service_date, nearest["departure_s"])
        dist = nearest.get("distance_m") or 999999

        if target_seq < current_seq - 1:
            return {
                "isLive": False,
                "expected_dt": None,
                "delayMinutes": None,
                "vehicleAtStop": False,
                "passed": True,
                "currentStopName": current_stop_name,
                "nextStopName": current_stop_name,
                "vehicleRef": live_vehicle.get("vehicleRef"),
                "vehicleUniqueId": live_vehicle.get("vehicleUniqueId"),
                "matchScore": live_vehicle.get("_matchScore"),
                "matchReason": live_vehicle.get("_matchReason"),
            }

        raw_delay = (now_local() - current_scheduled_dt).total_seconds() / 60

        if raw_delay < -60 or raw_delay > 120:
            delay = 0
        else:
            delay = int(round(raw_delay))

        expected_dt = scheduled_dt + timedelta(minutes=delay)

        vehicle_at_stop = (
            target_seq == current_seq
            and dist <= 90
            and abs((now_local() - scheduled_dt).total_seconds()) <= 6 * 60
        )

        return {
            "isLive": True,
            "expected_dt": expected_dt,
            "delayMinutes": delay,
            "vehicleAtStop": bool(vehicle_at_stop),
            "passed": False,
            "currentStopName": current_stop_name,
            "nextStopName": current_stop_name,
            "vehicleRef": live_vehicle.get("vehicleRef"),
            "vehicleUniqueId": live_vehicle.get("vehicleUniqueId"),
            "matchScore": live_vehicle.get("_matchScore"),
            "matchReason": live_vehicle.get("_matchReason"),
        }

    return {
        "isLive": True,
        "expected_dt": scheduled_dt,
        "delayMinutes": 0,
        "vehicleAtStop": False,
        "passed": False,
        "currentStopName": live_vehicle.get("currentStopName"),
        "nextStopName": live_vehicle.get("currentStopName"),
        "vehicleRef": live_vehicle.get("vehicleRef"),
        "vehicleUniqueId": live_vehicle.get("vehicleUniqueId"),
        "matchScore": live_vehicle.get("_matchScore"),
        "matchReason": live_vehicle.get("_matchReason"),
    }


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
def startup_event():
    load_gtfs()


# ============================================================
# STATIC
# ============================================================

@app.get("/", response_class=FileResponse)
def index():
    candidates = [
        "templates/index.html",
        "index.html",
        "static/index.html",
    ]

    for p in candidates:
        if os.path.exists(p):
            return FileResponse(p)

    raise HTTPException(status_code=404, detail="index.html not found")


@app.get("/health")
def health():
    return {
        "ok": True,
        "time": now_local().isoformat(),
    }


@app.get("/api/health")
def api_health():
    return health()


# ============================================================
# STATUS
# ============================================================

def calendar_range_debug() -> Dict[str, Optional[str]]:
    starts = []
    ends = []

    for c in GTFS["calendar"].values():
        starts.append(c["start_date"])
        ends.append(c["end_date"])

    for dmap in GTFS["calendar_dates"].values():
        starts.extend(dmap.keys())
        ends.extend(dmap.keys())

    return {
        "calendar_start_min": min(starts).strftime("%Y%m%d") if starts else None,
        "calendar_end_max": max(ends).strftime("%Y%m%d") if ends else None,
    }


@app.get("/api/status")
def api_status():
    vehicles_all, raw_count = get_live_all_cached()

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
            "operatorFilter": DEFAULT_OPERATOR_REFS,
            "error": LIVE_CACHE["error"],
            "lastHttpStatus": LIVE_CACHE.get("last_http_status"),
            "lastFetchTime": LIVE_CACHE.get("last_fetch_time"),
        },
        "gtfs": {
            "ok": bool(GTFS["ok"]),
            "error": GTFS["error"],
            "source": GTFS["source"],
            "counts": {
                "agency": len(GTFS["agency"]),
                "stops": len(GTFS["stops"]),
                "routes": len(GTFS["routes"]),
                "trips": len(GTFS["trips"]),
                "stop_times_trips": len(GTFS["stop_times_by_trip"]),
                "stop_departures_index_stops": len(GTFS["stop_times_by_stop"]),
                "shapes": len(GTFS["shapes"]),
            },
            "calendarRange": calendar_range_debug(),
        },
        "serverTime": now_local().isoformat(),
        "timezone": str(TZ),
    }


@app.get("/status")
def status_alias():
    return api_status()


@app.get("/api/gtfs/status")
def api_gtfs_status():
    return {
        "loaded": bool(GTFS["ok"]),
        "error": GTFS["error"],
        "source": GTFS["source"],
        "counts": {
            "agency": len(GTFS["agency"]),
            "stops": len(GTFS["stops"]),
            "routes": len(GTFS["routes"]),
            "trips": len(GTFS["trips"]),
            "stop_times_trips": len(GTFS["stop_times_by_trip"]),
            "stop_departures_index_stops": len(GTFS["stop_times_by_stop"]),
            "shapes": len(GTFS["shapes"]),
        },
        "calendarRange": calendar_range_debug(),
        "agencyFilter": "",
    }


@app.get("/api/gtfs-debug")
def api_gtfs_debug():
    return api_gtfs_status()


@app.post("/api/reload-gtfs")
def api_reload_gtfs():
    load_gtfs()
    return api_gtfs_status()


# ============================================================
# SEARCH
# ============================================================

@app.get("/api/search")
def api_search(q: str = Query("", min_length=0), limit: int = 40):
    if not GTFS["ok"]:
        raise HTTPException(status_code=500, detail=GTFS["error"])

    q = q.strip()
    nq = clean_name(q)
    line_q = norm_line(q)

    stops_out = []
    routes_out = []

    if q:
        for stop in GTFS["stops"].values():
            name = stop.get("stop_name") or ""
            sid = stop.get("stop_id") or ""
            code = stop.get("short_code") or ""

            if (
                nq in clean_name(name)
                or q.lower() in sid.lower()
                or q.lower() in code.lower()
            ):
                stops_out.append(stop)

            if len(stops_out) >= limit:
                break

        for route in GTFS["routes"].values():
            short_name = route.get("short_name") or ""
            long_name = route.get("long_name") or ""
            rid = route.get("route_id") or ""

            if (
                line_q == norm_line(short_name)
                or line_q in norm_line(short_name)
                or nq in clean_name(long_name)
                or q.lower() in rid.lower()
            ):
                routes_out.append(route)

            if len(routes_out) >= limit:
                break

    return {
        "query": q,
        "stops": stops_out,
        "routes": routes_out,
    }


@app.get("/api/stops")
def api_stops(q: str = "", limit: int = 80):
    return api_search(q=q, limit=limit)


@app.get("/api/routes")
def api_routes():
    routes = sorted(
        GTFS["routes"].values(),
        key=lambda r: norm_line(r.get("short_name") or r.get("route_id") or ""),
    )
    return {"routes": routes}


# ============================================================
# STOP DEPARTURES
# ============================================================

def scheduled_departures_for_stop(
    stop_id: str,
    minutes: int,
    include_past_seconds: int = 45,
) -> List[Dict[str, Any]]:
    if stop_id not in GTFS["stops"]:
        raise HTTPException(status_code=404, detail="Stop not found")

    nl = now_local()
    window_start = nl - timedelta(seconds=include_past_seconds)
    window_end = nl + timedelta(minutes=minutes)

    rows: List[Dict[str, Any]] = []

    candidate_service_dates = [
        nl.date() - timedelta(days=1),
        nl.date(),
        nl.date() + timedelta(days=1),
    ]

    stop_rows = GTFS["stop_times_by_stop"].get(stop_id, [])

    for service_date in candidate_service_dates:
        for st in stop_rows:
            trip_id = st["trip_id"]
            trip = GTFS["trips"].get(trip_id)

            if not trip:
                continue

            if not service_active(trip["service_id"], service_date):
                continue

            if str(st.get("pickup_type") or "") == "1":
                continue

            sched_dt = departure_datetime(service_date, st["departure_s"])

            if sched_dt < window_start or sched_dt > window_end:
                continue

            meta = GTFS["trip_meta"].get(trip_id, {})
            route = route_for_trip(trip_id)
            line = route.get("short_name") or meta.get("line") or route.get("route_id") or "?"

            dest = (
                trip.get("headsign")
                or meta.get("headsign")
                or meta.get("last_stop_name")
                or ""
            )

            rows.append({
                "trip_id": trip_id,
                "tripId": trip_id,
                "serviceDate": service_date.isoformat(),
                "stop_id": stop_id,
                "stopId": stop_id,
                "stop_sequence": st["stop_sequence"],
                "stopSequence": st["stop_sequence"],
                "scheduled_s": st["departure_s"],
                "scheduledTime": sched_dt.isoformat(),
                "aimedTime": sched_dt.isoformat(),

                "line": line,
                "routeShortName": line,
                "route_short_name": line,
                "routeId": route.get("route_id") or trip.get("route_id"),
                "route_id": route.get("route_id") or trip.get("route_id"),

                "destination": dest,
                "destinationName": dest,
                "headsign": dest,

                "platform": stop_id,
                "stopName": GTFS["stops"].get(stop_id, {}).get("stop_name") or stop_id,
            })

    rows.sort(key=lambda x: x["scheduledTime"])
    return rows


def enrich_departures_with_live(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    vehicles_all, _ = get_live_all_cached()
    live_vehicles = filter_live_vehicles(
        vehicles_all,
        line=None,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    match_cache: Dict[str, Any] = {}
    nl = now_local()

    out = []

    for row in rows:
        trip_id = row["trip_id"]
        service_date = date.fromisoformat(row["serviceDate"])

        st = {
            "trip_id": trip_id,
            "stop_id": row["stop_id"],
            "stop_sequence": row["stop_sequence"],
            "departure_s": row["scheduled_s"],
            "arrival_s": row["scheduled_s"],
        }

        live_vehicle = match_live_for_trip(
            trip_id,
            service_date,
            live_vehicles,
            match_cache,
        )

        live_info = live_info_for_trip_stop(
            trip_id,
            service_date,
            st,
            live_vehicle,
        )

        sched_dt = parse_iso_dt(row["scheduledTime"])
        expected_dt = live_info.get("expected_dt") if live_info.get("isLive") else None
        display_dt = expected_dt or sched_dt

        minutes_until = int(round((display_dt - nl).total_seconds() / 60)) if display_dt else None

        if minutes_until is not None and minutes_until < -2:
            continue

        is_due = minutes_until is not None and minutes_until <= 1

        row.update({
            "isLive": bool(live_info.get("isLive")),
            "is_live": bool(live_info.get("isLive")),
            "live": bool(live_info.get("isLive")),

            "source": "LIVE" if live_info.get("isLive") else "GTFS",
            "statusText": "Élő adat" if live_info.get("isLive") else "Menetrendi adat",
            "note": "Élő adat" if live_info.get("isLive") else "Menetrendi adat",

            "expectedTime": iso_or_none(expected_dt),
            "displayTime": iso_or_none(display_dt),
            "time": hhmm_dt(display_dt),
            "displayClock": hhmm_dt(display_dt),

            "scheduledClock": hhmm_dt(sched_dt),
            "aimedClock": hhmm_dt(sched_dt),

            "minutesUntil": minutes_until,
            "minutes": minutes_until,
            "dueIn": minutes_until,

            "delayMinutes": live_info.get("delayMinutes"),
            "delay": live_info.get("delayMinutes"),

            "isDue": bool(is_due),
            "due": bool(is_due),

            "vehicleAtStop": bool(live_info.get("vehicleAtStop")),
            "atStop": bool(live_info.get("vehicleAtStop")),

            "vehicleRef": live_info.get("vehicleRef"),
            "vehicleUniqueId": live_info.get("vehicleUniqueId"),

            "currentStopName": live_info.get("currentStopName"),
            "nextStopName": live_info.get("nextStopName"),

            "liveMatchScore": live_info.get("matchScore"),
            "liveMatchReason": live_info.get("matchReason"),
        })

        out.append(row)

    out.sort(key=lambda x: x.get("displayTime") or x.get("scheduledTime") or "")
    return out


@app.get("/api/stop/{stop_id}/departures")
def api_stop_departures(
    stop_id: str,
    minutes: int = Query(DEFAULT_DEPARTURE_WINDOW_MINUTES, ge=5, le=240),
):
    if not GTFS["ok"]:
        raise HTTPException(status_code=500, detail=GTFS["error"])

    stop = GTFS["stops"].get(stop_id)

    if not stop:
        raise HTTPException(status_code=404, detail="Stop not found")

    rows = scheduled_departures_for_stop(stop_id, minutes)
    rows = enrich_departures_with_live(rows)

    return {
        "stop": stop,
        "stop_id": stop_id,
        "stopId": stop_id,
        "stop_name": stop.get("stop_name"),
        "stopName": stop.get("stop_name"),
        "departures": rows,
        "items": rows,
        "serverTime": now_local().isoformat(),
        "legend": "Fehér: menetrend (GTFS) · Zöld: élő (LIVE)",
    }


@app.get("/api/departures/{stop_id}")
def api_departures_alias(
    stop_id: str,
    minutes: int = Query(DEFAULT_DEPARTURE_WINDOW_MINUTES, ge=5, le=240),
):
    return api_stop_departures(stop_id=stop_id, minutes=minutes)


# ============================================================
# TRIP VIEW
# ============================================================

@app.get("/api/trip/{trip_id:path}")
def api_trip(
    trip_id: str,
    serviceDate: Optional[str] = None,
    vehicleRef: Optional[str] = None,
):
    if not GTFS["ok"]:
        raise HTTPException(status_code=500, detail=GTFS["error"])

    if trip_id not in GTFS["trips"]:
        raise HTTPException(status_code=404, detail="Trip not found")

    trip = GTFS["trips"][trip_id]
    meta = GTFS["trip_meta"].get(trip_id, {})
    route = route_for_trip(trip_id)

    if serviceDate:
        try:
            service_date = date.fromisoformat(serviceDate)
        except Exception:
            service_date = today_local()
    else:
        service_date = today_local()

        if not service_active(trip["service_id"], service_date):
            yesterday = today_local() - timedelta(days=1)
            tomorrow = today_local() + timedelta(days=1)

            if service_active(trip["service_id"], yesterday):
                service_date = yesterday
            elif service_active(trip["service_id"], tomorrow):
                service_date = tomorrow

    vehicles_all, _ = get_live_all_cached()
    live_vehicles = filter_live_vehicles(
        vehicles_all,
        line=route.get("short_name") or meta.get("line"),
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    match_cache: Dict[str, Any] = {}
    live_vehicle = None

    if vehicleRef:
        for v in live_vehicles:
            if str(v.get("vehicleRef")) == str(vehicleRef) or str(v.get("vehicleUniqueId")) == str(vehicleRef):
                live_vehicle = v
                live_vehicle["_matchScore"] = 99
                live_vehicle["_matchReason"] = "vehicle_ref"
                break

    if live_vehicle is None:
        live_vehicle = match_live_for_trip(trip_id, service_date, live_vehicles, match_cache)

    nl = now_local()
    stops_out = []

    any_live = False
    delay_values = []
    next_stop_name = None

    for st in GTFS["stop_times_by_trip"].get(trip_id, []):
        stop = GTFS["stops"].get(st["stop_id"], {})
        sched_dt = departure_datetime(service_date, st["departure_s"])

        live_info = live_info_for_trip_stop(
            trip_id,
            service_date,
            st,
            live_vehicle,
        )

        is_live = bool(live_info.get("isLive"))
        expected_dt = live_info.get("expected_dt") if is_live else None
        display_dt = expected_dt or sched_dt

        minutes_until = int(round((display_dt - nl).total_seconds() / 60)) if display_dt else None

        if is_live:
            any_live = True

        if live_info.get("delayMinutes") is not None:
            delay_values.append(live_info.get("delayMinutes"))

        if next_stop_name is None and minutes_until is not None and minutes_until >= 0:
            next_stop_name = stop.get("stop_name")

        stops_out.append({
            "stop_id": st["stop_id"],
            "stopId": st["stop_id"],
            "stop_name": stop.get("stop_name") or st["stop_id"],
            "stopName": stop.get("stop_name") or st["stop_id"],
            "stop_sequence": st["stop_sequence"],
            "stopSequence": st["stop_sequence"],

            "scheduledTime": sched_dt.isoformat(),
            "aimedTime": sched_dt.isoformat(),
            "scheduledClock": hhmm_dt(sched_dt),
            "aimedClock": hhmm_dt(sched_dt),

            "expectedTime": iso_or_none(expected_dt),
            "displayTime": iso_or_none(display_dt),
            "time": hhmm_dt(display_dt),
            "displayClock": hhmm_dt(display_dt),

            "isLive": is_live,
            "is_live": is_live,
            "live": is_live,
            "source": "LIVE" if is_live else "GTFS",

            "delayMinutes": live_info.get("delayMinutes"),
            "delay": live_info.get("delayMinutes"),

            "minutesUntil": minutes_until,
            "minutes": minutes_until,

            "vehicleAtStop": bool(live_info.get("vehicleAtStop")),
            "atStop": bool(live_info.get("vehicleAtStop")),
            "passed": bool(live_info.get("passed")),
        })

    if delay_values:
        delay_minutes = int(round(delay_values[-1]))
    else:
        delay_minutes = None

    line = route.get("short_name") or meta.get("line") or "?"
    destination = trip.get("headsign") or meta.get("headsign") or meta.get("last_stop_name") or ""

    return {
        "trip_id": trip_id,
        "tripId": trip_id,
        "serviceDate": service_date.isoformat(),

        "route": route,
        "route_id": route.get("route_id") or trip.get("route_id"),
        "routeId": route.get("route_id") or trip.get("route_id"),

        "line": line,
        "routeShortName": line,
        "route_short_name": line,

        "destination": destination,
        "destinationName": destination,
        "headsign": destination,

        "isLive": bool(any_live),
        "live": bool(any_live),
        "source": "LIVE" if any_live else "GTFS",
        "statusText": "Élő adat" if any_live else "Menetrendi adat",

        "delayMinutes": delay_minutes,
        "delay": delay_minutes,

        "vehicleRef": live_vehicle.get("vehicleRef") if live_vehicle else None,
        "vehicleUniqueId": live_vehicle.get("vehicleUniqueId") if live_vehicle else None,

        "currentStopName": live_vehicle.get("currentStopName") if live_vehicle else None,
        "nextStopName": next_stop_name or (live_vehicle.get("currentStopName") if live_vehicle else None),

        "liveMatchScore": live_vehicle.get("_matchScore") if live_vehicle else None,
        "liveMatchReason": live_vehicle.get("_matchReason") if live_vehicle else None,

        "stops": stops_out,
        "items": stops_out,
    }


# ============================================================
# ROUTES / LINES
# ============================================================

@app.get("/api/route/{line}/directions")
def api_route_directions(line: str):
    if not GTFS["ok"]:
        raise HTTPException(status_code=500, detail=GTFS["error"])

    routes = routes_for_line(line)
    route_ids = {r["route_id"] for r in routes}

    if not routes:
        raise HTTPException(status_code=404, detail="Route not found")

    candidates: Dict[str, Dict[str, Any]] = {}

    for tid, trip in GTFS["trips"].items():
        if trip["route_id"] not in route_ids:
            continue

        meta = GTFS["trip_meta"].get(tid, {})
        direction_id = trip.get("direction_id") or ""
        headsign = trip.get("headsign") or meta.get("headsign") or ""
        key = f"{direction_id}::{clean_name(headsign)}"

        old = candidates.get(key)

        if old is None or meta.get("stop_count", 0) > old.get("stop_count", 0):
            times = GTFS["stop_times_by_trip"].get(tid, [])
            stops = []

            for st in times:
                stop = GTFS["stops"].get(st["stop_id"], {})
                stops.append({
                    "stop_id": st["stop_id"],
                    "stopId": st["stop_id"],
                    "stop_name": stop.get("stop_name") or st["stop_id"],
                    "stopName": stop.get("stop_name") or st["stop_id"],
                    "stop_lat": stop.get("stop_lat"),
                    "stop_lon": stop.get("stop_lon"),
                    "stop_sequence": st["stop_sequence"],
                    "stopSequence": st["stop_sequence"],
                    "time": seconds_to_hhmm(st["departure_s"]),
                })

            candidates[key] = {
                "trip_id": tid,
                "tripId": tid,
                "route_id": trip["route_id"],
                "routeId": trip["route_id"],
                "direction_id": direction_id,
                "directionId": direction_id,
                "headsign": headsign,
                "destination": headsign,
                "from": {
                    "stop_id": meta.get("first_stop_id"),
                    "stop_name": meta.get("first_stop_name"),
                },
                "to": {
                    "stop_id": meta.get("last_stop_id"),
                    "stop_name": meta.get("last_stop_name"),
                },
                "stop_count": meta.get("stop_count", 0),
                "stopCount": meta.get("stop_count", 0),
                "stops": stops,
            }

    directions = list(candidates.values())
    directions.sort(key=lambda x: (x.get("direction_id") or "", x.get("headsign") or ""))

    return {
        "line": line,
        "routes": routes,
        "directions": directions,
    }


@app.get("/api/line/{line}/directions")
def api_line_directions_alias(line: str):
    return api_route_directions(line)


@app.get("/api/route/{line}/vehicles")
def api_route_vehicles(line: str):
    vehicles_all, raw_count = get_live_all_cached()

    active = filter_live_vehicles(
        vehicles_all,
        line=line,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    return {
        "line": line,
        "rawCount": raw_count,
        "activeCount": len(active),
        "vehicles": [public_vehicle(v) for v in active],
    }


@app.get("/api/line/{line}/vehicles")
def api_line_vehicles_alias(line: str):
    return api_route_vehicles(line)


@app.get("/api/route/{line}/shape")
def api_route_shape(line: str):
    routes = routes_for_line(line)
    route_ids = {r["route_id"] for r in routes}

    shapes_out = []

    for tid, trip in GTFS["trips"].items():
        if trip["route_id"] not in route_ids:
            continue

        shape_id = trip.get("shape_id")

        if shape_id and shape_id in GTFS["shapes"]:
            shapes_out.append({
                "trip_id": tid,
                "tripId": tid,
                "shape_id": shape_id,
                "shapeId": shape_id,
                "points": GTFS["shapes"][shape_id],
            })

        if len(shapes_out) >= 6:
            break

    return {
        "line": line,
        "shapes": shapes_out,
    }


# ============================================================
# LIVE / MAP
# ============================================================

@app.get("/api/live")
def api_live(
    line: Optional[str] = None,
    fresh: bool = True,
):
    vehicles_all, raw_count = get_live_all_cached()

    active = filter_live_vehicles(
        vehicles_all,
        line=line,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=fresh,
    )

    return {
        "vehicles": [public_vehicle(v) for v in active],
        "activeCount": len(active),
        "rawCount": raw_count,
        "line": line,
        "note": "" if active else "No live data for this filter.",
    }


@app.get("/api/vehicles")
def api_vehicles(
    line: Optional[str] = None,
    fresh: bool = True,
):
    return api_live(line=line, fresh=fresh)


@app.get("/api/map/vehicles")
def api_map_vehicles(
    line: Optional[str] = None,
):
    return api_live(line=line, fresh=True)


# ============================================================
# SINGLE STOP INFO
# ============================================================

@app.get("/api/stop/{stop_id}")
def api_stop(stop_id: str):
    stop = GTFS["stops"].get(stop_id)

    if not stop:
        raise HTTPException(status_code=404, detail="Stop not found")

    return {
        "stop": stop,
        "stop_id": stop_id,
        "stopId": stop_id,
        "stop_name": stop.get("stop_name"),
        "stopName": stop.get("stop_name"),
    }


# ============================================================
# DEBUG LIVE MATCHING
# ============================================================

@app.get("/api/debug/stop/{stop_id}/live-match")
def api_debug_stop_live_match(stop_id: str, minutes: int = 90):
    rows = scheduled_departures_for_stop(stop_id, minutes)
    enriched = enrich_departures_with_live(rows)

    return {
        "stop_id": stop_id,
        "departures": enriched[:40],
    }


@app.get("/api/debug/live")
def api_debug_live(line: Optional[str] = None):
    vehicles_all, raw_count = get_live_all_cached()
    active = filter_live_vehicles(
        vehicles_all,
        line=line,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    return {
        "effectiveFeedUrl": DFT_FEED_URL,
        "keyPresent": bool(DFT_API_KEY or "api_key=" in DFT_FEED_URL),
        "rawCount": raw_count,
        "activeCount": len(active),
        "lastError": LIVE_CACHE.get("error"),
        "lastHttpStatus": LIVE_CACHE.get("last_http_status"),
        "lastFetchTime": LIVE_CACHE.get("last_fetch_time"),
        "sample": [public_vehicle(v) for v in active[:10]],
    }


# ============================================================
# FALLBACK
# ============================================================

@app.exception_handler(Exception)
async def generic_exception_handler(request, exc):
    return JSONResponse(
        status_code=500,
        content={
            "error": str(exc),
            "path": str(request.url.path),
        },
    )
