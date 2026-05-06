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
from typing import Dict, Any, List, Optional, Tuple, Set

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import FileResponse


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


# ============================================================
# APP
# ============================================================

app = FastAPI(title="Bluestar / Unilink GTFS + SIRI-VM")


# ============================================================
# CACHES
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
}


# ============================================================
# SMALL HELPERS
# ============================================================

def now_local() -> datetime:
    return datetime.now(TZ)


def parse_gtfs_date(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def parse_hms_to_seconds(s: str) -> int:
    h, m, sec = s.split(":")
    return int(h) * 3600 + int(m) * 60 + int(sec)


def seconds_to_hhmm(seconds: int) -> str:
    seconds = seconds % (24 * 3600)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    return f"{h:02d}:{m:02d}"


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
    if len(s) != 10:
        return False
    return s[4] == "-" and s[7] == "-"


def norm_line(s: Optional[str]) -> str:
    return (s or "").strip().lower().replace(" ", "")


def norm_operator(s: Optional[str]) -> str:
    return (s or "").strip().upper()


def departure_datetime(service_date: date, dep_seconds: int) -> datetime:
    base = datetime(service_date.year, service_date.month, service_date.day, tzinfo=TZ)
    return base + timedelta(seconds=dep_seconds)


def to_dt_iso(iso: str) -> datetime:
    return datetime.fromisoformat(iso).astimezone(TZ)


def decode_feed_bytes(content: bytes) -> bytes:
    if not content:
        return b""

    # ZIP
    if content[:2] == b"PK":
        with zipfile.ZipFile(io.BytesIO(content), "r") as zf:
            names = zf.namelist()
            if not names:
                return b""
            return zf.read(names[0])

    # GZIP
    if content[:2] == b"\x1f\x8b":
        return gzip.decompress(content)

    return content


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
# GTFS LOADER
# ============================================================

def _read_gtfs_csv_from_zip(zf: zipfile.ZipFile, name: str) -> List[Dict[str, str]]:
    with zf.open(name) as f:
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
        GTFS["error"] = f"GTFS not found. Tried zip={GTFS_ZIP_PATH} and dir={GTFS_DIR}"
        return

    try:
        if source_kind == "zip":
            zf_obj = zipfile.ZipFile(source_value, "r")
            names = set(zf_obj.namelist())

            def read_csv(name: str) -> List[Dict[str, str]]:
                return _read_gtfs_csv_from_zip(zf_obj, name)

            def has_file(name: str) -> bool:
                return name in names

            close_after = zf_obj

        else:
            def read_csv(name: str) -> List[Dict[str, str]]:
                return _read_gtfs_csv_from_dir(source_value, name)

            def has_file(name: str) -> bool:
                return os.path.exists(os.path.join(source_value, name))

            close_after = None

        try:
            agency_rows = read_csv("agency.txt") if has_file("agency.txt") else []
            stops_rows = read_csv("stops.txt")
            routes_rows = read_csv("routes.txt")
            trips_rows = read_csv("trips.txt")
            stop_times_rows = read_csv("stop_times.txt")

            agency: Dict[str, Dict[str, Any]] = {}
            for r in agency_rows:
                aid = r.get("agency_id") or "default"
                agency[aid] = {
                    "agency_id": aid,
                    "agency_name": r.get("agency_name") or "",
                    "agency_url": r.get("agency_url") or "",
                    "agency_timezone": r.get("agency_timezone") or "",
                }

            calendar: Dict[str, Any] = {}
            calendar_dates: Dict[str, Dict[date, int]] = {}

            if has_file("calendar.txt"):
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

            if has_file("calendar_dates.txt"):
                for r in read_csv("calendar_dates.txt"):
                    sid = r["service_id"]
                    d = parse_gtfs_date(r["date"])
                    ex = int(r["exception_type"])
                    calendar_dates.setdefault(sid, {})[d] = ex

            stops: Dict[str, Dict[str, Any]] = {}
            for r in stops_rows:
                sid = r["stop_id"]
                stops[sid] = {
                    "stop_id": sid,
                    "stop_name": r.get("stop_name") or sid,
                    "stop_code": r.get("stop_code") or "",
                    "stop_lat": float(r["stop_lat"]) if r.get("stop_lat") else None,
                    "stop_lon": float(r["stop_lon"]) if r.get("stop_lon") else None,
                }

            routes: Dict[str, Dict[str, Any]] = {}
            for r in routes_rows:
                rid = r["route_id"]
                routes[rid] = {
                    "route_id": rid,
                    "agency_id": r.get("agency_id") or "",
                    "short_name": (r.get("route_short_name") or "").strip(),
                    "long_name": (r.get("route_long_name") or "").strip(),
                    "route_type": r.get("route_type") or "",
                    "route_color": r.get("route_color") or "",
                    "route_text_color": r.get("route_text_color") or "",
                }

            trips: Dict[str, Dict[str, Any]] = {}
            for r in trips_rows:
                tid = r["trip_id"]
                trips[tid] = {
                    "trip_id": tid,
                    "route_id": r["route_id"],
                    "service_id": r["service_id"],
                    "headsign": (r.get("trip_headsign") or "").strip(),
                    "direction_id": r.get("direction_id"),
                    "block_id": r.get("block_id") or "",
                    "shape_id": r.get("shape_id") or "",
                }

            stop_times_by_stop: Dict[str, List[Tuple[int, str, int]]] = {}
            stop_times_by_trip: Dict[str, List[Dict[str, Any]]] = {}

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
                    "pickup_type": r.get("pickup_type") or "",
                    "drop_off_type": r.get("drop_off_type") or "",
                })

            for sid in stop_times_by_stop:
                stop_times_by_stop[sid].sort(key=lambda x: x[0])

            for tid in stop_times_by_trip:
                stop_times_by_trip[tid].sort(key=lambda x: x["stop_sequence"])

            shapes: Dict[str, List[Dict[str, Any]]] = {}
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

                for sid in shapes:
                    shapes[sid].sort(key=lambda x: x["seq"])

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


def trip_id_candidates_from_live(v: Dict[str, Any]) -> List[str]:
    refs = []

    dfr = v.get("dataFrameRef") or now_local().date().isoformat()
    dvjr = v.get("datedVehicleJourneyRef") or ""
    line = v.get("publishedLineName") or v.get("lineRef") or ""

    if dvjr:
        refs.append(dvjr)
        refs.append(f"BLUS:{dfr}:{dvjr}")
        refs.append(f"{dfr}:{dvjr}")

        if line:
            refs.append(f"BLUS:{dfr}:{line}:{dvjr}")

    return refs


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

            block_ref = pick_from_mvj("BlockRef")
            vehicle_ref = pick_from_mvj("VehicleRef")

            vehicle_unique_id = pick_from_va("VehicleUniqueId") or pick_from_mvj("VehicleUniqueId")
            ticket_machine_service_code = pick_from_va("TicketMachineServiceCode") or pick_from_mvj("TicketMachineServiceCode")
            journey_code = pick_from_va("JourneyCode") or pick_from_mvj("JourneyCode")

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

            v: Dict[str, Any] = {
                "itemIdentifier": item_identifier,
                "recordedAtTime": recorded,
                "recordedAtTimeLocal": recorded_dt.isoformat() if recorded_dt else None,
                "validUntilTime": valid_until,

                "dataFrameRef": data_frame_ref,
                "datedVehicleJourneyRef": dated_vehicle_journey_ref,

                "lineRef": line_ref,
                "publishedLineName": published_line_name,
                "operatorRef": operator_ref,
                "directionRef": direction_ref,

                "originRef": origin_ref,
                "originName": origin_name,
                "destinationRef": destination_ref,
                "destinationName": destination_name,

                "longitude": float(lon) if lon else None,
                "latitude": float(lat) if lat else None,
                "bearing": float(bearing) if bearing else None,

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

            candidates = trip_id_candidates_from_live(v)

            for ctid in candidates:
                if ctid in GTFS["trips"]:
                    v["tripId"] = ctid
                    break
            else:
                v["tripId"] = candidates[0] if candidates else None

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
    line: Optional[str],
    operator: Optional[str],
    max_age_seconds: int,
    fresh_only: bool,
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
        ref = v.get("vehicleRef") or v.get("vehicleUniqueId")

        if not ref:
            no_ref.append(v)
            continue

        old = latest.get(ref)
        if old is None:
            latest[ref] = v
        else:
            old_dt = parse_iso_dt(old.get("recordedAtTime")) or datetime.min.replace(tzinfo=TZ)
            new_dt = parse_iso_dt(v.get("recordedAtTime")) or datetime.min.replace(tzinfo=TZ)

            if new_dt > old_dt:
                latest[ref] = v

    result = list(latest.values()) + no_ref
    result.sort(key=lambda x: (x.get("publishedLineName") or x.get("lineRef") or "", x.get("vehicleRef") or ""))

    return result


# ============================================================
# STARTUP
# ============================================================

@app.on_event("startup")
def _startup():
    load_gtfs()


# ============================================================
# STATIC
# ============================================================

@app.get("/", response_class=FileResponse)
def index():
    return FileResponse("index.html")


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
# DEBUG / STATUS
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
                "stops": len(GTFS["stops"]),
                "routes": len(GTFS["routes"]),
                "trips": len(GTFS["trips"]),
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


@app.get("/api/gtfs-debug")
def api_gtfs_debug():
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


@app.get("/api/live-debug")
def api_live_debug():
    vehicles_all, _ = get_live_all_cached()

    active = filter_live_vehicles(
        vehicles_all,
        line=None,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    return {
        "effectiveFeedUrl": DFT_FEED_URL,
        "keyPreview": (DFT_API_KEY[:4] + "..." + DFT_API_KEY[-4:]) if DFT_API_KEY else "",
        "keyPresent": bool(DFT_API_KEY) or ("api_key=" in DFT_FEED_URL),
        "vehicleCount": len(active),
        "rawCount": len(vehicles_all),
        "lastError": LIVE_CACHE["error"] or "",
        "lastHttpStatus": LIVE_CACHE.get("last_http_status"),
        "lastFetchTime": LIVE_CACHE.get("last_fetch_time"),
        "sample": [public_vehicle(v) for v in active[:8]],
    }


# ============================================================
# SEARCH
# ============================================================

@app.get("/api/search")
def api_search(q: str = Query("", min_length=0, max_length=64)):
    qn = (q or "").strip().lower()

    stops = []
    routes = []

    if qn and GTFS["ok"]:
        for s in GTFS["stops"].values():
            name = (s.get("stop_name") or "").lower()
            sid = (s.get("stop_id") or "").lower()
            code = (s.get("stop_code") or "").lower()

            if qn in name or qn in sid or qn in code:
                stops.append({
                    "stop_id": s["stop_id"],
                    "stop_name": s["stop_name"],
                    "stop_code": s.get("stop_code") or "",
                    "stop_lat": s.get("stop_lat"),
                    "stop_lon": s.get("stop_lon"),
                })

                if len(stops) >= 40:
                    break

        for r in GTFS["routes"].values():
            sn = (r.get("short_name") or "").lower()
            ln = (r.get("long_name") or "").lower()
            rid = (r.get("route_id") or "").lower()

            if qn in sn or qn in ln or qn in rid:
                routes.append({
                    "route_id": r["route_id"],
                    "short_name": r.get("short_name") or "",
                    "long_name": r.get("long_name") or "",
                    "route_color": r.get("route_color") or "",
                    "route_text_color": r.get("route_text_color") or "",
                })

                if len(routes) >= 40:
                    break

    return {
        "stops": stops,
        "routes": routes,
    }


# ============================================================
# ROUTE / VISZONYLAT
# ============================================================

def build_route_directions(line: str) -> List[Dict[str, Any]]:
    matching_routes = routes_for_line(line)

    if not matching_routes:
        return []

    route_ids = {r["route_id"] for r in matching_routes}
    service_date = now_local().date()

    candidate_trips = []

    for trip in GTFS["trips"].values():
        if trip.get("route_id") not in route_ids:
            continue

        if service_active(trip["service_id"], service_date):
            candidate_trips.append(trip)

    if not candidate_trips:
        candidate_trips = [
            t for t in GTFS["trips"].values()
            if t.get("route_id") in route_ids
        ]

    groups: Dict[str, Dict[str, Any]] = {}

    for trip in candidate_trips:
        tid = trip["trip_id"]
        stop_seq = GTFS["stop_times_by_trip"].get(tid, [])

        if not stop_seq:
            continue

        direction_id = trip.get("direction_id") or ""
        headsign = trip.get("headsign") or ""

        if not headsign:
            last_stop_id = stop_seq[-1]["stop_id"]
            headsign = GTFS["stops"].get(last_stop_id, {}).get("stop_name", last_stop_id)

        key = f"{trip.get('route_id')}|{direction_id}|{headsign}"

        current = groups.get(key)

        if current is None or len(stop_seq) > len(current["raw_stops"]):
            groups[key] = {
                "trip": trip,
                "raw_stops": stop_seq,
                "direction_id": direction_id,
                "headsign": headsign,
            }

    directions = []

    for g in groups.values():
        trip = g["trip"]
        raw_stops = g["raw_stops"]

        stops = []

        for st in raw_stops:
            sid = st["stop_id"]
            stop = GTFS["stops"].get(sid, {"stop_name": sid})

            stops.append({
                "stop_id": sid,
                "stop_name": stop.get("stop_name") or sid,
                "stop_lat": stop.get("stop_lat"),
                "stop_lon": stop.get("stop_lon"),
                "stop_sequence": st.get("stop_sequence"),
            })

        first_stop = stops[0] if stops else None
        last_stop = stops[-1] if stops else None

        shape_id = trip.get("shape_id") or ""
        shape = GTFS["shapes"].get(shape_id, [])

        directions.append({
            "trip_id": trip["trip_id"],
            "route_id": trip["route_id"],
            "direction_id": g["direction_id"],
            "headsign": g["headsign"],
            "from": first_stop,
            "to": last_stop,
            "stop_count": len(stops),
            "stops": stops,
            "shape_id": shape_id,
            "shape": shape,
        })

    directions.sort(key=lambda x: (str(x.get("direction_id") or ""), x.get("headsign") or ""))

    return directions


@app.get("/api/route/{line}")
def api_route(line: str):
    if not GTFS["ok"]:
        raise HTTPException(status_code=503, detail=GTFS["error"] or "GTFS not loaded")

    matching_routes = routes_for_line(line)

    if not matching_routes:
        raise HTTPException(status_code=404, detail="Route not found")

    directions = build_route_directions(line)

    vehicles_all, _ = get_live_all_cached()

    active_vehicles = filter_live_vehicles(
        vehicles_all,
        line=line,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    return {
        "line": line,
        "routes": matching_routes,
        "directions": directions,
        "activeVehicleCount": len(active_vehicles),
        "vehicles": [public_vehicle(v) for v in active_vehicles],
    }


@app.get("/route/{line}")
def route_alias(line: str):
    return api_route(line)


@app.get("/api/route/{line}/vehicles")
def api_route_vehicles(line: str):
    vehicles_all, _ = get_live_all_cached()

    active_vehicles = filter_live_vehicles(
        vehicles_all,
        line=line,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    return {
        "line": line,
        "count": len(active_vehicles),
        "vehicles": [public_vehicle(v) for v in active_vehicles],
    }


@app.get("/api/route/{line}/map")
def api_route_map(line: str):
    if not GTFS["ok"]:
        raise HTTPException(status_code=503, detail=GTFS["error"] or "GTFS not loaded")

    directions = build_route_directions(line)

    vehicles_all, _ = get_live_all_cached()

    active_vehicles = filter_live_vehicles(
        vehicles_all,
        line=line,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    return {
        "line": line,
        "directions": directions,
        "vehicles": [public_vehicle(v) for v in active_vehicles],
        "vehicleCount": len(active_vehicles),
    }


# ============================================================
# VEHICLES / MAP
# ============================================================

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

    return {
        "vehicles": [public_vehicle(v) for v in active],
        "count": len(active),
        "maxAgeSeconds": max_age_seconds,
        "operatorFilter": [operator] if operator else DEFAULT_OPERATOR_REFS,
    }


@app.get("/vehicles")
def vehicles_alias(
    line: Optional[str] = None,
    operator: Optional[str] = None,
    max_age_seconds: int = Query(DEFAULT_MAX_AGE_SECONDS, ge=30, le=3600),
    fresh_only: bool = True,
):
    return api_vehicles(
        line=line,
        operator=operator,
        max_age_seconds=max_age_seconds,
        fresh_only=fresh_only,
    )


# ============================================================
# STOPS / DEPARTURES
# ============================================================

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
    limit: int = Query(40, ge=1, le=120),
):
    if not GTFS["ok"]:
        raise HTTPException(status_code=503, detail=GTFS["error"] or "GTFS not loaded")

    if stop_id not in GTFS["stops"]:
        raise HTTPException(status_code=404, detail="Stop not found")

    nl = now_local()
    service_date = nl.date()
    window_end = nl + timedelta(minutes=minutes)

    scheduled = []
    st_list = GTFS["stop_times_by_stop"].get(stop_id, [])

    # previous day is needed for GTFS trips after 24:00
    for sdate in [service_date - timedelta(days=1), service_date]:
        for dep_s, trip_id, seq in st_list:
            trip = GTFS["trips"].get(trip_id)

            if not trip:
                continue

            if not service_active(trip["service_id"], sdate):
                continue

            dep_dt = departure_datetime(sdate, dep_s)

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
                "stop_sequence": seq,
            })

    scheduled.sort(key=lambda x: x["scheduledTime"])
    scheduled = scheduled[:max(limit * 2, limit)]

    vehicles_all, _ = get_live_all_cached()

    active = filter_live_vehicles(
        vehicles_all,
        line=None,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    live_candidates = []

    for v in active:
        for c in v.get("calls") or []:
            if c.get("stopRef") != stop_id:
                continue

            exp = parse_iso_dt(c.get("expDep") or c.get("expArr"))
            aimed = parse_iso_dt(c.get("aimedDep") or c.get("aimedArr"))

            if not exp and bool(v.get("vehicleAtStop")) and (v.get("currentStopRef") == stop_id):
                exp = parse_iso_dt(v.get("recordedAtTime"))

            if not exp:
                continue

            if exp < nl - timedelta(minutes=2) or exp > window_end:
                continue

            live_candidates.append({
                "vehicleRef": v.get("vehicleRef"),
                "vehicleUniqueId": v.get("vehicleUniqueId"),
                "line": (v.get("publishedLineName") or v.get("lineRef") or "").strip(),
                "destination": (v.get("destinationName") or "").strip(),
                "expectedTime": exp.isoformat(),
                "aimedTime": aimed.isoformat() if aimed else None,
                "vehicleAtStop": bool(v.get("vehicleAtStop")) and (v.get("currentStopRef") == stop_id),
                "tripId": v.get("tripId"),
            })

    results = []
    used_live = set()

    for sch in scheduled:
        sch_dt = to_dt_iso(sch["scheduledTime"])
        best_i = None
        best_diff = None

        for i, lv in enumerate(live_candidates):
            if i in used_live:
                continue

            if norm_line(lv["line"]) != norm_line(sch["line"]):
                continue

            lv_dt = to_dt_iso(lv["expectedTime"])
            diff = abs((lv_dt - sch_dt).total_seconds())

            if diff <= 20 * 60:
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
                "vehicleUniqueId": lv.get("vehicleUniqueId"),
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
                "vehicleUniqueId": None,
                "vehicleAtStop": False,
            })

    for i, lv in enumerate(live_candidates):
        if i in used_live:
            continue

        trip_id = lv.get("tripId")
        if trip_id not in GTFS["trips"]:
            trip_id = None

        results.append({
            "trip_id": trip_id,
            "line": lv["line"],
            "destination": lv["destination"],
            "scheduledTime": None,
            "expectedTime": lv["expectedTime"],
            "isLive": True,
            "delayMin": None,
            "vehicleRef": lv.get("vehicleRef"),
            "vehicleUniqueId": lv.get("vehicleUniqueId"),
            "vehicleAtStop": lv.get("vehicleAtStop", False),
        })

    def sort_key(x):
        return x["expectedTime"] or x["scheduledTime"] or "9999-12-31T00:00:00+00:00"

    results.sort(key=sort_key)
    results = results[:limit]

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


@app.get("/stop/{stop_id}/departures")
def stop_departures_alias(stop_id: str, minutes: int = 120, limit: int = 40):
    return api_stop_departures(stop_id=stop_id, minutes=minutes, limit=limit)


# ============================================================
# TRIP / JÁRAT
# ============================================================

@app.get("/api/trip/{trip_id}")
def api_trip(trip_id: str):
    if not GTFS["ok"]:
        raise HTTPException(status_code=503, detail=GTFS["error"] or "GTFS not loaded")

    trip = GTFS["trips"].get(trip_id)

    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")

    route = GTFS["routes"].get(trip["route_id"], {})
    stops_seq = GTFS["stop_times_by_trip"].get(trip_id, [])

    service_date = now_local().date()

    out_stops = []

    for st in stops_seq:
        stop_id = st["stop_id"]
        stop = GTFS["stops"].get(stop_id, {"stop_name": stop_id})

        dep_dt = departure_datetime(service_date, st["departure_s"])
        arr_dt = departure_datetime(service_date, st["arrival_s"])

        out_stops.append({
            "stop_id": stop_id,
            "stop_name": stop.get("stop_name") or stop_id,
            "stop_lat": stop.get("stop_lat"),
            "stop_lon": stop.get("stop_lon"),
            "scheduledTime": dep_dt.isoformat(),
            "arrivalTime": arr_dt.isoformat(),
            "stop_sequence": st.get("stop_sequence"),
        })

    shape_id = trip.get("shape_id") or ""
    shape = GTFS["shapes"].get(shape_id, [])

    return {
        "trip_id": trip_id,
        "route_id": trip["route_id"],
        "line": (route.get("short_name") or "").strip() or (route.get("long_name") or "").strip(),
        "destination": trip.get("headsign") or "",
        "direction_id": trip.get("direction_id"),
        "block_id": trip.get("block_id"),
        "shape_id": shape_id,
        "shape": shape,
        "stops": out_stops,
    }


@app.get("/api/vehicle/{vehicle_ref}")
def api_vehicle(vehicle_ref: str):
    vehicles_all, _ = get_live_all_cached()

    active = filter_live_vehicles(
        vehicles_all,
        line=None,
        operator=None,
        max_age_seconds=DEFAULT_MAX_AGE_SECONDS,
        fresh_only=True,
    )

    for v in active:
        if (
            (v.get("vehicleRef") or "") == vehicle_ref
            or (v.get("vehicleUniqueId") or "") == vehicle_ref
        ):
            calls_map = {}
            calls_full = []

            for c in v.get("calls") or []:
                sr = c.get("stopRef")
                exp = c.get("expDep") or c.get("expArr")
                aimed = c.get("aimedDep") or c.get("aimedArr")

                if sr and exp:
                    calls_map[sr] = exp

                calls_full.append({
                    "stopRef": sr,
                    "stopName": c.get("stopName"),
                    "aimedTime": aimed,
                    "expectedTime": exp,
                    "vehicleAtStop": c.get("vehicleAtStop", False),
                    "isMonitored": c.get("isMonitored", False),
                })

            pv = public_vehicle(v)
            pv["calls"] = calls_map
            pv["callsFull"] = calls_full

            return pv

    raise HTTPException(status_code=404, detail="Vehicle not found or not fresh")


# ============================================================
# RELOAD
# ============================================================

@app.post("/api/reload-gtfs")
def api_reload_gtfs():
    load_gtfs()

    return {
        "ok": GTFS["ok"],
        "error": GTFS["error"],
        "source": GTFS["source"],
        "counts": {
            "stops": len(GTFS["stops"]),
            "routes": len(GTFS["routes"]),
            "trips": len(GTFS["trips"]),
            "shapes": len(GTFS["shapes"]),
        },
    }
