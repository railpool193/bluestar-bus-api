from __future__ import annotations
import os, asyncio, aiohttp, json, time
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timezone
from dateutil import parser as dparser

BODS_FEED_URL = os.getenv("BODS_FEED_URL") or os.getenv("SIRI_API_URL")
ALLOWED_OPERATORS = {s.strip() for s in os.getenv("ALLOWED_OPERATORS", "Bluestar,Unilink,uni-link,UNILINK").split(",")}
# Unilink vonalak: U1..U9, Nightrider variánsok; Bluestar: 1.., 17, 18, 19 stb.
ALLOWED_LINE_NAMES = set(
    ["U1","U1A","U1E","U2","U2B","U6","U6H","U9","U9C",
     "1","2","3","4","7","9","12","17","18","19","PR1","QuayConnect","QC"]
)

_cache: Dict[str, Any] = {"when": 0, "vehicles": []}
CACHE_SECONDS = 10

def _get(d: dict, *path, default=None):
    cur = d
    for p in path:
        if cur is None:
            return default
        cur = cur.get(p)
    return cur if cur is not None else default

async def fetch_siri_raw() -> dict:
    if not BODS_FEED_URL:
        return {}
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as s:
        async with s.get(BODS_FEED_URL) as r:
            r.raise_for_status()
            txt = await r.text()
            try:
                return json.loads(txt)
            except:
                # néhány feed XML-t ad – itt csak JSON támogatott
                return {}

def _simplify_vehicle(m: dict) -> Optional[dict]:
    mvj = _get(m, "MonitoredVehicleJourney", default={})
    op_name = _get(mvj, "OperatorRef", default="").strip()
    line = (_get(mvj, "PublishedLineName", default="")
            or _get(mvj, "LineRef", default="")).strip()
    if not line and not op_name:
        return None

    # operátor és vonal szűrés
    allow = (any(x.lower() in (op_name or "").lower() for x in [o.lower() for o in ALLOWED_OPERATORS])
             or line in ALLOWED_LINE_NAMES)
    if not allow:
        return None

    fr = _get(mvj, "FramedVehicleJourneyRef", default={})
    veh = {
        "vehicle_ref": _get(mvj, "VehicleRef", default=""),
        "line_name": line,
        "operator": op_name,
        "destination": _get(mvj, "DestinationName", default=""),
        "lat": float(_get(mvj, "VehicleLocation", "Latitude", default=0) or 0),
        "lon": float(_get(mvj, "VehicleLocation", "Longitude", default=0) or 0),
        "bearing": _get(mvj, "Bearing", default=None),
        "dated_vehicle_journey_ref": _get(fr, "DatedVehicleJourneyRef", default=""),
        "data_frame_ref": _get(fr, "DataFrameRef", default=""),
        "origin_aimed": _get(mvj, "OriginAimedDepartureTime", default=""),
        "last_update": _get(m, "RecordedAtTime", default=""),
        "delay_sec": _get(mvj, "Delay", default=None),
        "block_ref": _get(mvj, "BlockRef", default=""),
        "vehicle_journey_name": _get(mvj, "VehicleJourneyName", default=""),
        "journey_note": _get(mvj, "JourneyNote", default=""),
        "extra": {}
    }

    # flottaszám (több feed a VehicleRef-ben vagy JourneyNote-ban tartja)
    note = (veh["vehicle_journey_name"] or "") + " " + (veh["journey_note"] or "")
    fleet = None
    for token in [veh["vehicle_ref"], note]:
        if not token: 
            continue
        # legegyszerűbb: 3-5 számjegy egymás után
        import re
        m2 = re.search(r"\b(\d{3,5})\b", str(token))
        if m2:
            fleet = m2.group(1)
            break
    veh["fleet"] = fleet
    return veh

def _best_time_parse(s: str) -> Optional[datetime]:
    if not s: return None
    try:
        dt = dparser.isoparse(s)
        if not dt.tzinfo:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None

async def vehicles_live(force=False) -> List[dict]:
    now = time.time()
    if not force and (now - _cache["when"] < CACHE_SECONDS) and _cache["vehicles"]:
        return _cache["vehicles"]
    raw = await fetch_siri_raw()
    vs: List[dict] = []
    if not raw:
        _cache["when"] = now
        _cache["vehicles"] = []
        return []

    deliveries = raw.get("Siri", {}).get("ServiceDelivery", {}).get("VehicleMonitoringDelivery", [])
    for d in deliveries:
        mvms = d.get("VehicleActivity", [])
        for item in mvms:
            v = _simplify_vehicle(item)
            if v: 
                # időparszolás
                v["last_update_dt"] = _best_time_parse(v["last_update"])
                v["origin_aimed_dt"] = _best_time_parse(v["origin_aimed"])
                vs.append(v)

    # duplikátumok kiszűrése vehicle_ref + journey_ref alapján
    uniq = {}
    for v in vs:
        k = (v["vehicle_ref"], v["dated_vehicle_journey_ref"] or v["line_name"])
        if k not in uniq:
            uniq[k] = v
        else:
            # tartsuk meg az újabbat
            a, b = uniq[k], v
            if (b["last_update_dt"] or datetime.min) > (a["last_update_dt"] or datetime.min):
                uniq[k] = b
    res = list(uniq.values())
    _cache["vehicles"] = res
    _cache["when"] = now
    return res

def select_vehicle_for_trip(trip_key: str, line_name: str, candidates: List[dict]) -> Optional[dict]:
    """
    Trip nézet – megtaláljuk a bejelentkezett járművet.
    1) Pontos egyezés: DatedVehicleJourneyRef tartalmazza a trip_id-t
    2) Közelítő egyezés: PublishedLineName == route_short_name + időközelség az origin aimed time-hoz
    3) Ha több találat: legfrissebb last_update
    """
    trip_key_low = (trip_key or "").lower()
    filtered = []

    for v in candidates:
        # 1) közvetlen ref egyezés
        dvj = (v.get("dated_vehicle_journey_ref") or "").lower()
        if trip_key_low and trip_key_low in dvj:
            filtered = [v]; break

    if not filtered:
        # 2) vonalnév + idő
        approx = [v for v in candidates if v.get("line_name") == line_name]
        if approx:
            # a legfrissebb/legvalószínűbb
            approx.sort(key=lambda x: (x.get("last_update_dt") or datetime.min), reverse=True)
            filtered = [approx[0]]

    return filtered[0] if filtered else None
