# siri_live.py

import os
import time
from typing import List, Dict, Any, Optional
import requests
import xmltodict
from datetime import datetime, timezone

BODS_FEED_ID = os.getenv("BODS_FEED_ID", "")
BODS_API_KEY = os.getenv("BODS_API_KEY", "")
BODS_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{BODS_FEED_ID}/?api_key={BODS_API_KEY}"

# pici cache, hogy ne spameljük a feedet
_cache: Dict[str, Any] = {"t": 0, "data": None}
CACHE_TTL = 15  # mp

class LiveDataError(Exception):
    pass

def _fetch_xml() -> str:
    if not BODS_API_KEY:
        raise LiveDataError("Hiányzik a BODS_API_KEY")
    if not BODS_FEED_ID:
        raise LiveDataError("Hiányzik a BODS_FEED_ID")

    now = time.time()
    if _cache["data"] is not None and now - _cache["t"] < CACHE_TTL:
        return _cache["data"]

    resp = requests.get(BODS_URL, timeout=20)
    resp.raise_for_status()
    xml = resp.text
    _cache["data"] = xml
    _cache["t"] = now
    return xml

def _safe_get(path: List[str], src: Optional[dict]) -> Optional[Any]:
    node = src
    for p in path:
        if not isinstance(node, dict) or p not in node:
            return None
        node = node[p]
    return node

def _to_iso_uk(dt_str: str) -> Optional[str]:
    """
    SIRI-ben lévő ISO időket ([...]+00:00 vagy +01:00) visszaadjuk ISO formában.
    """
    try:
        # A SIRI-ben ISO 8601 jön, parse-oljuk és ISO-ként adjuk vissza
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        # hagyjuk meg az eredeti tz-t (UTC/BST) – ez fontos az összehasonlításhoz
        return dt.isoformat()
    except Exception:
        return None

def _extract_time(mvc: dict) -> Optional[Dict[str, Any]]:
    """
    Visszaad egy dict-et:
      {
        "iso": "...",            # ISO datetime string
        "live": True/False,      # True ha Expected, False ha csak Aimed
      }
    """
    exp = _safe_get(["ExpectedDepartureTime"], mvc)
    if exp:
        iso = _to_iso_uk(exp)
        if iso:
            return {"iso": iso, "live": True}

    # nincs Expected → próbáljuk az Aimed-et
    aimed = _safe_get(["AimedDepartureTime"], mvc)
    if aimed:
        iso = _to_iso_uk(aimed)
        if iso:
            return {"iso": iso, "live": False}

    # ha egyik sincs, próbáljuk az érkezést
    exp_arr = _safe_get(["ExpectedArrivalTime"], mvc)
    if exp_arr:
        iso = _to_iso_uk(exp_arr)
        if iso:
            return {"iso": iso, "live": True}
    aimed_arr = _safe_get(["AimedArrivalTime"], mvc)
    if aimed_arr:
        iso = _to_iso_uk(aimed_arr)
        if iso:
            return {"iso": iso, "live": False}

    return None

def get_next_departures(stop_id: str, minutes: int = 60) -> Dict[str, Any]:
    """
    Visszaadja a következő indulásokat az adott stop_id-hez. 
    Ha van Expected -> live=True, ha csak Aimed -> live=False.
    """
    xml = _fetch_xml()
    data = xmltodict.parse(xml)

    vas = _safe_get(["Siri", "ServiceDelivery", "VehicleMonitoringDelivery"], data)
    if not vas:
        # néha lista, néha dict – normalizáljuk
        vmd = _safe_get(["Siri", "ServiceDelivery", "VehicleMonitoringDelivery"], data) or {}
        vas = vmd

    # Normalizálás: vmd lehet lista vagy dict
    if isinstance(vas, dict):
        va_list = vas.get("VehicleActivity", [])
    else:
        # több delivery → összefűzzük
        va_list = []
        for vmd in vas:
            part = vmd.get("VehicleActivity", [])
            if isinstance(part, list):
                va_list.extend(part)
            elif isinstance(part, dict):
                va_list.append(part)

    if not isinstance(va_list, list):
        va_list = [va_list] if va_list else []

    out: List[Dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for va in va_list:
        mvj = _safe_get(["MonitoredVehicleJourney"], va) or {}
        mc  = _safe_get(["MonitoredCall"], mvj) or {}
        sp  = _safe_get(["StopPointRef"], mc) or _safe_get(["StopPointRef"], mvj)

        if not sp or sp != stop_id:
            continue

        tinfo = _extract_time(mc)
        if not tinfo:
            continue

        # időablak-szűrés – mindegy, hogy Expected vagy Aimed
        try:
            dep_dt = datetime.fromisoformat(tinfo["iso"])
        except Exception:
            continue

        # ha naive jött (nem kéne), tegyük UTC-nek
        if dep_dt.tzinfo is None:
            dep_dt = dep_dt.replace(tzinfo=timezone.utc)

        diff_min = (dep_dt - now).total_seconds() / 60.0
        if diff_min < 0 or diff_min > minutes:
            continue

        route = _safe_get(["LineRef"], mvj) or ""
        dest  = _safe_get(["DestinationName"], mvj) or _safe_get(["DestinationRef"], mvj) or ""

        out.append({
            "route": str(route),
            "destination": str(dest),
            "departure_time": dep_dt.strftime("%H:%M"),
            "iso": tinfo["iso"],
            "live": bool(tinfo["live"]),
        })

    # időrendezés
    out.sort(key=lambda x: x["iso"])

    return {
        "stop_id": stop_id,
        "minutes": minutes,
        "departures": out
    }
