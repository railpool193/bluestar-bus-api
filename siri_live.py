# siri_live.py
import os
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import requests
import xmltodict

# --- Beállítások környezetből ----------------------------------------------------

BODS_FEED_ID = os.getenv("BODS_FEED_ID", "").strip()
BODS_API_KEY = os.getenv("BODS_API_KEY", "").strip()
BODS_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{BODS_FEED_ID}/"

# --- Egyszerű cache, hogy ne verjük a BODS-ot minden kérésnél ---------------------

_cache: Dict[str, Any] = {"t": 0.0, "data": None}
CACHE_TTL = 15  # másodperc

class LiveDataError(Exception):
    pass

# --- Kis segédek ------------------------------------------------------------------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _parse_iso(ts: str) -> Optional[datetime]:
    """
    ISO8601 -> datetime (UTC). BODS 'Z' végű időket is kezeli.
    """
    if not ts:
        return None
    try:
        # sokszor '2025-08-16T12:34:56Z' formában jön
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def _fmt_hhmm(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%H:%M")

def _fetch_xml_text() -> str:
    if not BODS_API_KEY:
        raise LiveDataError("Hiányzik a BODS_API_KEY környezeti változó.")
    if not BODS_FEED_ID:
        raise LiveDataError("Hiányzik a BODS_FEED_ID környezeti változó.")

    params = {"api_key": BODS_API_KEY}
    resp = requests.get(BODS_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.text

def _get_cached_doc() -> Dict[str, Any]:
    """
    Cache-elt XML → dict (xmltodict). 15 mp-ig reuse.
    """
    now = time.time()
    if _cache["data"] is not None and now - _cache["t"] < CACHE_TTL:
        return _cache["data"]

    xml_text = _fetch_xml_text()
    as_dict = xmltodict.parse(xml_text, force_list=("VehicleActivity", "OnwardCall"))
    _cache["data"] = as_dict
    _cache["t"] = now
    return as_dict

# --- SIRI kibontás ----------------------------------------------------------------

def _iter_vehicle_activities(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Kinyeri a VehicleActivity listát a SIRI keltezésű dict-ből.
    """
    try:
        svc = doc["Siri"]["ServiceDelivery"]
        va_list = svc["VehicleMonitoringDelivery"]["VehicleActivity"]
        # force_list már megtette, de biztos ami biztos:
        if isinstance(va_list, dict):
            va_list = [va_list]
        return va_list
    except Exception:
        return []

def _pick_time(call: Dict[str, Any]) -> (Optional[datetime], bool):
    """
    Visszaadja a legjobb időt és hogy élő-e.
    Sorrend:
      1) ExpectedDepartureTime
      2) ExpectedArrivalTime
      3) AimedDepartureTime
      4) AimedArrivalTime
    Élőnek akkor tekintjük, ha 'Expected*' mezőt használtunk.
    """
    expected_keys = ("ExpectedDepartureTime", "ExpectedArrivalTime")
    aimed_keys = ("AimedDepartureTime", "AimedArrivalTime")

    for k in expected_keys:
        dt = _parse_iso(call.get(k))
        if dt:
            return dt, True
    for k in aimed_keys:
        dt = _parse_iso(call.get(k))
        if dt:
            return dt, False
    return None, False

def _collect_matches_for_stop(va: Dict[str, Any], stop_id: str) -> List[Dict[str, Any]]:
    """
    Megnézi a VehicleActivity-n belül a MonitoredCall-t és OnwardCalls-t is,
    és visszaad minden olyan hívást, ahol StopPointRef == stop_id.
    """
    results: List[Dict[str, Any]] = []

    mvj = va.get("MonitoredVehicleJourney", {}) or {}
    # 1) MonitoredCall (épp aktuális / következő hívás)
    mc = mvj.get("MonitoredCall") or {}
    if (mc.get("StopPointRef") or "").strip() == stop_id:
        results.append(mc)

    # 2) OnwardCalls (következő megállók)
    onward = mvj.get("OnwardCalls") or {}
    onward_calls = onward.get("OnwardCall") or []
    if isinstance(onward_calls, dict):
        onward_calls = [onward_calls]
    for oc in onward_calls:
        if (oc.get("StopPointRef") or "").strip() == stop_id:
            results.append(oc)

    return results

# --- Nyilvános függvény: következő indulások --------------------------------------

def get_next_departures(stop_id: str, minutes: int = 60) -> List[Dict[str, Any]]:
    """
    Visszaadja a következő indulásokat a megadott megállóból a következő N percben.

    Visszatérési formátum (példa):
    [
      {
        "route": "18",
        "destination": "Thornhill",
        "departure_time": "12:34",
        "is_live": true
      },
      ...
    ]
    """
    # Biztonság kedvéért:
    minutes = max(1, min(180, int(minutes)))

    doc = _get_cached_doc()
    va_list = _iter_vehicle_activities(doc)
    if not va_list:
        return []

    now = _now_utc()
    horizon = now + timedelta(minutes=minutes)

    departures: List[Dict[str, Any]] = []

    for va in va_list:
        mvj = va.get("MonitoredVehicleJourney", {}) or {}
        line_ref = (mvj.get("LineRef") or "").strip()
        dest = (mvj.get("DestinationName") or "").strip()

        # Nézzük végig, hogy ez a busz mikor érinti a kívánt megállót
        calls = _collect_matches_for_stop(va, stop_id)
        for call in calls:
            when, is_live = _pick_time(call)
            if not when:
                continue

            if when < now or when > horizon:
                continue

            departures.append(
                {
                    "route": line_ref,
                    "destination": dest,
                    "departure_time": _fmt_hhmm(when),
                    "is_live": bool(is_live),
                }
            )

    # idő szerint növekvő
    departures.sort(key=lambda x: x["departure_time"])
    return departures
