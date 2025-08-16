# siri_live.py
import os
import time
from typing import List, Dict, Any, Optional
import requests
import xmltodict

BODS_FEED_ID = os.getenv("BODS_FEED_ID", "7721")  # Go South Coast (Bluestar)
BODS_API_KEY = os.getenv("BODS_API_KEY")  # ide kell a kulcs
BODS_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{BODS_FEED_ID}/"

# nagyon egyszerű 15 mp-es cache, hogy ne pörgessük a feedet
_cache: Dict[str, Any] = {"t": 0, "data": None}
CACHE_TTL = 15

class LiveDataError(Exception):
    pass

def _fetch_xml() -> str:
    if not BODS_API_KEY:
        raise LiveDataError("Hiányzik a BODS_API_KEY környezeti változó.")
    params = {"api_key": BODS_API_KEY}
    resp = requests.get(BODS_URL, params=params, timeout=25)
    resp.raise_for_status()
    return resp.text

def _simplify_vehicle_activity(va: Dict[str, Any]) -> Dict[str, Any]:
    # A SIRI szerkezete mély – óvatosan járunk benne
    mvj = va.get("MonitoredVehicleJourney", {}) or {}

    # Biztonságos lekérés segédfüggvénnyel
    def get(path: List[str], src: Optional[Dict[str, Any]] = None):
        node = src if src is not None else mvj
        for p in path:
            if not isinstance(node, dict):
                return None
            node = node.get(p)
        return node

    # Pozíció
    loc = get(["VehicleLocation"]) or {}
    lat = None
    lon = None
    if isinstance(loc, dict):
        lat = loc.get("Latitude")
        lon = loc.get("Longitude")

    # Következő megálló és idő
    call = get(["MonitoredCall"]) or {}
    expected = call.get("ExpectedArrivalTime") or call.get("ExpectedDepartureTime")
    stop_name = call.get("StopPointName")
    stop_ref = call.get("StopPointRef")

    # Egyedi azonosító(k)
    vehicle_ref = mvj.get("VehicleRef")
    line_ref = mvj.get("LineRef")
    direction = mvj.get("DirectionRef")
    op_ref = mvj.get("OperatorRef")
    dest_name = mvj.get("DestinationName")

    # Késés (ha van)
    delay = get(["Delay"])

    return {
        "vehicle_id": vehicle_ref,
        "operator": op_ref,
        "route": line_ref,
        "direction": direction,
        "destination": dest_name,
        "latitude": float(lat) if lat not in (None, "") else None,
        "longitude": float(lon) if lon not in (None, "") else None,
        "next_stop_ref": stop_ref,
        "next_stop_name": stop_name,
        "expected_time": expected,
        "delay": delay,
        # Eredeti SIRI azonosító a gyűjtéshez:
        "recorded_at_time": va.get("RecordedAtTime"),
        "valid_until_time": va.get("ValidUntilTime"),
        "bearing": get(["Bearing"]),
        "in_congestion": get(["InCongestion"]),
        "data_source": "BODS SIRI-VM",
    }

def get_live_json(line_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    # egyszerű cache
    now = time.time()
    if _cache["data"] is None or now - _cache["t"] > CACHE_TTL:
        xml_text = _fetch_xml()
        doc = xmltodict.parse(xml_text)
        _cache["data"] = doc
        _cache["t"] = now

    doc = _cache["data"]
    # Útvonal: SIRI > ServiceDelivery > VehicleMonitoringDelivery > VehicleActivity
    sd = (doc.get("Siri") or doc.get("SIRI") or {}).get("ServiceDelivery", {})
    vmd_list = sd.get("VehicleMonitoringDelivery", [])
    if isinstance(vmd_list, dict):
        vmd_list = [vmd_list]

    vehicles: List[Dict[str, Any]] = []
    for vmd in vmd_list:
        va_list = vmd.get("VehicleActivity", []) or []
        if isinstance(va_list, dict):
            va_list = [va_list]
        for va in va_list:
            item = _simplify_vehicle_activity(va)
            if line_filter:
                # egyszerű szűrés: pontos egyezés a LineRef-re
                if (item.get("route") or "").lower() != line_filter.lower():
                    continue
            vehicles.append(item)

    return vehicles
