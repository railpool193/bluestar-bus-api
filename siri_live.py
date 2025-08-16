# siri_live.py
import os
import time
import csv
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

import requests
import xmltodict

# --- Beállítások: BODS SIRI-VM feed ---
BODS_FEED_ID = os.getenv("BODS_FEED_ID", "").strip()
BODS_API_KEY = os.getenv("BODS_API_KEY", "").strip()
BODS_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{BODS_FEED_ID}/?api_key={BODS_API_KEY}"

# 15 mp-es nagyon egyszerű cache a SIRI XML-hez,
# hogy ne hívjuk feleslegesen a feedet minden kérésnél
_cache: Dict[str, Any] = {"t": 0.0, "data": ""}  # t = epoch second, data = xml str
CACHE_TTL = 15  # seconds


# --- Hibák ---
class LiveDataError(Exception):
    pass


# --- Segédek ---
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso_to_local_hhmm(iso_str: str) -> str:
    """
    ISO8601 (pl. 2025-08-16T13:25:00Z) -> helyi idő HH:MM
    """
    if not iso_str:
        return ""
    # kezeljük a végén a 'Z'-t is
    if iso_str.endswith("Z"):
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    else:
        # ha nincs timezone, tekintsük UTC-nek
        try:
            dt = datetime.fromisoformat(iso_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        except Exception:
            return ""
    # konvert helyi időre és HH:MM formázás
    return dt.astimezone().strftime("%H:%M")


def _safe_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _get(path: List[str], src: Optional[Dict[str, Any]]) -> Any:
    """
    Biztonságos lekérés beágyazott dict/list struktúrából.
    path pl.: ["Siri", "ServiceDelivery", "VehicleMonitoringDelivery"]
    """
    node = src
    for p in path:
        if node is None:
            return None
        if isinstance(node, list):
            # ha lista, akkor az első elemet próbáljuk (SIRI-nél gyakori)
            node = node[0] if node else None
        if not isinstance(node, dict):
            return None
        node = node.get(p)
    return node


# --- BODS XML letöltés cache-sel ---
def _fetch_xml() -> str:
    if not BODS_FEED_ID or not BODS_API_KEY:
        raise LiveDataError("Hiányzik a BODS_FEED_ID vagy a BODS_API_KEY környezeti változó.")

    now = time.time()
    if now - _cache["t"] < CACHE_TTL and _cache["data"]:
        return _cache["data"]

    resp = requests.get(BODS_URL, timeout=20)
    resp.raise_for_status()
    xml_text = resp.text

    _cache["t"] = now
    _cache["data"] = xml_text
    return xml_text


# --- Élő indulások kigyűjtése ---
def get_next_departures(stop_id: str, minutes: int = 60) -> Dict[str, Any]:
    """
    SIRI-VM alapján visszaadja a következő indulásokat a megadott megállóra.
    A lista elemei: route, destination, time (HH:MM), live (bool), source ("live"/"timetable").
    """
    try:
        xml_text = _fetch_xml()
    except Exception as e:
        raise LiveDataError(f"Nem sikerült letölteni a BODS feedet: {e}")

    try:
        siri = xmltodict.parse(xml_text, dict_constructor=dict)
    except Exception as e:
        raise LiveDataError(f"Nem sikerült feldolgozni a SIRI XML-t: {e}")

    svc = _get(["Siri", "ServiceDelivery"], siri) or {}

    # Próbáljuk először a VehicleMonitoringDelivery-t (gyakoribb a BODS feedben),
    # de támogatjuk a StopMonitoringDelivery felépítést is.
    vmd = _safe_list(_get(["VehicleMonitoringDelivery"], svc))
    smd = _safe_list(_get(["StopMonitoringDelivery"], svc))

    results: List[Dict[str, Any]] = []
    cutoff = _now_utc().timestamp() + minutes * 60

    # ---- 1) VehicleMonitoringDelivery / VehicleActivity / MonitoredVehicleJourney / OnwardCalls ----
    for delivery in vmd:
        for va in _safe_list(delivery.get("VehicleActivity")):
            mvj = _get(["MonitoredVehicleJourney"], va) or {}
            line = mvj.get("LineRef") or mvj.get("PublishedLineName") or ""
            dest = (mvj.get("DestinationName") or
                    _get(["DirectionName"], mvj) or
                    mvj.get("OperatorRef") or "")

            # OnwardCalls -> OnwardCall[*] – keressük a mi stop_id-nkat
            onward_calls = _safe_list(_get(["OnwardCalls", "OnwardCall"], mvj))
            found_time_iso = ""
            live_flag = False

            for oc in onward_calls:
                if str(oc.get("StopPointRef", "")).strip() != str(stop_id).strip():
                    continue

                # élő idő előnyben
                exp = oc.get("ExpectedDepartureTime") or oc.get("ExpectedArrivalTime") or ""
                aim = oc.get("AimedDepartureTime") or oc.get("AimedArrivalTime") or ""

                if exp:
                    found_time_iso = exp
                    live_flag = True
                elif aim:
                    found_time_iso = aim
                    live_flag = False
                break  # a mi megállónkat megtaláltuk

            if not found_time_iso:
                # fallback: MonitoredCall (ha a jármű éppen annál a megállónál van)
                mc = _get(["MonitoredCall"], mvj) or {}
                if str(mc.get("StopPointRef", "")).strip() == str(stop_id).strip():
                    exp = mc.get("ExpectedDepartureTime") or mc.get("ExpectedArrivalTime") or ""
                    aim = mc.get("AimedDepartureTime") or mc.get("AimedArrivalTime") or ""
                    if exp:
                        found_time_iso = exp
                        live_flag = True
                    elif aim:
                        found_time_iso = aim
                        live_flag = False

            if not found_time_iso:
                continue

            # Szűrés időablakra
            try:
                if found_time_iso.endswith("Z"):
                    ts = datetime.fromisoformat(found_time_iso.replace("Z", "+00:00")).timestamp()
                else:
                    dt = datetime.fromisoformat(found_time_iso)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    ts = dt.timestamp()
            except Exception:
                continue

            if ts > cutoff:
                continue

            results.append({
                "route": str(line),
                "destination": str(dest),
                "time": _iso_to_local_hhmm(found_time_iso),
                "live": bool(live_flag),
                "source": "live" if live_flag else "timetable",
            })

    # ---- 2) StopMonitoringDelivery / MonitoredStopVisit (ha lenne ilyen szerkezet) ----
    for delivery in smd:
        for msv in _safe_list(delivery.get("MonitoredStopVisit")):
            mvj = _get(["MonitoredVehicleJourney"], msv) or {}
            line = mvj.get("LineRef") or mvj.get("PublishedLineName") or ""
            dest = (mvj.get("DestinationName") or
                    _get(["DirectionName"], mvj) or
                    mvj.get("OperatorRef") or "")

            call = _get(["MonitoredCall"], mvj) or {}
            if str(call.get("StopPointRef", "")).strip() != str(stop_id).strip():
                continue

            exp = call.get("ExpectedDepartureTime") or call.get("ExpectedArrivalTime") or ""
            aim = call.get("AimedDepartureTime") or call.get("AimedArrivalTime") or ""
            if exp:
                iso = exp
                live_flag = True
            elif aim:
                iso = aim
                live_flag = False
            else:
                continue

            try:
                if iso.endswith("Z"):
                    ts = datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp()
                else:
                    dt = datetime.fromisoformat(iso)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    ts = dt.timestamp()
            except Exception:
                continue

            if ts > cutoff:
                continue

            results.append({
                "route": str(line),
                "destination": str(dest),
                "time": _iso_to_local_hhmm(iso),
                "live": bool(live_flag),
                "source": "live" if live_flag else "timetable",
            })

    # rendezzük idő szerint (HH:MM string alapján, ez a helyi idő)
    results.sort(key=lambda x: x.get("time", ""))

    return {
        "stop_id": stop_id,
        "minutes": minutes,
        "departures": results,
    }


# --- Megálló-kereső a GTFS-ből ---
def search_stops_by_name(query: str, limit: int = 8) -> List[Dict[str, str]]:
    """
    Egyszerű, kis memóriájú keresés a data/stops.txt-ben.
    Visszaad: [{ "stop_id": "...", "name": "...", "code": "...", "label": "Név (ID)" }, ...]
    """
    q = (query or "").strip().lower()
    if not q:
        return []

    path = os.path.join("data", "stops.txt")
    if not os.path.exists(path):
        return []

    results: List[Dict[str, str]] = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for row in rdr:
                name = (row.get("stop_name") or "").strip()
                sid = (row.get("stop_id") or "").strip()
                code = (row.get("stop_code") or "").strip()

                if not name or not sid:
                    continue

                # egyszerű részszavas keresés
                if q in name.lower():
                    label = f"{name} ({sid})" if not code else f"{name} ({code})"
                    results.append({
                        "stop_id": sid,
                        "name": name,
                        "code": code,
                        "label": label
                    })
                if len(results) >= limit:
                    break
    except Exception:
        return []

    return results
