# siri_live.py
import os
import time
import logging
from typing import List, Dict, Any, Optional
import requests
import xmltodict
from datetime import datetime, timedelta, timezone

logger = logging.getLogger("bluestar.live")

BODS_FEED_ID = os.getenv("BODS_FEED_ID", "")  # pl. 7721
BODS_API_KEY = os.getenv("BODS_API_KEY", "")
BODS_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{BODS_FEED_ID}/"

# nagyon egyszerű 15 mp-es cache, hogy ne pörgesse a feedet
_cache: Dict[str, Any] = {"t": 0.0, "data": None}
CACHE_TTL = 15  # másodperc

class LiveDataError(Exception):
    pass

def _fetch_xml() -> str:
    """Letölti a BODS SIRI-VM XML-t, és bőbeszédűen logol."""
    if not BODS_API_KEY:
        raise LiveDataError("Hiányzik a BODS_API_KEY env változó.")

    params = {"api_key": BODS_API_KEY}
    logger.info("BODS lekérés indul: url=%s  feed_id=%s", BODS_URL, BODS_FEED_ID)
    try:
        resp = requests.get(BODS_URL, params=params, timeout=30)
        logger.info("BODS válasz: status=%s content-length=%s",
                    resp.status_code, resp.headers.get("Content-Length"))
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        logger.exception("BODS letöltési hiba: %s", e)
        raise LiveDataError(f"HTTP hiba a BODS lekérésnél: {e}") from e

def _parse_vm(xml_text: str) -> Dict[str, Any]:
    """XML → dict, biztonságos logolással."""
    try:
        parsed = xmltodict.parse(xml_text, dict_constructor=dict)
        root_keys = list(parsed.keys())[:5]
        logger.debug("XML parse kész. Gyökér kulcsok: %s", root_keys)
        return parsed
    except Exception as e:
        logger.exception("SIRI-VM XML parse hiba: %s", e)
        raise LiveDataError(f"XML parse hiba: {e}") from e

def _simplify_vehicle_activity(va: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """VehicleActivity → egyszerű sor (route, dest, aimed/expected, stop_ref)."""
    try:
        mvj = va.get("MonitoredVehicleJourney", {})
        line_ref = mvj.get("LineRef")
        dest = mvj.get("DestinationName") or mvj.get("DestinationRef")

        mc = mvj.get("MonitoredCall", {}) or {}
        stop_ref = mc.get("StopPointRef") or ""
        stop_name = mc.get("StopPointName") or ""
        aimed = mc.get("AimedDepartureTime") or mc.get("AimedArrivalTime")
        expected = mc.get("ExpectedDepartureTime") or mc.get("ExpectedArrivalTime")

        def _fmt(ts: Optional[str]) -> Optional[str]:
            if not ts:
                return None
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return dt.strftime("%H:%M")
            except Exception:
                return ts

        row = {
            "route": str(line_ref) if line_ref else "",
            "destination": str(dest) if dest else "",
            "stop_ref": str(stop_ref),
            "stop_name": str(stop_name),
            "aimed": _fmt(aimed),
            "expected": _fmt(expected),
            "live": expected is not None,
        }
        return row
    except Exception as e:
        logger.debug("VehicleActivity feldolgozási hiba, átugorjuk: %s", e)
        return None

def _from_cache() -> Optional[List[Dict[str, Any]]]:
    now = time.time()
    if _cache["data"] is not None and (now - _cache["t"]) < CACHE_TTL:
        age = int(now - _cache["t"])
        logger.debug("BODS cache találat (%ss régi), elemek: %s", age, len(_cache["data"]))
        return _cache["data"]
    return None

def _to_cache(rows: List[Dict[str, Any]]) -> None:
    _cache["t"] = time.time()
    _cache["data"] = rows
    logger.debug("BODS cache frissítve: elemek=%s, ttl=%ss", len(rows), CACHE_TTL)

def _load_rows() -> List[Dict[str, Any]]:
    """Letölti és feldolgozza az összes VehicleActivity-t egyszerű sorokká."""
    cached = _from_cache()
    if cached is not None:
        return cached

    xml_text = _fetch_xml()
    data = _parse_vm(xml_text)

    try:
        sd = data.get("Siri", {}).get("ServiceDelivery", {})
        vmd = sd.get("VehicleMonitoringDelivery", [])
        if isinstance(vmd, dict):
            vmd = [vmd]

        rows: List[Dict[str, Any]] = []
        total_va = 0

        for block in vmd:
            va_list = block.get("VehicleActivity", []) or []
            if isinstance(va_list, dict):
                va_list = [va_list]
            total_va += len(va_list)

            for va in va_list:
                row = _simplify_vehicle_activity(va)
                if row:
                    rows.append(row)

        logger.info("SIRI-VM: összes VehicleActivity=%s, feldolgozott sorok=%s",
                    total_va, len(rows))
        _to_cache(rows)
        return rows
    except Exception as e:
        logger.exception("SIRI-VM feldolgozási hiba: %s", e)
        raise LiveDataError(f"SIRI-VM feldolgozási hiba: {e}") from e

def get_live_departures(stop_id: str, minutes: int = 60) -> List[Dict[str, Any]]:
    """Megadott StopPointRef-hez a következő indulások (élő)."""
    rows = _load_rows()

    now_utc = datetime.now(timezone.utc)
    future_utc = now_utc + timedelta(minutes=minutes)

    def _in_window(hhmm: Optional[str]) -> bool:
        if not hhmm:
            return False
        try:
            h, m = map(int, hhmm.split(":"))
            candidate = now_utc.replace(hour=h, minute=m, second=0, microsecond=0)
            if candidate < now_utc - timedelta(hours=12):
                candidate += timedelta(days=1)
            if candidate > now_utc + timedelta(hours=12):
                candidate -= timedelta(days=1)
            return now_utc <= candidate <= future_utc
        except Exception:
            return True

    before = len(rows)
    filtered = [r for r in rows if r.get("stop_ref") == stop_id]
    logger.info("SIRI-VM szűrés: stop_id=%s → %s/%s sor passzol",
                stop_id, len(filtered), before)

    out: List[Dict[str, Any]] = []
    for r in filtered:
        when = r.get("expected") or r.get("aimed")
        if _in_window(when):
            out.append({
                "route": r.get("route", ""),
                "destination": r.get("destination", ""),
                "time": when or "",
                "live": bool(r.get("expected")),
            })

    logger.info("SIRI-VM eredmény: %s indulás a(z) %s megállóra (%s perc)",
                len(out), stop_id, minutes)
    out.sort(key=lambda x: x.get("time", ""))
    return out
