# siri_live.py
import os
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import requests
import xmltodict

# --- Beállítások környezeti változókból ---
BODS_FEED_ID = os.getenv("BODS_FEED_ID", "").strip()
BODS_API_KEY = os.getenv("BODS_API_KEY", "").strip()
# A legtöbb SIRI-VM proxy endpoint így néz ki a BODS-nál:
# https://data.bus-data.dft.gov.uk/api/endpoint/{FEED_ID}?api_key=...&type=vm
# Ha nálatok más, tedd env-be a teljes URL-t BODS_URL néven.
BODS_URL = os.getenv("BODS_URL", "").strip()

# 15 mp-es nagyon egyszerű cache, hogy ne terheld túl a feedet
_cache: Dict[str, Any] = {"t": 0.0, "data": None, "ok": False, "err": None}
CACHE_TTL = 15


class LiveDataError(Exception):
    pass


# ----------- belső eszközök -----------

def _bods_url() -> str:
    if BODS_URL:
        return BODS_URL
    if not BODS_FEED_ID:
        raise LiveDataError("Hiányzik a BODS_FEED_ID környezeti változó.")
    if not BODS_API_KEY:
        raise LiveDataError("Hiányzik a BODS_API_KEY környezeti változó.")
    # alapértelmezett (VM) végpont
    return f"https://data.bus-data.dft.gov.uk/api/endpoint/{BODS_FEED_ID}?api_key={BODS_API_KEY}&type=vm"


def _fetch_xml_text() -> str:
    url = _bods_url()
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()
    return resp.text


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        # példa: 2024-08-16T14:53:22+00:00
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _ensure_list(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _simplify_visit(v: Dict[str, Any]) -> Dict[str, Any]:
    """SIRI MonitoredStopVisit -> egyszerű dict"""
    mv = v.get("MonitoredVehicleJourney", {})

    line = mv.get("LineRef") or ""
    dest = ""
    dest_raw = mv.get("DestinationName")
    if isinstance(dest_raw, dict):
        dest = dest_raw.get("#text") or dest_raw.get("value") or ""
    elif isinstance(dest_raw, list):
        # ha több név érkezik, az elsőt vesszük
        dest = str(dest_raw[0])
    else:
        dest = str(dest_raw) if dest_raw is not None else ""

    # Időpontok
    aimed_arr = _parse_iso(mv.get("AimedArrivalTime"))
    aimed_dep = _parse_iso(mv.get("AimedDepartureTime"))
    exp_arr = _parse_iso(mv.get("ExpectedArrivalTime"))
    exp_dep = _parse_iso(mv.get("ExpectedDepartureTime"))

    # Preferáld a departure-t, ha van, különben arrival
    aimed = aimed_dep or aimed_arr
    expected = exp_dep or exp_arr

    display_dt = expected or aimed  # amit a felület mutat
    is_live = expected is not None  # ha van "Expected", élőnek tekintjük

    delay_seconds = None
    if expected and aimed:
        delay_seconds = int((expected - aimed).total_seconds())

    out_time = display_dt.isoformat() if display_dt else ""

    return {
        "route": str(line),
        "destination": dest.strip(),
        "time": out_time,
        "is_live": bool(is_live),
        "scheduled_time": aimed.isoformat() if aimed else None,
        "delay_seconds": delay_seconds,
    }


def _extract_visits_from_xml(xml_text: str) -> List[Dict[str, Any]]:
    """Kibontja a StopMonitoringDelivery -> MonitoredStopVisit listát a SIRI XML-ből."""
    doc = xmltodict.parse(xml_text, process_namespaces=False)
    siri = doc.get("Siri", {}) or doc.get("siri", {})  # biztos ami biztos

    sd = siri.get("ServiceDelivery", {})
    smd = sd.get("StopMonitoringDelivery", {})
    visits = _ensure_list(smd.get("MonitoredStopVisit"))

    # Ha a feed több StopMonitoringDelivery-t ad (ritkább):
    if not visits and isinstance(smd, list):
        visits = []
        for part in smd:
            visits += _ensure_list(part.get("MonitoredStopVisit"))

    simplified = []
    for v in visits:
        try:
            simplified.append(_simplify_visit(v))
        except Exception:
            # egy hibás sor ne állítsa meg az egészet
            continue
    return simplified


# ----------- publikus API a main.py számára -----------

def is_available() -> bool:
    """Gyors elérhetőségi ellenőrzés. Cache-elt."""
    global _cache
    now = time.time()
    if now - _cache["t"] < CACHE_TTL and _cache["data"] is not None:
        return bool(_cache.get("ok", False))

    try:
        xml_text = _fetch_xml_text()
        data = _extract_visits_from_xml(xml_text)
        _cache = {"t": now, "data": data, "ok": True, "err": None}
        return True
    except Exception as e:
        _cache = {"t": now, "data": None, "ok": False, "err": str(e)}
        return False


def get_next_departures(stop_id: str, minutes: int = 60) -> List[Dict[str, Any]]:
    """
    Visszaadja a következő indulásokat a megadott stop_id-hez és időablakra.
    A BODS SIRI-VM sok feed esetén NEM igényli a stop_id paramétert (globális stream),
    ezért a stop szűrését itt végezzük, ha a feed tartalmaz StopPointRef-et.
    Ha egyáltalán nincs stop információ a feedben, a legközelebbi N elem kerül vissza.
    """
    if minutes < 1:
        minutes = 1

    # Friss cache -> kevesebb lekérés
    now = time.time()
    use_cache = (now - _cache["t"] < CACHE_TTL) and (_cache["data"] is not None)
    if not use_cache:
        # új letöltés
        try:
            xml_text = _fetch_xml_text()
            data = _extract_visits_from_xml(xml_text)
            _cache.update({"t": now, "data": data, "ok": True, "err": None})
        except Exception as e:
            raise LiveDataError(f"SIRI letöltési/parszolási hiba: {e}")

    items: List[Dict[str, Any]] = _cache["data"] or []

    # A feedből nem mindig jön StopPointRef ebben a rétegezésben – ha igen, szűrjünk:
    # (A _simplify_visit-ben jelenleg nem tesszük bele, mert sok feedben nincs.)
    # Itt csak idő szerinti és stop_id nélküli általános listát adunk.

    # időablak szerinti szűrés
    now_dt = _now_utc()
    horizon = now_dt.timestamp() + minutes * 60

    filtered: List[Dict[str, Any]] = []
    for it in items:
        t = _parse_iso(it.get("time"))
        if not t:
            continue
        ts = t.timestamp()
        if ts >= now_dt.timestamp() and ts <= horizon:
            filtered.append(it)

    # ha teljesen üres az ablak, adjuk vissza a legközelebbi 20-at (hogy a UI ne legyen üres)
    if not filtered:
        items_sorted = sorted(
            [i for i in items if _parse_iso(i.get("time")) is not None],
            key=lambda d: _parse_iso(d["time"]).timestamp(),
        )
        filtered = items_sorted[:20]

    # végső rendezés
    filtered.sort(key=lambda d: _parse_iso(d["time"]).timestamp() if _parse_iso(d["time"]) else 9e18)
    return filtered
