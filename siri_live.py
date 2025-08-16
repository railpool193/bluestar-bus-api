# siri_live.py
import os
import io
import time
import zipfile
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

import requests
import xmltodict


# --- Konfiguráció & nagyon egyszerű cache -----------------------------------

BODS_FEED_ID = os.getenv("BODS_FEED_ID", "").strip()
BODS_API_KEY = os.getenv("BODS_API_KEY", "").strip()

# A SIRI-VM feed ZIP-je:
# pl.: https://data.bus-data.dft.gov.uk/api/v1/datafeed/7721/?api_key=XXXX
BODS_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{BODS_FEED_ID}/?api_key={BODS_API_KEY}"

# 15 mp-es XML cache, hogy ne kérdezzük túl sűrűn a BODS-ot
_cache: Dict[str, Any] = {"t": 0.0, "xml": None}
CACHE_TTL = 15  # seconds


# --- Segédek -----------------------------------------------------------------

class LiveDataError(Exception):
    """BODS élő adat hiba."""
    pass


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    """ISO8601 → datetime (UTC). Kezeli a 'Z' végződést is."""
    if not ts:
        return None
    try:
        ts2 = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts2)
        # Ha nincs timezone, tekintsük UTC-nek
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _fmt_hhmm(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    loc = dt.astimezone()  # helyi időzóna
    return loc.strftime("%H:%M")


def _safe_get(path: List[str], src: Any) -> Optional[Any]:
    """
    Biztonságos lekérés mélyen lévő dict/list struktúrából.
    path pl.: ["MonitoredVehicleJourney", "MonitoredCall", "StopPointRef"]
    """
    node = src
    for p in path:
        if isinstance(node, dict):
            node = node.get(p)
        else:
            return None
    return node


# --- BODS letöltés, XML bontás ------------------------------------------------

def _fetch_xml_text() -> str:
    """Letölti és visszaadja a SIRI-VM XML szöveget (ZIP-ből kibontva)."""
    if not BODS_FEED_ID or not BODS_API_KEY:
        raise LiveDataError("Hiányzik a BODS_FEED_ID / BODS_API_KEY környezeti változó.")

    # Cache
    now = time.time()
    if _cache["xml"] is not None and (now - _cache["t"] < CACHE_TTL):
        return _cache["xml"]

    resp = requests.get(BODS_URL, timeout=30)
    resp.raise_for_status()

    # A feed ZIP fájlt ad – ki kell bontani, majd megkeresni benne az XML-t
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    xml_name = None
    for n in zf.namelist():
        if n.lower().endswith(".xml"):
            xml_name = n
            break
    if not xml_name:
        raise LiveDataError("A feed ZIP-ben nem találtam XML-t.")

    xml_text = zf.read(xml_name).decode("utf-8", errors="ignore")

    _cache["xml"] = xml_text
    _cache["t"] = now
    return xml_text


def _read_siri_doc() -> Dict[str, Any]:
    """
    Visszaad egy xmltodict-tel felolvasott SIRI-VM struktúrát.
    A tipikus gyökér: 'Siri' → 'ServiceDelivery' → 'VehicleMonitoringDelivery' → 'VehicleActivity'
    """
    xml_text = _fetch_xml_text()
    doc = xmltodict.parse(xml_text)
    # Biztonság kedvéért lépjünk egyenesen lefelé – a kulcsok case-sensitive-k!
    return doc


# --- SIRI → egyszerűsített rekordok ------------------------------------------

def _simplify_vehicle_activity(va: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Egy VehicleActivity csomópontból (xmltodict dict) csinál egy "flat" rekordot.
    Ahol lehet, fallback-eljünk aimed/expected mezőkre.
    """
    mvj = _safe_get(["MonitoredVehicleJourney"], va) or {}
    mc = _safe_get(["MonitoredVehicleJourney", "MonitoredCall"], va) or {}

    line_name = (
        mvj.get("PublishedLineName")
        or mvj.get("LineRef")
        or ""
    )

    destination = (
        mvj.get("DestinationName")
        or mvj.get("DestinationRef")
        or ""
    )

    stop_ref = mc.get("StopPointRef") or ""
    stop_name = mc.get("StopPointName") or mvj.get("DestinationName") or ""

    # Times: próbáljunk ExpectedDepartureTime → AimedDepartureTime → ExpectedArrivalTime → AimedArrivalTime sorrendben
    exp_dep = mc.get("ExpectedDepartureTime")
    aim_dep = mc.get("AimedDepartureTime")
    exp_arr = mc.get("ExpectedArrivalTime")
    aim_arr = mc.get("AimedArrivalTime")

    # Válasszunk egy "legjobb" időt (UTC dt)
    dt = (
        _parse_iso(exp_dep)
        or _parse_iso(aim_dep)
        or _parse_iso(exp_arr)
        or _parse_iso(aim_arr)
    )

    # További, néha más névvel érkező mezők fallbackje:
    if not dt:
        # Idő esetleg máshol
        dt = _parse_iso(mvj.get("OriginAimedDepartureTime") or mvj.get("OriginExpectedDepartureTime"))

    if not dt:
        # Ha semmi, akkor nincs értékelhető rekord
        return None

    return {
        "route": str(line_name).strip(),
        "destination": str(destination).strip(),
        "stop_id": str(stop_ref).strip(),
        "stop_name": str(stop_name).strip(),
        "dt_utc": dt,                            # belső használatra
        "departure_time": _fmt_hhmm(dt),         # HH:MM (helyi)
        "realtime": True                         # élő forrás: SIRI-VM
    }


def _iter_vehicle_activities(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Kiveszi a VehicleActivity-ket és egyszerűsíti.
    Többféle struktúrát próbál lekezelni (list / dict stb.)
    """
    try:
        siri = doc.get("Siri") or doc.get("siri")
        if not siri:
            return []

        sd = siri.get("ServiceDelivery") or {}
        vmd = sd.get("VehicleMonitoringDelivery") or sd.get("VehicleMonitoringDeliveries") or {}
        # vmd lehet lista vagy dict:
        vmd_list = vmd if isinstance(vmd, list) else [vmd]

        out: List[Dict[str, Any]] = []
        for one in vmd_list:
            va_list = one.get("VehicleActivity") or []
            if isinstance(va_list, dict):
                va_list = [va_list]
            for va in va_list:
                flat = _simplify_vehicle_activity(va)
                if flat:
                    out.append(flat)
        return out
    except Exception:
        return []


# --- Közeli indulások stop_id + időablak szerint -----------------------------

def _filter_by_stop_and_window(items: List[Dict[str, Any]], stop_id: str, minutes: int) -> List[Dict[str, Any]]:
    """
    Szűrés: adott stop_id és most→(most+minutes) időablak.
    """
    stop_id = (stop_id or "").strip()
    if not stop_id:
        return []

    now = _now_utc()
    latest = now.replace(microsecond=0) + (minutes * 60) * (datetime.fromtimestamp(0, tz=timezone.utc) - datetime.fromtimestamp(0, tz=timezone.utc))  # dummy; lentebb számolunk normálisan

    # Fent egy fura hack lenne; inkább számoljunk külön:
    latest = now + (minutes * 60) * (datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc) - datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc))
    # Előbbi sem tetszik Python-nak 3.12 alatt. Legyen egyszerű:
    latest = now + timedelta(minutes=minutes)  # egyszerű és tiszta

    out: List[Dict[str, Any]] = []
    for it in items:
        if it.get("stop_id") != stop_id:
            continue
        dt = it.get("dt_utc")
        if not isinstance(dt, datetime):
            continue
        if now <= dt <= latest:
            out.append(it)

    # Rendezzük idő szerint
    out.sort(key=lambda x: x.get("dt_utc"))
    # A dt_utc belső mező – a külvilágnak nem kell
    for it in out:
        it.pop("dt_utc", None)
    return out


# A fenti miatt kell:
from datetime import timedelta


# --- Publikus függvények ------------------------------------------------------

def get_departures(stop_id: str, minutes: int = 60) -> List[Dict[str, Any]]:
    """
    Egyszerű lista az elkövetkező indulásokról adott megállóhoz.
    Minden rekordon: route, destination, stop_id, stop_name, departure_time (HH:MM), realtime=True
    """
    doc = _read_siri_doc()
    va_items = _iter_vehicle_activities(doc)
    return _filter_by_stop_and_window(va_items, stop_id=stop_id, minutes=minutes)


def build_departures(stop_id: str, minutes: int = 60) -> Dict[str, Any]:
    """
    Ugyanaz, mint get_departures, csak dict-ben, kompatibilis a frontenddel:
    {"stop_id":..., "minutes":..., "departures":[...]}
    """
    return {
        "stop_id": stop_id,
        "minutes": minutes,
        "departures": get_departures(stop_id=stop_id, minutes=minutes)
    }


# --- Kért wrap: ha a main.py valami mást hívna, ezzel biztos realtime lesz ----

def _add_realtime_flag(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for it in items or []:
        row = dict(it)
        row["realtime"] = True
        if not row.get("departure_time"):
            # semmi értelmes idő – hagyjuk, vagy próbáljuk pótolni?
            pass
        out.append(row)
    return out


def get_departures_with_realtime(stop_id: str, minutes: int = 60):
    """
    Wrap – ha valahol ezt hívod, biztosan realtime flag-et kapsz.
    (Itt egyébként a get_departures is már realtime=True-t ad.)
    """
    data = get_departures(stop_id=stop_id, minutes=minutes)
    return _add_realtime_flag(data)
