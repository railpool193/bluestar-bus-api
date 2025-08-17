import os
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional

# ---- Konfiguráció / környezeti változók ----
BODS_BASE = os.getenv("BODS_BASE", "https://data.bus-data.dft.gov.uk/api/v1")
BODS_API_KEY = os.getenv("BODS_API_KEY")  # szükséges az éles live-hoz
BODS_PRODUCER = os.getenv("BODS_PRODUCER")  # pl. "DepartmentForTransport" vagy üres

# A BODS SIRI-VM feed endpoint (összes jármű / országos stream)
# Paraméterezés: ?api_key=...  (opcionálisan &producerRef=...)
DATAFEED_URL = f"{BODS_BASE.rstrip('/')}/datafeed/"

# Egyszerű, kis TTL-es cache, hogy ne hívjuk túl gyakran a feedet
_CACHE: Dict[str, tuple[float, ET.Element]] = {}  # key -> (ts, xml_root)
_CACHE_TTL = 20  # mp


def _configured() -> bool:
    """Van-e értelmes live konfiguráció."""
    return bool(BODS_API_KEY)


def is_live_available() -> bool:
    """
    Gyors elérhetőség-ellenőrzés.
    Ha nincs BODS_API_KEY, akkor False, különben megpróbál egy 5 mp-es GET-et.
    """
    if not _configured():
        return False
    try:
        params = {"api_key": BODS_API_KEY}
        if BODS_PRODUCER:
            params["producerRef"] = BODS_PRODUCER
        r = requests.get(DATAFEED_URL, params=params, timeout=5)
        return r.status_code == 200
    except Exception:
        return False


def _fetch_xml() -> Optional[ET.Element]:
    """Letölti (vagy cache-ből adja) a SIRI-VM XML-t."""
    if not _configured():
        return None

    now = time.time()
    cached = _CACHE.get("vm")
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    params = {"api_key": BODS_API_KEY}
    if BODS_PRODUCER:
        params["producerRef"] = BODS_PRODUCER

    r = requests.get(DATAFEED_URL, params=params, timeout=12)
    r.raise_for_status()
    root = ET.fromstring(r.content)
    _CACHE["vm"] = (now, root)
    return root


def _parse_iso(ts: str) -> Optional[datetime]:
    """ISO idő parsing (Z kezelése)."""
    if not ts:
        return None
    try:
        # példa: 2025-08-17T18:25:00Z
        if ts.endswith("Z"):
            return datetime.fromisoformat(ts[:-1]).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def get_live_departures(stop_id: str, limit: int = 20) -> List[Dict]:
    """
    Visszaad néhány élő indulást az adott megállóra.
    A visszatérés formátuma kompatibilis a fronttal:
    dict(route, destination, time_iso, is_live=True)
    """
    root = _fetch_xml()
    if root is None:
        return []

    ns = {
        "siri": "http://www.siri.org.uk/siri",
        "vm": "http://www.siri.org.uk/siri"
    }

    results: List[Dict] = []
    # Keresünk MonitoredStopVisit bejegyzéseket
    for msv in root.findall(".//siri:MonitoredStopVisit", ns):
        j = msv.find("siri:MonitoredVehicleJourney", ns)
        if j is None:
            continue

        sp = j.findtext("siri:MonitoredCall/siri:StopPointRef", default="", namespaces=ns)
        if not sp:
            continue

        # StopPointRef egyezés (egyes feedekben lehet "prefix:STOPID" – ezért tartalmazás is jó fallback)
        if sp != stop_id and stop_id not in sp:
            continue

        line = (j.findtext("siri:PublishedLineName", default="", namespaces=ns)
                or j.findtext("siri:LineRef", default="", namespaces=ns)
                or "")
        dest = j.findtext("siri:DestinationName", default="", namespaces=ns) or ""

        # Először Expected (live), ha nincs, akkor Aimed
        expected = j.findtext("siri:MonitoredCall/siri:ExpectedDepartureTime", default="", namespaces=ns)
        aimed = j.findtext("siri:MonitoredCall/siri:AimedDepartureTime", default="", namespaces=ns)
        when = _parse_iso(expected) or _parse_iso(aimed)
        if not when:
            continue

        # csak jövőbeni indulásokat listázzunk
        if when < datetime.now(timezone.utc) - timedelta(minutes=1):
            continue

        results.append({
            "route": line.strip(),
            "destination": dest.strip(),
            "time_iso": when.astimezone(timezone.utc).isoformat(),
            "is_live": bool(expected)  # expected => valóban live
        })

        if len(results) >= limit:
            break

    # idő szerint növekvő
    results.sort(key=lambda x: x["time_iso"])
    return results
