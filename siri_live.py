# siri_live.py
# ----------------------
# SIRI VehicleMonitoring feed letöltése és stop szerinti "következő indulások" kinyerése.

from __future__ import annotations

import os
import requests
import xmltodict
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional


class SiriLiveError(Exception):
    """Belső kivétel a SIRI hibák jelzésére."""


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise SiriLiveError(f"Missing environment variable: {name}")
    return val


def _parse_iso(ts: str) -> datetime:
    """
    ISO 8601 időpontok parse-olása (a BODS +00:00 / Z formátumokat is ad).
    """
    # Normálizáljuk: az xmltodict már stringet ad vissza
    try:
        # Python 3.11+: fromisoformat kezeli a +00:00 formát is
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception as exc:
        raise SiriLiveError(f"Bad timestamp in feed: {ts}") from exc


def _fetch_vm_xml(feed_id: str, api_key: str) -> Dict[str, Any]:
    """
    BODS datafeed letöltése (VehicleMonitoring XML).
    A datafeed végpont NEM támogatja a StopMonitoring query paramétereket,
    csak az api_key-t – a többit nekünk kell szűrni a kliens oldalon.
    """
    url = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{feed_id}/"
    params = {"api_key": api_key}

    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise SiriLiveError(f"Network error while calling BODS: {exc}") from exc

    # XML → dict
    try:
        data = xmltodict.parse(resp.text)
        return data
    except Exception as exc:
        raise SiriLiveError("Failed to parse SIRI XML") from exc


def _iter_vehicle_activity(vm_dict: Dict[str, Any]):
    """
    Biztonságos iterátor a VehicleActivity elemekre.
    """
    try:
        svc = vm_dict["Siri"]["ServiceDelivery"]
        vmd = svc["VehicleMonitoringDelivery"]
        # A feed lehet listás vagy egy elemű – normalizáljuk
        if isinstance(vmd, list):
            # Vegyük az első (legfrissebb) delivery-t
            vmd = vmd[0]
        va = vmd.get("VehicleActivity", [])
        if isinstance(va, dict):
            va = [va]
        for item in va:
            yield item
    except KeyError:
        # Üres / nincs aktivitás
        return


def get_live_departures(
    stop_id: str,
    minutes: int,
    api_key: Optional[str] = None,
    feed_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Élő indulások adott megállóra a SIRI VehicleMonitoring feedből.

    Logika:
    - letöltjük a feedet;
    - végigmegyünk a VehicleActivity elemeken;
    - azokat vesszük, ahol MonitoredCall/StopPointRef == stop_id;
    - kivesszük az ExpectedDepartureTime | AimedDepartureTime értéket;
    - csak a 'minutes' időablakba esőket adjuk vissza.
    """
    if not stop_id:
        raise SiriLiveError("stop_id is required")

    api_key = api_key or _require_env("BODS_API_KEY")
    feed_id = feed_id or _require_env("BODS_FEED_ID")

    vm = _fetch_vm_xml(feed_id=feed_id, api_key=api_key)

    now = datetime.now(timezone.utc)
    horizon = now + timedelta(minutes=int(minutes))

    results: List[Dict[str, Any]] = []

    for act in _iter_vehicle_activity(vm):
        mj = act.get("MonitoredVehicleJourney", {}) or {}

        # A megálló, ahol most várható megállás
        mc = mj.get("MonitoredCall", {}) or {}
        stop_ref = mc.get("StopPointRef")

        if not stop_ref or str(stop_ref).strip().upper() != str(stop_id).strip().upper():
            continue

        # Idő – prefer ExpectedDepartureTime, fallback AimedDepartureTime/ExpectedArrivalTime
        ts = (
            mc.get("ExpectedDepartureTime")
            or mc.get("AimedDepartureTime")
            or mc.get("ExpectedArrivalTime")
            or mc.get("AimedArrivalTime")
        )
        if not ts:
            continue

        dep = _parse_iso(ts)
        if not (now <= dep <= horizon):
            continue

        # Adatok: vonalszám, cél
        route = mj.get("PublishedLineName") or mj.get("LineRef") or ""
        dest = mj.get("DestinationName") or mj.get("DestinationRef") or ""

        results.append(
            {
                "route": str(route),
                "destination": str(dest),
                "time": dep.isoformat(),
            }
        )

    # rendezés idő szerint
    results.sort(key=lambda x: x["time"])
    return results


# --- Visszafelé kompatibilis alias (ha a frontend ezt hívja) ---
def get_next_departures(
    stop_id: str,
    minutes: int,
    api_key: Optional[str] = None,
    feed_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Alias a régi névre – ugyanazt csinálja, mint get_live_departures.
    """
    return get_live_departures(stop_id=stop_id, minutes=minutes, api_key=api_key, feed_id=feed_id)
