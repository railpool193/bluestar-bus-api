# -*- coding: utf-8 -*-
"""
siri_live.py
-------------
BODS SIRI-VM (VehicleMonitoring) feed feldolgozó.
A feed XML-jét letölti, megkeresi az adott Stop ID-hoz tartozó közelgő indulásokat,
és rendezett JSON-szerű listát ad vissza.

Használat (példa):
    from siri_live import get_live_departures
    deps = get_live_departures(
        stop_id="1980SN12619A",
        minutes=60,
        api_key=os.environ["BODS_API_KEY"],
        feed_id=os.environ.get("BODS_FEED_ID", "7721"),
    )
"""

from __future__ import annotations
import os
import requests
import xmltodict
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Iterable, Union


class SiriLiveError(Exception):
    """Általános hiba a SIRI-VM feldolgozás során."""


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    """ISO idő (pl. 2025-08-16T18:10:00+00:00) -> aware UTC datetime."""
    if not s:
        return None
    try:
        # Python 3.11+ tudja ezt közvetlenül is, de biztosra megyünk:
        # cseréljük a 'Z'-t +00:00-ra, ha lenne
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        # biztosítsuk, hogy aware legyen (ha netán nincs tz info)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def _as_list(x: Union[List[Any], Any, None]) -> List[Any]:
    """xmltodict gyakran dict-et ad list helyett; normalizáljuk listára."""
    if x is None:
        return []
    if isinstance(x, list):
        return x
    return [x]


def _best_time(call: Dict[str, Any]) -> Optional[datetime]:
    """
    MonitoredCall/OnwardCall elemből válasszunk időt: ExpectedDepartureTime,
    aztán AimedDepartureTime, aztán ExpectedArrivalTime/AimedArrivalTime.
    """
    keys = [
        "ExpectedDepartureTime",
        "AimedDepartureTime",
        "ExpectedArrivalTime",
        "AimedArrivalTime",
    ]
    for k in keys:
        if k in call:
            dt = _parse_dt(call.get(k))
            if dt:
                return dt
    return None


def _extract_calls_for_stop(journey: Dict[str, Any], stop_id: str) -> List[datetime]:
    """
    Egy MonitoredVehicleJourney-ből kiszedi az adott StopPointRef-hez tartozó időpontokat.
    - MonitoredCall (aktuális)
    - OnwardCalls/OnwardCall (következő megállók)
    - fallback: ha OriginRef == stop_id, akkor OriginAimedDepartureTime/ExpectedDepartureTime
    """
    times: List[datetime] = []

    # MonitoredCall
    mc = journey.get("MonitoredCall")
    if isinstance(mc, dict) and mc.get("StopPointRef") == stop_id:
        t = _best_time(mc)
        if t:
            times.append(t)

    # OnwardCalls
    oc_root = journey.get("OnwardCalls")
    if isinstance(oc_root, dict):
        onward_calls = _as_list(oc_root.get("OnwardCall"))
        for oc in onward_calls:
            if isinstance(oc, dict) and oc.get("StopPointRef") == stop_id:
                t = _best_time(oc)
                if t:
                    times.append(t)

    # Fallback: ha épp a kiinduló megálló az adott stop
    if journey.get("OriginRef") == stop_id:
        # Előnyben a "Expected..." ha van, különben "Aimed..."
        cand = (
            journey.get("OriginExpectedDepartureTime")
            or journey.get("OriginAimedDepartureTime")
        )
        t = _parse_dt(cand)
        if t:
            times.append(t)

    return times


def _pick_destination(journey: Dict[str, Any]) -> str:
    """Cél meghatározása emberbarát módon."""
    return (
        journey.get("DestinationName")
        or journey.get("DestinationRef")
        or "—"
    )


def _pick_route(journey: Dict[str, Any]) -> str:
    """Járatszám/published line name kiválasztása."""
    return (
        journey.get("PublishedLineName")
        or journey.get("LineRef")
        or "?"
    )


def get_live_departures(
    stop_id: str,
    minutes: int,
    api_key: Optional[str] = None,
    feed_id: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """
    Visszaadja az indulásokat az adott Stop ID-hoz.
    - stop_id: pl. '1980SN12619A'
    - minutes: időablak (0..N perc a jelentéstől számítva)
    - api_key: BODS API kulcs
    - feed_id: BODS feed id (Bluestarhoz tipikusan '7721')
    """
    api_key = api_key or os.environ.get("BODS_API_KEY")
    feed_id = feed_id or os.environ.get("BODS_FEED_ID")

    if not api_key:
        raise SiriLiveError("Hiányzik a BODS_API_KEY (környezeti változó).")
    if not feed_id:
        raise SiriLiveError("Hiányzik a BODS_FEED_ID (környezeti változó).")

    url = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{feed_id}/"
    params = {"api_key": api_key}

    sess = session or requests.Session()
    try:
        r = sess.get(url, params=params, timeout=30)
        r.raise_for_status()
    except requests.HTTPError as e:
        # A BODS időnként HTML hibát ad; továbbítjuk a részleteket
        raise SiriLiveError(f"HTTP hiba a BODS felől: {e} – {getattr(e.response, 'text', '')[:500]}") from e
    except Exception as e:
        raise SiriLiveError(f"Hálózati hiba a BODS hívásban: {e}") from e

    try:
        siri = xmltodict.parse(r.content)
    except Exception as e:
        raise SiriLiveError(f"Nem sikerült XML-t parse-olni: {e}") from e

    # Navigáljunk a VehicleActivity-ig
    try:
        sd = siri["Siri"]["ServiceDelivery"]
        vmd = sd["VehicleMonitoringDelivery"]
        activities = _as_list(vmd.get("VehicleActivity"))
    except Exception as e:
        # Ha nincs activity, térjünk vissza üres listával
        activities = []

    now_utc = datetime.now(timezone.utc)
    window_end = now_utc + timedelta(minutes=max(0, minutes))

    results: List[Dict[str, Any]] = []

    for act in activities:
        if not isinstance(act, dict):
            continue

        journey = act.get("MonitoredVehicleJourney")
        if not isinstance(journey, dict):
            continue

        # időpont(ok) ehhez a megállóhoz
        times = _extract_calls_for_stop(journey, stop_id)

        for t in times:
            if t is None:
                continue
            # Szűrés az időablakra
            if t < now_utc or t > window_end:
                continue

            results.append(
                {
                    "route": _pick_route(journey),
                    "destination": _pick_destination(journey),
                    "time_utc": t.isoformat(),
                    "timestamp_unix": int(t.timestamp()),
                    "source": "BODS SIRI-VM",
                }
            )

    # Rendezés idő szerint
    results.sort(key=lambda x: x["timestamp_unix"])
    return results
# --- a siri_live.py legvégére tedd ---
def get_next_departures(stop_id: str, minutes: int, api_key=None, feed_id=None):
    """Visszafelé kompatibilis alias a régi hívásnévhez."""
    return get_live_departures(stop_id=stop_id, minutes=minutes, api_key=api_key, feed_id=feed_id)
