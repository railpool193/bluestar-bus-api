import os
import aiohttp
import xmltodict
from datetime import datetime
from dateutil import parser as dtparse
from typing import List, Dict, Any, Optional

BODS_API_KEY = os.getenv("BODS_API_KEY", "").strip()

SIRI_BASE = "https://data.bus-data.dft.gov.uk/api/siri/2.0/stop-monitoring"


def _as_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def _parse_time(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return dtparse.parse(s)
    except Exception:
        return None


def _fmt_hhmm(t: Optional[datetime]) -> Optional[str]:
    if not t:
        return None
    return t.strftime("%H:%M")


def _text(v) -> str:
    if v is None:
        return ""
    if isinstance(v, list):
        # SIRI gyakran listát ad vissza DestinationName-nél
        return " / ".join([_text(i) for i in v])
    return str(v)


def _build_url(stop_id: str, minutes: int) -> str:
    # PreviewInterval ISO8601: PT{minutes}M
    return (
        f"{SIRI_BASE}"
        f"?api_key={BODS_API_KEY}"
        f"&MonitoringRef={stop_id}"
        f"&MaximumStopVisits=60"
        f"&PreviewInterval=PT{max(1, minutes)}M"
    )


async def get_next_departures(stop_id: str, minutes: int = 60) -> List[Dict[str, Any]]:
    """
    Visszaadja a következő indulásokat a BODS SIRI-VM Stop Monitoring végpontról.
    A visszatérési lista elemei:
      {
        "route": "18",
        "destination": "Millbrook",
        "time_hhmm": "13:52",
        "aimed_hhmm": "13:50",
        "expected_hhmm": "13:52",
        "is_live": true
      }
    """
    if not BODS_API_KEY:
        raise RuntimeError("BODS_API_KEY hiányzik a környezeti változók közül.")

    url = _build_url(stop_id, minutes)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as sess:
        async with sess.get(url) as resp:
            if resp.status >= 400:
                txt = await resp.text()
                raise RuntimeError(f"SIRI hívás hiba (HTTP {resp.status}): {txt}")

            data = await resp.text()

    try:
        siri = xmltodict.parse(data)
    except Exception as e:
        raise RuntimeError(f"SIRI XML feldolgozási hiba: {e}")

    service_delivery = (
        siri.get("Siri", {})
            .get("ServiceDelivery", {})
    )

    # Néha több StopMonitoringDelivery is van – vegyük össze
    deliveries = _as_list(service_delivery.get("StopMonitoringDelivery"))

    visits_all = []
    for d in deliveries:
        visits_all.extend(_as_list(d.get("MonitoredStopVisit")))

    results: List[Dict[str, Any]] = []

    for v in visits_all:
        mvj = v.get("MonitoredVehicleJourney", {}) if isinstance(v, dict) else {}
        call = mvj.get("MonitoredCall", {}) or mvj.get("OnwardCalls", {})  # fallback

        line = _text(mvj.get("PublishedLineName") or mvj.get("LineRef"))
        dest = _text(mvj.get("DestinationName"))

        aimed = _parse_time(
            call.get("AimedDepartureTime")
            or call.get("AimedArrivalTime")
            or mvj.get("OriginAimedDepartureTime")
        )
        expected = _parse_time(
            call.get("ExpectedDepartureTime")
            or call.get("ExpectedArrivalTime")
        )

        # live jelzés: van Expected és eltér az Aimed-től, vagy van "RecordedAtTime"
        recorded = _parse_time(v.get("RecordedAtTime"))
        is_live = bool(expected and (not aimed or expected != aimed)) or bool(recorded)

        results.append({
            "route": line,
            "destination": dest,
            "time_hhmm": _fmt_hhmm(expected or aimed),
            "aimed_hhmm": _fmt_hhmm(aimed),
            "expected_hhmm": _fmt_hhmm(expected),
            "is_live": is_live
        })

    # idő szerint rendezés
    def _key(item):
        t = item.get("expected_hhmm") or item.get("aimed_hhmm")
        try:
            return datetime.strptime(t, "%H:%M").time() if t else datetime.max.time()
        except Exception:
            return datetime.max.time()

    results.sort(key=_key)
    return results
