import os
import asyncio
from typing import List, Dict
import aiohttp
import xmltodict
from datetime import datetime, timezone

BODS_API_KEY = os.getenv("BODS_API_KEY", "")
BODS_FEED_ID = os.getenv("BODS_FEED_ID", "")
# BODS SIRI-VM StopMonitoring végpont – a BODS a SIRI v1 datafeed útvonalat használja
# A legtöbb üzemeltetőnél a paraméter neve "MonitoringRef". Ha nálatok "StopMonitoringRef",
# elég az alábbit átírni stop_param = "StopMonitoringRef"-re.
STOP_PARAM_NAME = os.getenv("BODS_STOP_PARAM_NAME", "MonitoringRef")

BASE_URL = f"https://data.bus-data.dft.gov.uk/api/v1/datafeed/{BODS_FEED_ID}/"

def _iso_now() -> datetime:
    return datetime.now(timezone.utc)

async def get_live_departures(stop_id: str, minutes: int = 60, limit: int = 30) -> List[Dict]:
    """
    SIRI-VM StopMonitoring: élő indulások lekérése egy megállóra.
    Visszatér: list[ {route, destination, time_iso, is_live=True} ]
    """
    if not BODS_API_KEY or not BODS_FEED_ID:
        return []

    params = {
        "api_key": BODS_API_KEY,
        STOP_PARAM_NAME: stop_id,
        "MaximumStopVisits": str(limit),
        "PreviewInterval": f"PT{minutes}M",
    }

    # A BODS szerver XML-lel válaszol
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as sess:
        async with sess.get(BASE_URL, params=params) as resp:
            if resp.status != 200:
                # ha 400/404: nincs StopMonitoring ezen a feeden → üres lista (fallback a GTFS-re)
                return []
            text = await resp.text()

    try:
        siri = xmltodict.parse(text)
    except Exception:
        return []

    # Navigálunk a SIRI StopMonitoring ágig
    # Vannak feedek, ahol VehicleMonitoringDelivery jön – azt itt nem dolgozzuk fel.
    service = siri.get("Siri", {}).get("ServiceDelivery", {})
    smd = service.get("StopMonitoringDelivery")
    if not smd:
        # egyes feedek listát adnak:
        deliveries = service.get("StopMonitoringDelivery", [])
        if isinstance(deliveries, list) and deliveries:
            smd = deliveries[0]
    if not smd:
        return []

    visits = smd.get("MonitoredStopVisit", [])
    if isinstance(visits, dict):
        visits = [visits]

    out: List[Dict] = []
    for v in visits:
        mvj = v.get("MonitoredVehicleJourney", {})
        line = mvj.get("LineRef") or ""
        dest = (mvj.get("DestinationName") or mvj.get("DestinationRef") or "").strip()

        # Időpontok: ExpectedDepartureTime > AimedDepartureTime
        expected = mvj.get("MonitoredCall", {}).get("ExpectedDepartureTime")
        aimed = mvj.get("MonitoredCall", {}).get("AimedDepartureTime") or mvj.get("OriginAimedDepartureTime")

        time_iso = expected or aimed
        if not time_iso:
            continue

        out.append({
            "route": str(line),
            "destination": str(dest),
            "time_iso": str(time_iso),
            "is_live": True
        })

    # idő szerint rendezzük
    out.sort(key=lambda x: x["time_iso"])
    return out[:limit]
