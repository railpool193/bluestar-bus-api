import os
from datetime import datetime, timezone, timedelta
import httpx
import xml.etree.ElementTree as ET

BODS_API = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/{feed_id}/?api_key={api_key}"

def _text(node, tag):
    n = node.find(tag)
    return n.text.strip() if n is not None and n.text else None

def _parse_time(s: str) -> str:
    # normalizáljuk ISO-ra (mindig UTC-re hagyjuk)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        return s

async def get_next_departures(stop_id: str, minutes: int):
    """SIRI-VM feedből (BODS) élő indulások adott megállóra."""
    feed_id = os.environ.get("BODS_SIRI_FEED_ID")
    api_key = os.environ.get("BODS_API_KEY")
    if not feed_id or not api_key:
        return []

    url = BODS_API.format(feed_id=feed_id, api_key=api_key)
    async with httpx.AsyncClient(timeout=40) as client:
        r = await client.get(url)
        r.raise_for_status()
        xml = r.text

    root = ET.fromstring(xml)
    ns = {
        "s": "http://www.siri.org.uk/siri"
    }

    now_utc = datetime.now(timezone.utc)
    limit = now_utc + timedelta(minutes=minutes)

    out = []

    # VehicleMonitoringDelivery / VehicleActivity
    for vmd in root.findall(".//s:VehicleMonitoringDelivery", ns):
        for va in vmd.findall("s:VehicleActivity", ns):
            mvj = va.find("s:MonitoredVehicleJourney", ns)
            if mvj is None:
                continue

            # 1) Közvetlen MonitoredCall
            mc = mvj.find("s:MonitoredCall", ns)
            if mc is not None:
                sp = _text(mc, "s:StopPointRef")
                if sp and sp.upper() == stop_id.upper():
                    line = _text(mvj, "s:PublishedLineName") or _text(mvj, "s:LineRef") or "?"
                    dest = _text(mvj, "s:DestinationName") or "?"
                    when = _text(mc, "s:ExpectedDepartureTime") or _text(mc, "s:AimedDepartureTime")
                    if when:
                        iso = _parse_time(when)
                        try:
                            dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                        except Exception:
                            dt = None
                        if dt and now_utc <= dt <= limit:
                            out.append({"route": line, "destination": dest, "time_iso": iso})
                    continue

            # 2) OnwardCalls bejárása
            oc_container = mvj.find("s:OnwardCalls", ns)
            if oc_container is not None:
                for oc in oc_container.findall("s:OnwardCall", ns):
                    sp = _text(oc, "s:StopPointRef")
                    if sp and sp.upper() == stop_id.upper():
                        line = _text(mvj, "s:PublishedLineName") or _text(mvj, "s:LineRef") or "?"
                        dest = _text(mvj, "s:DestinationName") or "?"
                        when = _text(oc, "s:ExpectedDepartureTime") or _text(oc, "s:AimedDepartureTime")
                        if when:
                            iso = _parse_time(when)
                            try:
                                dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                            except Exception:
                                dt = None
                            if dt and now_utc <= dt <= limit:
                                out.append({"route": line, "destination": dest, "time_iso": iso})

    # idő szerint rendezés
    out.sort(key=lambda x: x["time_iso"])
    # route/destination üres értékek tisztítása
    for d in out:
        d["route"] = (d["route"] or "").strip()
        d["destination"] = (d["destination"] or "").strip()

    return out
