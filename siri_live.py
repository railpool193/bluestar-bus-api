import os
import requests
import xmltodict

# --- Beállítások (ENV) ---
BODS_FEED_URL = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/"
BODS_API_KEY = os.getenv("BODS_API_KEY") or os.getenv("BODS_API", "")

# --- Saját kivételek ---
class SiriAuthError(Exception):
    pass

class SiriNoData(Exception):
    pass

# --- Health check ---
def health_check():
    if not BODS_API_KEY:
        return False, "Missing BODS_API_KEY"
    # nem hívjuk meg a nagy feedet csak azért, hogy 'ok' legyen
    return True, None

# --- HTTP kérés csomagoló ---
def _req(params: dict) -> str:
    p = {"api_key": BODS_API_KEY, **params}
    r = requests.get(BODS_FEED_URL, params=p, timeout=20)
    if r.status_code in (401, 403):
        raise SiriAuthError(f"HTTP {r.status_code} – check BODS_API_KEY")
    r.raise_for_status()
    return r.text

# --- XML → list konverzió ---
def _parse_departures(xml_text: str):
    doc = xmltodict.parse(xml_text)
    sd = (doc.get("Siri") or {}).get("ServiceDelivery") or {}
    deliveries = sd.get("StopMonitoringDelivery") or []
    if isinstance(deliveries, dict):
        deliveries = [deliveries]

    visits = []
    for d in deliveries:
        msv = d.get("MonitoredStopVisit")
        if not msv:
            continue
        if isinstance(msv, dict):
            msv = [msv]
        for v in msv:
            mvj = v.get("MonitoredVehicleJourney", {}) or {}
            call = mvj.get("MonitoredCall", {}) or {}

            line = mvj.get("LineRef")
            dest = mvj.get("DestinationName") or mvj.get("DestinationRef")
            aimed = call.get("AimedDepartureTime")
            expected = call.get("ExpectedDepartureTime")

            if not (line and dest and (aimed or expected)):
                continue

            visits.append({
                "route": str(line),
                "destination": str(dest),
                "time": str(expected or aimed),
                "is_live": bool(expected),   # ha Expected van, akkor élő
            })
    return visits

# --- Publikus függvény a lekéréshez ---
def get_live_departures(stop_id: str, minutes: int = 60):
    if not BODS_API_KEY:
        raise SiriAuthError("Missing BODS_API_KEY")

    params = {
        "MonitoringRef": stop_id,
        "MaximumStopVisits": 60,
        "PreviewInterval": f"PT{max(1, int(minutes))}M",
    }
    xml_text = _req(params)
    results = _parse_departures(xml_text)
    if not results:
        raise SiriNoData("No departures in the time window")
    # Rendezés ISO idő szerint
    results.sort(key=lambda x: x["time"])
    return results

# --- Stop kereső (helykitöltő) ---
def search_stops(query: str):
    """
    Ha van GTFS-ből épített megálló-adatbázisod, itt csatlakoztasd.
    Most üres listát adunk vissza, a UI ettől még használható Stop ID-val.
    """
    q = (query or "").strip().lower()
    return []
