
import requests
import zipfile
import pandas as pd
from datetime import datetime, timedelta
import os
from io import BytesIO

# --- CONFIG ------------------------------------------------------------------
# NOTE: Update this URL when Bluestar publishes a new GTFS period.
# You can find the fresh link on: https://www.bluestarbus.co.uk/open-data
# This example points to a likely naming convention; change if it 404s.
GTFS_URL = os.getenv("GTFS_URL", "https://www.bluestarbus.co.uk/open-data/download/gtfs-2025-08-11-to-2025-08-31.zip")
DATA_DIR = os.getenv("DATA_DIR", "data")

# Vincent's Walk stop IDs
STOP_CK = "1980SN12619E"   # Vincent's Walk [CK]
STOP_CM = "1980HAA13371"   # Vincent's Walk [CM]

# --- IMPLEMENTATION -----------------------------------------------------------

def _ensure_gtfs_downloaded():
    os.makedirs(DATA_DIR, exist_ok=True)
    # If core files are already present, assume ready
    needed = ["stops.txt", "stop_times.txt", "trips.txt", "routes.txt"]
    if all(os.path.exists(os.path.join(DATA_DIR, f)) for f in needed):
        return

    resp = requests.get(GTFS_URL, timeout=60)
    resp.raise_for_status()

    with zipfile.ZipFile(BytesIO(resp.content)) as zf:
        zf.extractall(DATA_DIR)

def _to_today_datetime(dep_time_str: str) -> datetime:
    """Convert a GTFS time (HH:MM:SS, possibly >=24h) to today's datetime."""
    try:
        h, m, s = map(int, dep_time_str.split(':'))
    except Exception:
        return None

    now = datetime.now()
    # Handle times >= 24h per GTFS spec by rolling into next day
    add_days = h // 24
    h = h % 24
    dt = now.replace(hour=h, minute=m, second=s, microsecond=0)
    if add_days:
        dt = dt + timedelta(days=add_days)
    return dt

def get_next_departures(stop_id: str, minutes_ahead: int = 60):
    """Return next departures for the given stop within minutes_ahead."""
    _ensure_gtfs_downloaded()

    # Load minimal set
    stops = pd.read_csv(os.path.join(DATA_DIR, "stops.txt"))
    stop_times = pd.read_csv(os.path.join(DATA_DIR, "stop_times.txt"))
    trips = pd.read_csv(os.path.join(DATA_DIR, "trips.txt"))
    routes = pd.read_csv(os.path.join(DATA_DIR, "routes.txt"))

    # Filter to this stop
    st = stop_times[stop_times["stop_id"] == stop_id].copy()

    now = datetime.now()
    horizon = now + timedelta(minutes=minutes_ahead)

    out = []
    # Merge to avoid many lookups
    st = st.merge(trips[["trip_id", "route_id", "trip_headsign"]], on="trip_id", how="left")
    st = st.merge(routes[["route_id", "route_short_name"]], on="route_id", how="left")

    for _, row in st.iterrows():
        dep_str = str(row.get("departure_time", ""))
        dep_dt = _to_today_datetime(dep_str)
        if dep_dt is None:
            continue

        if now <= dep_dt <= horizon:
            out.append({
                "route": str(row.get("route_short_name", "")),
                "destination": str(row.get("trip_headsign", "")),
                "departure_time": dep_dt.strftime("%H:%M"),
            })

    # Sort by time
    out.sort(key=lambda x: x["departure_time"])
    return out
