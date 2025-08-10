# gtfs_utils.py — csak helyi /data GTFS fájlokkal dolgozik (nincs letöltés)

import os
import zipfile
from io import TextIOWrapper
from datetime import datetime, timedelta
import pandas as pd

DATA_DIR = os.getenv("DATA_DIR", "data")  # itt vannak a GTFS fájlok vagy a gtfs.zip

def _available_mode():
    """Megnézi, hogy TXT fájlok vannak-e a /data-ban, vagy egy gtfs.zip."""
    needed = ["stops.txt", "stop_times.txt", "trips.txt", "routes.txt"]
    if all(os.path.exists(os.path.join(DATA_DIR, f)) for f in needed):
        return "folder"
    zip_path = os.path.join(DATA_DIR, "gtfs.zip")
    if os.path.exists(zip_path):
        return "zip"
    raise FileNotFoundError(
        "GTFS not found. Upload TXT files into 'data/' or upload a ZIP as 'data/gtfs.zip'."
    )

def _read_csv(name: str) -> pd.DataFrame:
    mode = _available_mode()
    if mode == "folder":
        return pd.read_csv(os.path.join(DATA_DIR, name))
    else:
        # beolvasás közvetlenül a ZIP-ből
        with zipfile.ZipFile(os.path.join(DATA_DIR, "gtfs.zip")) as z:
            with z.open(name, "r") as f:
                return pd.read_csv(TextIOWrapper(f, encoding="utf-8"))

def _to_today_datetime(dep_time_str: str) -> datetime | None:
    # dep_time lehet 24:xx:xx feletti is — kezeljük
    try:
        h, m, s = map(int, str(dep_time_str).split(":"))
    except Exception:
        return None
    add_days = h // 24
    h = h % 24
    now = datetime.now()
    dt = now.replace(hour=h, minute=m, second=s, microsecond=0)
    if add_days:
        dt += timedelta(days=add_days)
    return dt

def get_next_departures(stop_id: str, minutes_ahead: int = 60) -> list[dict]:
    """Következő indulások a megadott megállóból a következő N percben."""
    stop_times = _read_csv("stop_times.txt")
    trips = _read_csv("trips.txt")
    routes = _read_csv("routes.txt")

    # Csak az adott megálló
    st = stop_times[stop_times["stop_id"] == stop_id].copy()

    # Dúsítás, hogy legyen viszonylatszám és cél
    st = st.merge(trips[["trip_id", "route_id", "trip_headsign"]], on="trip_id", how="left")
    st = st.merge(routes[["route_id", "route_short_name"]], on="route_id", how="left")

    now = datetime.now()
    horizon = now + timedelta(minutes=minutes_ahead)

    out: list[dict] = []
    for _, row in st.iterrows():
        dep_dt = _to_today_datetime(row.get("departure_time", ""))
        if dep_dt is None:
            continue
        if now <= dep_dt <= horizon:
            out.append({
                "route": str(row.get("route_short_name", "")),
                "destination": str(row.get("trip_headsign", "")),
                "departure_time": dep_dt.strftime("%H:%M"),
            })

    out.sort(key=lambda x: x["departure_time"])
    return out
