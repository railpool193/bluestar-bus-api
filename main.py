# main.py
import os
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, Query, Path as FPath, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

import pandas as pd

# Saját modul: élő adatok a BODS SIRI-VM-ből
import siri_live  # ensure siri_live.py is in the same directory

APP_DIR = Path(__file__).resolve().parent
DATA_DIR = APP_DIR / "data"
STOPS_FILE = DATA_DIR / "stops.txt"
INDEX_HTML = APP_DIR / "index.html"

logger = logging.getLogger("uvicorn.error")
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Bluestar Bus API", version="1.0.0")

# CORS – ha máshonnan is akarod hívni
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------- GTFS: stops betöltése és egyszerű kereső -------- #

_stops_df: Optional[pd.DataFrame] = None


def _load_stops() -> pd.DataFrame:
    global _stops_df
    if _stops_df is not None:
        return _stops_df

    if not STOPS_FILE.exists():
        logger.error("GTFS stops.txt nem található: %s", STOPS_FILE)
        raise FileNotFoundError(f"Missing GTFS file: {STOPS_FILE}")

    df = pd.read_csv(STOPS_FILE, dtype=str).fillna("")
    # Szükséges oszlopok biztosítása
    for col in ["stop_id", "stop_name"]:
        if col not in df.columns:
            raise ValueError(f"stops.txt missing column: {col}")

    # Néhány feed stop_code-ot is ad – ha van, őrizzük meg
    if "stop_code" not in df.columns:
        df["stop_code"] = ""

    # Könnyebb kereséshez készítsünk egy 'q' mezőt
    df["q"] = (df["stop_name"].str.lower() + " " + df["stop_code"].str.lower()).str.strip()
    _stops_df = df
    logger.info("GTFS stops betöltve: %d sor", len(df))
    return _stops_df


def search_stops(q: str, limit: int = 10) -> List[Dict[str, str]]:
    df = _load_stops()
    qn = q.strip().lower()
    if not qn:
        return []

    hits = df[df["q"].str.contains(qn, na=False)].head(limit)
    out = []
    for _, r in hits.iterrows():
        disp = r["stop_name"]
        if r.get("stop_code") and str(r["stop_code"]).strip().lower() != "nan" and r["stop_code"] != "":
            disp = f"{disp} ({r['stop_code']})"
        out.append(
            {
                "display_name": disp,
                "stop_id": r["stop_id"],
                "stop_code": r.get("stop_code", ""),
            }
        )
    return out


# ---------------------- ROUTES ---------------------- #

@app.get("/", include_in_schema=False)
def root():
    """
    Visszaadja az index.html-t (egyszerű UI).
    Ha nincs index.html, akkor egy linkgyűjtő JSON megy vissza.
    """
    if INDEX_HTML.exists():
        return FileResponse(str(INDEX_HTML))
    # Fallback: JSON „kezdőlap”
    return {
        "message": "Bluestar Bus API",
        "links": {
            "docs": "/docs",
            "status": "/status",
            "stops_example": "/stops?q=hanover",
            "departures_example": "/departures/1980SN12619A?minutes=60",
        },
    }


@app.get("/status")
def status():
    """
    Egészségjelentés: GTFS és SIRI elérhetőség.
    """
    gtfs_ok = False
    gtfs_error = None
    try:
        gtfs_ok = len(_load_stops()) > 0
    except Exception as e:
        gtfs_error = str(e)

    siri_ok = False
    siri_error = None
    try:
        # siri_live modul egyszerű "ping": API kulcs meglétét és cache-t is érinti
        siri_ok = siri_live.is_available()
    except Exception as e:
        siri_error = str(e)

    return {
        "status": "ok" if gtfs_ok else "degraded",
        "gtfs_loaded": gtfs_ok,
        "siri_available": siri_ok,
        "gtfs_error": gtfs_error,
        "siri_error": siri_error,
    }


@app.get("/stops")
def stops(q: str = Query(..., description="Részlet a megálló nevéből vagy kódjából"), limit: int = 10):
    """
    Megálló keresés (GTFS).
    """
    try:
        results = search_stops(q, limit=limit)
        return {"query": q, "results": results}
    except Exception as e:
        logger.exception("Hiba a megálló keresés során")
        raise HTTPException(status_code=500, detail=f"Hiba a keresés közben: {e}")


@app.get("/departures/{stop_id}")
def departures(
    stop_id: str = FPath(..., description="GTFS stop_id, pl. 1980SN12619A"),
    minutes: int = Query(60, ge=1, le=240, description="Előrenézési idő percben (1–240)"),
):
    """
    Indulások (SIRI-VM élő adatok). Visszaadjuk a következő érkezéseket a megadott időablakban.
    A `siri_live.get_next_departures` feladata, hogy a dict-eket a következő kulcsokkal adja:
      - route (str)
      - destination (str)
      - time (ISO vagy HH:MM)
      - is_live (bool) – ha True, az idő élő
      - scheduled_time (opcionális)
      - delay_seconds (opcionális)
    """
    try:
        results = siri_live.get_next_departures(stop_id=stop_id, minutes=minutes)
        # Biztonsági normalizálás
        norm: List[Dict[str, Any]] = []
        for d in results:
            norm.append(
                {
                    "route": str(d.get("route", "")),
                    "destination": str(d.get("destination", "")),
                    "time": str(d.get("time", "")),
                    "is_live": bool(d.get("is_live", False)),
                    "scheduled_time": d.get("scheduled_time"),
                    "delay_seconds": d.get("delay_seconds"),
                }
            )
        return {"stop_id": stop_id, "minutes": minutes, "departures": norm}
    except siri_live.LiveDataError as e:
        # A modul explicit hibája: 502
        logger.warning("SIRI hiba: %s", e)
        return JSONResponse(
            status_code=502,
            content={"stop_id": stop_id, "minutes": minutes, "departures": [], "error": str(e)},
        )
    except Exception as e:
        logger.exception("Hiba az indulások lekérésekor")
        raise HTTPException(status_code=500, detail=f"Hiba az indulások lekérésekor: {e}")


# --------- Opcionális: Uvicorn helyi futtatáshoz --------- #
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
