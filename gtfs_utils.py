# gtfs_utils.py
# ---------------------------------------
# GTFS betöltés + megálló-keresés és stop_id <-> stop_code mappelés
# ---------------------------------------

from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Optional

import pandas as pd

# Betöltött GTFS táblák ide kerülnek
_GTFS: Dict[str, pd.DataFrame] = {}


def load_gtfs(folder: str | Path) -> None:
    """GTFS fájlok betöltése (legalább stops.txt szükséges)."""
    folder = Path(folder)
    stops = pd.read_csv(folder / "stops.txt", dtype=str).fillna("")
    _GTFS["stops"] = stops

    # Ha mást is használsz (routes/trips/stop_times), itt töltsd be:
    # _GTFS["routes"] = pd.read_csv(folder / "routes.txt", dtype=str).fillna("")
    # _GTFS["trips"]  = pd.read_csv(folder / "trips.txt", dtype=str).fillna("")
    # _GTFS["stop_times"] = pd.read_csv(folder / "stop_times.txt", dtype=str).fillna("")


def search_stops_by_name(q: str, limit: int = 12) -> List[Dict]:
    """
    Név szerinti keresés. Visszaad:
      - display_name: "Stop Name (code)"
      - stop_code: ATCO/NaPTAN kód (ha nincs, stop_id)
      - stop_id
      - stop_name
    """
    df = _GTFS["stops"]
    ql = q.strip().lower()
    if not ql:
        return []

    hits = df[df["stop_name"].str.lower().str.contains(ql, na=False)].copy()

    def make_display(row):
        code = (row.get("stop_code", "") or row["stop_id"]).strip()
        return f'{row["stop_name"]} ({code})'

    hits["display_name"] = hits.apply(make_display, axis=1)

    results: List[Dict] = []
    for _, row in hits.head(limit).iterrows():
        results.append(
            {
                "display_name": row["display_name"],
                "stop_code": (row.get("stop_code", "") or row["stop_id"]).strip(),
                "stop_id": row["stop_id"],
                "stop_name": row["stop_name"],
            }
        )
    return results


def map_to_stop_code(stop_ref: str) -> Optional[str]:
    """
    Ha a bemenet nem ATCO/NaPTAN (stop_code), próbáljuk stop_id -> stop_code mappelni.
    Ha nincs stop_code, fallback: maga a stop_id.
    Ha nem találtuk, None.
    """
    if not stop_ref:
        return None

    df = _GTFS["stops"]

    # 1) Ha már stop_code
    if "stop_code" in df.columns:
        m1 = df[df["stop_code"] == stop_ref]
        if not m1.empty:
            return stop_ref

    # 2) Ha stop_id
    m2 = df[df["stop_id"] == stop_ref]
    if not m2.empty:
        sc = (m2.iloc[0].get("stop_code", "") or "").strip()
        return sc if sc else stop_ref

    return None


def sibling_stop_codes_by_name(stop_name: str) -> List[str]:
    """
    Azonos stop_name-hez tartozó összes stop_code (ha nincs, stop_id).
    Hasznos A/B/C/CK/CM/CO peron-variánsokhoz.
    """
    df = _GTFS["stops"]
    sib = df[df["stop_name"] == stop_name].copy()

    codes: List[str] = []
    for _, r in sib.iterrows():
        code = (r.get("stop_code", "") or r["stop_id"]).strip()
        if code:
            codes.append(code)

    # dedup
    seen = set()
    out: List[str] = []
    for c in codes:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out
