from __future__ import annotations

import re
from typing import Any, Optional, Set


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\ufeff", "").replace("_", " ").strip())


def human_name(value: Any) -> str:
    text = clean_text(value)
    text = re.sub(r"\bStand\s+([A-Z0-9]+)\b", r"[\1]", text, flags=re.I)
    text = re.sub(r"\bStop\s+([A-Z0-9]+)\b", r"[\1]", text, flags=re.I)
    return re.sub(r"\s+", " ", text).strip()


def norm(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", clean_text(value).lower())


def line_norm(value: Any) -> str:
    return re.sub(r"\s+", "", clean_text(value).upper())


def safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or str(value).strip() == "":
            return default
        return int(float(str(value)))
    except Exception:
        return default


def safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(str(value))
    except Exception:
        return None


def stop_code_from_name(stop_name: str) -> str:
    match = re.search(r"\[([A-Z0-9]{1,6})\]", clean_text(stop_name), flags=re.I)
    return match.group(1).upper() if match else ""


def public_stop_code(row: dict[str, Any]) -> str:
    by_name = stop_code_from_name(row.get("stop_name", ""))
    if by_name:
        return by_name
    platform = clean_text(row.get("platform_code"))
    if platform and len(platform) <= 6 and re.search(r"[A-Za-z]", platform):
        return platform.upper()
    return "BUS"


def short_destination(value: str) -> str:
    text = human_name(value).replace("Southampton, ", "")
    known = [
        ("Winchester Bus Station", "Winchester"), ("Hanover Buildings", "Southampton"),
        ("Vincents Walk", "Southampton"), ("Bargate", "Southampton"),
        ("City Centre", "City"), ("Adanac Park", "Adanac Park"),
        ("Lordshill", "Lordshill"), ("Weston", "Weston"), ("Millbrook", "Millbrook"),
        ("Sholing", "Sholing"), ("Hamble", "Hamble"), ("Romsey", "Romsey"),
        ("Eastleigh", "Eastleigh"), ("Chandlers Ford", "Chandlers Ford"),
        ("North Harbour", "North Harbour"), ("Thornhill", "Thornhill"),
        ("Fair Oak", "Fair Oak"), ("Hedge End", "Hedge End"), ("Totton", "Totton"),
        ("Calmore", "Calmore"), ("Bevois Valley", "Bevois Valley"),
    ]
    normalized = norm(text)
    for source, replacement in known:
        if norm(source) in normalized:
            return replacement
    text = re.sub(r"\s*\[[A-Z0-9]+\]\s*$", "", text).strip()
    return text[:27].rstrip() + "…" if len(text) > 28 else text


def destination_match(first: str, second: str) -> bool:
    normalized_first, normalized_second = norm(first), norm(second)
    if not normalized_first or not normalized_second:
        return False
    if normalized_first == normalized_second or normalized_first in normalized_second or normalized_second in normalized_first:
        return True
    short_first, short_second = norm(short_destination(first)), norm(short_destination(second))
    return bool(short_first and short_second and (short_first == short_second or short_first in short_second or short_second in short_first))


def extract_codes(*values: Any) -> Set[str]:
    result: Set[str] = set()
    for value in values:
        text = clean_text(value)
        if not text:
            continue
        result.add(norm(text))
        for match in re.finditer(r"\d{3,6}", text):
            result.add(match.group(0))
            result.add(match.group(0)[-4:])
        for part in re.split(r"[^A-Za-z0-9]+", text):
            if len(part.strip()) >= 3:
                result.add(part.strip().upper())
    return {value for value in result if value}


def fleet_from_vehicle_ref(vehicle_ref: str) -> str:
    text = clean_text(vehicle_ref)
    if not text:
        return ""
    numbers = re.findall(r"\d{2,6}", text)
    return numbers[-1][-4:] if numbers else text[-6:]
