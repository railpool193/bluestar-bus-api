from __future__ import annotations

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from app.utils.text_utils import clean_text, extract_codes, fleet_from_vehicle_ref, human_name, line_norm, safe_float, short_destination
from app.utils.time_utils import parse_iso_dt


class SIRIParseError(ValueError):
    pass


@dataclass(frozen=True)
class ParsedSIRI:
    vehicles: tuple[dict[str, Any], ...]
    raw_count: int


def xml_text(node: Optional[ET.Element], path: str = "") -> str:
    if node is None:
        return ""
    names = [part for part in path.split("/") if part]
    candidates = [node]
    for name in names:
        candidates = [child for parent in candidates for child in parent if child.tag.split("}")[-1] == name]
        if not candidates:
            for element in node.iter():
                if element.tag.split("}")[-1] == name:
                    candidates = [element]
                    break
    target = candidates[0] if names and candidates else node
    return clean_text(target.text) if target is not None and target.text else ""


def children_by_local(root: ET.Element, name: str) -> list[ET.Element]:
    return [element for element in root.iter() if element.tag.split("}")[-1] == name]


def _call_time(call: Optional[ET.Element], expected: bool) -> Optional[datetime]:
    if call is None:
        return None
    fields = ("ExpectedDepartureTime", "ExpectedArrivalTime") if expected else ("AimedDepartureTime", "AimedArrivalTime")
    return parse_iso_dt(xml_text(call, fields[0]) or xml_text(call, fields[1]))


def parse_vehicle_monitoring(
    payload: bytes,
    *,
    reference_time: datetime,
    operator_filter: str = "",
    max_age_seconds: int = 360,
    max_xml_bytes: int = 8 * 1024 * 1024,
) -> ParsedSIRI:
    if len(payload) > max_xml_bytes:
        raise SIRIParseError("SIRI XML exceeds configured size limit")
    lowered = payload.lower()
    if b"<!doctype" in lowered or b"<!entity" in lowered:
        raise SIRIParseError("DTD and ENTITY declarations are not allowed")
    try:
        root = ET.fromstring(payload)
    except ET.ParseError as exc:
        raise SIRIParseError(f"Malformed SIRI XML: {exc}") from exc

    vehicles: list[dict[str, Any]] = []
    records = children_by_local(root, "MonitoredVehicleJourney")
    parents = {child: parent for parent in root.iter() for child in parent}
    for journey in records:
        try:
            operator = xml_text(journey, "OperatorRef").upper()
            if operator_filter and operator and operator != operator_filter.upper():
                continue
            line = xml_text(journey, "PublishedLineName") or xml_text(journey, "LineRef")
            if not line:
                continue
            destination = xml_text(journey, "DestinationName") or xml_text(journey, "DestinationRef")
            vehicle_ref = xml_text(journey, "VehicleRef") or xml_text(journey, "VehicleMonitoringRef")
            dated_ref = xml_text(journey, "FramedVehicleJourneyRef/DatedVehicleJourneyRef") or xml_text(journey, "DatedVehicleJourneyRef")
            block_ref = xml_text(journey, "BlockRef")
            activity = parents.get(journey)
            while activity is not None and activity.tag.split("}")[-1] not in {"VehicleActivity", "VehicleMonitoringDelivery"}:
                activity = parents.get(activity)
            recorded = parse_iso_dt(xml_text(journey, "RecordedAtTime") or xml_text(activity, "RecordedAtTime") or xml_text(journey, "ValidUntilTime"))
            if recorded:
                age = (reference_time - recorded).total_seconds()
                if age > max_age_seconds or age < -60:
                    continue
            call = next(iter(children_by_local(journey, "MonitoredCall")), None)
            aimed, expected = _call_time(call, False), _call_time(call, True)
            delay = int(round((expected - aimed).total_seconds() / 60)) if aimed and expected else None
            at_stop = xml_text(call, "VehicleAtStop").lower() == "true" if call is not None else False
            fleet = fleet_from_vehicle_ref(vehicle_ref)
            vehicles.append({
                "line": clean_text(line), "lineNorm": line_norm(line),
                "destination": short_destination(destination), "destinationFull": human_name(destination),
                "operator": operator, "vehicleRef": vehicle_ref, "fleet": fleet,
                "datedVehicleJourneyRef": dated_ref, "blockRef": block_ref,
                "codes": list(extract_codes(vehicle_ref, fleet, dated_ref, block_ref)),
                "latitude": safe_float(xml_text(journey, "VehicleLocation/Latitude")),
                "longitude": safe_float(xml_text(journey, "VehicleLocation/Longitude")),
                "bearing": safe_float(xml_text(journey, "Bearing")) or 0,
                "recordedAt": recorded.isoformat() if recorded else "",
                "currentStopRef": xml_text(call, "StopPointRef") if call is not None else "",
                "currentStopName": human_name(xml_text(call, "StopPointName")) if call is not None else "",
                "vehicleAtStop": at_stop,
                "aimedTime": aimed.isoformat() if aimed else "", "expectedTime": expected.isoformat() if expected else "",
                "liveTime": (expected or aimed).isoformat() if expected or aimed else "",
                "delayMinutes": delay, "status": "At stop" if at_stop else "Moving",
            })
        except Exception:
            continue
    return ParsedSIRI(tuple(vehicles), len(records))
