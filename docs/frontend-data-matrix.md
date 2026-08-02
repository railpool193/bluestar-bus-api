# Frontend data matrix

Legend: **yes** is directly available; **derived** is safely calculated; **registry** needs curated fleet data; **unreliable** must not be asserted as fact.

| Field | GTFS | SIRI/BODS | Derived | Fleet registry | Reliability / current source |
|---|---:|---:|---:|---:|---|
| route number | yes | yes |  |  | Reliable; GTFS `route_short_name`, SIRI line |
| route colour | yes |  |  |  | Reliable when `route_color` is populated; blue fallback is presentation only |
| destination | yes | yes | yes |  | Short form may be derived from full headsign |
| destinationFull | yes | yes |  |  | Feed quality varies but source is explicit |
| fleet |  | yes | yes |  | Parsed from supplied refs/codes only; do not invent |
| vehicleRef |  | yes |  |  | Reliable feed identifier, not necessarily fleet number |
| vehicle model |  |  |  | registry | Not reliably present in BODS |
| current stop |  | yes |  |  | SIRI monitored-call semantics; may be absent/stale |
| next stop | yes | partial | yes |  | Derivable only after reliable trip/progress match |
| vehicleAtStop |  | yes |  |  | Reliable when supplied |
| moving / at-stop / near-stop |  | partial | derived |  | At-stop direct; moving/near-stop need location/progress thresholds |
| scheduled time | yes |  |  |  | Reliable for active service date |
| expected time |  | yes | derived |  | Live time direct where present; otherwise never label schedule as expected |
| delayMinutes |  | yes/partial | derived |  | Direct if present, else live minus schedule after a reliable match |
| bearing |  | yes |  |  | May be absent |
| latitude / longitude | stops/shapes | yes |  |  | Vehicle position is SIRI; stop/shape positions are GTFS |
| tripId | yes | yes/partial |  |  | `DatedVehicleJourneyRef` may need normalization; mismatch rejects stop matching |
| serviceDate | yes | partial | derived |  | Derived with GTFS calendar and local service-day rules |
| shape | yes |  |  |  | Reliable when `shape_id` and points exist |
| stop sequence | yes | partial | derived |  | GTFS authoritative; live position requires matching |
| favourite state |  |  | local |  | Browser-local and reliable only on that profile/device |

## Optional fleet registry

`data/fleet_registry.json` is optional curated metadata, not a live-data substitute:

```json
{
  "schemaVersion": 1,
  "vehicles": {
    "1804": {
      "model": "Curated manufacturer/model",
      "operator": "BLUS",
      "vehicleRefs": ["known-feed-reference"],
      "source": "maintainer-verified source",
      "verifiedAt": "YYYY-MM-DD"
    }
  }
}
```

Lookup order should be exact fleet, then exact `vehicleRef` alias. Invalid entries are ignored, loading failure is non-fatal, and unmatched vehicles expose no model. Future implementation should validate schema, keep attribution/source metadata, and test unknown-fleet behaviour.
