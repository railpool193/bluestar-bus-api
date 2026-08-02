# Frontend data matrix

“Derived” means safely calculable after reliable matching. Blank means not a source. Unreliable values must be omitted or qualified.

| Field | GTFS | BODS/SIRI | Bustimes API | Derived | localStorage | Reliability rule |
|---|---:|---:|---:|---:|---:|---|
| line | yes | yes |  |  |  | reliable |
| routeColour | yes |  |  |  |  | fallback colour is presentation only |
| destination | yes | yes |  | yes |  | wording varies |
| destinationFull | yes | yes |  |  |  | wording varies |
| fleet |  | yes | yes | partial |  | may identify movable ticket equipment |
| vehicleRef |  | yes |  |  |  | not a physical-vehicle guarantee |
| registration |  |  | yes |  |  | omit when missing/ambiguous/withdrawn-only |
| vehicleType |  |  | yes |  |  | omit when missing/ambiguous/withdrawn-only |
| fuel |  |  | yes |  |  | community metadata may be incomplete |
| doubleDecker |  |  | yes |  |  | community metadata may be incomplete |
| coach |  |  | yes |  |  | community metadata may be incomplete |
| electric |  |  | yes |  |  | community metadata may be incomplete |
| livery |  |  | name only |  |  | external CSS is never rendered |
| branding |  |  | yes |  |  | escape external text |
| garage |  |  | yes |  |  | never use for identity matching |
| specialFeatures |  |  | yes |  |  | plain strings only |
| currentStop |  | yes |  |  |  | may be absent/stale |
| nextStop | yes | partial |  | yes |  | requires reliable trip progress |
| vehicleAtStop |  | yes |  |  |  | reliable when supplied |
| movementStatus |  | partial |  | yes |  | near-stop needs a geometry threshold |
| scheduledTime | yes |  |  |  |  | reliable for active service date |
| expectedTime |  | yes |  | yes |  | never relabel schedule as expected |
| delayMinutes |  | partial |  | yes |  | requires reliable live/schedule match |
| bearing |  | yes |  |  |  | may be absent |
| latitude | stops/shapes | yes |  |  |  | entity-dependent source |
| longitude | stops/shapes | yes |  |  |  | entity-dependent source |
| tripId | yes | partial |  |  |  | normalize references cautiously |
| serviceDate | yes | partial |  | yes |  | local service-day rules |
| shape | yes |  |  |  |  | only where GTFS geometry exists |
| stopSequence | yes | partial |  | yes |  | progress requires matching |
| favouriteState |  |  |  |  | yes | browser profile/device only |

Vehicle information: **bustimes.org**. It is metadata enrichment, not the live
source and not an official Bluestar fleet register.
