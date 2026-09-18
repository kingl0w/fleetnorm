# The event schema

One normalized event describes one thing a truck reported. The normative
definition is [`schema/event.schema.json`](../schema/event.schema.json). The Go
type in `internal/event` mirrors it, and a test fails if the two drift apart, so
either one can be read as the truth.

Current version: **0.1.0**.

## The stability promise

Every event carries a `schema_version`, and it follows semantic versioning.

Within a major version, fields may be added, and optional fields that were
usually absent may start appearing. A consumer that ignores what it does not
recognize will keep working across every release in that major version.

A change that could break such a consumer needs a new major version. Removing a
field, renaming one, narrowing what a value may contain, or changing what a value
means all qualify. Adding a new severity level or a new `source_type` counts as
narrowing from the consumer's side, so those wait for a major bump too.

At 0.x the shape is settled and in use, but has not yet survived a full release
cycle with outside consumers. Treat 1.0.0 as the point where the promise above
becomes binding rather than intended.

## Required fields

Every event has all eight of these.

| Field | Type | Meaning |
| --- | --- | --- |
| `schema_version` | string | Semantic version of this schema, such as `0.1.0`. |
| `event_id` | string | Stable identifier for this event, unique within its `source`. Used for deduplication, so it must be the same string if the same event is read twice. |
| `vin` | string | Vehicle identification number, as the source reported it. |
| `occurred_at` | string | When the fault happened, RFC3339 with a zero UTC offset. |
| `received_at` | string | When fleetnorm ingested it, RFC3339 with a zero UTC offset. |
| `source` | string | The adapter that produced the event, which is normally the adapter's configured name. |
| `source_type` | string | One of `oem`, `tsp`, `file`. |
| `severity` | string | One of `info`, `low`, `medium`, `high`, `critical`. |

Both timestamps are required to be UTC. An adapter reading a source that reports
local time converts before emitting, and validation rejects anything carrying a
nonzero offset. Two timestamps exist because the difference between them is
often the interesting number: a fault that occurred six hours before anyone saw
it is a different problem from one that arrived in seconds.

## Optional fields

None of these are guaranteed. A consumer should treat every one as possibly
absent, and absent is not zero.

| Field | Type | Meaning |
| --- | --- | --- |
| `unit_id` | string | The fleet's own asset number for the truck, such as `T-1187`. Often more useful than the VIN to the people reading the event. |
| `spn` | integer | J1939 Suspect Parameter Number, which identifies what is faulty. |
| `fmi` | integer | J1939 Failure Mode Identifier, 0 through 31, which identifies how it is faulty. |
| `occurrence_count` | integer | How many times the source has seen this fault. |
| `lamp_status` | string | The lamp state the source reported, passed through verbatim, such as `amber` or `red`. Not normalized, because every vendor spells it differently and guessing loses information. |
| `odometer_km` | number | Odometer reading in kilometres at the time of the event. |
| `location` | object | `{ "lat": number, "lon": number }`, where the truck was. |
| `description` | string | Whatever human readable text the source supplied. |
| `tags` | object | String to string map of anything else. See below. |
| `raw` | any | The source payload, verbatim. See below. |

`spn` and `fmi` are passed through without interpretation. fleetnorm does not
decode fault codes and does not plan to, because a decoder that is subtly wrong
is worse than none at all.

## Two fields that carry the rest

Normalizing is lossy. Two fields exist so that nothing actually disappears.

### raw

`raw` holds the source payload exactly as it arrived, before anything was
interpreted. It can be any JSON value: usually an object, sometimes a string if
the source was not JSON at all.

Nothing may drop it. If a consumer needs something fleetnorm did not model, or
suspects the normalization got something wrong, `raw` is what makes the answer
recoverable instead of gone. It is the difference between a translator and a
lossy filter.

The one operational caution is that `raw` is never logged at info level, since a
source payload can carry more than the event does.

### tags

`tags` is where source fields that have no column in the schema go. An adapter
that finds a field it does not recognize puts it in `tags` rather than
discarding it. Values are strings; anything that was not a string in the source
keeps its JSON text as the value.

Owners can also use tags for their own labels, such as which terminal a truck
runs out of, and routing rules can match on them.

## The reserved tag namespace

Tag keys that begin with `fleetnorm.` are reserved for annotations fleetnorm
generates about an event. **Adapters must not write them.** An event carrying an
unrecognized key under that prefix fails validation, which keeps the prefix
meaningful: if a key starts with `fleetnorm.`, fleetnorm put it there.

Everything else in `tags` is yours.

### Current annotations

| Tag | Value | Meaning |
| --- | --- | --- |
| `fleetnorm.spn_out_of_range` | `"true"` | The source reported an `spn` above the 19 bit J1939 ceiling of 524287. |

`spn_out_of_range` marks a value as suspect, not as wrong. The SPN itself is kept
exactly as it arrived, never dropped and never clamped, because the original
number is what lets someone work out what the source meant. Treat it as
untrustworthy for decoding and reach for `raw` if the answer matters.

## An example

A full event, with every optional field populated. More examples live in
[`schema/examples/`](../schema/examples/), and a test validates each one against
the schema.

```json
{
  "schema_version": "0.1.0",
  "event_id": "replay-000002",
  "vin": "3AKJHHDR8LSLT1234",
  "occurred_at": "2026-09-14T13:40:00Z",
  "received_at": "2026-09-14T13:40:02Z",
  "source": "replay",
  "source_type": "file",
  "severity": "critical",
  "unit_id": "T-1187",
  "spn": 3226,
  "fmi": 20,
  "occurrence_count": 4,
  "lamp_status": "red",
  "odometer_km": 412883.5,
  "location": { "lat": 41.8781, "lon": -87.6298 },
  "description": "Aftertreatment 1 SCR Intake NOx sensor: data drifted high",
  "tags": {
    "derate_pending": "true",
    "source_fault_code": "SPN3226-FMI20",
    "terminal": "chicago-yard"
  },
  "raw": {
    "faultCode": "SPN3226-FMI20",
    "lamp": "RED_STOP",
    "occurrences": 4,
    "reportedAt": "2026-09-14T08:40:00-05:00",
    "vehicleId": "3AKJHHDR8LSLT1234"
  }
}
```

Note what the example shows about `raw`. The source reported a local timestamp
with an offset and a vendor specific fault code string. The normalized event has
UTC and separate `spn` and `fmi` numbers, and the original of both is still
sitting in `raw` where anyone can check the work.

## Validating an event

Consumers are welcome to validate against the published schema. It is standard
JSON Schema, draft 2020-12, with no custom keywords. The object is closed:
additional top level properties are rejected, which is what forces unmodeled
fields into `tags` instead of letting them accumulate at the top level.
