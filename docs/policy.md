# Routing rules

A rule says which events go where. It has two parts: a `match` that decides
whether the rule applies to an event, and a `route` listing the outputs that
should receive it.

```yaml
rules:
  - match: { severity: [critical, high] }
    route: [my-shop, console]

  - match: { source: replay, spn: [3226, 3216, "4000-4100"] }
    route: [my-shop]

  - match: {}
    route: [console]
```

## How rules are evaluated

Rules run from top to bottom, and **every rule that matches fires**. This is not
first match wins. An event that satisfies three rules goes to the union of all
three destinations.

Each destination receives the event once, however many rules named it. In the
example above a critical event with SPN 3226 matches all three rules, and the
result is one delivery to `my-shop` and one to `console`, in that order. Order
follows the rules: a destination takes its position from the first rule that
chose it.

An event that matches no rule goes nowhere. That is a legitimate outcome and it
is recorded in the audit log as a drop with the reason `no matching rule`, so a
quiet pipeline can be told apart from a broken one.

Why every rule rather than the first? Because the common shape is one rule per
concern. A rule that sends critical faults to the shop, a rule that sends
everything from one terminal to that terminal's own system, a rule that logs
everything. Under first match wins those interfere with each other, and getting
the order right becomes a puzzle. Here they compose.

## Inside one match

Every condition a match states must hold. Conditions are combined with AND, so
`{ source: replay, severity: [high] }` needs both.

A condition left out is not a condition at all. A match that mentions only
`severity` says nothing about VIN, and an event with no VIN would still satisfy
it. A match with no conditions at all, written `{}`, is a catch all.

Where a condition takes a list, the list is OR. `severity: [critical, high]`
matches either.

## What can be matched

| Key | Form | Matches when |
| --- | --- | --- |
| `vin` | string | The event's `vin` is exactly this. |
| `unit_id` | string | The event's `unit_id` is exactly this. |
| `source` | string | The event's `source` is exactly this. |
| `source_type` | string | The event's `source_type` is exactly this. One of `oem`, `tsp`, `file`. |
| `severity` | list of strings | The event's `severity` is one of these. |
| `spn` | list of numbers and ranges | The event's `spn` is one of these, or inside one of the ranges. |
| `fmi` | number | The event's `fmi` is exactly this. |
| `tags` | map of string to string | The event has every one of these tags, with exactly these values. |

All comparisons are exact and case sensitive. A rule that says `severity: [HIGH]`
matches nothing, although the config loader rejects that particular mistake at
startup. A VIN written in lowercase will not match a VIN stored in uppercase.

### SPN lists and ranges

Entries in an `spn` list are either a single number or an inclusive range written
as a string:

```yaml
spn: [3226, 3216, "4000-4100"]
```

That matches SPN 3226, SPN 3216, and everything from 4000 through 4100 including
both ends. An inverted range such as `"4100-4000"` is rejected when the config
loads.

### Absent is not zero, and it is not a wildcard

An event with no `spn` does not match a rule that asks about `spn`. The same
holds for `fmi`. A rule of `fmi: 0` matches only events whose FMI is actually 0,
and never events that have no FMI at all. This matters because plenty of events
carry no fault codes, and treating a missing code as zero would route them as
though they carried FMI 0, which is a real failure mode meaning "data valid but
above normal".

Tags behave the same way. A rule asking for `tags: { trailer: "" }` matches an
event that has a `trailer` tag whose value is the empty string, not an event
with no `trailer` tag.

## Some worked rules

Send anything serious to the shop, and keep a copy of everything on the console:

```yaml
rules:
  - match: { severity: [critical, high] }
    route: [my-shop]
  - match: {}
    route: [console]
```

Send aftertreatment faults from one specific truck to a specialist, no matter how
severe they are:

```yaml
rules:
  - match: { vin: 3AKJHHDR8LSLT1234, spn: ["3216-3250"] }
    route: [emissions-specialist]
```

Route by something the source told us that the schema does not model, using a tag
the adapter preserved:

```yaml
rules:
  - match: { tags: { derate_pending: "true" } }
    route: [my-shop, dispatch]
```

Route everything from one terminal to that terminal's own system, using a tag the
owner added:

```yaml
rules:
  - match: { tags: { terminal: chicago-yard } }
    route: [chicago-maintenance]
```

## Validation

Rules are checked when the config loads, and a bad one prevents startup rather
than failing quietly later:

* Every name in a `route` must be a configured output. A typo is an error, not a
  silent drop.
* A rule must route somewhere. An empty `route` is an error.
* Severity values and `source_type` values must be ones the schema defines.
* `fmi` must be between 0 and 31.
* SPN ranges must not be inverted or negative.

Every problem is reported at once, so one run tells you everything to fix.

## What happens after a rule matches

Matching decides destinations. What happens next is delivery, and it is worth
knowing where the boundaries are.

Each output has its own queue and its own goroutine. An event routed to three
outputs is handed to three queues, and each one proceeds independently. A slow
endpoint delays only its own deliveries.

If an output's queue is full, the event is dropped for that output, and the drop
is recorded with the reason `output queue full`. It is not retried later and it
does not block the others. Blocking would mean one broken endpoint stops
everything, which is the failure this design is avoiding. If drops appear in the
audit log, the queue is too small or the endpoint is too slow, and both are
visible in `/metrics`.

Delivery itself is at least once. A webhook that fails is retried with growing
delays and some jitter, up to `max_attempts`, unless the failure is one that
will not improve. A 4xx response other than 408 or 429 is permanent and is not
retried.

Every one of those outcomes, delivered, failed, and dropped, is written to the
audit log with the event id, the output, the attempt count, and the reason. An
owner asking where a particular fault went can answer the question from the
database rather than from logs.

## Deduplication

Before routing, fleetnorm checks whether it has already seen the event, keyed by
the adapter name and the event's `event_id`. A repeat is dropped before it
reaches the rules, which is what stops a source that resends the same events from
producing duplicate deliveries.

The record of what has been seen is kept for `store.dedupe_retention`, seven days
by default, and swept afterwards. An event older than that window turning up
again would be delivered again. That is the at least once promise doing what it
says.

One consequence is worth stating: `event_id` must be stable. If a source assigns
a new id to the same underlying fault each time it reports it, deduplication
cannot help, and the rules will fire every time.
