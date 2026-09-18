# Writing an adapter

An adapter pulls events from one source and hands back normalized ones. That is
all it does. It does not decide where events go, deduplicate them, retry
deliveries, or write to the database. The pipeline owns all of that.

```go
type Adapter interface {
    Name() string
    Poll(ctx context.Context, since Cursor) ([]event.Event, Cursor, error)
}
```

`Name()` returns the adapter's configured name. It scopes cursors and
deduplication, so it has to be stable across restarts. Renaming an adapter in the
config makes fleetnorm forget how far it had read and what it had already
delivered.

## The cursor

```go
type Cursor string
```

A cursor is your bookmark, and it is opaque to everything except the adapter that
issued it. fleetnorm stores the last one you returned and hands it back on the
next poll, unchanged. Put whatever you need in it: a record count, a timestamp, a
vendor page token, a small JSON document.

The empty cursor means the adapter has never polled and should start from the
beginning.

Two rules govern it. Only advance past events you actually returned, or past
records you deliberately skipped, never past records you have not looked at. And
a poll that fails returns the cursor it was given, unchanged, because advancing
on failure loses events permanently.

## Two kinds of failure

This is the part to get right, and the two cases are handled quite differently.

A **per record failure** is one record that will not parse or will not validate.
It is not a reason to stop. Skip it, report it through the `SkipFunc` you were
given so it reaches the audit log as a drop with a reason and a locator, advance
past it, and return the good events with a nil error. One malformed row must not
block the rows behind it, possibly forever. A vendor that starts emitting one
broken record an hour should cost you one audit row an hour, not a stalled feed.

A **whole poll failure** means no progress was made at all: the source is
unreachable, credentials were rejected, the response body will not decode. Return
no events, return the cursor unchanged, and return the error. The pipeline logs
it, counts it, and tries again on the next tick.

The distinction is about whether you can keep going, not about how serious the
problem is. A 500 from a vendor is a whole poll failure. One record with a
timestamp in a format nobody has seen before is a per record failure.

## The ceiling on skipping

```go
const (
    SkipCeilingMinRecords = 10
    SkipCeilingFraction   = 0.5
)

func SkipCeilingExceeded(seen, skipped int) bool
```

If a poll sees at least ten records and skips more than half of them, return an
error and do not advance the cursor, even though each individual failure was a
per record one.

This exists because of how the alternative fails. An adapter that skips
everything looks healthy. No errors, a cursor advancing normally, and a trickle
of events, or none at all. Nobody gets paged for an absence. A vendor renames a
field, every record starts failing validation, and the fleet finds out weeks
later when a truck that threw a derate fault never reached the shop. Skipping is
for noise. Skipping everything means the source changed shape and a person needs
to look at it.

Call `adapter.SkipCeilingExceeded(seen, skipped)` rather than reimplementing the
rule, so every adapter trips at the same place.

Report skips only once the poll is known to have succeeded. A poll that trips the
ceiling made no progress and those records will be read again, so auditing them
now would only duplicate rows on every retry.

## Naming a record that has no id

A skipped record often has no usable `event_id`, which may be exactly why it was
skipped. The audit log still needs to name it:

```go
adapter.SyntheticID("replay", "line:42")  // "replay:line:42"
```

The locator points at the record in the source. Use `line:N` for anything line
oriented, such as CSV or JSON Lines, and `index:N` for the Nth element of a JSON
array, counting from zero the way a JSON tool would. A vendor API should use
whatever identifies the record on their side, such as a page and item number or a
request id.

Prefixing with the adapter name means a synthetic id can never collide with a
real `event_id` from that source.

These ids are only ever used in audit rows for records that were skipped. They
are never attached to an emitted event. An event fleetnorm routes carries an
`event_id` that its source chose. A record with no id is not an event that can be
deduplicated, and it does not get one invented for it.

## What to do before emitting an event

The pipeline routes whatever `Poll` returns, so all of the following is the
adapter's responsibility.

Convert timestamps to UTC. Both `occurred_at` and `received_at` must have a zero
offset, and validation rejects anything else. A timestamp that arrived as
`2026-09-14T08:40:00-05:00` is emitted as `13:40:00Z`.

Populate `raw` with the source payload exactly as it arrived. Lossy
normalization is this project's main risk and `raw` is how a consumer recovers
what you failed to model. Never drop it.

Move unrecognized source fields into `tags`. Never discard a field because there
is no column for it. Values that were not strings keep their JSON text.

Call `e.Annotate()` and then `e.Validate()`, and emit only what validates.

Set `source` to your adapter name and `source_type` to `oem`, `tsp` or `file`.

### The reserved tag prefix

Tag keys beginning with `fleetnorm.` belong to fleetnorm's own annotations.
Adapters must not write them. Validation rejects any unrecognized key under that
prefix, so an adapter that squats there simply stops emitting events. The current
annotations are listed in [schema.md](schema.md).

### An explicit source overrides the adapter name

If a record already carries a `source`, the file adapter keeps it rather than
overwriting it with the adapter's own name. That is what makes replaying a
captured vendor stream useful: the events stay labelled `geotab` instead of
`replay`.

The consequence is worth stating plainly. A routing rule matching on the
adapter's name will not match those records. A rule of `match: {source: replay}`
sees nothing from a file whose records say `"source": "geotab"`. Match on
`source_type`, `vin` or `severity` instead, or remove the field from the replay
file.

## The file adapter does the opposite, on purpose

`internal/adapter/file` defaults to strict. One record it cannot normalize fails
the whole poll and the cursor does not move. That is the opposite of everything
above, and it is a deliberate deviation rather than the pattern to copy.

The reasoning is about who wrote the input. A file adapter reads a local file
that the fleet owner controls and probably wrote by hand. A malformed record
there is a mistake to fix, and failing loudly puts it in front of the person who
can fix it. An OEM or telematics adapter has no such luxury. It reads whatever a
vendor sends, forever, and has to keep going.

Setting `strict: false` on a file adapter, or calling `file.NewLenient`, gives
the standard behavior instead: skip, audit, advance, and respect the ceiling.
Both paths live in that one adapter on purpose, so there is a working reference
implementation of each.

If you are writing an adapter that talks to a network, follow the contract, not
the file adapter's default.

## Before you call it done

Does `Name()` return the configured name, and is it stable across restarts?

Does the cursor advance only past records you handled, and stay unchanged when a
poll fails?

Do per record failures skip, report, and continue with a nil error?

Do whole poll failures return no events and leave the cursor alone?

Is `adapter.SkipCeilingExceeded` consulted before any skip is reported?

Are timestamps converted to UTC, is `raw` populated verbatim, and are
unrecognized fields in `tags` with nothing under `fleetnorm.`?

Is `Annotate()` called, then `Validate()`, before anything is emitted?

Are there table driven tests covering both kinds of failure?

## The Geotab adapter

`internal/adapter/geotab` reads `FaultData` from a MyGeotab `GetFeed` change
stream. It is the first adapter reading data neither fleetnorm nor the fleet
owner wrote, so it follows the contract above rather than the file adapter's
strict default: one unusable record is skipped and audited, and the feed keeps
moving.

Four things about Geotab's model do not line up with ours, and each one is a
decision recorded here rather than a surprise in the code.

### event_id carries the version

```
event_id = "<geotab id>:<version>"
```

`GetFeed` resends a record whenever it changes, with a newer version. Dismissing
a fault writes `dismissDateTime` and `dismissUser`; an `count` that increments
does the same. Both are resends of a record we have already delivered.

If `event_id` were the Geotab id alone, every one of those revisions would hit
`MarkSeen`, be recognized as already delivered, and vanish. Nobody downstream
would ever learn that a critical fault was dismissed, because the only event
saying so was deduplicated away.

So every revision is its own event. The original id still survives, on every
event, in two tags:

| Tag | Value |
| --- | --- |
| `geotab.source_id` | the Geotab `id`, unchanged across revisions |
| `geotab.version` | the `version` of this particular revision |

A consumer that wants the latest state of one fault groups by
`geotab.source_id` and takes the highest `geotab.version`.

There is an open schema question behind this, about whether a normalized event
should be able to say "this supersedes that" as a first-class field rather than
by convention in tags. It is written up in [schema.md](schema.md) under open
questions. The tags are what exists today; nothing in the code decides that
question.

### seed_from is required, and a missing one fails at load

`GetFeed` called with no `fromVersion` returns no data at all. It returns only
`toVersion`, the newest version in the system, so that the next call has
somewhere to start. Backfill comes from a `fromDate` in the `search` object,
which the API uses independently of `fromVersion` and only on the first request.

A feed started with neither a cursor nor a seed date therefore reads nothing,
forever. The cursor advances, no errors are raised, `/healthz` stays green, and
no events arrive. The skip ceiling cannot catch it, because nothing is being
skipped — there is nothing there at all.

`seed_from` is required on every geotab adapter and a config without it does not
load. It is either a duration back from the time of the poll, `720h`, or an
absolute RFC3339 timestamp, `2026-01-01T00:00:00Z`. Wanting only new data is
written `seed_from: 0s`, which is a statement of intent rather than an omission.

It is used on the first request only, where it is sent as `search.fromDate` with
no `fromVersion`. Once a cursor exists it is ignored entirely. The seeded poll
logs at info with the date it used and how many events that produced, so the
seed is visible in the log rather than inferred from an empty database.

### The enrichment cache

`FaultData` carries id references, not resolved objects, so `diagnostic`,
`failureMode`, `controller` and `device` each need a separate `Get`. That is
where the interesting fields come from:

| Normalized field | Resolved from |
| --- | --- |
| `spn` | `diagnostic`, whose `code` is an SPN when its `diagnosticType` says so |
| `fmi` | `failureMode`, whose `code` is the FMI |
| `vin` | `device.vehicleIdentificationNumber` |
| `unit_id` | `device.name` |
| `geotab.controller` | `controller.name` |

The `Get` budget is 500 calls a minute and `GetFeed` is 60, so the tight limit is
the one enrichment spends. A page of 10,000 faults naively resolved would be
40,000 `Get` calls against a 500/min budget, which is eight minutes of API
allowance for one poll.

Two things keep it in budget. Ids are cached, keyed by entity type and id, with a
configurable refresh interval (`cache_refresh`, default 1h) and a bounded size,
so a diagnostic or a device is fetched once and not again. What is left after the
cache is batched: all the misses of one type go out in a single
`ExecuteMultiCall`, which counts as one request rather than one per id. A fleet
has a few hundred devices and a few hundred distinct diagnostics, so after the
first poll the steady state is close to zero `Get` traffic.

The cache sits behind a small `Resolver` interface so a future OEM adapter can
reuse it. It has not been promoted to a shared package: one adapter is not a
pattern.

**A failed lookup never drops an event.** If a reference cannot be resolved, the
event is emitted with the affected field absent and
`geotab.unresolved` listing which references failed, as a comma separated list
of FaultData property names such as `diagnostic,device`. Absent is a fact the
schema can represent. A dropped fault is not.

One consequence worth naming: `spn` is set only when the diagnostic's
`diagnosticType` is `SuspectParameterNumber`. Geotab diagnostics also cover
OBD-II and proprietary fault codes, whose `code` is a number from a different
scheme. Putting one of those in `spn` would be a wrong answer, which is worse
than a missing one, so those land in `geotab.diagnostic_code` and
`geotab.diagnostic_type` instead. A `failureMode` code outside 0–31 is handled
the same way, in `geotab.failure_mode_code`.

### Severity

Geotab's `severity` is already coalesced from five underlying properties in a
documented precedence order, so this adapter maps that one field rather than
inventing a second ranking from the lamps.

| Geotab `severity` | fleetnorm `severity` |
| --- | --- |
| `Critical` | `critical` |
| `Warning` | `medium` |
| `None` | `info` |
| `Unknown` | `medium` |
| anything else | `medium`, plus `geotab.severity_raw` |

An unrecognized value maps up rather than down: it is not evidence the fault is
minor. The original string is kept in `geotab.severity_raw` so nothing about the
guess is hidden, and `raw` still has the record as it arrived.

### Field mapping

| Normalized | From |
| --- | --- |
| `event_id` | `<id>:<version>` |
| `occurred_at` | `dateTime`, converted to UTC |
| `received_at` | the poll time |
| `vin` | resolved device VIN, falling back to the device id |
| `unit_id` | resolved device name |
| `source` | the adapter's configured name |
| `source_type` | `tsp` |
| `spn` | resolved diagnostic code, when it is an SPN |
| `fmi` | resolved failure mode code, when it is 0–31 |
| `occurrence_count` | `count` |
| `severity` | mapped from `severity`, see above |
| `lamp_status` | `faultLampState`, when present |
| `description` | `faultDescription`, when present |
| `raw` | the FaultData JSON verbatim, before enrichment |

A device with no VIN still gets a `vin`, because the field is required: the
device id is used and `geotab.vin_fallback` is set to `device_id`, so nobody
mistakes it for a real VIN.

`lamp_status` comes from `faultLampState`, which is the J1939 lamp state and so
is exactly what the schema field is for. The four booleans — `amberWarningLamp`,
`redStopLamp`, `malfunctionLamp` and `protectWarningLamp` — are separate
properties that happen to be lamp related, and they stay in tags only. All five,
`faultLampState` included, are also tagged, so the field and the tag are not an
either/or. A fault with no `faultLampState` leaves `lamp_status` absent rather
than empty.

### Tag keys

Every tag this adapter writes, all under the `geotab.` prefix. A tag is absent
when the source did not report the field; none of them is ever written as an
empty string.

| Tag | Meaning |
| --- | --- |
| `geotab.source_id` | the Geotab record id, stable across revisions |
| `geotab.version` | the version of this revision |
| `geotab.unresolved` | comma separated references that could not be resolved |
| `geotab.severity_raw` | the original `severity` when it was unrecognized |
| `geotab.vin_fallback` | `device_id` when `vin` holds a device id, not a VIN |
| `geotab.amber_warning_lamp` | `amberWarningLamp` |
| `geotab.red_stop_lamp` | `redStopLamp` |
| `geotab.malfunction_lamp` | `malfunctionLamp` |
| `geotab.protect_warning_lamp` | `protectWarningLamp` |
| `geotab.fault_lamp_state` | `faultLampState` |
| `geotab.controller` | resolved controller name |
| `geotab.class_code` | `classCode` |
| `geotab.fault_state` | `faultState` |
| `geotab.source_address` | `sourceAddress` |
| `geotab.dismiss_date_time` | `dismissDateTime`, in UTC |
| `geotab.dismiss_user` | the dismissing user's id |
| `geotab.diagnostic_code` | diagnostic code that is not an SPN |
| `geotab.diagnostic_type` | the diagnostic type that code belongs to |
| `geotab.failure_mode_code` | failure mode code outside the 0–31 FMI range |
| `geotab.effect_on_component` | `effectOnComponent`, enriched faults only |
| `geotab.recommendation` | `recommendation`, enriched faults only |
| `geotab.risk_of_breakdown` | `riskOfBreakdown`, enriched faults only |

`faultDescription`, `effectOnComponent`, `recommendation` and `riskOfBreakdown`
exist only on enriched faults. Their absence is normal, never a validation
failure, and never an empty tag.

### Sessions and the server redirect

The adapter authenticates with `Authenticate`, which exchanges the password for
a session, and every call after that carries a `sessionId` instead. The password
appears on exactly one request and never again.

Authentication is lazy: it happens on the first call, not at construction, so
building an adapter does no network IO and a bad server name fails on the first
poll rather than at startup.

`Authenticate` returns two things that belong together: the credentials holding
the `sessionId`, and a `path`. The path is either the literal string
`ThisServer`, meaning the server you authenticated against is the right one, or
a different server, meaning **every later call must go there**. A customer
database that does not live on `my.geotab.com` is a normal deployment, not an
edge case.

The `sessionId` and the resolved server are stored as one value and are never
used apart, because a `sessionId` is only valid against the server that issued
it. Carrying one to a different host fails as an authentication error, which
names the wrong cause and would send someone looking at the credentials. A
redirect is logged at info, so a database on another server is visible rather
than something you infer.

Sessions last up to 14 days. The adapter does not track that: it uses the
session until the server rejects it. A JSON-RPC error carrying
`InvalidUserException` means the session is expired or revoked, so the client
re-authenticates once and retries the call. If the retry is rejected the same
way, the credentials are wrong rather than stale, and it becomes a whole poll
failure instead of a login attempt on every tick.

Re-authentication is shared. Concurrent calls that all hit the same expired
session produce one re-authentication between them, not one each: whoever gets
there first replaces the session, and everyone else picks up the new one. This
is a mutex held across the `Authenticate` call, which is the point — it
serializes the authentications rather than running them in parallel.

`ExtendSession` is deliberately not used. It is deprecated and answers with an
`ArgumentException` saying so, so re-authenticating is the supported path.

### Failures, cursor and rate limits

Whole poll failures are rejected credentials, including a session that is still
refused after re-authenticating, a refused connection, an undecodable response,
and a `GetFeed` result with no `toVersion` — there is no cursor to advance to,
so none is invented.

Per record skips are FaultData records that fail validation after mapping, most
often a record with no `id` or no `version`. The synthetic id is
`<adapter-name>:<geotab-id>` when the id parsed and `<adapter-name>:index:<n>`
when it did not.

The cursor is the feed's `toVersion`, stored verbatim. It is Geotab's bookmark,
not ours to interpret.

Rate limits are respected with a client side limiter rather than discovered by
being throttled: `GetFeed` is paced to 60 calls a minute and `Get` to 500.
`poll_interval` is config driven with a documented minimum of one second, which
is what the `GetFeed` limit allows.

### Config

```yaml
adapters:
  - type: geotab
    name: fleet-geotab
    server: my.geotab.com          # optional, default my.geotab.com
    database: mydb
    username_env: GEOTAB_USER
    password_env: GEOTAB_PASS
    seed_from: 720h                # required on a new feed
    poll_interval: 30s
    results_limit: 10000           # optional, max 50000
    cache_refresh: 1h              # optional
```

Credentials follow the existing pattern: the config names environment variables,
the values are resolved once at load so a missing one fails at startup rather
than at the first poll, and they are held unexported and redacted in `String`
and `GoString`. They are never logged.
