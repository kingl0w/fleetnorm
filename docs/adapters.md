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

Several things about Geotab's model do not line up with ours, and each one is a
decision recorded here rather than a surprise in the code.

Several of the behaviors below are marked **observed**. They were found by
pointing the adapter at a live MyGeotab demo database, and the entity reference
does not describe them: it documents the fields, not these shapes. Where the two
disagree this adapter follows what the server sent, and the captured records
that showed it are kept verbatim in `internal/adapter/geotab/fixtures` so a test
holds each one in place.

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

#### FaultData has no per-record version, so the revision is a content hash

**Observed, and verified over `GetFeed`.** Everything above was written from the
API's description of the feed. Against a live demo database, the `FeedResult`
carries `toVersion`, which is the cursor, but the individual FaultData records
carry no `version` field at all. The record shape is otherwise byte-identical
between `Get` and `GetFeed`. So for FaultData the hash is the normal case, not
the exception, and an event with `geotab.version` is the one to be surprised by.

| Record | `event_id` | Tags |
| --- | --- | --- |
| `version` absent, the normal case | `<id>:<first 16 hex of sha256(canonical record)>` | `geotab.version` omitted, `geotab.event_id_source` = `hash` |
| `version` present | `<id>:<version>` | `geotab.version` set |

The hash keeps the property the version was meant to provide. A revision that
changed anything hashes differently and survives dedupe; a resend that says the
same thing hashes the same and is swallowed. A consumer that wants the latest
state of one fault groups by `geotab.source_id` and orders by `occurred_at` and
then `received_at`; "take the highest `geotab.version`" applies only to events
that have one.

**The version path stays, and is not dead code.** Other entity types do carry a
version, Diagnostic records from the same database among them
(`"0000000000000eb9"`), and one demo database does not prove what a production
FaultData feed sends. When a record has a `version`, it wins, the hash is not
computed, and the first half of this section applies as written. The adapter as
first written did the opposite and skipped any record without one, which against
a live feed is every record.

**The hash is of a canonical form, not of the bytes received.** Hashing `raw`
directly would make JSON key order part of the id. Geotab's serializer is
consistent today, but nothing guarantees that across server versions or
instances, and a European server need not agree with a North American one. If
the order ever shifted, every event already delivered would hash differently and
fire again as new. So the record is decoded and re-encoded with its keys sorted
and its whitespace dropped, and that is what is hashed: the id depends on
content, not formatting. Numbers are compared by value, so `2` and `2.0` are the
same content. Canonicalization is for the hash only. `raw` on the event is
always the record verbatim, as it arrived.

**Known limitation: an identical reactivation is deduped.** A fault that goes
active, clears, and reactivates with every field identical to the first time,
`dateTime` included, is the same content, hashes to the same `event_id`, and is
swallowed as a repeat. `dateTime` should advance on a reactivation, and `count`
usually will, so this is almost certainly unreachable in practice. It is a real
property of identifying a revision by its content rather than by a counter, and
it is written here so that it is a known limit and not a surprise.

**What the demo database cannot tell us.** It held 39 faults with `toVersion` at
5. That is no evidence about behavior at volume, and no modified record has been
observed being resent: nothing was dismissed and no `count` incremented while
anyone was watching. That `GetFeed` resends a FaultData record when it changes,
which is the premise of this whole section, remains theory. If it turns out the
feed never resends, the revision part of `event_id` is harmless but idle; if it
resends in some other shape, such as a new `id`, this section needs revisiting.

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
`diagnosticType` is `SuspectParameter`. Geotab diagnostics also cover J1708,
OBD-II and the GO device's own faults, whose `code` is a number from a different
scheme. Putting one of those in `spn` would be a wrong answer, which is worse
than a missing one, so those land in `geotab.diagnostic_code` and
`geotab.diagnostic_type` instead. A `failureMode` code outside 0–31 is handled
the same way, in `geotab.failure_mode_code`.

**Observed: the constant is `SuspectParameter`.** This adapter first shipped
comparing against `SuspectParameterNumber`, which no record carries. Nothing
failed: every real J1939 fault simply missed the guard and went to
`geotab.diagnostic_code` with no `spn`, which is the exact silent wrong answer
the guard exists to prevent. `TestCapturedSuspectParameterMapsToSPN` runs a
captured Diagnostic through the adapter so the constant cannot drift back. The
types seen in one live database: `SuspectParameter`, `Sid`, `Pid`, `ObdFault`,
`ObdWwhFault`, `GoFault`, `GoDiagnostic`, `DataDiagnostic`. Only the first is an
SPN.

### geotab.diagnostic_standard

`diagnosticType` says what kind of code a diagnostic carries, but not which
standard it belongs to, and that is the better question for "is this a vehicle
fault or the device talking about itself". A `Sid` is a real vehicle fault on the
older J1708 standard, and by type alone it is indistinguishable from an OBD code
once it is in tags. The resolved diagnostic's `source` answers it:

| Diagnostic `source` | `geotab.diagnostic_standard` |
| --- | --- |
| `SourceJ1939Id` | `j1939` |
| `SourceJ1708Id` | `j1708` |
| `SourceObdId` | `obd` |
| `SourceGeotabGoId` | `device` |
| anything else | the raw value, as it arrived |

It is written on every event whose diagnostic resolved, SPN or not. It
supplements the `spn` guard and does not replace it: `SourceJ1939Id` appears on
`Sid` diagnostics as well as `SuspectParameter`, so a J1939 source does not make
a code an SPN.

### References arrive in two shapes

**Observed.** An id reference is documented as an object with an `id`. A live
server sends the same property, on the same entity type, either way:

```json
"controller": "ControllerNoneId"
"controller": { "id": "ControllerObdBodyId" }
```

What was seen, per property:

| Property | Shape seen |
| --- | --- |
| `FaultData.controller` | object |
| `FaultData.failureMode` | bare string (`"NoFailureModeId"`) |
| `FaultData.diagnostic`, `FaultData.device` | object |
| `Diagnostic.controller` | both: a bare string on one record, an object on the next |
| `Diagnostic.source` | bare string |

One database is not proof that `device` never arrives as a string, so the
adapter does not encode that table. Every reference it reads, `dismissUser` and
`Diagnostic.source` included, decodes through one type that accepts a bare
string, an object with an `id`, or `null`, and the rest of the code sees a single
representation. Before this, a bare string failed the decode of the whole
record: it was skipped as "not a FaultData object", and since every live record
carries `failureMode` as a bare string, every one of them would have been
skipped and the poll failed at the skip ceiling.

Nothing is assumed about an id's length or format either. The same captured
record carries `b1` and `b1C` next to `aysJxXoc3v0-Y6PGVSjoOxA`.

### Sentinels

**Observed.** Geotab does not say "none", "any" or "not a real object" with
`null` or an absent property. It sends a string constant shaped like an id:

- `NoFailureModeId`
- `ControllerNoneId`, `ControllerGoDeviceId`, `ControllerAnyId`, `ControllerObdBodyId`
- `EngineTypeNoneId`, `EngineTypeGenericId`
- `UnitOfMeasureNoneId`, `ParameterGroupNoneId`
- `FaultStatusActiveId`, inside `faultStates.effectiveStatus`
- `SourceJ1939Id`, `SourceJ1708Id`, `SourceObdId`, `SourceGeotabGoId`, `SourceSystemId`

That list is what one database showed and is not guaranteed complete, so the
adapter recognizes the shape, a capitalized word ending in `Id`, as well as the
list. Generated MyGeotab ids start with a lower case letter, so they never match
it. One helper, `classify` in `sentinel.go`, sorts an id into an ordinary id, a
known sentinel meaning *nothing here*, a known sentinel that *means something*,
or an *unknown* sentinel.

**A sentinel is never passed to an enrichment `Get`.** There is nothing behind
`NoFailureModeId` or `ControllerGoDeviceId` to fetch: the lookup is an error or
a meaningless answer, and either way it spends the 500/min budget. That rule
holds everywhere. What a sentinel means beyond it is decided per site:

| Site | Policy |
| --- | --- |
| `failureMode` | `NoFailureModeId` means no `fmi`, and nothing is tagged. Any other sentinel is never resolved and is listed in `geotab.sentinel_unknown`. |
| `controller` | `ControllerNoneId` means no controller, and nothing is tagged. The other known ones are written to `geotab.controller` as themselves, since "the GO device" is an answer. An unknown one is too, and is also listed in `geotab.sentinel_unknown`. |
| `Diagnostic.source` | Never a reference. Mapped to `geotab.diagnostic_standard`, raw when unlisted. |
| `diagnostic`, `device` | Not sentinel sites, always resolved. See below: this is deliberate. |
| `engineType`, `unitOfMeasure`, `parameterGroup`, `faultStates.effectiveStatus` | Not read by this adapter, so no policy. They reach consumers in `raw`. |

**The `diagnostic` and `device` exemption is not an inconsistency to fix.** It
looks like one: the shape rule keeps `ControllerNewId` away from `Get`, and yet a
diagnostic id of exactly that shape is resolved. The reason is evidence. A `Get`
on Diagnostic against the live demo database returned real, resolvable records
with these ids:

`DiagnosticIgnitionId`, `DiagnosticAux3Id`, `DiagnosticPositionValidId`,
`DiagnosticEngineHoursStaleId`, `DiagnosticGpsAntennaUnpluggedId`,
`DiagnosticDeviceHasBeenUnpluggedId`

All sentinel-shaped, all real, and in the same entity type as opaque ids like
`aysJxXoc3v0-Y6PGVSjoOxA`. For Diagnostic the shape says nothing about whether a
record exists. Applying the sentinel rule there would refuse to resolve
well-known diagnostics, and an unresolved diagnostic has no code: every such
fault would lose its `spn` or `geotab.diagnostic_code`, silently, which is the
failure this adapter has already had once.

Whether real ids at `controller` and `failureMode` are also `XxxId`-shaped is
unknown. Every live record seen had no failure mode. If they are, they will not
be resolved, and `geotab.sentinel_unknown` is what will show it: a real
controller turning up there is the prompt to revisit that site's policy.

An unknown sentinel is handled conservatively: it is not resolved, it does not
fail the record, and it is made visible in `geotab.sentinel_unknown` as
`property=value` pairs, such as `controller=ControllerNewId`. A sentinel is never
listed in `geotab.unresolved`, because that tag means a lookup was tried and
failed, and none was.

### Severity

Geotab's `severity` is already coalesced from five underlying properties in a
documented precedence order, so when a record has one, this adapter maps that
one field and the lamps are not consulted. When it has none, the lamps may raise
the severity and never lower it; that rule is below.

| Geotab `severity` | fleetnorm `severity` |
| --- | --- |
| `Critical` | `critical` |
| `Warning` | `medium` |
| `None` | `info` |
| `Unknown` | `medium` |
| anything else | `medium`, plus `geotab.severity_raw` |
| absent | `medium`, or `critical` when the red stop lamp is on; plus `geotab.severity_absent` |

An unrecognized value maps up rather than down: it is not evidence the fault is
minor. The original string is kept in `geotab.severity_raw` so nothing about the
guess is hidden, and `raw` still has the record as it arrived.

**Observed: live FaultData has no `severity` field at all.** Not `null`, absent,
and on the records seen that was the rule rather than the exception. `severity`
is required by our schema and `unknown` is not in its enum, so something has to
be chosen, and it is chosen here rather than by a zero value: **`medium`**, with
`geotab.severity_absent` set to `true`.

The alternative was the lowest severity. The reasoning against it: because
absent is the common case, the default is the severity most Geotab events will
carry, and the two ways of being wrong are not the same size. Defaulting low
makes a real engine fault that arrived without a severity look ignorable, and
nothing downstream can tell that happened. Defaulting to `medium` over-reports
GO device noise, but that error is visible and filterable: the event says
`geotab.severity_absent`, and `geotab.diagnostic_standard` = `device` picks out
the device's own faults. It is also the same rule as the row above, for the same
reason: no information about severity is not information that it is low.

`geotab.severity_absent` is what keeps a defaulted `medium` from being read as
one Geotab reported. It is set in every absent case, whatever the lamps did.

#### Lamps raise an absent severity, and never lower it

Only when `severity` is absent:

| Lamps | `severity` | `geotab.severity_derived` |
| --- | --- | --- |
| `redStopLamp` true | `critical` | `lamp` |
| `amberWarningLamp`, `malfunctionLamp` or `protectWarningLamp` true | `medium` | not set |
| all four false, or absent | `medium`, the default | not set |

The rule moves in one direction. Raising on the red stop lamp is not a ranking
this adapter invented: J1939 defines that lamp as "stop the vehicle now", so
`critical` is the standard's meaning, not ours. What is declined is the
downgrade. All four lamps false was the shape of every live record seen, on GO
device faults where the lamps carry no information at all, so reading it as
"minor" would treat the absence of a signal as evidence, and bury faults in the
one severity nobody looks at.

The middle row changes nothing against today's default. It is explicit in the
code anyway, so the rule survives a change to the default.

`geotab.severity_derived` = `lamp` is set only when the derivation changed the
outcome, which today means the red stop lamp. That keeps a derived severity
distinguishable from one Geotab reported and from the plain default. A record
that has a `severity`, recognized or not, keeps its own mapping even with the
red stop lamp on: the derivation does not fire and neither tag is set.

### Field mapping

| Normalized | From |
| --- | --- |
| `event_id` | `<id>:<hash of the canonical record>`, or `<id>:<version>` when the record has one |
| `occurred_at` | `dateTime`, converted to UTC |
| `received_at` | the poll time |
| `vin` | resolved device VIN, falling back to the device id |
| `unit_id` | resolved device name |
| `source` | the adapter's configured name |
| `source_type` | `tsp` |
| `spn` | resolved diagnostic code, when it is an SPN |
| `fmi` | resolved failure mode code, when it is 0–31 |
| `occurrence_count` | `count` |
| `severity` | mapped from `severity`; when absent, the default raised by the lamps, see above |
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
| `geotab.version` | the version of this revision, when the record has one; live FaultData has none |
| `geotab.event_id_source` | `hash` when `event_id` carries a content hash, which is the normal case for FaultData |
| `geotab.unresolved` | comma separated references that could not be resolved |
| `geotab.sentinel_unknown` | comma separated `property=value` for sentinels not in the known list |
| `geotab.severity_raw` | the original `severity` when it was unrecognized |
| `geotab.severity_absent` | `true` when the record had no `severity` |
| `geotab.severity_derived` | `lamp` when an absent severity was raised by the red stop lamp |
| `geotab.vin_fallback` | `device_id` when `vin` holds a device id, not a VIN |
| `geotab.amber_warning_lamp` | `amberWarningLamp` |
| `geotab.red_stop_lamp` | `redStopLamp` |
| `geotab.malfunction_lamp` | `malfunctionLamp` |
| `geotab.protect_warning_lamp` | `protectWarningLamp` |
| `geotab.fault_lamp_state` | `faultLampState` |
| `geotab.controller` | resolved controller name, or the sentinel itself, such as `ControllerGoDeviceId` |
| `geotab.class_code` | `classCode` |
| `geotab.fault_state` | `faultState` |
| `geotab.source_address` | `sourceAddress` |
| `geotab.dismiss_date_time` | `dismissDateTime`, in UTC |
| `geotab.dismiss_user` | the dismissing user's id |
| `geotab.diagnostic_code` | diagnostic code that is not an SPN |
| `geotab.diagnostic_type` | the diagnostic type that code belongs to |
| `geotab.diagnostic_standard` | `j1939`, `j1708`, `obd`, `device`, or the raw diagnostic `source` |
| `geotab.failure_mode_code` | failure mode code outside the 0–31 FMI range |
| `geotab.effect_on_component` | `effectOnComponent`, enriched faults only |
| `geotab.recommendation` | `recommendation`, enriched faults only |
| `geotab.risk_of_breakdown` | `riskOfBreakdown`, enriched faults only |

`faultDescription`, `effectOnComponent`, `recommendation` and `riskOfBreakdown`
exist only on enriched faults. Their absence is normal, never a validation
failure, and never an empty tag.

### The test fixtures are generated

The FaultData the geotab tests read lives in
`internal/adapter/geotab/testdata`, and it is generated rather than written by
hand:

```sh
make fixtures      # go run ./cmd/genfixtures
```

**None of it comes from a fleet.** The VINs are impossible rather than merely
unassigned, since every one contains a letter a real VIN cannot, and device
names are `unit-0001`. The SPNs are a short hand-written table of well-known
J1939 numbers with their meanings in a comment, not a dictionary. SAE sells the
digital annex and this does not reproduce it.

**The reference shapes are real.** Four Diagnostic records, a
`SuspectParameter`, an `ObdFault`, a `Sid` and a `GoFault`, and the one FaultData
record that referenced the `GoFault`, were captured from a live MyGeotab demo
database and are in the generator verbatim. They are system reference data and a
device health fault from a demo database, not anything about a vehicle. The
non-SPN diagnostics the edge scene resolves against are those records rather
than invented ones, and the captured FaultData record is in the edge feed byte
for byte. The J1939 faults themselves are still synthetic, because the demo
database only emits GO device health faults and the `spn` path needs events, but
the synthetic SPN diagnostics are now built in the captured one's shape,
sentinels and bare string `controller` included.

The generator is deterministic. The same `-seed` produces byte-identical files,
which is what lets `TestFixturesAreGenerated` regenerate at the committed seed
and compare against what is checked in. A generator change that nobody
regenerated fails there rather than leaving the two quietly describing different
things.

Each file is a `GetFeed` response, `data` plus `toVersion`, so a fixture can be
handed to the test double as-is:

| File | What it covers |
| --- | --- |
| `entities.json` | the reference set enrichment resolves against, by type and id |
| `feed_ordinary.json` | faults where everything resolves |
| `feed_revisions.json` | one id at several versions, an incrementing count, a dismissal |
| `feed_edge.json` | the captured `ObdFault`, `Sid` and `GoFault` diagnostics, the captured FaultData record, bare string references, known and unknown sentinels, an FMI outside 0–31, an unresolved reference, a device with no VIN, enriched and bare faults, lamp combinations, an unrecognized severity, an absent severity with the lamps off and with the red stop lamp on |
| `feed_skip.json` | records that fail validation, staying under the skip ceiling |
| `feed_ceiling.json` | enough failures to trip the ceiling |

**A new mapping case goes in the generator, not the JSON.** Editing a fixture by
hand makes it disagree with the generator, and the drift test will say so on the
next run. Add the case to `internal/adapter/geotab/fixtures`, run
`make fixtures`, and commit both.

The generator's command lives in `cmd/genfixtures` rather than beside the
fixtures because the go tool ignores every directory named `testdata`: a
generator in there would never be built or vetted, and would rot with nothing
failing. The generation itself is a package so the drift test can call it
without shelling out.

Some geotab tests are still hand-written, and deliberately. The ones that build
a variant with `strings.Replace` on a literal, and assert against specific SPN,
VIN and controller values, are testing one mapping decision each; pointing them
at generated records would mean rewriting their assertions rather than swapping
their input.

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
