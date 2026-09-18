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
