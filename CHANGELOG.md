# Changelog

All notable changes to this project are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The event schema is versioned separately from the binary. Its version is carried
on every event as `schema_version` and its stability promise lives in
[docs/schema.md](docs/schema.md). This release emits schema `0.1.0`.

## [Unreleased]

### Added

- **Generated Geotab test fixtures.** `make fixtures` regenerates the synthetic
  FaultData in `internal/adapter/geotab/testdata` from
  `cmd/genfixtures`. Deterministic at a fixed seed, and a test fails if the
  committed fixtures drift from the generator. The fixtures are synthetic and
  contain no real fleet data.

## [0.1.0] - 2026-09-18

First release. Emits events at schema version `0.1.0`.

### Added

- **Normalized event schema.** One open format for a commercial vehicle fault
  event, defined by `schema/event.schema.json` with a Go type that a test keeps
  in step with it. Every event carries `raw`, the source payload verbatim, and
  `tags` for anything the schema does not model, so normalizing loses nothing.
- **Adapter contract.** `Poll` returns normalized events and an opaque cursor.
  One unusable record is skipped, audited with a synthetic id and stepped over;
  a source that cannot be read at all fails the whole poll and leaves the cursor
  alone. A poll that skips more than half of at least ten records fails instead
  of looking healthy.
- **File adapter.** Replays events from JSON or CSV, so the whole pipeline runs
  with no credentials and no network. Strict by default, which is a deliberate
  deviation from the adapter contract for input the fleet owner wrote.
- **Geotab adapter.** Reads `FaultData` from a MyGeotab `GetFeed` change stream.
  Authenticates with a session and follows the server redirect the API returns.
  `event_id` carries the record version, so a dismissal or a count change is a
  new event rather than something dedupe swallows. A seed date is required,
  because a feed started without one reads nothing forever while every health
  signal stays green. Diagnostics, failure modes, controllers and devices are
  resolved through a cache with batched lookups, and a lookup that fails costs a
  field rather than the event.
- **Outputs.** `stdout` for JSON lines, and `webhook` with HMAC SHA256 request
  signing, exponential backoff with jitter, `Retry-After` support, and no
  redirect following on a signed request.
- **Router.** Every matching rule fires, not just the first, and destinations
  are deduplicated per event. Rules can be named, and the name is recorded in
  the audit log against every event the rule routes.
- **Store.** SQLite, no cgo. Adapter cursors, the dedupe set, and an audit log
  recording every routing decision including drops and events no rule matched.
  Both the dedupe set and the audit log have configurable retention;
  `audit_retention: 0` keeps audit rows forever.
- **Pipeline.** Per output goroutines behind bounded queues, so one slow
  endpoint cannot stall polling or the other outputs. A full queue drops the
  event and records the drop.
- **Operational endpoints.** `/healthz` and `/metrics`.
- **Config.** Strict YAML validation at startup, reporting every problem rather
  than the first. Credentials are named as environment variables, resolved once
  at load, and redacted in `String` and `GoString`.

### Known limitations

- The Geotab adapter has not been run against a live MyGeotab database.
- No schema migrations: the store creates tables with `IF NOT EXISTS`, so the
  first non-additive change needs a migration path or a documented export and
  reimport.
- Events are marked seen before delivery, so a crash in between loses the event
  rather than duplicating it.
- Delivery is at least once, never exactly once.

[Unreleased]: https://github.com/kingl0w/fleetnorm/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/kingl0w/fleetnorm/releases/tag/v0.1.0
