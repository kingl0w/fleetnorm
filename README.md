# fleetnorm

**v0.1.0** — event schema `0.1.0`. The Geotab adapter has been run end to end
against a live MyGeotab database: seeded poll, cursor polls, enrichment, routing
and audit, all verified on 2026-09-23. It has not yet run at production volume,
and no fault revision has been observed being resent.

A fleet that runs trucks from more than one manufacturer receives fault data in
as many shapes as it has brands. International reports through OnCommand
Connection, Detroit through Detroit Connect, PACCAR through SmartLinq, Cummins
through Connected Diagnostics, and any telematics provider layered on top adds
another shape again. Every one of those feeds describes the same thing, a truck
telling someone that something is wrong, and no two of them describe it the same
way.

The data belongs to the fleet that paid for it. Getting it somewhere useful,
such as the shop that actually turns the wrenches, is the part nobody built.

fleetnorm reads those feeds, turns every event into one open format, and
forwards each event to whatever destinations the owner chooses. It is a
translator and a router. That is the whole idea.

## What fleetnorm is not

It is not a dashboard. There is no UI beyond a health check and a metrics
endpoint, and there never will be.

It is not a scan tool or a diagnostic decoder. It passes an SPN and an FMI
through without interpreting them, because the moment it starts guessing what a
fault means it becomes something a technician has to second guess.

It is not a replacement for OEM telematics. Those systems remain the source. This
reads what they publish and moves it along.

It is not a data broker. Events go where the config says and nowhere else, and
the audit log is there so an owner can prove it.

## The schema is the product

Everything here exists to produce one normalized event type, defined in
[`schema/event.schema.json`](schema/event.schema.json) and mirrored by the Go
type in `internal/event`. A test fails if the two ever drift apart.

Every event carries a `schema_version`. The version follows semantic versioning
and the promise attached to it is simple. Within a major version, fields may be
added and optional fields may start being populated, so a consumer that ignores
what it does not recognize keeps working. Anything that could break such a
consumer, removing a field, renaming one, changing what a value means, requires a
new major version. The current version is 0.1.0, which is to say the shape is
settled but the contract has not been through a full release cycle yet.

Two rules exist because normalizing is lossy and pretending otherwise is how
data disappears. The original source payload is always preserved verbatim in
`raw`, so anything fleetnorm failed to model can still be recovered downstream.
Source fields that have no home in the schema are moved into `tags` rather than
dropped.

See [docs/schema.md](docs/schema.md) for the field reference.

## Quickstart

No credentials, no vendor account, no network. The file adapter replays events
from a file so the whole pipeline can be exercised locally.

```sh
git clone https://github.com/ianfrushon/fleetnorm
cd fleetnorm
go build -o fleetnorm ./cmd/fleetnorm

export SHOP_SECRET=whatever-you-like
./fleetnorm -config configs/example.yaml
```

Four events from `testdata/events.json` are normalized and printed to stdout as
JSON, one per line. The two serious ones are also POSTed to the webhook named in
the config, which by default points at an endpoint that does not exist, so you
will watch the retry logic work. Point it at something real, or delete that
output and its rule, and the rest keeps running.

While it runs:

```sh
curl localhost:8080/healthz    # ok
curl localhost:8080/metrics    # what was polled, delivered, dropped
```

Afterwards the audit log has a row for every routing decision:

```sh
sqlite3 fleetnorm.db 'select event_id, output, status, attempts, error from audit'
```

## Configuration

One YAML file describes three things: where events come from, where they can go,
and which ones go where.

```yaml
adapters:
  - type: file
    name: replay
    path: ./testdata/events.json
    poll_interval: 5s

outputs:
  - type: stdout
    name: console
  - type: webhook
    name: my-shop
    url: https://shop.example/ingest
    secret_env: SHOP_SECRET
    timeout: 10s
    retry: { max_attempts: 5, backoff: exponential }

rules:
  - match: { severity: [critical, high] }
    route: [my-shop, console]
  - match: {}
    route: [console]
```

Rules run from top to bottom and every rule that matches fires, so an event can
reach several destinations at once. Each destination receives it once no matter
how many rules chose it. A rule with an empty match catches everything. The full
reference is in [docs/policy.md](docs/policy.md).

Secrets are never written in the config. `secret_env` names an environment
variable, and the value is read once at startup. If it is missing, fleetnorm
refuses to start rather than sending unsigned events to an endpoint that expects
signatures.

The config is validated strictly when it loads. Unknown keys, unknown output
names in a rule, an unreadable adapter path, and a missing secret are all errors,
and every problem found is reported at once instead of one per run.

## Delivery

Delivery is at least once. Each output runs in its own goroutine behind a bounded
queue, so a slow endpoint cannot stall polling or hold up other outputs. Failed
webhook deliveries are retried with exponential backoff and jitter up to the
configured attempt limit, and a destination that asks for a specific delay with
`Retry-After` gets it. A 4xx response that is not 408 or 429 is treated as
permanent and is not retried, because five identical rejections help nobody.

If a queue fills, the event is dropped and the drop is recorded. That is a
deliberate trade. Blocking would mean one broken endpoint stops the whole
pipeline.

Every outcome lands in the audit log, including drops and the events no rule
matched. Each row names the rule that chose the destination, so the log answers
where a fault went and why. The audit log is the evidence that the owner
controlled where their data went, so it records the decisions fleetnorm made,
not just the successful ones.

Audit rows are kept for `store.audit_retention`, ninety days by default, and
swept on the same tick as the dedupe set. `audit_retention: 0` keeps them
forever, which is a supported choice rather than an oversight; the growth is then
yours to watch. Sweeps that remove rows say so in the log at info.

Requests carry an HMAC SHA256 signature over the exact request body in
`X-Fleetnorm-Signature`, so a receiver can verify the events came from this
fleetnorm and arrived unmodified. Redirects are never followed on a signed
request, since that would hand the signature to whatever host the redirect names.

## Building

```sh
make test     # go test ./...
make race     # the same under the race detector
make build    # static binaries for linux/amd64, darwin/arm64, windows/amd64
```

There is no cgo anywhere, so cross compiling is an ordinary build. The binary is
self contained apart from the config file and the SQLite database it creates.

## Documentation

* [docs/schema.md](docs/schema.md), the event format and what every field means
* [docs/policy.md](docs/policy.md), how routing rules are written and evaluated
* [docs/adapters.md](docs/adapters.md), how to write a new adapter
* [CHANGELOG.md](CHANGELOG.md), what changed in each release

## Status

v0.1.0, emitting events at schema version `0.1.0`. The two are versioned
separately: the schema promise in [docs/schema.md](docs/schema.md) is about the
event format, not about this binary.

Milestone one is complete and tested: the file adapter, the stdout and webhook
outputs, the router, the store and the schema. Milestone two adds the Geotab
adapter, which is complete, tested against generated fixtures, and has been run
end to end against a live MyGeotab database.

Further OEM and telematics adapters are additive: they implement the same
interface the file adapter does.

Known limitations at this version:

* No schema migrations. The store creates its tables with `IF NOT EXISTS` and
  has no versioned migration path, so the first non-additive change needs one, or
  a documented export and reimport.
* Events are marked seen before delivery, so a crash in between loses the event
  rather than duplicating it. Fixing that needs a durable queue.
* Delivery is at least once, never exactly once.

## License

Apache 2.0. See [LICENSE](LICENSE).
