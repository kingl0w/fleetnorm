# live harness

Runs fleetnorm against the live MyGeotab demo database from a cold start and
stops on its own once deliveries stop growing. The results in the changelog
came from these scripts.

Needs `GEOTAB_USER` and `GEOTAB_PASS` exported in the shell. From the repo root:

```
live/run.sh fast    # console only: an output that keeps up, expect blocked 0s
live/run.sh slow    # console plus a webhook answering after 200ms: expect the
                    # drain to block and the webhook to leave blocking mode
                    # minutes after console does
live/run.sh smoke   # replays testdata through both outputs, no credentials
```

`run.sh` builds the binary, deletes the mode's store so the drain starts fresh
from `seed_from`, starts `slow.py` if the config has a webhook, runs fleetnorm
at debug level, and prints one progress line every 5s with per-output
delivered counts. It stops when the total has not grown for 20 reads (about
100s: the drain ending plus about three empty live polls at a 30s
`poll_interval`), sends SIGINT, waits for the clean shutdown, then runs
`report.sh`. Completion is a stall, not a target count, because the simulator
keeps adding faults. Knobs: `POLL_EVERY`, `STABLE_POLLS`, `TIMEOUT`,
`SLOW_DELAY`.

`report.sh <mode>` reads the run's log and last metrics: the drain summary,
blocking-mode episodes aggregated per output, live polls after the drain,
delivery counters, warnings and errors, and whether shutdown was clean.

Outputs land here and are ignored by git: `<mode>.log` (JSON logs),
`<mode>.events.jsonl` (what console wrote), `<mode>.metrics` (the last
`/metrics` read), `<mode>.endpoint.log`, the store and the built binary.
