#!/usr/bin/env bash
# live/run.sh <fast|slow>: build fleetnorm, run it against the live demo
# database from a fresh store, stop once deliveries stop growing, report.
#
# completion is "delivered count unchanged for STABLE_POLLS reads", not a
# target count: the simulator keeps adding faults, so no exact count is ever
# right for long. the default window (20 x 5s) covers the drain ending plus
# about three empty live polls at poll_interval 30s.
set -euo pipefail
cd "$(dirname "$0")"
mode=${1:?usage: run.sh fast|slow}
POLL_EVERY=${POLL_EVERY:-5}       # seconds between /metrics reads
STABLE_POLLS=${STABLE_POLLS:-20}  # reads with no new deliveries before stopping
TIMEOUT=${TIMEOUT:-1800}          # hard stop, seconds
SLOW_DELAY=${SLOW_DELAY:-0.2}     # seconds per request at the slow endpoint

(cd .. && go build -o live/fleetnorm ./cmd/fleetnorm)
rm -f "$mode".db "$mode".db-shm "$mode".db-wal "$mode".log "$mode".events.jsonl "$mode".metrics

pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT

if grep -q webhook "$mode.yaml"; then
  python3 slow.py "$SLOW_DELAY" > "$mode.endpoint.log" 2>&1 &
  ep=$!; pids+=($ep)
  until curl -s -o /dev/null localhost:9009; do sleep 0.2; done
  echo "slow endpoint on :9009, ${SLOW_DELAY}s per request (log: $mode.endpoint.log)"
fi

./fleetnorm -config "$mode.yaml" -log-level debug > "$mode.events.jsonl" 2> "$mode.log" &
fn=$!
pids+=($fn)
echo "fleetnorm pid $fn, log: live/$mode.log, events: live/$mode.events.jsonl"

# one line per output plus a total, e.g. "console=2817 slow=1200 total=4017"
delivered() {
  curl -sf localhost:8080/metrics | awk '
    /^fleetnorm_deliveries_total\{/ && /status="delivered"/ {
      match($0, /output="[^"]*"/); printf "%s=%s ", substr($0, RSTART+8, RLENGTH-9), $NF; s += $NF }
    END { printf "total=%d\n", s }'
}

start=$(date +%s); last=-1; stable=0
while :; do
  sleep "$POLL_EVERY"
  elapsed=$(( $(date +%s) - start ))
  if ! kill -0 "$fn" 2>/dev/null; then
    echo "fleetnorm exited early:"; tail -n 3 "$mode.log"; break
  fi
  m=$(delivered || true)
  if [ -z "$m" ]; then printf '[%4ds] waiting for /metrics\n' "$elapsed"; continue; fi
  total=${m##*total=}
  if [ "$total" -gt "$last" ]; then stable=0; else stable=$((stable + 1)); fi
  last=$total
  printf '[%4ds] %s stable=%d/%d\n' "$elapsed" "$m" "$stable" "$STABLE_POLLS"
  if [ "$stable" -ge "$STABLE_POLLS" ]; then echo "deliveries stopped growing, shutting down"; break; fi
  if [ "$elapsed" -ge "$TIMEOUT" ]; then echo "timeout after ${TIMEOUT}s, shutting down"; break; fi
done

curl -s localhost:8080/metrics > "$mode.metrics" || true
kill -INT "$fn" 2>/dev/null || true
wait "$fn" || true
if [ -n "${ep:-}" ]; then kill "$ep"; wait "$ep" || true; fi
./report.sh "$mode"
