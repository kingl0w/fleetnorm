#!/usr/bin/env bash
# live/report.sh <fast|slow>: what the run showed, from its log and last metrics.
set -euo pipefail
cd "$(dirname "$0")"
mode=${1:?usage: report.sh fast|slow}
log=$mode.log
line() { jq -r "select(.msg == \"$1\") | del(.time, .level, .msg) | \"$1 \" + (to_entries | map(\"\(.key)=\(.value)\") | join(\" \"))" "$log"; }

echo "== $mode run =="
line "fleetnorm started"
line "geotab feed seeded"
line "adapter reports a backlog, draining eagerly"
line "eager drain finished"
echo "-- blocking mode (per output; raw enter/leave lines stay in $log) --"
python3 - "$log" <<'PY'
import json, re, sys
from collections import defaultdict
unit = {"h": 3600, "m": 60, "s": 1, "ms": 1e-3, "µs": 1e-6, "ns": 1e-9}
secs = lambda d: sum(float(v) * unit[u] for v, u in re.findall(r"([\d.]+)(h|ms|µs|ns|m|s)", d))
fmt = lambda t: (f"{int(t // 60)}m" if t >= 60 else "") + f"{t % 60:.3f}s"
left, entered = defaultdict(list), defaultdict(int)
for l in open(sys.argv[1]):
    r = json.loads(l)
    if r.get("msg") == "output entered blocking mode":
        entered[r["output"]] += 1
    elif r.get("msg") == "output left blocking mode":
        left[r["output"]].append(secs(r["after"]))
for o in sorted(entered):
    t = left[o]
    open_ = entered[o] - len(t)
    print(f"{o}: {len(t)} episodes, {fmt(sum(t))} total, longest {fmt(max(t, default=0))}"
          + (f", {open_} still open at shutdown" if open_ else ""))
PY
echo "-- after the drain --"
echo "live polls: $(awk '/eager drain finished/ {d=1; next} d && /geotab GetFeed/ {n++} END {print n+0}' "$log")"
echo "-- deliveries --"
grep '^fleetnorm_deliveries_total{' "$mode.metrics" 2>/dev/null || echo "(no metrics captured)"
echo "console lines written: $(wc -l < "$mode.events.jsonl")"
[ -f "$mode.endpoint.log" ] && tail -n 1 "$mode.endpoint.log"
echo "-- warnings and errors: $(grep -c '"level":"\(WARN\|ERROR\)"' "$log" || true) --"
grep '"level":"\(WARN\|ERROR\)"' "$log" | head -n 20 || true
echo "-- shutdown --"
line "draining output queues"
grep -q '"msg":"fleetnorm stopped"' "$log" && echo "clean shutdown" || echo "NO clean shutdown line"
