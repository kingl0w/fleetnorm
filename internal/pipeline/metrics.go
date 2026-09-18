package pipeline

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"sync"
)

// counters, plus two gauges read live from the queues
const (
	MetricPolled     = "fleetnorm_events_polled_total"
	MetricDuplicate  = "fleetnorm_events_duplicate_total"
	MetricUnrouted   = "fleetnorm_events_unrouted_total"
	MetricPollErrors = "fleetnorm_poll_errors_total"
	MetricSkipped    = "fleetnorm_records_skipped_total"
	MetricDeliveries = "fleetnorm_deliveries_total"
	MetricQueueDepth = "fleetnorm_queue_depth"
	MetricQueueCap   = "fleetnorm_queue_capacity"
)

var help = map[string]string{
	MetricPolled:     "Events returned by an adapter.",
	MetricDuplicate:  "Events dropped because this adapter already delivered that event_id.",
	MetricUnrouted:   "Events no rule matched.",
	MetricPollErrors: "Polls that failed and made no progress.",
	MetricSkipped:    "Source records an adapter could not normalize.",
	MetricDeliveries: "Delivery outcomes, by output and status.",
	MetricQueueDepth: "Events waiting in an output's queue.",
	MetricQueueCap:   "How many events an output's queue holds before dropping.",
}

// Metrics is a counter registry sized for this service: a map, a mutex and a
// text renderer.
//
// ponytail: not a metrics library. if fleetnorm ever needs histograms, take the
// prometheus client dependency instead of growing this.
type Metrics struct {
	mu       sync.Mutex
	counters map[sample]int64
	gauges   map[sample]func() int
}

type sample struct{ name, labels string }

func NewMetrics() *Metrics {
	return &Metrics{
		counters: map[sample]int64{},
		gauges:   map[sample]func() int{},
	}
}

func (m *Metrics) inc(name, labels string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters[sample{name, labels}]++
}

// a value read at scrape time rather than accumulated
func (m *Metrics) gauge(name, labels string, read func() int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gauges[sample{name, labels}] = read
}

// Count returns one counter. for tests.
func (m *Metrics) Count(name, labels string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counters[sample{name, labels}]
}

// Write renders the Prometheus text format, in name order so a scrape diff means
// something changed.
func (m *Metrics) Write(w io.Writer) {
	m.mu.Lock()
	values := make(map[sample]int64, len(m.counters)+len(m.gauges))
	kinds := map[string]string{}
	for s, v := range m.counters {
		values[s] = v
		kinds[s.name] = "counter"
	}
	for s, read := range m.gauges {
		values[s] = int64(read())
		kinds[s.name] = "gauge"
	}
	m.mu.Unlock()

	byName := map[string][]sample{}
	for s := range values {
		byName[s.name] = append(byName[s.name], s)
	}
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, help[name], name, kinds[name])
		samples := byName[name]
		slices.SortFunc(samples, func(a, b sample) int { return strings.Compare(a.labels, b.labels) })
		for _, s := range samples {
			if s.labels == "" {
				fmt.Fprintf(w, "%s %d\n", name, values[s])
				continue
			}
			fmt.Fprintf(w, "%s{%s} %d\n", name, s.labels, values[s])
		}
	}
}

func labels(pairs ...string) string {
	var b strings.Builder
	for i := 0; i < len(pairs); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "%s=%q", pairs[i], pairs[i+1])
	}
	return b.String()
}
