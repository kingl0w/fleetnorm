package pipeline

import (
	"strings"
	"sync"
	"testing"
)

func TestMetricsWrite(t *testing.T) {
	m := NewMetrics()
	m.inc(MetricPolled, labels("adapter", "replay"))
	m.inc(MetricPolled, labels("adapter", "replay"))
	m.inc(MetricPolled, labels("adapter", "other"))
	m.inc(MetricUnrouted, "")
	m.inc(MetricDeliveries, labels("output", "my-shop", "status", "delivered"))
	m.gauge(MetricQueueDepth, labels("output", "my-shop"), func() int { return 7 })

	var b strings.Builder
	m.Write(&b)
	got := b.String()

	for _, want := range []string{
		"# TYPE " + MetricPolled + " counter",
		MetricPolled + `{adapter="replay"} 2`,
		MetricPolled + `{adapter="other"} 1`,
		MetricUnrouted + " 1",
		MetricDeliveries + `{output="my-shop",status="delivered"} 1`,
		"# TYPE " + MetricQueueDepth + " gauge",
		MetricQueueDepth + `{output="my-shop"} 7`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		if strings.HasPrefix(line, "# HELP") && strings.HasSuffix(line, " ") {
			t.Errorf("metric without help text: %q", line)
		}
	}

	//rendering is stable, so a scrape diff means something changed.
	var again strings.Builder
	m.Write(&again)
	if again.String() != got {
		t.Errorf("two renders differ:\n%s\n---\n%s", got, again.String())
	}
}

func TestMetricsAreConcurrencySafe(t *testing.T) {
	m := NewMetrics()
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			m.inc(MetricPolled, labels("adapter", "replay"))
			var b strings.Builder
			m.Write(&b)
		})
	}
	wg.Wait()
	if got := m.Count(MetricPolled, labels("adapter", "replay")); got != 50 {
		t.Errorf("count = %d, want 50", got)
	}
}
