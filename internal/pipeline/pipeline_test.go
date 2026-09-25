package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/adapter"
	"github.com/ianfrushon/fleetnorm/internal/config"
	"github.com/ianfrushon/fleetnorm/internal/event"
	"github.com/ianfrushon/fleetnorm/internal/output"
	"github.com/ianfrushon/fleetnorm/internal/router"
	"github.com/ianfrushon/fleetnorm/internal/store"
)

func TestMain(m *testing.M) {
	//no test should wait out a real backoff, or a real eager pause.
	baseBackoff, maxBackoff = time.Millisecond, 5*time.Millisecond
	eagerPause = time.Millisecond
	m.Run()
}

func testEvent(id string) event.Event {
	at := time.Date(2026, 9, 14, 13, 4, 5, 0, time.UTC)
	return event.Event{
		SchemaVersion: event.SchemaVersion,
		EventID:       id,
		VIN:           "1XKYDP9X1MJ123456",
		OccurredAt:    at,
		ReceivedAt:    at,
		Source:        "replay",
		SourceType:    event.SourceFile,
		Severity:      event.SeverityHigh,
	}
}

// fakeAdapter hands out one batch per poll and then nothing.
type fakeAdapter struct {
	name    string
	mu      sync.Mutex
	batches [][]event.Event
	polls   int
	err     error
	failOn  int  //fail this poll number only
	stuck   bool //never advance the cursor
}

func (f *fakeAdapter) Name() string { return f.name }

func (f *fakeAdapter) Poll(context.Context, adapter.Cursor) ([]event.Event, adapter.Cursor, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls++
	if f.err != nil || f.polls == f.failOn {
		return nil, "", errors.New("poll failed")
	}
	cursor := adapter.Cursor(strconv.Itoa(f.polls))
	if f.stuck {
		cursor = "same"
	}
	if len(f.batches) == 0 {
		return nil, cursor, nil
	}
	batch := f.batches[0]
	f.batches = f.batches[1:]
	return batch, cursor, nil
}

func (f *fakeAdapter) pollCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.polls
}

// backlogAdapter reports a backlog on the first pages polls, or always.
type backlogAdapter struct {
	*fakeAdapter
	pages  int
	always bool
}

func (b *backlogAdapter) Backlog() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.always || b.polls <= b.pages
}

func batches(pages, size int) [][]event.Event {
	var out [][]event.Event
	for p := range pages {
		var batch []event.Event
		for i := range size {
			batch = append(batch, testEvent(fmt.Sprintf("p%02d-e%02d", p, i)))
		}
		out = append(out, batch)
	}
	return out
}

// a source polled once an hour, so only the eager loop can finish in time
func hourly(a adapter.Adapter) []Source {
	return []Source{{Adapter: a, Interval: time.Hour}}
}

// fakeOutput records what it receives and can be told to misbehave.
type fakeOutput struct {
	name string

	mu        sync.Mutex
	got       []event.Event
	attempts  int
	failFirst int  //fail this many attempts before succeeding
	permanent bool //fail permanently on the first attempt
	delay     time.Duration
	retryIn   time.Duration //ask for this delay via a DelayError

	sent chan struct{} //one token per successful delivery
}

func newFakeOutput(name string) *fakeOutput {
	return &fakeOutput{name: name, sent: make(chan struct{}, 256)}
}

func (f *fakeOutput) Name() string { return f.name }

func (f *fakeOutput) Send(ctx context.Context, e event.Event) error {
	f.mu.Lock()
	f.attempts++
	attempt, delay := f.attempts, f.delay
	permanent, failFirst, retryIn := f.permanent, f.failFirst, f.retryIn
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if permanent {
		return fmt.Errorf("%w: rejected", output.ErrPermanent)
	}
	if attempt <= failFirst {
		err := errors.New("temporary trouble")
		if retryIn > 0 {
			return &output.DelayError{Delay: retryIn, Err: err}
		}
		return err
	}

	f.mu.Lock()
	f.got = append(f.got, e)
	f.mu.Unlock()
	select {
	case f.sent <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeOutput) events() []event.Event {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]event.Event(nil), f.got...)
}

func (f *fakeOutput) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

type harness struct {
	*Pipeline
	store *store.Store
	t     *testing.T
}

func newHarness(t *testing.T, rules []config.Rule, sources []Source, dests []Destination) *harness {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "fleetnorm.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	p, err := New(Options{
		Store:           st,
		Router:          router.New(rules),
		Metrics:         NewMetrics(),
		Sources:         sources,
		Destinations:    dests,
		DedupeRetention: time.Hour,
		DrainTimeout:    2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{Pipeline: p, store: st, t: t}
}

// run starts the pipeline, waits for want deliveries on each output, then stops
// it and returns. It fails the test rather than hanging forever.
func (h *harness) run(waitFor map[*fakeOutput]int) {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()

	deadline := time.After(10 * time.Second)
	for out, want := range waitFor {
		for range want {
			select {
			case <-out.sent:
			case <-deadline:
				cancel()
				<-done
				h.t.Fatalf("timed out waiting for %d deliveries to %s, got %d", want, out.name, len(out.events()))
			}
		}
	}
	cancel()
	if err := <-done; err != nil {
		h.t.Fatalf("Run: %v", err)
	}
}

func (h *harness) auditFor(id string) []store.Record {
	h.t.Helper()
	recs, err := h.store.AuditFor(context.Background(), id)
	if err != nil {
		h.t.Fatal(err)
	}
	return recs
}

func catchAll(dests ...string) []config.Rule { return []config.Rule{{Route: dests}} }

func source(a adapter.Adapter) []Source {
	return []Source{{Adapter: a, Interval: 10 * time.Millisecond}}
}

func destination(o output.Output, buffer, attempts int) Destination {
	return Destination{Output: o, Buffer: buffer, MaxAttempts: attempts}
}

func TestDeliversToEveryRoutedOutput(t *testing.T) {
	shop, console := newFakeOutput("my-shop"), newFakeOutput("console")
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{{testEvent("e1"), testEvent("e2")}}}

	h := newHarness(t, catchAll("my-shop", "console"), source(a),
		[]Destination{destination(shop, 16, 3), destination(console, 16, 3)})
	h.run(map[*fakeOutput]int{shop: 2, console: 2})

	for _, out := range []*fakeOutput{shop, console} {
		if got := len(out.events()); got != 2 {
			t.Errorf("%s got %d events, want 2", out.name, got)
		}
	}

	//both deliveries are in the audit log, one row per output.
	recs := h.auditFor("e1")
	if len(recs) != 2 {
		t.Fatalf("audit for e1 = %+v, want two rows", recs)
	}
	outputs := map[string]store.Record{}
	for _, r := range recs {
		outputs[r.Output] = r
	}
	for _, name := range []string{"my-shop", "console"} {
		r, ok := outputs[name]
		if !ok {
			t.Fatalf("no audit row for %s: %+v", name, recs)
		}
		if r.Status != store.StatusDelivered || r.Attempts != 1 || r.VIN == "" {
			t.Errorf("%s audit = %+v, want delivered on attempt 1 with a vin", name, r)
		}
	}
}

func TestDuplicatesAreDroppedNotRedelivered(t *testing.T) {
	console := newFakeOutput("console")
	//the same event on two consecutive polls, as a re-read file would produce.
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{
		{testEvent("e1")}, {testEvent("e1")}, {testEvent("e2")},
	}}

	h := newHarness(t, catchAll("console"), source(a), []Destination{destination(console, 16, 3)})
	h.run(map[*fakeOutput]int{console: 2}) //e1 and e2, not e1 twice

	got := console.events()
	if len(got) != 2 || got[0].EventID != "e1" || got[1].EventID != "e2" {
		t.Fatalf("delivered %+v, want e1 then e2", ids(got))
	}
	if n := h.Metrics().Count(MetricDuplicate, labels("adapter", "replay")); n != 1 {
		t.Errorf("duplicate count = %d, want 1", n)
	}
	if recs := h.auditFor("e1"); len(recs) != 1 {
		t.Errorf("audit for e1 = %+v, want one row: the duplicate was never routed", recs)
	}
}

func TestUnroutedEventIsAudited(t *testing.T) {
	console := newFakeOutput("console")
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{{testEvent("nowhere")}}}
	//nothing here is critical, so no rule matches and nothing is routed.
	rules := []config.Rule{{Match: config.Match{Severity: []string{"critical"}}, Route: []string{"console"}}}

	h := newHarness(t, rules, source(a), []Destination{destination(console, 16, 3)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	waitFor(t, func() bool { return len(h.auditFor("nowhere")) > 0 })
	cancel()
	<-done

	if got := len(console.events()); got != 0 {
		t.Errorf("console received %d events, want none", got)
	}

	recs := h.auditFor("nowhere")
	if len(recs) != 1 {
		t.Fatalf("audit = %+v, want one row", recs)
	}
	r := recs[0]
	if r.Status != store.StatusDropped || r.Output != "" || r.Error != "no matching rule" {
		t.Errorf("audit = %+v, want a drop with no output and a reason", r)
	}
	if n := h.Metrics().Count(MetricUnrouted, ""); n == 0 {
		t.Error("unrouted metric was not counted")
	}
}

func TestRetryThenSucceed(t *testing.T) {
	console := newFakeOutput("console")
	console.failFirst = 2
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{{testEvent("e1")}}}

	h := newHarness(t, catchAll("console"), source(a), []Destination{destination(console, 16, 5)})
	h.run(map[*fakeOutput]int{console: 1})

	if got := console.attemptCount(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	recs := h.auditFor("e1")
	if len(recs) != 1 || recs[0].Status != store.StatusDelivered || recs[0].Attempts != 3 {
		t.Errorf("audit = %+v, want delivered on attempt 3", recs)
	}
}

func TestRetriesAreExhausted(t *testing.T) {
	console := newFakeOutput("console")
	console.failFirst = 99
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{{testEvent("e1")}}}

	h := newHarness(t, catchAll("console"), source(a), []Destination{destination(console, 16, 3)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	waitFor(t, func() bool { return len(h.auditFor("e1")) > 0 })
	cancel()
	<-done

	if got := console.attemptCount(); got != 3 {
		t.Errorf("attempts = %d, want exactly max_attempts (3)", got)
	}
	recs := h.auditFor("e1")
	if len(recs) != 1 || recs[0].Status != store.StatusFailed || recs[0].Attempts != 3 {
		t.Fatalf("audit = %+v, want a permanent failure after 3 attempts", recs)
	}
	if recs[0].Error == "" {
		t.Error("a failure audit row must say why")
	}
}

// a 4xx is not worth five identical rejections.
func TestPermanentFailureIsNotRetried(t *testing.T) {
	console := newFakeOutput("console")
	console.permanent = true
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{{testEvent("e1")}}}

	h := newHarness(t, catchAll("console"), source(a), []Destination{destination(console, 16, 5)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	waitFor(t, func() bool { return len(h.auditFor("e1")) > 0 })
	cancel()
	<-done

	if got := console.attemptCount(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
	if recs := h.auditFor("e1"); recs[0].Status != store.StatusFailed || recs[0].Attempts != 1 {
		t.Errorf("audit = %+v, want failed after one attempt", recs)
	}
}

// one slow endpoint must not stall the pipeline: its queue fills and events are
// dropped with an audit row, rather than backpressure reaching the adapter.
func TestFullQueueDropsAndAudits(t *testing.T) {
	slow := newFakeOutput("slow")
	slow.delay = 50 * time.Millisecond

	var batch []event.Event
	for i := range 20 {
		batch = append(batch, testEvent(fmt.Sprintf("e%02d", i)))
	}
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{batch}}

	h := newHarness(t, catchAll("slow"), source(a), []Destination{destination(slow, 2, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	waitFor(t, func() bool {
		return h.Metrics().Count(MetricDeliveries, labels("output", "slow", "status", "dropped")) > 0
	})
	cancel()
	<-done

	var dropped int
	for i := range 20 {
		for _, r := range h.auditFor(fmt.Sprintf("e%02d", i)) {
			if r.Status == store.StatusDropped && r.Error == "output queue full" {
				dropped++
			}
		}
	}
	if dropped == 0 {
		t.Fatal("no event was audited as dropped for a full queue")
	}
	t.Logf("dropped %d of 20 with a queue of 2", dropped)
}

func TestShutdownDrainsQueuedEvents(t *testing.T) {
	console := newFakeOutput("console")
	console.delay = 20 * time.Millisecond
	var batch []event.Event
	for i := range 5 {
		batch = append(batch, testEvent(fmt.Sprintf("e%d", i)))
	}
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{batch}}

	h := newHarness(t, catchAll("console"), source(a), []Destination{destination(console, 16, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()

	<-console.sent //the first one is away; the rest are queued
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if got := len(console.events()); got != 5 {
		t.Errorf("delivered %d of 5 queued events during drain: %v", got, ids(console.events()))
	}
}

// when the drain window closes, whatever is left still gets an audit row.
func TestShutdownAuditsWhatItCannotDeliver(t *testing.T) {
	slow := newFakeOutput("slow")
	slow.delay = time.Second
	var batch []event.Event
	for i := range 4 {
		batch = append(batch, testEvent(fmt.Sprintf("e%d", i)))
	}
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{batch}}

	st, err := store.Open(filepath.Join(t.TempDir(), "fleetnorm.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	p, err := New(Options{
		Store: st, Router: router.New(catchAll("slow")), Metrics: NewMetrics(),
		Sources: source(a), Destinations: []Destination{destination(slow, 16, 1)},
		DedupeRetention: time.Hour,
		DrainTimeout:    10 * time.Millisecond, //no time to deliver anything
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	time.Sleep(50 * time.Millisecond) //let the batch reach the queue
	cancel()
	<-done

	var audited int
	for i := range 4 {
		for _, r := range mustAudit(t, st, fmt.Sprintf("e%d", i)) {
			audited++
			if r.Status == store.StatusDelivered {
				continue
			}
			if r.Status != store.StatusDropped {
				t.Errorf("e%d audited as %s, want delivered or dropped", i, r.Status)
			}
		}
	}
	if audited != 4 {
		t.Errorf("audited %d of 4 events, want every one accounted for", audited)
	}
}

func TestPollErrorsAreCountedNotFatal(t *testing.T) {
	console := newFakeOutput("console")
	a := &fakeAdapter{name: "replay", err: errors.New("source unreachable")}

	h := newHarness(t, catchAll("console"), source(a), []Destination{destination(console, 16, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	waitFor(t, func() bool { return h.Metrics().Count(MetricPollErrors, labels("adapter", "replay")) >= 2 })
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("a failing adapter must not stop the pipeline: %v", err)
	}
}

func TestSkipAuditor(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "fleetnorm.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	m := NewMetrics()

	SkipAuditor(st, m)(context.Background(), adapter.Skipped{
		Adapter: "replay", ID: "replay:line:42", Reason: "line 42: severity \"URGENT\" is not one of...",
	})

	recs := mustAudit(t, st, "replay:line:42")
	if len(recs) != 1 {
		t.Fatalf("audit = %+v, want one row", recs)
	}
	if recs[0].Status != store.StatusDropped || recs[0].Error == "" || recs[0].Output != "" {
		t.Errorf("audit = %+v, want a drop with a reason and no output", recs[0])
	}
	if n := m.Count(MetricSkipped, labels("adapter", "replay")); n != 1 {
		t.Errorf("skip count = %d, want 1", n)
	}
}

func TestBackoff(t *testing.T) {
	//exponential, jittered, and never past the cap.
	baseBackoff, maxBackoff = time.Second, 8*time.Second
	defer func() { baseBackoff, maxBackoff = time.Millisecond, 5*time.Millisecond }()

	for attempt := 1; attempt <= 8; attempt++ {
		want := min(baseBackoff<<(attempt-1), maxBackoff)
		for range 50 {
			got := backoff(attempt, errors.New("nope"))
			if got < want/2 || got > want {
				t.Fatalf("backoff(%d) = %v, want between %v and %v", attempt, got, want/2, want)
			}
		}
	}

	//a destination that asked for a delay gets exactly that.
	asked := &output.DelayError{Delay: 42 * time.Second, Err: errors.New("busy")}
	if got := backoff(1, asked); got != 42*time.Second {
		t.Errorf("backoff with Retry-After = %v, want 42s", got)
	}
}

func TestNewRejectsBadOptions(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "fleetnorm.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ok := Options{
		Store: st, Router: router.New(catchAll("console")),
		Sources:      source(&fakeAdapter{name: "replay"}),
		Destinations: []Destination{destination(newFakeOutput("console"), 1, 1)},
	}
	bad := map[string]func(*Options){
		"no store":        func(o *Options) { o.Store = nil },
		"no router":       func(o *Options) { o.Router = nil },
		"no sources":      func(o *Options) { o.Sources = nil },
		"no destinations": func(o *Options) { o.Destinations = nil },
		"zero buffer":     func(o *Options) { o.Destinations = []Destination{destination(newFakeOutput("console"), 0, 1)} },
		"zero attempts":   func(o *Options) { o.Destinations = []Destination{destination(newFakeOutput("console"), 1, 0)} },
		"duplicate names": func(o *Options) {
			o.Destinations = []Destination{destination(newFakeOutput("console"), 1, 1), destination(newFakeOutput("console"), 1, 1)}
		},
	}
	for name, mutate := range bad {
		t.Run(name, func(t *testing.T) {
			o := ok
			mutate(&o)
			if _, err := New(o); err == nil {
				t.Error("New() = nil error, want one")
			}
		})
	}
}

func ids(events []event.Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.EventID
	}
	return out
}

func mustAudit(t *testing.T, st *store.Store, id string) []store.Record {
	t.Helper()
	recs, err := st.AuditFor(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

// waitFor polls a condition instead of guessing at a sleep.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for condition")
}

// the audit log records which rule sent an event to an output, which is the
// first question anyone asks once there are more rules than fit on a screen.
func TestAuditRecordsTheRule(t *testing.T) {
	shop, console := newFakeOutput("my-shop"), newFakeOutput("console")
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{{testEvent("e1")}}}

	rules := []config.Rule{
		{Name: "urgent", Match: config.Match{Severity: []string{"high"}}, Route: []string{"my-shop"}},
		{Route: []string{"console"}}, //unnamed: positional label
	}
	h := newHarness(t, rules, source(a),
		[]Destination{destination(shop, 16, 3), destination(console, 16, 3)})
	h.run(map[*fakeOutput]int{shop: 1, console: 1})

	want := map[string]string{"my-shop": "urgent", "console": "rule[1]"}
	recs := h.auditFor("e1")
	if len(recs) != 2 {
		t.Fatalf("audit for e1 = %+v, want two rows", recs)
	}
	for _, r := range recs {
		if got := r.Rule; got != want[r.Output] {
			t.Errorf("audit row for %s has rule %q, want %q", r.Output, got, want[r.Output])
		}
	}
}

// an event nothing matched has no rule to name
func TestUnroutedAuditHasNoRule(t *testing.T) {
	console := newFakeOutput("console")
	a := &fakeAdapter{name: "replay", batches: [][]event.Event{{testEvent("e1")}}}

	//a rule that cannot match the test event
	rules := []config.Rule{{Name: "never", Match: config.Match{VIN: "nope"}, Route: []string{"console"}}}
	h := newHarness(t, rules, source(a), []Destination{destination(console, 16, 3)})
	h.run(nil)

	recs := h.auditFor("e1")
	if len(recs) != 1 {
		t.Fatalf("audit for e1 = %+v, want one row", recs)
	}
	if recs[0].Rule != "" {
		t.Errorf("unrouted audit row has rule %q, want empty", recs[0].Rule)
	}
	if recs[0].Status != store.StatusDropped || recs[0].Error != "no matching rule" {
		t.Errorf("unrouted audit = %+v", recs[0])
	}
}

// a record the adapter could not use never reached a rule either
func TestSkipAuditHasNoRule(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "fleetnorm.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	SkipAuditor(st, NewMetrics())(context.Background(), adapter.Skipped{
		Adapter: "replay", ID: "replay:line:7", Reason: "vin is required",
	})
	recs, err := st.AuditFor(context.Background(), "replay:line:7")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("audit = %+v, want one row", recs)
	}
	if recs[0].Rule != "" || recs[0].Output != "" {
		t.Errorf("skip audit = %+v, want no rule and no output", recs[0])
	}
}

// the audit sweep runs on the same tick as the dedupe sweep. zero retention
// means never, which is what a permanent record looks like.
func TestAuditSweep(t *testing.T) {
	for _, tc := range []struct {
		name      string
		retention time.Duration
		wantKept  bool
	}{
		{name: "old rows are swept", retention: time.Hour},
		{name: "zero keeps everything", retention: 0, wantKept: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st, err := store.Open(filepath.Join(t.TempDir(), "fleetnorm.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()

			//a row well outside any retention window
			if err := st.Audit(context.Background(), store.Record{
				EventID: "ancient", Output: "console", Rule: "rule[0]",
				Status: store.StatusDelivered, Attempts: 1,
				RoutedAt: time.Now().UTC().Add(-365 * 24 * time.Hour),
			}); err != nil {
				t.Fatal(err)
			}

			out := newFakeOutput("console")
			a := &fakeAdapter{name: "replay", batches: [][]event.Event{{testEvent("e1")}}}
			p, err := New(Options{
				Store: st, Router: router.New(catchAll("console")), Metrics: NewMetrics(),
				Sources: source(a), Destinations: []Destination{destination(out, 16, 1)},
				DedupeRetention: time.Hour,
				AuditRetention:  tc.retention,
				DrainTimeout:    time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}

			//the sweep runs once as the loop starts, before the first tick
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- p.Run(ctx) }()
			time.Sleep(50 * time.Millisecond)
			cancel()
			<-done

			got := mustAudit(t, st, "ancient")
			if tc.wantKept && len(got) != 1 {
				t.Errorf("audit row was swept with retention 0: %+v", got)
			}
			if !tc.wantKept && len(got) != 0 {
				t.Errorf("audit row survived the sweep: %+v", got)
			}
			//the fresh row is never swept either way
			if recs := mustAudit(t, st, "e1"); len(recs) == 0 {
				t.Error("the current event's audit row went missing")
			}
		})
	}
}

func TestBacklogDrainsWithoutWaitingForTheTick(t *testing.T) {
	console := newFakeOutput("console")
	a := &backlogAdapter{fakeAdapter: &fakeAdapter{name: "paged", batches: batches(4, 5)}, pages: 3}
	h := newHarness(t, catchAll("console"), hourly(a), []Destination{destination(console, 64, 1)})
	h.run(map[*fakeOutput]int{console: 20})
	//three full pages, then the partial one that ends the sequence
	if got := a.pollCount(); got != 4 {
		t.Errorf("polled %d times, want 4: every page in one sequence", got)
	}
}

// the whole point: a backlog against a queue far smaller than a page, and a
// slow output, loses nothing. compare TestFullQueueDropsAndAudits.
func TestBacklogIsDeliveredWithBackpressure(t *testing.T) {
	slow := newFakeOutput("slow")
	slow.delay = 2 * time.Millisecond
	a := &backlogAdapter{fakeAdapter: &fakeAdapter{name: "paged", batches: batches(3, 20)}, pages: 2}
	h := newHarness(t, catchAll("slow"), hourly(a), []Destination{destination(slow, 2, 1)})
	h.run(map[*fakeOutput]int{slow: 60})

	if got := h.Metrics().Count(MetricDeliveries, labels("output", "slow", "status", "dropped")); got != 0 {
		t.Fatalf("%d events dropped during a drain; backpressure should make that impossible", got)
	}
	for p := range 3 {
		for i := range 20 {
			id := fmt.Sprintf("p%02d-e%02d", p, i)
			recs := h.auditFor(id)
			if len(recs) != 1 || recs[0].Status != store.StatusDelivered {
				t.Errorf("%s: audit %+v, want one delivered row", id, recs)
			}
		}
	}
}

func TestBacklogStopsWhenTheCursorDoesNotAdvance(t *testing.T) {
	console := newFakeOutput("console")
	a := &backlogAdapter{fakeAdapter: &fakeAdapter{name: "stuck", stuck: true}, always: true}
	h := newHarness(t, catchAll("console"), hourly(a), []Destination{destination(console, 8, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	waitFor(t, func() bool { return a.pollCount() >= 2 })
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	//the first poll, then one eager page that did not move the cursor
	if got := a.pollCount(); got != 2 {
		t.Errorf("polled %d times, want 2: a stuck cursor must not be polled in a loop", got)
	}
}

func TestBacklogEndsOnPollError(t *testing.T) {
	console := newFakeOutput("console")
	a := &backlogAdapter{fakeAdapter: &fakeAdapter{name: "flaky", batches: batches(5, 1), failOn: 2}, pages: 5}
	h := newHarness(t, catchAll("console"), hourly(a), []Destination{destination(console, 8, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	waitFor(t, func() bool { return a.pollCount() >= 2 })
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	if got := a.pollCount(); got != 2 {
		t.Errorf("polled %d times, want 2: a failed page waits for the tick", got)
	}
	if got := h.Metrics().Count(MetricPollErrors, labels("adapter", "flaky")); got != 1 {
		t.Errorf("poll errors = %d, want 1", got)
	}
}

func TestBacklogPageCap(t *testing.T) {
	defer func(n int) { maxEagerPages = n }(maxEagerPages)
	maxEagerPages = 3
	console := newFakeOutput("console")
	a := &backlogAdapter{fakeAdapter: &fakeAdapter{name: "endless"}, always: true}
	h := newHarness(t, catchAll("console"), hourly(a), []Destination{destination(console, 8, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	waitFor(t, func() bool { return a.pollCount() >= 3 })
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done
	if got := a.pollCount(); got != 3 {
		t.Errorf("polled %d times, want 3: the cap counts the sequence", got)
	}
}

// the tail of a drain: fetching has stopped, the slow output is still working
// through what the drain queued, and a live tick brings a fresh event. with a
// two slot queue it would drop; instead it waits its turn and is delivered.
func TestLiveEventBlocksWhileOutputIsBehind(t *testing.T) {
	slow := newFakeOutput("slow")
	slow.delay = 5 * time.Millisecond
	//one backlog page, one empty eager page that ends the drain, then a live
	//tick 10ms later while the 100ms of backlog is still in the queue
	pages := [][]event.Event{batches(1, 20)[0], {}, {testEvent("fresh")}}
	a := &backlogAdapter{fakeAdapter: &fakeAdapter{name: "paged", batches: pages}, pages: 1}
	h := newHarness(t, catchAll("slow"), source(a), []Destination{destination(slow, 2, 1)})
	h.run(map[*fakeOutput]int{slow: 21})

	if got := h.Metrics().Count(MetricDeliveries, labels("output", "slow", "status", "dropped")); got != 0 {
		t.Fatalf("%d events dropped; a fresh event behind a backlog must wait, not drop", got)
	}
	if recs := h.auditFor("fresh"); len(recs) != 1 || recs[0].Status != store.StatusDelivered {
		t.Errorf("fresh event audit %+v, want one delivered row", recs)
	}
	d := h.dests["slow"]
	if d.behind() {
		t.Error("output still in blocking mode after everything was delivered")
	}
	if d.blockedFor == 0 {
		t.Error("output never entered blocking mode, so the test proved nothing")
	}
}

func TestPermanentFailureStillSettles(t *testing.T) {
	rejecting := newFakeOutput("rejecting")
	rejecting.permanent = true
	a := &backlogAdapter{fakeAdapter: &fakeAdapter{name: "paged", batches: batches(1, 5)}, pages: 1}
	h := newHarness(t, catchAll("rejecting"), hourly(a), []Destination{destination(rejecting, 2, 3)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()
	waitFor(t, func() bool {
		return h.Metrics().Count(MetricDeliveries, labels("output", "rejecting", "status", "failed")) == 5
	})
	waitFor(t, func() bool { return !h.dests["rejecting"].behind() })
	cancel()
	<-done
	if h.dests["rejecting"].behind() {
		t.Error("five permanent failures left the output in blocking mode")
	}
}

func TestOutputsLeaveBlockingModeIndependently(t *testing.T) {
	fast, slow := newFakeOutput("fast"), newFakeOutput("slow")
	slow.delay = 5 * time.Millisecond
	pages := [][]event.Event{batches(1, 20)[0], {}}
	a := &backlogAdapter{fakeAdapter: &fakeAdapter{name: "paged", batches: pages}, pages: 1}
	h := newHarness(t, catchAll("fast", "slow"), hourly(a),
		[]Destination{destination(fast, 2, 1), destination(slow, 2, 1)})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Run(ctx) }()

	waitFor(t, func() bool { return len(fast.events()) == 20 && !h.dests["fast"].behind() })
	if !h.dests["slow"].behind() {
		t.Error("the slow output left blocking mode with most of its backlog still queued")
	}
	waitFor(t, func() bool { return len(slow.events()) == 20 && !h.dests["slow"].behind() })
	cancel()
	<-done
}
