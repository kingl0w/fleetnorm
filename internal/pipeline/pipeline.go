// package pipeline connects adapters to outputs: poll, deduplicate, route,
// deliver, and record every decision in the audit log.
//
// delivery is at least once. each output has its own goroutine and bounded
// queue, so one slow endpoint cannot stall the others or block polling. a full
// queue drops the event and says so in the audit log rather than pushing
// backpressure all the way to the source.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/adapter"
	"github.com/ianfrushon/fleetnorm/internal/event"
	"github.com/ianfrushon/fleetnorm/internal/output"
	"github.com/ianfrushon/fleetnorm/internal/router"
	"github.com/ianfrushon/fleetnorm/internal/store"
)

const DefaultDrainTimeout = 15 * time.Second

// tuning knobs. variables rather than constants only so tests can shorten them.
var (
	//between delivery attempts, before jitter
	baseBackoff = time.Second
	maxBackoff  = time.Minute

	//how often expired dedupe entries are swept, not the window itself
	sweepInterval = time.Hour
)

// Source is one adapter and how often to poll it.
type Source struct {
	Adapter  adapter.Adapter
	Interval time.Duration
}

// Destination is one output and the queue and retry budget it runs with.
type Destination struct {
	Output      output.Output
	Buffer      int
	MaxAttempts int
}

type Options struct {
	Store           *store.Store
	Router          *router.Router
	Metrics         *Metrics
	Sources         []Source
	Destinations    []Destination
	DedupeRetention time.Duration
	DrainTimeout    time.Duration //how long shutdown waits for queues to empty
}

type Pipeline struct {
	opts  Options
	dests map[string]*dest
}

type dest struct {
	name        string
	out         output.Output
	maxAttempts int
	ch          chan event.Event
}

func New(o Options) (*Pipeline, error) {
	switch {
	case o.Store == nil:
		return nil, errors.New("pipeline needs a store")
	case o.Router == nil:
		return nil, errors.New("pipeline needs a router")
	case len(o.Sources) == 0:
		return nil, errors.New("pipeline needs at least one source")
	case len(o.Destinations) == 0:
		return nil, errors.New("pipeline needs at least one destination")
	}
	if o.Metrics == nil {
		o.Metrics = NewMetrics()
	}
	if o.DrainTimeout <= 0 {
		o.DrainTimeout = DefaultDrainTimeout
	}

	p := &Pipeline{opts: o, dests: make(map[string]*dest, len(o.Destinations))}
	for _, d := range o.Destinations {
		name := d.Output.Name()
		if _, dup := p.dests[name]; dup {
			return nil, fmt.Errorf("two destinations named %q", name)
		}
		if d.Buffer <= 0 || d.MaxAttempts <= 0 {
			return nil, fmt.Errorf("destination %q: buffer and max_attempts must be positive", name)
		}
		queue := make(chan event.Event, d.Buffer)
		p.dests[name] = &dest{name: name, out: d.Output, maxAttempts: d.MaxAttempts, ch: queue}
		o.Metrics.gauge(MetricQueueDepth, labels("output", name), func() int { return len(queue) })
		o.Metrics.gauge(MetricQueueCap, labels("output", name), func() int { return cap(queue) })
	}
	return p, nil
}

func (p *Pipeline) Metrics() *Metrics { return p.opts.Metrics }

// Run polls every source until ctx is cancelled, then drains what is queued.
// events still queued when the window closes are audited as drops, so the log
// accounts for them too.
func (p *Pipeline) Run(ctx context.Context) error {
	//workers keep a context of their own, so a delivery in flight survives the
	//cancellation that starts shutdown, up to the drain timeout
	drainCtx, stopDraining := context.WithCancel(context.WithoutCancel(ctx))
	defer stopDraining()

	var workers sync.WaitGroup
	for _, d := range p.dests {
		workers.Go(func() { p.work(drainCtx, d) })
	}

	var pollers sync.WaitGroup
	for _, s := range p.opts.Sources {
		pollers.Go(func() { p.poll(ctx, s) })
	}
	pollers.Go(func() { p.sweep(ctx) })
	pollers.Wait()

	slog.Info("draining output queues", "timeout", p.opts.DrainTimeout)
	for _, d := range p.dests {
		close(d.ch)
	}
	drained := make(chan struct{})
	go func() {
		workers.Wait()
		close(drained)
	}()
	select {
	case <-drained:
	//stop delivering, but let the workers keep reading so everything left in a
	//queue gets an audit row
	case <-time.After(p.opts.DrainTimeout):
		stopDraining()
		workers.Wait()
	}
	return nil
}

// OnSkip records a record an adapter could not use.
func (p *Pipeline) OnSkip(ctx context.Context, s adapter.Skipped) {
	SkipAuditor(p.opts.Store, p.opts.Metrics)(ctx, s)
}

// SkipAuditor turns a skipped record into a dropped audit row. it takes the
// store rather than a pipeline, so adapters can be built before the pipeline
// that will run them.
func SkipAuditor(st *store.Store, m *Metrics) adapter.SkipFunc {
	return func(ctx context.Context, s adapter.Skipped) {
		m.inc(MetricSkipped, labels("adapter", s.Adapter))
		audit(ctx, st, store.Record{
			EventID: s.ID,
			Status:  store.StatusDropped,
			Error:   s.Reason,
		})
	}
}

func (p *Pipeline) poll(ctx context.Context, s Source) {
	name := s.Adapter.Name()
	cursor, err := p.opts.Store.Cursor(ctx, name)
	if err != nil {
		slog.Error("cannot read adapter cursor, starting from the beginning", "adapter", name, "error", err)
	}

	tick := time.NewTicker(s.Interval)
	defer tick.Stop()
	for {
		cursor = p.pollOnce(ctx, s.Adapter, cursor)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// pollOnce returns the cursor to use next: the new one on success, the one it
// was given on failure.
func (p *Pipeline) pollOnce(ctx context.Context, a adapter.Adapter, cursor string) string {
	name := a.Name()
	events, next, err := a.Poll(ctx, adapter.Cursor(cursor))
	if err != nil {
		if ctx.Err() != nil {
			return cursor //shutting down, not a failure worth reporting
		}
		p.opts.Metrics.inc(MetricPollErrors, labels("adapter", name))
		slog.Error("poll failed", "adapter", name, "error", err)
		return cursor
	}
	for _, e := range events {
		p.opts.Metrics.inc(MetricPolled, labels("adapter", name))
		p.dispatch(ctx, name, e)
	}
	if string(next) != cursor {
		//losing a cursor means replaying, which dedupe absorbs
		if err := p.opts.Store.SetCursor(ctx, name, string(next)); err != nil {
			slog.Error("cannot save adapter cursor", "adapter", name, "error", err)
		}
	}
	return string(next)
}

// dispatch deduplicates one event, routes it, and queues it for each
// destination. every outcome that is not a delivery attempt is audited here.
func (p *Pipeline) dispatch(ctx context.Context, adapterName string, e event.Event) {
	//ponytail: marked seen before delivery, so a crash in between loses the event
	//rather than duplicating it. the fix is a durable queue, not a reordering:
	//marking afterwards only moves the window.
	isNew, err := p.opts.Store.MarkSeen(ctx, adapterName, e.EventID)
	if err != nil {
		slog.Error("dedupe check failed, delivering anyway", "adapter", adapterName, "event_id", e.EventID, "error", err)
	} else if !isNew {
		p.opts.Metrics.inc(MetricDuplicate, labels("adapter", adapterName))
		slog.Debug("duplicate event dropped", "adapter", adapterName, "event_id", e.EventID)
		return
	}

	dests := p.opts.Router.Route(e)
	if len(dests) == 0 {
		p.opts.Metrics.inc(MetricUnrouted, "")
		p.audit(ctx, e, "", store.StatusDropped, 0, "no matching rule")
		return
	}
	for _, name := range dests {
		d := p.dests[name]
		if d == nil { //config validation should have caught this
			p.audit(ctx, e, name, store.StatusDropped, 0, "no such output")
			continue
		}
		select {
		case d.ch <- e:
		//dropping beats blocking the poller behind one slow endpoint
		default:
			p.opts.Metrics.inc(MetricDeliveries, labels("output", name, "status", string(store.StatusDropped)))
			p.audit(ctx, e, name, store.StatusDropped, 0, "output queue full")
			slog.Warn("output queue full, event dropped", "output", name, "event_id", e.EventID)
		}
	}
}

func (p *Pipeline) work(ctx context.Context, d *dest) {
	for e := range d.ch {
		//shutting down: keep reading so everything queued is accounted for,
		//but stop attempting deliveries
		if ctx.Err() != nil {
			p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusDropped)))
			p.audit(ctx, e, d.name, store.StatusDropped, 0, "shut down before delivery")
			continue
		}
		p.deliver(ctx, d, e)
	}
}

func (p *Pipeline) deliver(ctx context.Context, d *dest, e event.Event) {
	var err error
	for attempt := 1; attempt <= d.maxAttempts; attempt++ {
		err = d.out.Send(ctx, e)
		switch {
		case err == nil:
			p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusDelivered)))
			p.audit(ctx, e, d.name, store.StatusDelivered, attempt, "")
			return

		//five identical rejections help nobody
		case errors.Is(err, output.ErrPermanent):
			p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusFailed)))
			p.audit(ctx, e, d.name, store.StatusFailed, attempt, err.Error())
			slog.Error("permanent delivery failure", "output", d.name, "event_id", e.EventID, "error", err)
			return

		case ctx.Err() != nil:
			p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusDropped)))
			p.audit(ctx, e, d.name, store.StatusDropped, attempt, "shut down mid-delivery: "+err.Error())
			return
		}

		slog.Warn("delivery attempt failed", "output", d.name, "event_id", e.EventID,
			"attempt", attempt, "of", d.maxAttempts, "error", err)
		if attempt == d.maxAttempts {
			break
		}
		if !sleep(ctx, backoff(attempt, err)) {
			p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusDropped)))
			p.audit(ctx, e, d.name, store.StatusDropped, attempt, "shut down between attempts: "+err.Error())
			return
		}
	}

	p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusFailed)))
	p.audit(ctx, e, d.name, store.StatusFailed, d.maxAttempts, err.Error())
	slog.Error("giving up on delivery", "output", d.name, "event_id", e.EventID,
		"attempts", d.maxAttempts, "error", err)
}

// backoff is exponential with equal jitter, half fixed and half random, so a
// shared outage does not bring every queue back in lockstep. a Retry-After from
// the destination wins, already capped by the output.
func backoff(attempt int, err error) time.Duration {
	if d, ok := output.RetryAfter(err); ok {
		return d
	}
	d := min(baseBackoff<<(attempt-1), maxBackoff)
	return d/2 + rand.N(d/2+1)
}

// sleep waits out a backoff, reporting false if shutdown interrupted it.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (p *Pipeline) sweep(ctx context.Context) {
	tick := time.NewTicker(sweepInterval)
	defer tick.Stop()
	for {
		cutoff := time.Now().UTC().Add(-p.opts.DedupeRetention)
		n, err := p.opts.Store.SweepSeen(ctx, cutoff)
		if err != nil && ctx.Err() == nil {
			slog.Error("dedupe sweep failed", "error", err)
		}
		if n > 0 {
			slog.Info("swept expired dedupe entries", "removed", n, "older_than", cutoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (p *Pipeline) audit(ctx context.Context, e event.Event, out string, status store.Status, attempts int, cause string) {
	audit(ctx, p.opts.Store, store.Record{
		EventID:  e.EventID,
		VIN:      e.VIN,
		Output:   out,
		Status:   status,
		Attempts: attempts,
		Error:    cause,
	})
}

// audit writes the record whatever else is happening. a cancelled context must
// not be the reason the evidence is missing.
func audit(ctx context.Context, st *store.Store, r store.Record) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := st.Audit(ctx, r); err != nil {
		slog.Error("cannot write audit record", "event_id", r.EventID, "output", r.Output,
			"status", r.Status, "error", err)
	}
}
