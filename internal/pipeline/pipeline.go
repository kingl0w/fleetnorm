// package pipeline connects adapters to outputs: poll, deduplicate, route,
// deliver, and record every decision in the audit log.
//
// delivery is at least once. each output has its own goroutine and bounded
// queue, so one slow endpoint cannot stall the others or block polling. during
// live polling a full queue drops the event and says so in the audit log rather
// than pushing backpressure all the way to the source: a slow output must not
// stall fresh faults. while an adapter reports a backlog the opposite holds:
// the data is historical, nothing is urgent, and a full queue blocks the poller
// instead, so a drain cannot drop by construction. that holds per output until
// its queue has delivered everything the drain put there: a fresh fault
// arriving behind historical ones is the more valuable of the two, and dropping
// it to protect the backlog would be backwards.
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

	//between pages of an eager drain. the adapter's own limiter is the real
	//guard against its vendor's rate limit; this keeps an adapter without one
	//from spinning, and leaves the geotab feed 20% under its 60/min.
	eagerPause = 1200 * time.Millisecond

	//consecutive eager pages before falling back to the tick, whatever the
	//adapter says. a backlog that long is either enormous or a bug.
	maxEagerPages = 500
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

	//how long audit rows are kept. zero means never sweep, which is what a
	//permanent record looks like.
	AuditRetention time.Duration

	DrainTimeout time.Duration //how long shutdown waits for queues to empty
}

type Pipeline struct {
	opts  Options
	dests map[string]*dest
}

type dest struct {
	name        string
	out         output.Output
	maxAttempts int
	ch          chan queued

	//outstanding counts events enqueued under backpressure that have not yet
	//reached a terminal outcome. above zero, the queue is still working through
	//a backlog and every enqueue to this output waits instead of dropping. the
	//poller raises it and the worker lowers it, hence the lock.
	mu          sync.Mutex
	outstanding int
	since       time.Time     //when outstanding last left zero
	blockedFor  time.Duration //total time spent above zero, for the log
}

// behind reports whether this output is still delivering a backlog.
func (d *dest) behind() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.outstanding > 0
}

// track counts one event enqueued under backpressure. it runs before the
// enqueue, so the worker can never settle an event that was not yet tracked.
func (d *dest) track() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.outstanding == 0 {
		d.since = time.Now()
		slog.Debug("output entered blocking mode", "output", d.name)
	}
	d.outstanding++
}

// settle counts one tracked event reaching any terminal outcome: delivered,
// failed, or dropped at shutdown. every outcome counts, or the counter would
// never return to zero and the output would block forever.
func (d *dest) settle() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.outstanding--
	if d.outstanding == 0 {
		took := time.Since(d.since)
		d.blockedFor += took
		slog.Debug("output left blocking mode", "output", d.name,
			"after", took.Round(time.Millisecond).String())
	}
}

// queued is an event on its way to one output, carrying the label of the rule
// that sent it there so the audit row written at delivery time can name it.
type queued struct {
	event   event.Event
	rule    string
	tracked bool //counted in dest.outstanding; the worker settles it
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
		queue := make(chan queued, d.Buffer)
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
		r := p.pollOnce(ctx, s.Adapter, cursor, false)
		if r.ok && r.backlog {
			r = p.drain(ctx, s.Adapter, r)
		}
		cursor = r.cursor
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// pollResult is what one poll left behind.
type pollResult struct {
	cursor  string        //the one to use next: new on success, unchanged on failure
	ok      bool          //the poll succeeded
	backlog bool          //the adapter says there is more behind this page
	events  int           //events the page produced
	blocked time.Duration //time spent waiting on full output queues
}

// pollOnce runs one poll and dispatches what it returned. block says whether a
// full output queue waits or drops; a page the adapter reports as part of a
// backlog always waits, whatever the caller said.
func (p *Pipeline) pollOnce(ctx context.Context, a adapter.Adapter, cursor string, block bool) pollResult {
	name := a.Name()
	events, next, err := a.Poll(ctx, adapter.Cursor(cursor))
	if err != nil {
		if ctx.Err() == nil { //shutting down is not a failure worth reporting
			p.opts.Metrics.inc(MetricPollErrors, labels("adapter", name))
			slog.Error("poll failed", "adapter", name, "error", err)
		}
		return pollResult{cursor: cursor}
	}
	r := pollResult{cursor: string(next), ok: true, events: len(events)}
	if b, ok := a.(adapter.Backlogger); ok {
		r.backlog = b.Backlog()
	}
	//a full page means a backlog is being worked through, and a backlog is
	//delivered with backpressure rather than dropped: the data is historical,
	//and there is no reason to fetch faster than it can be delivered
	block = block || r.backlog
	for _, e := range events {
		p.opts.Metrics.inc(MetricPolled, labels("adapter", name))
		r.blocked += p.dispatch(ctx, name, e, block)
	}
	if string(next) != cursor {
		//losing a cursor means replaying, which dedupe absorbs
		if err := p.opts.Store.SetCursor(ctx, name, string(next)); err != nil {
			slog.Error("cannot save adapter cursor", "adapter", name, "error", err)
		}
	}
	return r
}

// drain keeps polling while the adapter reports a backlog, without waiting for
// the tick. it stops on the first page that fails, that does not move the
// cursor, or that the adapter says is the last, and on the page cap. a stop
// for any reason hands control back to the tick: the next attempt is a normal
// poll, and the backlog resumes from there if it is still one.
func (p *Pipeline) drain(ctx context.Context, a adapter.Adapter, first pollResult) pollResult {
	name := a.Name()
	start := time.Now()
	pages, events, blocked := 1, first.events, first.blocked
	slog.Info("adapter reports a backlog, draining eagerly", "adapter", name, "cursor", first.cursor)

	r := first
	for r.backlog && pages < maxEagerPages {
		if !sleep(ctx, eagerPause) {
			break
		}
		prev := r.cursor
		r = p.pollOnce(ctx, a, prev, true)
		if !r.ok {
			//guard 2: a failed poll ends the sequence and the tick retries it
			break
		}
		pages++
		events += r.events
		blocked += r.blocked
		slog.Debug("eager page", "adapter", name, "page", pages, "events", r.events,
			"blocked", r.blocked.Round(time.Millisecond).String(), "cursor", r.cursor)
		if r.cursor == prev {
			//guard 1: a backlog that does not move the cursor is not one that
			//polling again will clear
			slog.Warn("adapter reports a backlog but the cursor did not advance, waiting for the tick",
				"adapter", name, "cursor", r.cursor)
			break
		}
	}
	if r.backlog && pages >= maxEagerPages {
		slog.Warn("eager page cap reached with a backlog still reported, waiting for the tick",
			"adapter", name, "pages", pages)
	}
	slog.Info("eager drain finished", "adapter", name, "pages", pages, "events", events,
		"blocked", blocked.Round(time.Millisecond).String(),
		"took", time.Since(start).Round(time.Millisecond).String(), "cursor", r.cursor)
	return r
}

// dispatch deduplicates one event, routes it, and queues it for each
// destination. every outcome that is not a delivery attempt is audited here.
// with block set, a full queue waits instead of dropping; the time spent
// waiting is returned so a drain can say how much backpressure it met.
func (p *Pipeline) dispatch(ctx context.Context, adapterName string, e event.Event, block bool) time.Duration {
	//ponytail: marked seen before delivery, so a crash in between loses the event
	//rather than duplicating it. the fix is a durable queue, not a reordering:
	//marking afterwards only moves the window.
	isNew, err := p.opts.Store.MarkSeen(ctx, adapterName, e.EventID)
	if err != nil {
		slog.Error("dedupe check failed, delivering anyway", "adapter", adapterName, "event_id", e.EventID, "error", err)
	} else if !isNew {
		p.opts.Metrics.inc(MetricDuplicate, labels("adapter", adapterName))
		slog.Debug("duplicate event dropped", "adapter", adapterName, "event_id", e.EventID)
		return 0
	}

	targets := p.opts.Router.Route(e)
	if len(targets) == 0 {
		p.opts.Metrics.inc(MetricUnrouted, "")
		//no rule matched, so there is no rule label to record
		p.audit(ctx, e, "", "", store.StatusDropped, 0, "no matching rule")
		return 0
	}
	var blocked time.Duration
	for _, t := range targets {
		d := p.dests[t.Output]
		if d == nil { //config validation should have caught this
			p.audit(ctx, e, t.Output, t.Rule, store.StatusDropped, 0, "no such output")
			continue
		}
		//a backlog page waits, and so does anything enqueued while this output
		//is still delivering one: a fresh fault behind historical ones is the
		//more valuable of the two
		q := queued{event: e, rule: t.Rule, tracked: block || d.behind()}
		if q.tracked {
			d.track()
		}
		select {
		case d.ch <- q:
			continue
		default:
		}
		//the queue is full. live polling drops: dropping beats blocking the
		//poller behind one slow endpoint when fresh faults are behind it.
		if !q.tracked {
			p.opts.Metrics.inc(MetricDeliveries, labels("output", t.Output, "status", string(store.StatusDropped)))
			p.audit(ctx, e, t.Output, t.Rule, store.StatusDropped, 0, "output queue full")
			slog.Warn("output queue full, event dropped", "output", t.Output, "event_id", e.EventID, "rule", t.Rule)
			continue
		}
		//the only way out without an enqueue is shutdown, and that is audited
		//as what it is rather than as a full queue
		start := time.Now()
		select {
		case d.ch <- q:
			blocked += time.Since(start)
		case <-ctx.Done():
			d.settle()
			p.opts.Metrics.inc(MetricDeliveries, labels("output", t.Output, "status", string(store.StatusDropped)))
			p.audit(ctx, e, t.Output, t.Rule, store.StatusDropped, 0, "shut down before delivery")
		}
	}
	return blocked
}

func (p *Pipeline) work(ctx context.Context, d *dest) {
	for q := range d.ch {
		//shutting down: keep reading so everything queued is accounted for,
		//but stop attempting deliveries
		if ctx.Err() != nil {
			p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusDropped)))
			p.audit(ctx, q.event, d.name, q.rule, store.StatusDropped, 0, "shut down before delivery")
		} else {
			p.deliver(ctx, d, q)
		}
		//every path above is terminal, so this is the one place to settle
		if q.tracked {
			d.settle()
		}
	}
}

func (p *Pipeline) deliver(ctx context.Context, d *dest, q queued) {
	e := q.event
	var err error
	for attempt := 1; attempt <= d.maxAttempts; attempt++ {
		err = d.out.Send(ctx, e)
		switch {
		case err == nil:
			p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusDelivered)))
			p.audit(ctx, e, d.name, q.rule, store.StatusDelivered, attempt, "")
			return

		//five identical rejections help nobody
		case errors.Is(err, output.ErrPermanent):
			p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusFailed)))
			p.audit(ctx, e, d.name, q.rule, store.StatusFailed, attempt, err.Error())
			slog.Error("permanent delivery failure", "output", d.name, "event_id", e.EventID, "error", err)
			return

		case ctx.Err() != nil:
			p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusDropped)))
			p.audit(ctx, e, d.name, q.rule, store.StatusDropped, attempt, "shut down mid-delivery: "+err.Error())
			return
		}

		slog.Warn("delivery attempt failed", "output", d.name, "event_id", e.EventID,
			"attempt", attempt, "of", d.maxAttempts, "error", err)
		if attempt == d.maxAttempts {
			break
		}
		if !sleep(ctx, backoff(attempt, err)) {
			p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusDropped)))
			p.audit(ctx, e, d.name, q.rule, store.StatusDropped, attempt, "shut down between attempts: "+err.Error())
			return
		}
	}

	p.opts.Metrics.inc(MetricDeliveries, labels("output", d.name, "status", string(store.StatusFailed)))
	p.audit(ctx, e, d.name, q.rule, store.StatusFailed, d.maxAttempts, err.Error())
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
		now := time.Now().UTC()
		cutoff := now.Add(-p.opts.DedupeRetention)
		n, err := p.opts.Store.SweepSeen(ctx, cutoff)
		if err != nil && ctx.Err() == nil {
			slog.Error("dedupe sweep failed", "error", err)
		}
		if n > 0 {
			slog.Info("swept expired dedupe entries", "removed", n, "older_than", cutoff)
		}

		//zero means keep everything: a permanent record is a legitimate choice,
		//so it is spelled as a retention rather than as a missing feature
		if p.opts.AuditRetention > 0 {
			cutoff := now.Add(-p.opts.AuditRetention)
			n, err := p.opts.Store.SweepAudit(ctx, cutoff)
			if err != nil && ctx.Err() == nil {
				slog.Error("audit sweep failed", "error", err)
			}
			//at info even though the dedupe sweep is quieter: this is evidence
			//being deleted, and it should be visible in the log that it went
			if n > 0 {
				slog.Info("swept expired audit rows", "removed", n, "older_than", cutoff,
					"retention", p.opts.AuditRetention)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (p *Pipeline) audit(ctx context.Context, e event.Event, out, rule string, status store.Status, attempts int, cause string) {
	audit(ctx, p.opts.Store, store.Record{
		EventID:  e.EventID,
		VIN:      e.VIN,
		Output:   out,
		Rule:     rule,
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
