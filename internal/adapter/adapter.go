// package adapter is the contract every fleetnorm input satisfies.
//
// one unusable record is skipped, reported through SkipFunc and stepped over. a
// source that cannot be read at all returns no events and an unchanged cursor.
// read docs/adapters.md before writing one.
package adapter

import (
	"context"

	"github.com/ianfrushon/fleetnorm/internal/event"
)

// Cursor is an adapter's opaque bookmark. only its issuer reads it, and the
// zero value means start from the beginning.
type Cursor string

type Adapter interface {
	//stable across restarts: it scopes cursors and dedupe
	Name() string

	//events after since, plus the cursor to pass next time. whatever comes
	//back gets routed, so it must be normalized, annotated and valid.
	Poll(ctx context.Context, since Cursor) ([]event.Event, Cursor, error)
}

// Backlogger is optional. an adapter that can tell it left records behind
// implements it, and the pipeline polls again right away instead of waiting for
// the tick, delivering with backpressure instead of drop-on-full.
//
// Backlog is a stateful side channel: it describes the most recent Poll and is
// read right after Poll returns, on the goroutine that called it. that is the
// one goroutine per adapter model the pipeline uses, and it is the only model
// this is valid under. calling Poll and Backlog from different goroutines reads
// someone else's answer.
type Backlogger interface {
	Backlog() bool
}

// Skipped is a record an adapter could not turn into an event.
type Skipped struct {
	Adapter string //the adapter that read it
	ID      string //synthetic id, see SyntheticID
	Reason  string //why it was skipped, including a source locator
}

// SkipFunc records a skip in the audit log. call it only once the poll is known
// to have succeeded, since a poll that trips the ceiling reads those records
// again.
type SkipFunc func(ctx context.Context, s Skipped)

// SyntheticID names a record with no event_id of its own, for the audit log.
// locator is "line:42" or "index:7". never attached to an emitted event.
func SyntheticID(adapter, locator string) string { return adapter + ":" + locator }

// skipping nearly everything looks like health: no errors, a moving cursor, no
// events. past this it is an error instead. the floor keeps one bad row out of
// two from tripping it.
const (
	SkipCeilingMinRecords = 10
	SkipCeilingFraction   = 0.5
)

// SkipCeilingExceeded reports whether a poll skipped too much to be trusted.
func SkipCeilingExceeded(seen, skipped int) bool {
	return seen >= SkipCeilingMinRecords && float64(skipped) > SkipCeilingFraction*float64(seen)
}
