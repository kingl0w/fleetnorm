package store

import (
	"context"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func newStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "fleetnorm.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// at makes the store's clock return a fixed time, so retention is testable
// without sleeping.
func (s *Store) at(t time.Time) { s.now = func() time.Time { return t.UTC() } }

func TestOpenIsIdempotent(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "fleetnorm.db")

	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetCursor(ctx, "replay", "line:42"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	//reopening an existing database must not wipe or fail on the schema.
	s2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	got, err := s2.Cursor(ctx, "replay")
	if err != nil || got != "line:42" {
		t.Errorf("Cursor after reopen = %q, %v; want \"line:42\", nil", got, err)
	}
}

func TestCursor(t *testing.T) {
	ctx := t.Context()
	s := newStore(t)

	//an adapter that has never run has no cursor, and that is not an error:
	//it is how a file adapter knows to start from the top.
	got, err := s.Cursor(ctx, "replay")
	if err != nil {
		t.Fatalf("Cursor of unknown adapter: %v", err)
	}
	if got != "" {
		t.Errorf("Cursor of unknown adapter = %q, want empty", got)
	}

	for _, want := range []string{"line:1", "line:2", ""} {
		if err := s.SetCursor(ctx, "replay", want); err != nil {
			t.Fatal(err)
		}
		if got, err := s.Cursor(ctx, "replay"); err != nil || got != want {
			t.Errorf("Cursor = %q, %v; want %q", got, err, want)
		}
	}

	//cursors are per adapter.
	if err := s.SetCursor(ctx, "replay", "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetCursor(ctx, "other", "b"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Cursor(ctx, "replay"); got != "a" {
		t.Errorf("replay cursor = %q, want \"a\"", got)
	}
	if got, _ := s.Cursor(ctx, "other"); got != "b" {
		t.Errorf("other cursor = %q, want \"b\"", got)
	}
}

func TestMarkSeen(t *testing.T) {
	ctx := t.Context()
	s := newStore(t)

	tests := []struct {
		adapter, eventID string
		want             bool
	}{
		{"replay", "e1", true},  //first sighting
		{"replay", "e1", false}, //duplicate
		{"replay", "e1", false}, //still a duplicate
		{"replay", "e2", true},  //different event
		{"other", "e1", true},   //same id, different adapter: ids are adapter-scoped
		{"replay", "", true},    //empty ids are the adapter's problem, not ours
		{"replay", "", false},
	}
	for _, tt := range tests {
		got, err := s.MarkSeen(ctx, tt.adapter, tt.eventID)
		if err != nil {
			t.Fatalf("MarkSeen(%q, %q): %v", tt.adapter, tt.eventID, err)
		}
		if got != tt.want {
			t.Errorf("MarkSeen(%q, %q) = %v, want %v", tt.adapter, tt.eventID, got, tt.want)
		}
	}
}

func TestSweepSeen(t *testing.T) {
	ctx := t.Context()
	s := newStore(t)
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

	s.at(now.Add(-48 * time.Hour))
	s.MarkSeen(ctx, "replay", "old")
	s.at(now.Add(-1 * time.Hour))
	s.MarkSeen(ctx, "replay", "recent")
	s.at(now)

	n, err := s.SweepSeen(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("SweepSeen removed %d, want 1", n)
	}

	//the swept id is deliverable again (at-least-once), the recent one is not.
	if isNew, _ := s.MarkSeen(ctx, "replay", "old"); !isNew {
		t.Error("swept event id should be treated as new")
	}
	if isNew, _ := s.MarkSeen(ctx, "replay", "recent"); isNew {
		t.Error("in-window event id should still be deduped")
	}

	//sweeping again removes nothing.
	if n, err := s.SweepSeen(ctx, now.Add(-24*time.Hour)); err != nil || n != 0 {
		t.Errorf("second sweep removed %d, %v; want 0, nil", n, err)
	}
}

func TestAudit(t *testing.T) {
	ctx := t.Context()
	s := newStore(t)
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	s.at(now)

	want := []Record{
		{EventID: "e1", VIN: "1XK", Output: "console", Rule: "everything", Status: StatusDelivered, Attempts: 1, RoutedAt: now},
		{EventID: "e1", VIN: "1XK", Output: "my-shop", Rule: "urgent", Status: StatusFailed, Attempts: 5, RoutedAt: now, Error: "502 Bad Gateway"},
		//no rule: a record skipped before routing never had one
		{EventID: "e1", VIN: "1XK", Output: "my-shop", Status: StatusDropped, Attempts: 0, RoutedAt: now, Error: "buffer full"},
	}
	for _, r := range want {
		if err := s.Audit(ctx, Record{
			EventID: r.EventID, VIN: r.VIN, Output: r.Output, Rule: r.Rule,
			Status: r.Status, Attempts: r.Attempts, Error: r.Error,
		}); err != nil { //RoutedAt left zero: it must default to the clock
			t.Fatal(err)
		}
	}
	//an unrelated event must not show up in the first event's history.
	if err := s.Audit(ctx, Record{EventID: "e2", VIN: "3AK", Output: "console", Status: StatusDelivered, Attempts: 1}); err != nil {
		t.Fatal(err)
	}

	got, err := s.AuditFor(ctx, "e1")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("AuditFor(e1) =\n%+v\nwant\n%+v", got, want)
	}
	if got, _ := s.AuditFor(ctx, "nobody"); got != nil {
		t.Errorf("AuditFor of unknown event = %+v, want nil", got)
	}
}

func TestAuditKeepsExplicitTime(t *testing.T) {
	ctx := t.Context()
	s := newStore(t)
	s.at(time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC))

	//sub-millisecond precision is deliberately not stored; a local zone is.
	routed := time.Date(2026, 9, 14, 8, 30, 15, 123_456_789, time.FixedZone("CDT", -5*60*60))
	if err := s.Audit(ctx, Record{EventID: "e1", Output: "console", Status: StatusDelivered, RoutedAt: routed}); err != nil {
		t.Fatal(err)
	}
	got, err := s.AuditFor(ctx, "e1")
	if err != nil {
		t.Fatal(err)
	}
	if want := routed.UTC().Truncate(time.Millisecond); !got[0].RoutedAt.Equal(want) {
		t.Errorf("routed_at = %v, want %v", got[0].RoutedAt, want)
	}
	if _, off := got[0].RoutedAt.Zone(); off != 0 {
		t.Errorf("routed_at came back with offset %d, want UTC", off)
	}
}

// every output writes audit rows from its own goroutine. Whatever the
// connection settings, concurrent writes must not error or lose rows.
func TestConcurrentWrites(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	const writers, each = 8, 25

	var wg sync.WaitGroup
	errs := make(chan error, writers*each)
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				id := string(rune('a'+w)) + string(rune('0'+i%10))
				if _, err := s.MarkSeen(ctx, "replay", id); err != nil {
					errs <- err
				}
				if err := s.Audit(ctx, Record{EventID: id, Output: "console", Status: StatusDelivered, Attempts: 1}); err != nil {
					errs <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent write: %v", err)
	}

	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM audit`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != writers*each {
		t.Errorf("audit rows = %d, want %d", n, writers*each)
	}
}

func TestSweepAudit(t *testing.T) {
	ctx := t.Context()
	s := newStore(t)
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

	s.at(now.Add(-48 * time.Hour))
	if err := s.Audit(ctx, Record{EventID: "old", Output: "console", Rule: "r", Status: StatusDelivered, Attempts: 1}); err != nil {
		t.Fatal(err)
	}
	s.at(now.Add(-1 * time.Hour))
	if err := s.Audit(ctx, Record{EventID: "recent", Output: "console", Rule: "r", Status: StatusDelivered, Attempts: 1}); err != nil {
		t.Fatal(err)
	}
	s.at(now)

	n, err := s.SweepAudit(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("SweepAudit removed %d, want 1", n)
	}
	if got, _ := s.AuditFor(ctx, "old"); got != nil {
		t.Errorf("swept row is still there: %+v", got)
	}
	if got, _ := s.AuditFor(ctx, "recent"); len(got) != 1 {
		t.Errorf("in-window row = %+v, want it kept", got)
	}
	//sweeping again removes nothing
	if n, err := s.SweepAudit(ctx, now.Add(-24*time.Hour)); err != nil || n != 0 {
		t.Errorf("second sweep removed %d, %v; want 0, nil", n, err)
	}
}

// the rule label survives the round trip, and an empty one is not an error
func TestAuditRuleRoundTrip(t *testing.T) {
	ctx := t.Context()
	s := newStore(t)
	for _, rule := range []string{"named-rule", "rule[3]", ""} {
		id := "e-" + rule
		if err := s.Audit(ctx, Record{EventID: id, Output: "console", Rule: rule, Status: StatusDelivered, Attempts: 1}); err != nil {
			t.Fatal(err)
		}
		got, err := s.AuditFor(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Rule != rule {
			t.Errorf("Rule = %+v, want %q", got, rule)
		}
	}
}
