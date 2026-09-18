package stdout

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/event"
)

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

func TestSend(t *testing.T) {
	var buf bytes.Buffer
	o := New("console", &buf)
	if o.Name() != "console" {
		t.Errorf("Name() = %q", o.Name())
	}
	if err := o.Send(t.Context(), testEvent("e1")); err != nil {
		t.Fatal(err)
	}

	line := buf.String()
	if !strings.HasSuffix(line, "\n") || strings.Count(line, "\n") != 1 {
		t.Errorf("want exactly one terminated line, got %q", line)
	}
	var got event.Event
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("output is not JSON: %v", err)
	}
	if got.EventID != "e1" {
		t.Errorf("event_id = %q, want e1", got.EventID)
	}
}

// Send is documented as safe for concurrent use. Run with -race.
func TestConcurrentSend(t *testing.T) {
	const n = 200
	var buf bytes.Buffer
	o := New("console", &buf)

	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := o.Send(context.Background(), testEvent(fmt.Sprintf("e%03d", i))); err != nil {
				t.Errorf("Send: %v", err)
			}
		}()
	}
	wg.Wait()

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	if len(lines) != n {
		t.Fatalf("got %d lines, want %d", len(lines), n)
	}
	seen := map[string]bool{}
	for i, line := range lines {
		var e event.Event
		if err := json.Unmarshal([]byte(line), &e); err != nil {
			t.Fatalf("line %d is not one complete JSON event (interleaved write?): %v\n%q", i+1, err, line)
		}
		if seen[e.EventID] {
			t.Errorf("event %s written twice", e.EventID)
		}
		seen[e.EventID] = true
	}
	if len(seen) != n {
		t.Errorf("got %d distinct events, want %d", len(seen), n)
	}
}

func TestSendRespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var buf bytes.Buffer
	if err := New("console", &buf).Send(ctx, testEvent("e1")); err == nil {
		t.Error("Send with a cancelled context should fail")
	}
	if buf.Len() != 0 {
		t.Errorf("wrote %q despite cancellation", buf.String())
	}
}
