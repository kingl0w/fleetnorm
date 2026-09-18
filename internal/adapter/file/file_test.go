package file

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/adapter"
	"github.com/ianfrushon/fleetnorm/internal/event"
)

var fixedNow = time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)

func write(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func newAdapter(t *testing.T, path string) *Adapter {
	t.Helper()
	a, err := New("replay", path)
	if err != nil {
		t.Fatal(err)
	}
	a.now = func() time.Time { return fixedNow }
	return a
}

func poll(t *testing.T, a *Adapter, since adapter.Cursor) ([]event.Event, adapter.Cursor) {
	t.Helper()
	events, cursor, err := a.Poll(t.Context(), since)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	return events, cursor
}

// sameJSON compares two JSON documents by value, not by byte.
func sameJSON(t *testing.T, got, want json.RawMessage) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal(got, &g); err != nil {
		t.Fatalf("got is not JSON: %s", got)
	}
	if err := json.Unmarshal(want, &w); err != nil {
		t.Fatalf("want is not JSON: %s", want)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("raw =\n%s\nwant\n%s", got, want)
	}
}

// compare checks everything but raw, which is compared as JSON.
func compare(t *testing.T, got event.Event, want event.Event, wantRaw string) {
	t.Helper()
	sameJSON(t, got.Raw, json.RawMessage(wantRaw))
	got.Raw, want.Raw = nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Errorf("event =\n%+v\nwant\n%+v", got, want)
	}
}

func ptr[T any](v T) *T { return &v }

const jsonEvents = `[
  {"event_id":"e1","vin":"1XKYDP9X1MJ123456","occurred_at":"2026-09-14T13:04:05Z","severity":"low"},
  {"event_id":"e2","vin":"3AKJHHDR8LSLT1234","occurred_at":"2026-09-14T08:40:00-05:00",
   "severity":"critical","spn":3226,"fmi":20,"lamp_status":"red",
   "location":{"lat":41.8781,"lon":-87.6298},
   "derate_pending":true,"shop_note":"call dispatch","retries":3}
]`

func TestPollJSON(t *testing.T) {
	a := newAdapter(t, write(t, "events.json", jsonEvents))
	events, cursor := poll(t, a, "")

	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	if cursor != "2" {
		t.Errorf("cursor = %q, want \"2\"", cursor)
	}

	//the adapter fills in what it owns and leaves the rest alone.
	compare(t, events[0], event.Event{
		SchemaVersion: event.SchemaVersion,
		EventID:       "e1",
		VIN:           "1XKYDP9X1MJ123456",
		OccurredAt:    time.Date(2026, 9, 14, 13, 4, 5, 0, time.UTC),
		ReceivedAt:    fixedNow,
		Source:        "replay",
		SourceType:    event.SourceFile,
		Severity:      event.SeverityLow,
	}, `{"event_id":"e1","vin":"1XKYDP9X1MJ123456","occurred_at":"2026-09-14T13:04:05Z","severity":"low"}`)

	//an offset timestamp is converted, not rejected; unknown fields become
	//tags carrying their JSON.
	compare(t, events[1], event.Event{
		SchemaVersion: event.SchemaVersion,
		EventID:       "e2",
		VIN:           "3AKJHHDR8LSLT1234",
		OccurredAt:    time.Date(2026, 9, 14, 13, 40, 0, 0, time.UTC),
		ReceivedAt:    fixedNow,
		Source:        "replay",
		SourceType:    event.SourceFile,
		Severity:      event.SeverityCritical,
		SPN:           ptr(3226),
		FMI:           ptr(20),
		LampStatus:    "red",
		Location:      &event.Location{Lat: 41.8781, Lon: -87.6298},
		Tags: map[string]string{
			"derate_pending": "true",
			"shop_note":      "call dispatch",
			"retries":        "3",
		},
	}, `{"event_id":"e2","vin":"3AKJHHDR8LSLT1234","occurred_at":"2026-09-14T08:40:00-05:00",
	    "severity":"critical","spn":3226,"fmi":20,"lamp_status":"red",
	    "location":{"lat":41.8781,"lon":-87.6298},
	    "derate_pending":true,"shop_note":"call dispatch","retries":3}`)
}

// a stream of objects must produce exactly what the array form produces.
func TestPollJSONLines(t *testing.T) {
	array := newAdapter(t, write(t, "events.json", jsonEvents))
	want, _ := poll(t, array, "")

	lines := `{"event_id":"e1","vin":"1XKYDP9X1MJ123456","occurred_at":"2026-09-14T13:04:05Z","severity":"low"}
{"event_id":"e2","vin":"3AKJHHDR8LSLT1234","occurred_at":"2026-09-14T08:40:00-05:00","severity":"critical","spn":3226,"fmi":20,"lamp_status":"red","location":{"lat":41.8781,"lon":-87.6298},"derate_pending":true,"shop_note":"call dispatch","retries":3}
`
	stream := newAdapter(t, write(t, "events.jsonl", lines))
	got, cursor := poll(t, stream, "")
	if cursor != "2" {
		t.Errorf("cursor = %q, want \"2\"", cursor)
	}
	for i := range want {
		compare(t, got[i], want[i], string(want[i].Raw))
	}
}

func TestPollCSV(t *testing.T) {
	const rows = `event_id,vin,occurred_at,severity,spn,fmi,lat,lon,odometer_km,description,terminal
c1,1XKYDP9X1MJ123456,2026-09-14T13:04:05Z,low,,,,,,,chicago-yard
c2,3AKJHHDR8LSLT1234,2026-09-14T13:40:00Z,critical,3226,20,41.8781,-87.6298,412883.5,NOx sensor drifted high,
`
	a := newAdapter(t, write(t, "events.csv", rows))
	events, cursor := poll(t, a, "")
	if len(events) != 2 || cursor != "2" {
		t.Fatalf("got %d events, cursor %q; want 2, \"2\"", len(events), cursor)
	}

	//empty cells are absent fields, not empty ones: no zero SPN, no empty tag.
	compare(t, events[0], event.Event{
		SchemaVersion: event.SchemaVersion,
		EventID:       "c1",
		VIN:           "1XKYDP9X1MJ123456",
		OccurredAt:    time.Date(2026, 9, 14, 13, 4, 5, 0, time.UTC),
		ReceivedAt:    fixedNow,
		Source:        "replay",
		SourceType:    event.SourceFile,
		Severity:      event.SeverityLow,
		Tags:          map[string]string{"terminal": "chicago-yard"},
	}, `{"event_id":"c1","vin":"1XKYDP9X1MJ123456","occurred_at":"2026-09-14T13:04:05Z","severity":"low",
	    "spn":"","fmi":"","lat":"","lon":"","odometer_km":"","description":"","terminal":"chicago-yard"}`)

	//typed columns become numbers, lat and lon fold into location.
	compare(t, events[1], event.Event{
		SchemaVersion: event.SchemaVersion,
		EventID:       "c2",
		VIN:           "3AKJHHDR8LSLT1234",
		OccurredAt:    time.Date(2026, 9, 14, 13, 40, 0, 0, time.UTC),
		ReceivedAt:    fixedNow,
		Source:        "replay",
		SourceType:    event.SourceFile,
		Severity:      event.SeverityCritical,
		SPN:           ptr(3226),
		FMI:           ptr(20),
		OdometerKM:    ptr(412883.5),
		Location:      &event.Location{Lat: 41.8781, Lon: -87.6298},
		Description:   "NOx sensor drifted high",
	}, `{"event_id":"c2","vin":"3AKJHHDR8LSLT1234","occurred_at":"2026-09-14T13:40:00Z","severity":"critical",
	    "spn":"3226","fmi":"20","lat":"41.8781","lon":"-87.6298","odometer_km":"412883.5",
	    "description":"NOx sensor drifted high","terminal":""}`)
}

func TestPollCursor(t *testing.T) {
	const one = `{"event_id":"e1","vin":"1XK","occurred_at":"2026-09-14T13:04:05Z","severity":"low"}`
	const two = `{"event_id":"e2","vin":"1XK","occurred_at":"2026-09-14T13:05:05Z","severity":"high"}`
	path := write(t, "events.jsonl", one+"\n")
	a := newAdapter(t, path)

	events, cursor := poll(t, a, "")
	if len(events) != 1 || cursor != "1" {
		t.Fatalf("first poll: %d events, cursor %q", len(events), cursor)
	}

	//polling again returns nothing: the cursor is the whole state.
	events, cursor = poll(t, a, cursor)
	if len(events) != 0 || cursor != "1" {
		t.Fatalf("second poll: %d events, cursor %q; want 0, \"1\"", len(events), cursor)
	}

	//appending returns only what is new.
	if err := os.WriteFile(path, []byte(one+"\n"+two+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	events, cursor = poll(t, a, cursor)
	if len(events) != 1 || events[0].EventID != "e2" || cursor != "2" {
		t.Fatalf("after append: %d events (%+v), cursor %q", len(events), events, cursor)
	}

	//a shorter file is a different file: replay it, dedupe absorbs the rest.
	if err := os.WriteFile(path, []byte(two+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	events, cursor = poll(t, a, cursor)
	if len(events) != 1 || events[0].EventID != "e2" || cursor != "1" {
		t.Fatalf("after truncate: %d events (%+v), cursor %q", len(events), events, cursor)
	}

	//an unusable cursor is an error, and leaves the caller's cursor alone.
	for _, bad := range []adapter.Cursor{"line:1", "-1", "1.5"} {
		got, cursor, err := a.Poll(t.Context(), bad)
		if err == nil {
			t.Errorf("Poll(%q) = %v, nil; want an error", bad, got)
		}
		if cursor != bad {
			t.Errorf("Poll(%q) returned cursor %q, want the one it was given", bad, cursor)
		}
	}
}

// a file the adapter cannot normalize fails the poll. Nothing is skipped.
func TestPollErrors(t *testing.T) {
	tests := []struct {
		name, file, content, want string
	}{
		{"no event_id", "e.json", `[{"vin":"1XK","occurred_at":"2026-09-14T13:04:05Z","severity":"low"}]`, "event_id is required"},
		{"no vin", "e.json", `[{"event_id":"e1","occurred_at":"2026-09-14T13:04:05Z","severity":"low"}]`, "vin is required"},
		{"no occurred_at", "e.json", `[{"event_id":"e1","vin":"1XK","severity":"low"}]`, "occurred_at is required"},
		{"bad severity", "e.json", `[{"event_id":"e1","vin":"1XK","occurred_at":"2026-09-14T13:04:05Z","severity":"URGENT"}]`, "severity"},
		{"bad source_type", "e.json", `[{"event_id":"e1","vin":"1XK","occurred_at":"2026-09-14T13:04:05Z","severity":"low","source_type":"carrier"}]`, "source_type"},
		{"reserved tag from a source field", "e.json", `[{"event_id":"e1","vin":"1XK","occurred_at":"2026-09-14T13:04:05Z","severity":"low","fleetnorm.made_up":"x"}]`, "reserved"},
		{"malformed JSON array", "e.json", `[{"event_id":`, "not a JSON array"},
		{"malformed JSON stream", "e.json", `{"event_id":"e1"}{"event_id":`, "record 2"},
		{"not an object", "e.json", `["just a string"]`, "record 1"},
		{"bad timestamp", "e.json", `[{"event_id":"e1","vin":"1XK","occurred_at":"last tuesday","severity":"low"}]`, "record 1"},

		{"csv bad number", "e.csv", "event_id,vin,occurred_at,severity,spn\nc1,1XK,2026-09-14T13:04:05Z,low,many\n", `column spn: "many" is not a number`},
		{"csv lat without lon", "e.csv", "event_id,vin,occurred_at,severity,lat\nc1,1XK,2026-09-14T13:04:05Z,low,41.8\n", "lat and lon must be given together"},
		{"csv bad json column", "e.csv", "event_id,vin,occurred_at,severity,tags\nc1,1XK,2026-09-14T13:04:05Z,low,{oops\n", "is not valid JSON"},
		{"csv ragged row", "e.csv", "event_id,vin\nc1\n", "wrong number of fields"},
		{"csv unnamed column", "e.csv", "event_id,,vin\nc1,x,1XK\n", "header column 2 has no name"},
		{"csv line number", "e.csv", "event_id,vin,occurred_at,severity\nc1,1XK,2026-09-14T13:04:05Z,low\nc2,1XK,2026-09-14T13:04:05Z,nonsense\n", "line 3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newAdapter(t, write(t, tt.file, tt.content))
			events, _, err := a.Poll(t.Context(), "")
			if err == nil {
				t.Fatalf("Poll() = %+v, nil; want an error about %q", events, tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Poll() error = %v, want it to mention %q", err, tt.want)
			}
			if events != nil {
				t.Errorf("Poll() returned %d events alongside an error, want none", len(events))
			}
		})
	}
}

func TestPollEmptyFiles(t *testing.T) {
	for _, f := range []struct{ name, content string }{
		{"events.json", ""},
		{"events.json", "[]"},
		{"events.csv", ""},
		{"events.csv", "event_id,vin\n"},
	} {
		a := newAdapter(t, write(t, f.name, f.content))
		events, cursor := poll(t, a, "")
		if len(events) != 0 || cursor != "0" {
			t.Errorf("%s %q: got %d events, cursor %q; want 0, \"0\"", f.name, f.content, len(events), cursor)
		}
	}
}

func TestNew(t *testing.T) {
	dir := t.TempDir()
	if _, err := New("replay", filepath.Join(dir, "nope.json")); err == nil {
		t.Error("New with a missing file should fail at startup")
	}
	if _, err := New("replay", dir); err == nil {
		t.Error("New with a directory should fail")
	}
	if _, err := New("", write(t, "e.json", "[]")); err == nil {
		t.Error("New without a name should fail")
	}
	a := newAdapter(t, write(t, "e.json", "[]"))
	if a.Name() != "replay" {
		t.Errorf("Name() = %q, want \"replay\"", a.Name())
	}
}

// knownFields decides what becomes a tag, so a field added to Event without a
//
//line here would silently start arriving as a tag instead.
func TestKnownFieldsMatchEvent(t *testing.T) {
	rt := reflect.TypeFor[event.Event]()
	fields := map[string]bool{}
	for i := range rt.NumField() {
		name, _, _ := strings.Cut(rt.Field(i).Tag.Get("json"), ",")
		fields[name] = true
		if !knownFields[name] {
			t.Errorf("Event field %q is missing from knownFields, so it would become a tag", name)
		}
	}
	for name := range knownFields {
		if !fields[name] {
			t.Errorf("knownFields has %q, which is not a field of Event", name)
		}
	}
}

// the shipped replay files are what the quickstart and configs/example.yaml
// point at. They have to work.
func TestShippedTestdata(t *testing.T) {
	for _, name := range []string{"events.json", "events.csv"} {
		t.Run(name, func(t *testing.T) {
			a, err := New("replay", filepath.Join("..", "..", "..", "testdata", name))
			if err != nil {
				t.Fatal(err)
			}
			events, cursor, err := a.Poll(t.Context(), "")
			if err != nil {
				t.Fatalf("Poll: %v", err)
			}
			if len(events) == 0 {
				t.Fatal("no events")
			}
			if got := string(cursor); got != strconv.Itoa(len(events)) {
				t.Errorf("cursor = %q, want %d", got, len(events))
			}
			//Poll promises valid events; check rather than assume.
			for _, e := range events {
				if err := e.Validate(); err != nil {
					t.Errorf("%s: %v", e.EventID, err)
				}
				if len(e.Raw) == 0 {
					t.Errorf("%s: raw was dropped", e.EventID)
				}
			}
		})
	}
}

// recorder stands in for the pipeline's audit sink.
type recorder struct{ skips []adapter.Skipped }

func (r *recorder) skip(_ context.Context, s adapter.Skipped) { r.skips = append(r.skips, s) }

func newLenient(t *testing.T, path string, rec *recorder) *Adapter {
	t.Helper()
	a, err := NewLenient("replay", path, rec.skip)
	if err != nil {
		t.Fatal(err)
	}
	a.now = func() time.Time { return fixedNow }
	return a
}

const badSeverity = `{"event_id":"%s","vin":"1XK","occurred_at":"2026-09-14T13:04:05Z","severity":"URGENT"}`
const goodEvent = `{"event_id":"%s","vin":"1XK","occurred_at":"2026-09-14T13:04:05Z","severity":"low"}`

// the standard contract: one bad record is stepped over, not a poll failure.
func TestLenientSkipsBadRecords(t *testing.T) {
	tests := []struct {
		name, file, content, wantID string
	}{
		{
			name: "json lines locate by line", file: "events.jsonl",
			content: fmt.Sprintf(goodEvent+"\n"+badSeverity+"\n"+goodEvent+"\n", "e1", "e2", "e3"),
			wantID:  "replay:line:2",
		},
		{
			name: "json array locates by index", file: "events.json",
			content: "[" + fmt.Sprintf(goodEvent+",\n"+badSeverity+",\n"+goodEvent, "e1", "e2", "e3") + "]",
			wantID:  "replay:index:1",
		},
		{
			name: "csv locates by line", file: "events.csv",
			content: "event_id,vin,occurred_at,severity\n" +
				"e1,1XK,2026-09-14T13:04:05Z,low\n" +
				"e2,1XK,2026-09-14T13:04:05Z,URGENT\n" +
				"e3,1XK,2026-09-14T13:04:05Z,low\n",
			wantID: "replay:line:3",
		},
		{
			name: "csv ragged row is a record, not a poll failure", file: "events.csv",
			content: "event_id,vin,occurred_at,severity\n" +
				"e1,1XK,2026-09-14T13:04:05Z,low\n" +
				"e2,1XK\n" +
				"e3,1XK,2026-09-14T13:04:05Z,low\n",
			wantID: "replay:line:3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			path := write(t, tt.file, tt.content)
			a := newLenient(t, path, rec)

			events, cursor, err := a.Poll(t.Context(), "")
			if err != nil {
				t.Fatalf("Poll: %v, want the good records and no error", err)
			}

			//the good records come through, in order, without the bad one.
			var ids []string
			for _, e := range events {
				ids = append(ids, e.EventID)
			}
			if !reflect.DeepEqual(ids, []string{"e1", "e3"}) {
				t.Errorf("events = %v, want [e1 e3]", ids)
			}

			//the bad one is reported once, with a locator and a reason.
			if len(rec.skips) != 1 {
				t.Fatalf("skips = %+v, want exactly one", rec.skips)
			}
			got := rec.skips[0]
			if got.ID != tt.wantID {
				t.Errorf("skip id = %q, want %q", got.ID, tt.wantID)
			}
			if got.Adapter != "replay" {
				t.Errorf("skip adapter = %q, want \"replay\"", got.Adapter)
			}
			if got.Reason == "" {
				t.Error("skip has no reason")
			}

			//the cursor steps past it: the bad record is not retried forever.
			if cursor != "3" {
				t.Fatalf("cursor = %q, want \"3\"", cursor)
			}
			events, cursor, err = a.Poll(t.Context(), cursor)
			if err != nil || len(events) != 0 || cursor != "3" {
				t.Errorf("re-poll = %d events, %q, %v; want 0, \"3\", nil", len(events), cursor, err)
			}
		})
	}
}

// mostly unusable input looks like health, no errors, a moving cursor, no
// events, so past the ceiling it becomes a poll failure instead.
func TestLenientCeiling(t *testing.T) {
	tests := []struct {
		name          string
		good, bad     int
		wantErr       bool
		wantSkipCalls int
	}{
		{name: "half of twelve is under the line", good: 6, bad: 6, wantSkipCalls: 6},
		{name: "seven of twelve is over it", good: 5, bad: 7, wantErr: true},
		{name: "one of two is noise, not a signal", good: 1, bad: 1, wantSkipCalls: 1},
		{name: "nine of ten trips it", good: 1, bad: 9, wantErr: true},
		{name: "all nine bad is still under the floor", good: 0, bad: 9, wantSkipCalls: 9},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b strings.Builder
			for i := range tt.good {
				fmt.Fprintf(&b, goodEvent+"\n", fmt.Sprintf("good%d", i))
			}
			for i := range tt.bad {
				fmt.Fprintf(&b, badSeverity+"\n", fmt.Sprintf("bad%d", i))
			}

			rec := &recorder{}
			a := newLenient(t, write(t, "events.jsonl", b.String()), rec)
			events, cursor, err := a.Poll(t.Context(), "")

			if tt.wantErr {
				if err == nil {
					t.Fatalf("Poll() = %d events, nil; want an error", len(events))
				}
				//no progress: the cursor stays put and nothing is audited,
				//because these records will be read again.
				if cursor != "" {
					t.Errorf("cursor = %q, want it unchanged", cursor)
				}
				if events != nil {
					t.Errorf("got %d events alongside the error, want none", len(events))
				}
				if len(rec.skips) != 0 {
					t.Errorf("audited %d skips for a poll that made no progress, want 0", len(rec.skips))
				}
				return
			}

			if err != nil {
				t.Fatalf("Poll: %v", err)
			}
			if len(events) != tt.good {
				t.Errorf("got %d events, want %d", len(events), tt.good)
			}
			if len(rec.skips) != tt.wantSkipCalls {
				t.Errorf("audited %d skips, want %d", len(rec.skips), tt.wantSkipCalls)
			}
			if want := strconv.Itoa(tt.good + tt.bad); cursor != adapter.Cursor(want) {
				t.Errorf("cursor = %q, want %q", cursor, want)
			}
		})
	}
}

// whole-poll failures stay whole-poll failures in lenient mode.
func TestLenientStillFailsOnUnusableInput(t *testing.T) {
	for _, tt := range []struct{ name, file, content, want string }{
		{"undecodable array", "e.json", `[{"event_id":`, "not a JSON array"},
		{"broken stream", "e.json", `{"event_id":"e1"}{"event_id":`, "record 2"},
		{"unnamed csv column", "e.csv", "event_id,,vin\nc1,x,1XK\n", "header column 2 has no name"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &recorder{}
			a := newLenient(t, write(t, tt.file, tt.content), rec)
			if _, cursor, err := a.Poll(t.Context(), ""); err == nil {
				t.Errorf("Poll() = nil error, want one about %q", tt.want)
			} else if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Poll() error = %v, want it to mention %q", err, tt.want)
			} else if cursor != "" {
				t.Errorf("cursor = %q, want it unchanged", cursor)
			}
			if len(rec.skips) != 0 {
				t.Errorf("audited %d skips for an undecodable source, want 0", len(rec.skips))
			}
		})
	}
}

// emitted events always carry the source's own event_id. Synthetic ids exist
// only to name a record in the audit log.
func TestSyntheticIDsNeverReachEvents(t *testing.T) {
	rec := &recorder{}
	content := fmt.Sprintf(goodEvent+"\n"+badSeverity+"\n", "e1", "e2")
	a := newLenient(t, write(t, "events.jsonl", content), rec)
	events, _, err := a.Poll(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if strings.Contains(e.EventID, ":") || e.EventID != "e1" {
			t.Errorf("event carries id %q, want the source's own id", e.EventID)
		}
	}
}
