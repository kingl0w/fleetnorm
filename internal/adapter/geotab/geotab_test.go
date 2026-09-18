package geotab

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/adapter"
	"github.com/ianfrushon/fleetnorm/internal/event"
	"github.com/ianfrushon/fleetnorm/internal/store"
)

var (
	fixedNow  = time.Date(2026, 9, 17, 12, 0, 0, 0, time.UTC)
	seedDate  = fixedNow.Add(-720 * time.Hour)
	seedFixed = func(now time.Time) time.Time { return now.Add(-720 * time.Hour) }
)

// a fault as MyGeotab documents it: ids everywhere, nothing resolved.
const bareFault = `{
  "id": "aBcD1",
  "version": "0000000000A1B2C3",
  "dateTime": "2026-09-14T08:40:00.000Z",
  "device": {"id": "b1"},
  "diagnostic": {"id": "DiagnosticSpn3226"},
  "failureMode": {"id": "FailureModeFmi20"},
  "controller": {"id": "ControllerEngine"},
  "count": 4,
  "severity": "Critical",
  "amberWarningLamp": false,
  "redStopLamp": true,
  "malfunctionLamp": false,
  "protectWarningLamp": false,
  "faultLampState": "Red",
  "faultState": "Active",
  "classCode": "Ecm",
  "sourceAddress": 11
}`

// the same fault as the enriched-fault endpoints return it
const enrichedFault = `{
  "id": "eF2",
  "version": "0000000000A1B2C4",
  "dateTime": "2026-09-14T09:00:00.000Z",
  "device": {"id": "b1"},
  "diagnostic": {"id": "DiagnosticSpn3226"},
  "failureMode": {"id": "FailureModeFmi20"},
  "count": 1,
  "severity": "Warning",
  "amberWarningLamp": true,
  "faultDescription": "Aftertreatment 1 SCR Intake NOx sensor: data drifted high",
  "effectOnComponent": "Reduced engine performance",
  "recommendation": "Schedule service",
  "riskOfBreakdown": 0.42,
  "dismissDateTime": "2026-09-15T10:00:00.000Z",
  "dismissUser": {"id": "u7"}
}`

// entities the multicall returns, keyed by the id asked for
var entities = map[string]string{
	"DiagnosticSpn3226": `{"id":"DiagnosticSpn3226","name":"SCR Intake NOx","code":3226,"diagnosticType":"SuspectParameterNumber"}`,
	"FailureModeFmi20":  `{"id":"FailureModeFmi20","name":"Data drifted high","code":20}`,
	"ControllerEngine":  `{"id":"ControllerEngine","name":"Engine #1"}`,
	"b1":                `{"id":"b1","name":"T-1187","vehicleIdentificationNumber":"3AKJHHDR8LSLT1234"}`,
}

// request is one decoded call the fake server saw.
type request struct {
	Method string
	Params map[string]json.RawMessage
}

// server is a fake MyGeotab. feeds are served in order, one per GetFeed call,
// and Get lookups are counted so a test can prove the cache is doing its job.
type server struct {
	t     *testing.T
	feeds []feedResult

	mu       sync.Mutex
	requests []request
	gets     map[string]int //entity id -> times looked up
	calls    int            //GetFeed calls served

	//set to make the next lookup of this type fail
	failType string
}

func newServer(t *testing.T, feeds ...feedResult) (*server, *http.Client, func()) {
	t.Helper()
	s := &server{t: t, feeds: feeds, gets: map[string]int{}}
	ts := httptest.NewServer(http.HandlerFunc(s.serve))
	client := ts.Client()
	client.Transport = rewrite{ts.URL, client.Transport}
	return s, client, ts.Close
}

// rewrite points every request at the test server, whatever host the adapter
// built from its configured server name.
type rewrite struct {
	base string
	next http.RoundTripper
}

func (r rewrite) RoundTrip(req *http.Request) (*http.Response, error) {
	u := *req.URL
	target := strings.TrimPrefix(r.base, "http://")
	u.Scheme, u.Host = "http", target
	clone := req.Clone(req.Context())
	clone.URL = &u
	clone.Host = target
	return r.next.RoundTrip(clone)
}

func (s *server) serve(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Method string                     `json:"method"`
		Params map[string]json.RawMessage `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.t.Errorf("undecodable request: %v", err)
		return
	}
	s.mu.Lock()
	s.requests = append(s.requests, request{Method: req.Method, Params: req.Params})
	s.mu.Unlock()

	switch req.Method {
	case "GetFeed":
		s.serveFeed(w)
	case "ExecuteMultiCall":
		s.serveMultiCall(w, req.Params["calls"])
	default:
		s.t.Errorf("unexpected method %q", req.Method)
	}
}

func (s *server) serveFeed(w http.ResponseWriter) {
	s.mu.Lock()
	i := s.calls
	s.calls++
	s.mu.Unlock()
	if i >= len(s.feeds) {
		writeResult(w, feedResult{ToVersion: "empty"})
		return
	}
	writeResult(w, s.feeds[i])
}

func (s *server) serveMultiCall(w http.ResponseWriter, calls json.RawMessage) {
	var list []struct {
		Params struct {
			TypeName string `json:"typeName"`
			Search   struct {
				ID string `json:"id"`
			} `json:"search"`
		} `json:"params"`
	}
	if err := json.Unmarshal(calls, &list); err != nil {
		s.t.Errorf("undecodable multicall: %v", err)
		return
	}
	s.mu.Lock()
	if len(list) > 0 && list[0].Params.TypeName == s.failType {
		s.mu.Unlock()
		http.Error(w, "upstream is having a day", http.StatusInternalServerError)
		return
	}
	out := make([][]json.RawMessage, len(list))
	for i, c := range list {
		s.gets[c.Params.Search.ID]++
		if body, ok := entities[c.Params.Search.ID]; ok {
			out[i] = []json.RawMessage{json.RawMessage(body)}
		} else {
			out[i] = []json.RawMessage{} //an unknown id is an empty array
		}
	}
	s.mu.Unlock()
	writeResult(w, out)
}

func writeResult(w http.ResponseWriter, result any) {
	b, err := json.Marshal(map[string]any{"result": result})
	if err != nil {
		panic(err)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(b)
}

// feedParams returns the params of the nth GetFeed request the server saw.
func (s *server) feedParams(t *testing.T, n int) map[string]json.RawMessage {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := 0
	for _, r := range s.requests {
		if r.Method != "GetFeed" {
			continue
		}
		if seen == n {
			return r.Params
		}
		seen++
	}
	t.Fatalf("no GetFeed request %d; saw %d requests", n, len(s.requests))
	return nil
}

func (s *server) getCount(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets[id]
}

func feed(toVersion string, records ...string) feedResult {
	data := make([]json.RawMessage, len(records))
	for i, r := range records {
		data[i] = json.RawMessage(r)
	}
	return feedResult{Data: data, ToVersion: toVersion}
}

func newAdapter(t *testing.T, hc *http.Client, opts ...func(*Options)) *Adapter {
	t.Helper()
	o := Options{
		Name: "fleet-geotab", Server: "my.geotab.com", Database: "mydb",
		Username: "driver@example.com", Password: "hunter2",
		SeedFrom: seedFixed, HTTPClient: hc,
	}
	for _, f := range opts {
		f(&o)
	}
	a, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	a.now = func() time.Time { return fixedNow }
	//the limiter is tested on its own; here it would only make every test wait
	a.client.feed_.every, a.client.get_.every = 0, 0
	return a
}

func poll(t *testing.T, a *Adapter, since adapter.Cursor) ([]event.Event, adapter.Cursor) {
	t.Helper()
	events, cursor, err := a.Poll(context.Background(), since)
	if err != nil {
		t.Fatalf("Poll: %v", err)
	}
	return events, cursor
}

func TestColdStartSeeds(t *testing.T) {
	s, hc, done := newServer(t, feed("v1", bareFault))
	defer done()
	a := newAdapter(t, hc)

	events, cursor := poll(t, a, "")
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if cursor != "v1" {
		t.Errorf("cursor = %q, want the feed's toVersion", cursor)
	}

	params := s.feedParams(t, 0)
	if _, ok := params["fromVersion"]; ok {
		t.Error("a cold start must not send fromVersion: the API ignores it and returns nothing")
	}
	var search struct {
		FromDate string `json:"fromDate"`
	}
	if err := json.Unmarshal(params["search"], &search); err != nil {
		t.Fatalf("no search object on a cold start: %v", err)
	}
	if want := seedDate.Format(time.RFC3339); search.FromDate != want {
		t.Errorf("fromDate = %q, want %q", search.FromDate, want)
	}
}

func TestSecondPollUsesCursor(t *testing.T) {
	s, hc, done := newServer(t, feed("v1", bareFault), feed("v2", enrichedFault))
	defer done()
	a := newAdapter(t, hc)

	poll(t, a, "")
	_, cursor := poll(t, a, "v1")
	if cursor != "v2" {
		t.Errorf("cursor = %q, want v2", cursor)
	}

	params := s.feedParams(t, 1)
	var from string
	if err := json.Unmarshal(params["fromVersion"], &from); err != nil {
		t.Fatalf("second poll sent no fromVersion: %v", err)
	}
	if from != "v1" {
		t.Errorf("fromVersion = %q, want v1", from)
	}
	if _, ok := params["search"]; ok {
		t.Error("seed_from must be ignored once a cursor exists")
	}
}

func TestVersionMakesADistinctEvent(t *testing.T) {
	dismissed := strings.Replace(bareFault, `"version": "0000000000A1B2C3"`,
		`"version": "0000000000A1B2FF", "dismissDateTime": "2026-09-15T10:00:00.000Z"`, 1)
	s, hc, done := newServer(t, feed("v1", bareFault), feed("v2", dismissed))
	defer done()
	a := newAdapter(t, hc)

	first, _ := poll(t, a, "")
	second, _ := poll(t, a, "v1")

	if first[0].EventID == second[0].EventID {
		t.Fatal("a revision must not reuse the first event_id: dedupe would swallow the dismissal")
	}
	if got, want := first[0].EventID, "aBcD1:0000000000A1B2C3"; got != want {
		t.Errorf("event_id = %q, want %q", got, want)
	}
	//the original id survives so a consumer can tie the revisions together
	for _, e := range []event.Event{first[0], second[0]} {
		if e.Tags[TagSourceID] != "aBcD1" {
			t.Errorf("%s: %s = %q, want the geotab id", e.EventID, TagSourceID, e.Tags[TagSourceID])
		}
	}
	if second[0].Tags[TagVersion] != "0000000000A1B2FF" {
		t.Errorf("%s = %q", TagVersion, second[0].Tags[TagVersion])
	}
	if second[0].Tags[TagDismissDateTime] == "" {
		t.Error("the dismissal that caused the resend is not in the event")
	}
	_ = s

	//and both survive the real dedupe, which is the point of the version
	st, err := store.Open(filepath.Join(t.TempDir(), "dedupe.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	//the revision is new, and only a genuine repeat is swallowed
	for i, tc := range []struct {
		e    event.Event
		want bool
	}{{first[0], true}, {second[0], true}, {second[0], false}} {
		fresh, err := st.MarkSeen(context.Background(), a.Name(), tc.e.EventID)
		if err != nil {
			t.Fatal(err)
		}
		if fresh != tc.want {
			t.Errorf("MarkSeen %d (%s) = %v, want %v", i, tc.e.EventID, fresh, tc.want)
		}
	}
}

func TestEnrichmentCacheAsksOnce(t *testing.T) {
	s, hc, done := newServer(t,
		feed("v1", bareFault, enrichedFault),
		feed("v2", strings.Replace(bareFault, `"id": "aBcD1"`, `"id": "third"`, 1)),
	)
	defer done()
	a := newAdapter(t, hc)

	poll(t, a, "")
	poll(t, a, "v1")

	//three records, two polls, one diagnostic id
	if n := s.getCount("DiagnosticSpn3226"); n != 1 {
		t.Errorf("looked up the diagnostic %d times, want 1: the cache is not holding", n)
	}
	if n := s.getCount("b1"); n != 1 {
		t.Errorf("looked up the device %d times, want 1", n)
	}
}

func TestCacheRefreshExpires(t *testing.T) {
	s, hc, done := newServer(t, feed("v1", bareFault), feed("v2", bareFault))
	defer done()
	a := newAdapter(t, hc, func(o *Options) { o.CacheRefresh = time.Minute })

	clock := fixedNow
	a.resolver.(*cache).now = func() time.Time { return clock }

	poll(t, a, "")
	clock = clock.Add(2 * time.Minute)
	poll(t, a, "v1")

	if n := s.getCount("b1"); n != 2 {
		t.Errorf("looked up the device %d times, want 2 once the entry went stale", n)
	}
}

func TestFailedEnrichmentKeepsTheEvent(t *testing.T) {
	s, hc, done := newServer(t, feed("v1", bareFault))
	defer done()
	s.failType = typeDiagnostic
	a := newAdapter(t, hc)

	events, _ := poll(t, a, "")
	if len(events) != 1 {
		t.Fatalf("got %d events, want the fault emitted without its SPN", len(events))
	}
	e := events[0]
	if e.SPN != nil {
		t.Errorf("spn = %d, want absent", *e.SPN)
	}
	if got, want := e.Tags[TagUnresolved], "diagnostic"; got != want {
		t.Errorf("%s = %q, want %q", TagUnresolved, got, want)
	}
	//everything that did resolve is still there
	if e.FMI == nil || *e.FMI != 20 {
		t.Errorf("fmi = %v, want 20", e.FMI)
	}
	if e.VIN != "3AKJHHDR8LSLT1234" {
		t.Errorf("vin = %q", e.VIN)
	}
}

func TestUnknownDeviceFallsBackToID(t *testing.T) {
	fault := strings.Replace(bareFault, `"device": {"id": "b1"}`, `"device": {"id": "bNope"}`, 1)
	_, hc, done := newServer(t, feed("v1", fault))
	defer done()
	a := newAdapter(t, hc)

	events, _ := poll(t, a, "")
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if got, want := events[0].VIN, "bNope"; got != want {
		t.Errorf("vin = %q, want the device id %q", got, want)
	}
	if events[0].Tags[TagVINFallback] != "device_id" {
		t.Errorf("%s = %q, want device_id", TagVINFallback, events[0].Tags[TagVINFallback])
	}
}

func TestFieldMapping(t *testing.T) {
	_, hc, done := newServer(t, feed("v1", bareFault, enrichedFault))
	defer done()
	a := newAdapter(t, hc)

	events, _ := poll(t, a, "")
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2: an enriched fault and a bare one both validate", len(events))
	}
	bare, enriched := events[0], events[1]

	if got, want := bare.OccurredAt, time.Date(2026, 9, 14, 8, 40, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("occurred_at = %v, want %v", got, want)
	}
	if _, off := bare.OccurredAt.Zone(); off != 0 {
		t.Errorf("occurred_at is not UTC")
	}
	if !bare.ReceivedAt.Equal(fixedNow) {
		t.Errorf("received_at = %v, want the poll time %v", bare.ReceivedAt, fixedNow)
	}
	if bare.Source != "fleet-geotab" || bare.SourceType != event.SourceTSP {
		t.Errorf("source = %q/%q", bare.Source, bare.SourceType)
	}
	if bare.SPN == nil || *bare.SPN != 3226 {
		t.Errorf("spn = %v, want 3226 from the diagnostic", bare.SPN)
	}
	if bare.FMI == nil || *bare.FMI != 20 {
		t.Errorf("fmi = %v, want 20 from the failure mode", bare.FMI)
	}
	if bare.OccurrenceCount == nil || *bare.OccurrenceCount != 4 {
		t.Errorf("occurrence_count = %v, want 4", bare.OccurrenceCount)
	}
	if bare.UnitID != "T-1187" {
		t.Errorf("unit_id = %q, want the device name", bare.UnitID)
	}
	if bare.Tags[TagUnresolved] != "" {
		t.Errorf("%s = %q, want nothing", TagUnresolved, bare.Tags[TagUnresolved])
	}

	//raw is the record as it arrived, before any of the above
	var raw map[string]any
	if err := json.Unmarshal(bare.Raw, &raw); err != nil {
		t.Fatalf("raw: %v", err)
	}
	if raw["diagnostic"].(map[string]any)["id"] != "DiagnosticSpn3226" {
		t.Errorf("raw was enriched; it must be verbatim: %v", raw["diagnostic"])
	}
	if _, enrichedIntoRaw := raw["spn"]; enrichedIntoRaw {
		t.Error("raw must not carry normalized fields")
	}

	//the enriched-only fields are present here and simply absent above
	if enriched.Description == "" {
		t.Error("description should come from faultDescription")
	}
	for _, key := range []string{TagEffectOnComponent, TagRecommendation, TagRiskOfBreakdown, TagDismissUser} {
		if enriched.Tags[key] == "" {
			t.Errorf("%s is missing on the enriched fault", key)
		}
	}
	for _, key := range []string{TagEffectOnComponent, TagRecommendation, TagRiskOfBreakdown, TagDismissDateTime} {
		if _, present := bare.Tags[key]; present {
			t.Errorf("%s is set on a bare fault; absence must not become an empty tag", key)
		}
	}
	if bare.Description != "" {
		t.Errorf("description = %q, want empty on a bare fault", bare.Description)
	}
}

func TestLampsAndPassThroughTags(t *testing.T) {
	_, hc, done := newServer(t, feed("v1", bareFault))
	defer done()
	a := newAdapter(t, hc)

	events, _ := poll(t, a, "")
	want := map[string]string{
		TagAmberWarningLamp:   "false",
		TagRedStopLamp:        "true",
		TagMalfunctionLamp:    "false",
		TagProtectWarningLamp: "false",
		TagFaultLampState:     "Red",
		TagController:         "Engine #1",
		TagClassCode:          "Ecm",
		TagFaultState:         "Active",
		TagSourceAddress:      "11",
	}
	for key, value := range want {
		if got := events[0].Tags[key]; got != value {
			t.Errorf("tag %s = %q, want %q", key, got, value)
		}
	}
	//a lamp the source did not report stays absent rather than reading false
	if _, present := events[0].Tags[TagProtectWarningLamp]; !present {
		t.Errorf("%s should be present here", TagProtectWarningLamp)
	}
	noLamps := `{"id":"x","version":"v","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"severity":"None"}`
	_, hc2, done2 := newServer(t, feed("v1", noLamps))
	defer done2()
	events, _ = poll(t, newAdapter(t, hc2), "")
	if _, present := events[0].Tags[TagAmberWarningLamp]; present {
		t.Errorf("%s must be absent when the source did not report it", TagAmberWarningLamp)
	}
}

func TestSeverityMapping(t *testing.T) {
	for _, tc := range []struct {
		geotab string
		want   event.Severity
		rawTag string
	}{
		{"Critical", event.SeverityCritical, ""},
		{"Warning", event.SeverityMedium, ""},
		{"None", event.SeverityInfo, ""},
		{"Unknown", event.SeverityMedium, ""},
		{"CatastrophicallyBad", defaultSeverity, "CatastrophicallyBad"},
		{"", defaultSeverity, ""},
	} {
		t.Run(tc.geotab+"|", func(t *testing.T) {
			fault := strings.Replace(bareFault, `"severity": "Critical"`, `"severity": "`+tc.geotab+`"`, 1)
			_, hc, done := newServer(t, feed("v1", fault))
			defer done()

			events, _ := poll(t, newAdapter(t, hc), "")
			if len(events) != 1 {
				t.Fatalf("got %d events, want 1", len(events))
			}
			if got := events[0].Severity; got != tc.want {
				t.Errorf("severity = %q, want %q", got, tc.want)
			}
			if got := events[0].Tags[TagSeverityRaw]; got != tc.rawTag {
				t.Errorf("%s = %q, want %q", TagSeverityRaw, got, tc.rawTag)
			}
		})
	}
}

func TestSkippedRecordIsAuditedAndThePollSucceeds(t *testing.T) {
	//no version, so there is no event_id that distinguishes revisions
	broken := `{"id":"noVersion","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"severity":"None"}`
	//no id at all, so it is named by its position
	nameless := `{"version":"v9","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"severity":"None"}`
	_, hc, done := newServer(t, feed("v1", bareFault, broken, nameless))
	defer done()

	var skipped []adapter.Skipped
	a := newAdapter(t, hc, func(o *Options) {
		o.OnSkip = func(_ context.Context, s adapter.Skipped) { skipped = append(skipped, s) }
	})

	events, cursor := poll(t, a, "")
	if len(events) != 1 {
		t.Fatalf("got %d events, want the one good record", len(events))
	}
	if cursor != "v1" {
		t.Errorf("cursor = %q, want it to advance past the skips", cursor)
	}
	if len(skipped) != 2 {
		t.Fatalf("audited %d skips, want 2: %+v", len(skipped), skipped)
	}
	want := []string{"fleet-geotab:noVersion", "fleet-geotab:index:2"}
	for i, s := range skipped {
		if s.ID != want[i] {
			t.Errorf("skip %d id = %q, want %q", i, s.ID, want[i])
		}
		if s.Adapter != "fleet-geotab" || s.Reason == "" {
			t.Errorf("skip %d = %+v", i, s)
		}
	}
}

func TestSkipCeiling(t *testing.T) {
	records := make([]string, 0, 12)
	for i := range 12 {
		records = append(records, fmt.Sprintf(
			`{"id":"x%d","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"severity":"None"}`, i))
	}
	_, hc, done := newServer(t, feed("v1", records...))
	defer done()

	var audited int
	a := newAdapter(t, hc, func(o *Options) {
		o.OnSkip = func(context.Context, adapter.Skipped) { audited++ }
	})

	_, cursor, err := a.Poll(context.Background(), "")
	if err == nil {
		t.Fatal("skipping every record must fail the poll, not look healthy")
	}
	if cursor != "" {
		t.Errorf("cursor = %q, want it unchanged after a failed poll", cursor)
	}
	if audited != 0 {
		t.Errorf("audited %d skips; a poll that made no progress reads them again", audited)
	}
}

func TestWholePollFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		code int
		want string
	}{
		{"auth rejected", `{"error":{"message":"bad","errors":[{"name":"InvalidUserException","message":"bad credentials"}]}}`, 200, "InvalidUserException"},
		{"undecodable", `<html>proxy error</html>`, 200, "undecodable"},
		{"server error", `nope`, 500, "500"},
		{"no toVersion", `{"result":{"data":[]}}`, 200, "toVersion"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
				w.Write([]byte(tc.body))
			}))
			defer ts.Close()
			hc := ts.Client()
			hc.Transport = rewrite{ts.URL, hc.Transport}

			events, cursor, err := newAdapter(t, hc).Poll(context.Background(), "v7")
			if err == nil {
				t.Fatal("want an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %v, want it to mention %q", err, tc.want)
			}
			if len(events) != 0 {
				t.Errorf("got %d events, want none", len(events))
			}
			if cursor != "v7" {
				t.Errorf("cursor = %q, want it unchanged", cursor)
			}
		})
	}
}

func TestCredentialsAreNotPrintable(t *testing.T) {
	_, hc, done := newServer(t)
	defer done()
	a := newAdapter(t, hc)

	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		out := fmt.Sprintf(format, a)
		for _, secret := range []string{"hunter2", "driver@example.com"} {
			if strings.Contains(out, secret) {
				t.Errorf("%s printed a credential: %s", format, out)
			}
		}
	}
}

func TestNewRequiresASeed(t *testing.T) {
	_, err := New(Options{Name: "g", Database: "db", Username: "u", Password: "p"})
	if err == nil || !strings.Contains(err.Error(), "seed_from") {
		t.Fatalf("err = %v, want it to name seed_from", err)
	}
}

func TestLimiterSpacesCalls(t *testing.T) {
	l := newLimiter(20 * time.Millisecond)
	start := time.Now()
	for range 3 {
		if err := l.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	//the first call is free, the next two wait
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Errorf("three calls took %v, want at least 40ms of spacing", elapsed)
	}
}

func TestLimiterHonorsContext(t *testing.T) {
	l := newLimiter(time.Hour)
	if err := l.wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := l.wait(ctx); err == nil {
		t.Error("a cancelled poll must not sit out the rate limit")
	}
}

func TestCacheIsBounded(t *testing.T) {
	var fetched int
	c := newCache(func(_ context.Context, typeName string, ids []string) (map[string]Entity, error) {
		fetched += len(ids)
		out := map[string]Entity{}
		for _, id := range ids {
			out[id] = Entity{Name: id}
		}
		return out, nil
	}, time.Hour, 2)

	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if _, err := c.Resolve(ctx, typeDevice, []string{id}); err != nil {
			t.Fatal(err)
		}
	}
	//"a" was evicted to make room for "c", so it is fetched again
	if _, err := c.Resolve(ctx, typeDevice, []string{"b", "c", "a"}); err != nil {
		t.Fatal(err)
	}
	if fetched != 4 {
		t.Errorf("fetched %d ids, want 4: three fresh plus the evicted one", fetched)
	}
}

func TestCacheDeduplicatesWithinOneCall(t *testing.T) {
	var batches [][]string
	c := newCache(func(_ context.Context, _ string, ids []string) (map[string]Entity, error) {
		batches = append(batches, ids)
		return map[string]Entity{ids[0]: {Name: ids[0]}}, nil
	}, time.Hour, 10)

	if _, err := c.Resolve(context.Background(), typeDevice, []string{"b1", "b1", "b1"}); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 1 || len(batches[0]) != 1 {
		t.Errorf("fetched %v, want one id asked for once", batches)
	}
}
