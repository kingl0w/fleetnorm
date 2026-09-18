package geotab

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// request is one call the fake server saw, kept raw so a test can prove what
// was and was not on the wire.
type request struct {
	Method string
	Params map[string]json.RawMessage
	Body   string
}

// server is a fake MyGeotab. feeds are served in order, one per GetFeed call,
// Get lookups are counted so a test can prove the cache is doing its job, and
// the session knobs let a test drive expiry and redirects.
type server struct {
	t     *testing.T
	host  string
	feeds []feedResult

	mu       sync.Mutex
	requests []request
	gets     map[string]int //entity id -> times looked up
	calls    int            //GetFeed calls served
	auths    int            //Authenticate calls served

	path     string //what Authenticate returns as path
	session  string //sessionId to issue; incremented per auth when empty
	authErr  string //when set, Authenticate fails with this exception name
	failType string //make the next lookup of this type fail

	rejectAll bool   //every non-auth call returns InvalidUserException
	expired   string //a call carrying this sessionId returns InvalidUserException
	rawCode   int    //status code for GetFeed, when set
	rawBody   string //verbatim GetFeed response, when set
}

// harness wires one http.Client to any number of fake servers, keyed by host,
// so a test can assert that a host stopped receiving requests.
type harness struct {
	t      *testing.T
	router *hostRouter
	client *http.Client
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, router: &hostRouter{routes: map[string]string{}, next: http.DefaultTransport}}
	h.client = &http.Client{Transport: h.router}
	return h
}

func (h *harness) serve(host string, feeds ...feedResult) *server {
	h.t.Helper()
	s := &server{t: h.t, host: host, feeds: feeds, gets: map[string]int{}, path: thisServer}
	ts := httptest.NewServer(http.HandlerFunc(s.serve))
	h.t.Cleanup(ts.Close)
	h.router.add(host, ts.URL)
	return s
}

// hostRouter sends each request to the server registered for its host. an
// unregistered host is an error rather than a silent fallthrough, which is what
// makes "nothing reached the old server" a real assertion.
type hostRouter struct {
	mu     sync.Mutex
	routes map[string]string
	next   http.RoundTripper
}

func (r *hostRouter) add(host, base string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.routes[host] = strings.TrimPrefix(base, "http://")
}

func (r *hostRouter) RoundTrip(req *http.Request) (*http.Response, error) {
	r.mu.Lock()
	target, ok := r.routes[req.URL.Hostname()]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("no fake server for host %q", req.URL.Hostname())
	}
	u := *req.URL
	u.Scheme, u.Host = "http", target
	clone := req.Clone(req.Context())
	clone.URL = &u
	clone.Host = target
	return r.next.RoundTrip(clone)
}

func newServer(t *testing.T, feeds ...feedResult) (*server, *http.Client, func()) {
	t.Helper()
	h := newHarness(t)
	return h.serve(defaultHost, feeds...), h.client, func() {}
}

const defaultHost = "my.geotab.com"

func (s *server) serve(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		s.t.Errorf("unreadable request: %v", err)
		return
	}
	var req struct {
		Method string                     `json:"method"`
		Params map[string]json.RawMessage `json:"params"`
	}
	if err := json.Unmarshal(raw, &req); err != nil {
		s.t.Errorf("undecodable request: %v", err)
		return
	}
	s.mu.Lock()
	s.requests = append(s.requests, request{Method: req.Method, Params: req.Params, Body: string(raw)})
	s.mu.Unlock()

	if req.Method == "Authenticate" {
		s.serveAuth(w)
		return
	}
	//every other method needs a live session. expiring one sessionId rather
	//than counting calls keeps this independent of how requests interleave.
	s.mu.Lock()
	reject := s.rejectAll || (s.expired != "" && strings.Contains(string(raw), s.expired))
	s.mu.Unlock()
	if reject {
		writeError(w, invalidUser, "session has expired")
		return
	}

	switch req.Method {
	case "GetFeed":
		s.serveFeed(w)
	case "ExecuteMultiCall":
		s.serveMultiCall(w, req.Params["calls"])
	default:
		s.t.Errorf("unexpected method %q", req.Method)
	}
}

func (s *server) serveAuth(w http.ResponseWriter) {
	s.mu.Lock()
	s.auths++
	n := s.auths
	if s.authErr != "" {
		err := s.authErr
		s.mu.Unlock()
		writeError(w, err, "bad credentials")
		return
	}
	id := s.session
	if id == "" {
		id = fmt.Sprintf("session-%d", n)
	}
	path := s.path
	s.mu.Unlock()

	writeResult(w, authResult{
		Credentials: sessionCreds{Database: "mydb", UserName: "driver@example.com", SessionID: id},
		Path:        path,
	})
}

func (s *server) serveFeed(w http.ResponseWriter) {
	s.mu.Lock()
	i := s.calls
	s.calls++
	code, body := s.rawCode, s.rawBody
	s.mu.Unlock()

	if code != 0 || body != "" {
		if code == 0 {
			code = http.StatusOK
		}
		w.WriteHeader(code)
		w.Write([]byte(body))
		return
	}
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

func writeError(w http.ResponseWriter, name, message string) {
	b, err := json.Marshal(map[string]any{"error": map[string]any{
		"message": message,
		"errors":  []map[string]string{{"name": name, "message": message}},
	}})
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

// setExpired makes every later call carrying this sessionId fail the way a
// real expiry does.
func (s *server) setExpired(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expired = id
}

func (s *server) authCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.auths
}

// methods returns every method the server was called with, in order.
func (s *server) methods() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.requests))
	for i, r := range s.requests {
		out[i] = r.Method
	}
	return out
}

func (s *server) snapshot() []request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]request(nil), s.requests...)
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
		name  string
		setup func(*server)
		want  string
	}{
		{"auth rejected", func(s *server) { s.authErr = invalidUser }, invalidUser},
		{"undecodable", func(s *server) { s.rawBody = `<html>proxy error</html>` }, "undecodable"},
		{"server error", func(s *server) { s.rawCode, s.rawBody = 500, "nope" }, "500"},
		{"no toVersion", func(s *server) { s.rawBody = `{"result":{"data":[]}}` }, "toVersion"},
		{"session never valid", func(s *server) { s.rejectAll = true }, "credentials are wrong rather than stale"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			srv := h.serve(defaultHost)
			tc.setup(srv)

			events, cursor, err := newAdapter(t, h.client).Poll(context.Background(), "v7")
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

// a session that is rejected forever must not become a login attempt per call
func TestReauthIsAttemptedOnce(t *testing.T) {
	h := newHarness(t)
	srv := h.serve(defaultHost)
	srv.rejectAll = true

	if _, _, err := newAdapter(t, h.client).Poll(context.Background(), "v1"); err == nil {
		t.Fatal("want an error")
	}
	//first auth, rejected call, re-auth, rejected retry
	if got := srv.authCount(); got != 2 {
		t.Errorf("authenticated %d times, want 2: one on first use and one retry", got)
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

func TestAuthenticatesOnceAndCarriesTheSession(t *testing.T) {
	h := newHarness(t)
	srv := h.serve(defaultHost, feed("v1", bareFault), feed("v2", enrichedFault))
	a := newAdapter(t, h.client)

	poll(t, a, "")
	poll(t, a, "v1")

	if got := srv.authCount(); got != 1 {
		t.Errorf("authenticated %d times, want 1: a session lasts up to 14 days", got)
	}
	if got, want := srv.methods()[0], "Authenticate"; got != want {
		t.Errorf("first call was %q, want %q", got, want)
	}

	for _, r := range srv.snapshot() {
		if r.Method == "Authenticate" {
			if !strings.Contains(r.Body, "hunter2") {
				t.Error("Authenticate did not carry the password")
			}
			continue
		}
		if strings.Contains(r.Body, "hunter2") {
			t.Errorf("%s carried the password: %s", r.Method, r.Body)
		}
		if !strings.Contains(r.Body, "session-1") {
			t.Errorf("%s did not carry the sessionId: %s", r.Method, r.Body)
		}
	}
}

// construction must not touch the network: a typo in the config should fail at
// load, not by hanging on a connect.
func TestNewDoesNoNetworkIO(t *testing.T) {
	h := newHarness(t)
	srv := h.serve(defaultHost, feed("v1"))
	newAdapter(t, h.client)
	if got := srv.authCount(); got != 0 {
		t.Errorf("authenticated %d times during construction, want 0", got)
	}
}

func TestThisServerKeepsTheConfiguredServer(t *testing.T) {
	h := newHarness(t)
	srv := h.serve(defaultHost, feed("v1", bareFault))
	srv.path = thisServer

	events, _ := poll(t, newAdapter(t, h.client), "")
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	if got := srv.authCount(); got != 1 {
		t.Errorf("authenticated %d times, want 1", got)
	}
}

// a customer database that does not live on the configured server is a normal
// deployment. the sessionId is only valid against the server that issued it.
func TestPathRedirectsEveryLaterCall(t *testing.T) {
	h := newHarness(t)
	configured := h.serve(defaultHost)
	configured.path = "https://my23.geotab.com"
	redirected := h.serve("my23.geotab.com", feed("v1", bareFault))

	events, cursor := poll(t, newAdapter(t, h.client), "")
	if len(events) != 1 || cursor != "v1" {
		t.Fatalf("got %d events, cursor %q; want the redirected server's feed", len(events), cursor)
	}

	//the configured server authenticated and then heard nothing more
	if got := configured.methods(); len(got) != 1 || got[0] != "Authenticate" {
		t.Errorf("configured server saw %v, want only Authenticate", got)
	}
	for _, m := range redirected.methods() {
		if m == "Authenticate" {
			t.Error("re-authenticated against the redirected server; the session was already valid there")
		}
	}
	if got := redirected.methods(); len(got) == 0 {
		t.Fatal("the redirected server received nothing")
	}
}

func TestExpiredSessionReauthenticatesOnceAndRetries(t *testing.T) {
	h := newHarness(t)
	srv := h.serve(defaultHost, feed("v1", bareFault), feed("v2", enrichedFault))
	a := newAdapter(t, h.client)

	poll(t, a, "")
	if got := srv.authCount(); got != 1 {
		t.Fatalf("authenticated %d times on the first poll, want 1", got)
	}

	//the session expires between polls
	srv.setExpired("session-1")
	events, cursor := poll(t, a, "v1")

	if got := srv.authCount(); got != 2 {
		t.Errorf("authenticated %d times, want exactly 2", got)
	}
	if len(events) != 1 || cursor != "v2" {
		t.Errorf("got %d events, cursor %q; the retry should have succeeded", len(events), cursor)
	}
	//and the retry carried the new session, not the dead one
	last := srv.snapshot()
	body := last[len(last)-1].Body
	if strings.Contains(body, "session-1") {
		t.Errorf("retry reused the expired sessionId: %s", body)
	}
}

// under -race: every in-flight call hitting an expired session must produce one
// re-authentication between them, not one each.
func TestConcurrentCallsReauthenticateOnce(t *testing.T) {
	h := newHarness(t)
	srv := h.serve(defaultHost)
	a := newAdapter(t, h.client)

	//prime a session, then expire exactly that one for everyone
	if _, err := a.client.current(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv.setExpired("session-1")

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//the error does not matter here; the authentication count does
			a.client.resolve(context.Background(), typeDevice, []string{"b1"})
		}()
	}
	wg.Wait()

	if got := srv.authCount(); got != 2 {
		t.Errorf("authenticated %d times, want 2: one to prime and one shared re-auth", got)
	}
}

func TestLampStatusComesFromFaultLampState(t *testing.T) {
	noLamp := `{"id":"x","version":"v","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"severity":"None"}`
	h := newHarness(t)
	h.serve(defaultHost, feed("v1", bareFault, noLamp))

	events, _ := poll(t, newAdapter(t, h.client), "")
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2", len(events))
	}
	present, absent := events[0], events[1]

	if got, want := present.LampStatus, "Red"; got != want {
		t.Errorf("lamp_status = %q, want %q", got, want)
	}
	//the tag stays too: the field and the tag are not an either/or
	if got, want := present.Tags[TagFaultLampState], "Red"; got != want {
		t.Errorf("%s = %q, want %q", TagFaultLampState, got, want)
	}

	if absent.LampStatus != "" {
		t.Errorf("lamp_status = %q, want absent when the source reported no lamp state", absent.LampStatus)
	}
	if _, set := absent.Tags[TagFaultLampState]; set {
		t.Errorf("%s must be absent, not an empty string", TagFaultLampState)
	}
}
