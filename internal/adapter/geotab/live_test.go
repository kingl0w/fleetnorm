package geotab

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/ianfrushon/fleetnorm/internal/adapter/geotab/fixtures"
	"github.com/ianfrushon/fleetnorm/internal/event"
)

// everything here pins a behavior found by pointing the adapter at a live
// MyGeotab database, none of which the entity reference documents. the captured
// records live in the fixtures package and are served verbatim.

// capturedServer resolves against the captured diagnostics and nothing else
// but the devices a test needs.
func capturedServer(t *testing.T, records ...string) (*server, *Adapter) {
	t.Helper()
	s, hc, _ := newServer(t, feed("v1", records...))
	s.entities = map[string]string{
		"b1":                      entities["b1"],
		fixtures.CapturedDeviceID: `{"id":"b1C","name":"unit-go-0001","vehicleIdentificationNumber":"FLEETNORMFAKE9001"}`,
	}
	for _, c := range []string{
		fixtures.CapturedSuspectParameter, fixtures.CapturedObdFault,
		fixtures.CapturedSid, fixtures.CapturedGoFault,
	} {
		s.entities[capturedID(t, c)] = c
	}
	return s, newAdapter(t, hc)
}

func capturedID(t *testing.T, captured string) string {
	t.Helper()
	var e struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(captured), &e); err != nil || e.ID == "" {
		t.Fatalf("captured record has no id: %v", err)
	}
	return e.ID
}

// faultFor is a minimal fault naming one diagnostic.
func faultFor(diagnosticID string) string {
	return fmt.Sprintf(`{"id":"f-%s","version":"v1","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"diagnostic":{"id":%q},"severity":"Warning"}`,
		diagnosticID, diagnosticID)
}

// the guard once compared against "SuspectParameterNumber", which no record
// carries, so every real J1939 fault lost its spn silently. this is a real
// Diagnostic: if the constant drifts back, this fails.
func TestCapturedSuspectParameterMapsToSPN(t *testing.T) {
	_, a := capturedServer(t, faultFor(capturedID(t, fixtures.CapturedSuspectParameter)))

	e := poll1(t, a)
	if e.SPN == nil || *e.SPN != 16 {
		t.Fatalf("spn = %v, want 16 from the captured SuspectParameter diagnostic", e.SPN)
	}
	if _, tagged := e.Tags[TagDiagnosticCode]; tagged {
		t.Errorf("an SPN was also routed to %s", TagDiagnosticCode)
	}
	if got := e.Tags[TagDiagnosticStandard]; got != "j1939" {
		t.Errorf("%s = %q, want j1939", TagDiagnosticStandard, got)
	}
}

func TestOtherDiagnosticTypesAreNotSPNs(t *testing.T) {
	for _, tc := range []struct {
		captured, kind, code, standard string
	}{
		{fixtures.CapturedSid, "Sid", "151", "j1708"},
		{fixtures.CapturedObdFault, "ObdFault", "36", "obd"},
		{fixtures.CapturedGoFault, "GoFault", "466", "device"},
	} {
		t.Run(tc.kind, func(t *testing.T) {
			_, a := capturedServer(t, faultFor(capturedID(t, tc.captured)))
			e := poll1(t, a)
			if e.SPN != nil {
				t.Errorf("spn = %d; a %s code is not an SPN", *e.SPN, tc.kind)
			}
			for tag, want := range map[string]string{
				TagDiagnosticCode:     tc.code,
				TagDiagnosticType:     tc.kind,
				TagDiagnosticStandard: tc.standard,
			} {
				if got := e.Tags[tag]; got != want {
					t.Errorf("%s = %q, want %q", tag, got, want)
				}
			}
		})
	}
}

// a source nobody mapped is tagged as it arrived rather than dropped or guessed
func TestUnmappedSourceIsTaggedRaw(t *testing.T) {
	s, a := capturedServer(t, faultFor("dX"))
	s.entities["dX"] = `{"id":"dX","code":7,"diagnosticType":"DataDiagnostic","source":"SourceSystemId"}`
	if got := poll1(t, a).Tags[TagDiagnosticStandard]; got != "SourceSystemId" {
		t.Errorf("%s = %q, want the raw source", TagDiagnosticStandard, got)
	}
}

func TestRefUnmarshalsBothShapes(t *testing.T) {
	for in, want := range map[string]string{
		`"ControllerNoneId"`:           "ControllerNoneId",
		`{"id":"ControllerObdBodyId"}`: "ControllerObdBodyId",
		`{"id":"b1C","name":"extra"}`:  "b1C",
		`null`:                         "",
		`{}`:                           "",
	} {
		var fd faultData
		if err := json.Unmarshal([]byte(`{"controller":`+in+`}`), &fd); err != nil {
			t.Errorf("controller %s: %v", in, err)
			continue
		}
		if fd.Controller.ID != want {
			t.Errorf("controller %s: id = %q, want %q", in, fd.Controller.ID, want)
		}
	}
	//a Diagnostic does it too: controller and source arrive as bare strings
	var d rawEntity
	if err := json.Unmarshal([]byte(fixtures.CapturedSuspectParameter), &d); err != nil {
		t.Fatalf("the captured diagnostic does not decode: %v", err)
	}
	if d.Source.ID != "SourceJ1939Id" {
		t.Errorf("source = %q", d.Source.ID)
	}
}

func TestClassify(t *testing.T) {
	for id, want := range map[string]sentinelKind{
		"NoFailureModeId":         sentinelNone,
		"ControllerNoneId":        sentinelNone,
		"ControllerGoDeviceId":    sentinelValue,
		"SourceJ1939Id":           sentinelValue,
		"SomethingBrandNewId":     sentinelUnknown,
		"aysJxXoc3v0-Y6PGVSjoOxA": notSentinel,
		"b1C":                     notSentinel,
		"b1":                      notSentinel,
		"aXyzId":                  notSentinel, //a generated id that happens to end in Id
		"FailureModeFmi20":        notSentinel,
		"Id":                      notSentinel,
		"":                        notSentinel,
	} {
		if got := classify(id); got != want {
			t.Errorf("classify(%q) = %d, want %d", id, got, want)
		}
	}
}

// the critical one: a sentinel has nothing behind it, so asking for it is an
// error or a meaningless lookup, and it spends the 500/min Get budget either
// way. every known sentinel, in both shapes, at both sites, plus one nobody has
// seen: the server must never hear about any of them.
func TestNoGetIsEverIssuedForASentinel(t *testing.T) {
	ids := []string{"SomethingBrandNewId"}
	for id := range knownSentinels {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	var records []string
	for i, id := range ids {
		for j, shape := range []string{`%q`, `{"id":%q}`} {
			r := fmt.Sprintf(shape, id)
			records = append(records, fmt.Sprintf(
				`{"id":"s%d-%d","version":"v","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"failureMode":%s,"controller":%s}`,
				i, j, r, r))
		}
	}
	s, a := capturedServer(t, records...)

	events, _ := poll(t, a, "")
	if len(events) != len(records) {
		t.Fatalf("got %d events from %d records; a sentinel must not cost a record", len(events), len(records))
	}
	for _, id := range ids {
		if n := s.getCount(id); n != 0 {
			t.Errorf("%s was looked up %d times, want never", id, n)
		}
	}
	if n := s.getCount("b1"); n != 1 {
		t.Errorf("the device was looked up %d times; the real references must still resolve", n)
	}
	for _, e := range events {
		if e.Tags[TagUnresolved] != "" {
			t.Errorf("%s: %s = %q; a sentinel is not a failed lookup", e.EventID, TagUnresolved, e.Tags[TagUnresolved])
		}
		if e.FMI != nil {
			t.Errorf("%s: fmi = %d from a sentinel", e.EventID, *e.FMI)
		}
	}
}

func TestSentinelPolicyPerSite(t *testing.T) {
	record := func(failureMode, controller string) string {
		return fmt.Sprintf(`{"id":"p","version":"v","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"failureMode":%q,"controller":{"id":%q}}`,
			failureMode, controller)
	}
	for _, tc := range []struct {
		name, failureMode, controller string
		wantController, wantUnknown   string
	}{
		{"none means absent", "NoFailureModeId", "ControllerNoneId", "", ""},
		{"a known value is kept as itself", "NoFailureModeId", "ControllerGoDeviceId", "ControllerGoDeviceId", ""},
		{"unknown is kept and tagged", "NoFailureModeId", "ControllerNewId", "ControllerNewId", "controller=ControllerNewId"},
		{"unknown at both sites", "FailureModeNewId", "ControllerNewId", "ControllerNewId", "controller=ControllerNewId,failureMode=FailureModeNewId"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, a := capturedServer(t, record(tc.failureMode, tc.controller))
			e := poll1(t, a)
			if got := e.Tags[TagController]; got != tc.wantController {
				t.Errorf("%s = %q, want %q", TagController, got, tc.wantController)
			}
			if got := e.Tags[TagSentinelUnknown]; got != tc.wantUnknown {
				t.Errorf("%s = %q, want %q", TagSentinelUnknown, got, tc.wantUnknown)
			}
			if _, present := e.Tags[TagUnresolved]; present {
				t.Errorf("%s = %q, want absent", TagUnresolved, e.Tags[TagUnresolved])
			}
			if n := s.getCount(tc.failureMode) + s.getCount(tc.controller); n != 0 {
				t.Errorf("%d lookups went out for sentinels", n)
			}
		})
	}
}

// live records have no severity field at all: not null, absent.
func TestAbsentSeverityGetsTheDocumentedDefault(t *testing.T) {
	_, a := capturedServer(t, `{"id":"n","version":"v","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"}}`)
	e := poll1(t, a)
	if e.Severity != defaultSeverity {
		t.Errorf("severity = %q, want the documented default %q", e.Severity, defaultSeverity)
	}
	if e.Tags[TagSeverityAbsent] != "true" {
		t.Errorf("%s = %q, want true: a defaulted severity must not read as a reported one", TagSeverityAbsent, e.Tags[TagSeverityAbsent])
	}
	if _, present := e.Tags[TagSeverityRaw]; present {
		t.Errorf("%s is set to an empty string; absent must not become an empty tag", TagSeverityRaw)
	}
}

// lamps may raise an absent severity and never lower it, and they are not
// consulted at all when Geotab reported one.
func TestLampsRaiseAnAbsentSeverityOnly(t *testing.T) {
	for _, tc := range []struct {
		name, fields string
		want         event.Severity
		derived      string
	}{
		{"red stop", `"redStopLamp":true,"amberWarningLamp":false`, event.SeverityCritical, "lamp"},
		{"red stop wins over amber", `"redStopLamp":true,"amberWarningLamp":true`, event.SeverityCritical, "lamp"},
		{"amber", `"amberWarningLamp":true,"redStopLamp":false`, event.SeverityMedium, ""},
		{"malfunction", `"malfunctionLamp":true`, event.SeverityMedium, ""},
		{"protect", `"protectWarningLamp":true`, event.SeverityMedium, ""},
		{"all false", `"amberWarningLamp":false,"redStopLamp":false,"malfunctionLamp":false,"protectWarningLamp":false`, defaultSeverity, ""},
		{"lamps absent", `"count":1`, defaultSeverity, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, a := capturedServer(t, `{"id":"l","version":"v","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},`+tc.fields+`}`)
			e := poll1(t, a)
			if e.Severity != tc.want {
				t.Errorf("severity = %q, want %q", e.Severity, tc.want)
			}
			if got, present := e.Tags[TagSeverityDerived]; got != tc.derived || present != (tc.derived != "") {
				t.Errorf("%s = %q (present %v), want %q", TagSeverityDerived, got, present, tc.derived)
			}
			if e.Tags[TagSeverityAbsent] != "true" {
				t.Errorf("%s must be set in every absent case", TagSeverityAbsent)
			}
		})
	}

	//a reported severity is Geotab's answer and the lamps do not overrule it
	for _, reported := range []string{"None", "Warning", "CatastrophicallyBad"} {
		_, a := capturedServer(t, `{"id":"l","version":"v","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"severity":"`+reported+`","redStopLamp":true}`)
		e := poll1(t, a)
		want, known := severityMap[reported]
		if !known {
			want = defaultSeverity
		}
		if e.Severity != want {
			t.Errorf("severity %q with the red stop lamp on = %q, want its own %q", reported, e.Severity, want)
		}
		for _, tag := range []string{TagSeverityDerived, TagSeverityAbsent} {
			if _, present := e.Tags[tag]; present {
				t.Errorf("severity %q: %s is set; derivation must not fire", reported, tag)
			}
		}
	}
}

// the version is the designed path and the hash is the fallback, and an event
// says which one made its id.
func TestEventIDSource(t *testing.T) {
	_, a := capturedServer(t, `{"id":"x","version":"00000000000000A1","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"}}`)
	e := poll1(t, a)
	if e.EventID != "x:00000000000000A1" || e.Tags[TagVersion] != "00000000000000A1" {
		t.Errorf("event_id %q, %s %q; want the version in both", e.EventID, TagVersion, e.Tags[TagVersion])
	}
	if got, present := e.Tags[TagEventIDSource]; present {
		t.Errorf("%s = %q on a versioned record, want absent", TagEventIDSource, got)
	}

	_, a = capturedServer(t, `{"id":"x","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"}}`)
	e = poll1(t, a)
	revision, isHash := strings.CutPrefix(e.EventID, "x:")
	if !isHash || len(revision) != 16 {
		t.Errorf("event_id = %q, want x:<16 hex>", e.EventID)
	}
	if e.Tags[TagEventIDSource] != "hash" {
		t.Errorf("%s = %q, want hash", TagEventIDSource, e.Tags[TagEventIDSource])
	}
	if _, present := e.Tags[TagVersion]; present {
		t.Errorf("%s is set on a record with no version", TagVersion)
	}
}

// the captured FaultData record, byte for byte, through a whole poll.
func TestCapturedGoFaultRoundTrips(t *testing.T) {
	s, a := capturedServer(t, fixtures.CapturedGoFaultData)
	e := poll1(t, a)

	if !strings.HasPrefix(e.EventID, "b1:") || len(e.EventID) <= len("b1:") {
		t.Errorf("event_id = %q, want b1:<revision>", e.EventID)
	}
	if _, present := e.Tags[TagVersion]; present {
		t.Errorf("%s = %q on a record that has no version", TagVersion, e.Tags[TagVersion])
	}
	if e.VIN != "FLEETNORMFAKE9001" || e.UnitID != "unit-go-0001" {
		t.Errorf("vin %q unit %q, want them resolved from device b1C", e.VIN, e.UnitID)
	}
	if e.SPN != nil || e.FMI != nil {
		t.Errorf("spn %v fmi %v, want neither on a GoFault with no failure mode", e.SPN, e.FMI)
	}
	if e.Severity != defaultSeverity {
		t.Errorf("severity = %q, want %q", e.Severity, defaultSeverity)
	}
	if e.LampStatus != "" {
		t.Errorf("lamp_status = %q with no faultLampState", e.LampStatus)
	}
	if e.OccurrenceCount == nil || *e.OccurrenceCount != 1 {
		t.Errorf("occurrence_count = %v, want 1", e.OccurrenceCount)
	}
	want := map[string]string{
		TagSourceID:           "b1",
		TagSeverityAbsent:     "true",
		TagEventIDSource:      "hash",
		TagDiagnosticCode:     "466",
		TagDiagnosticType:     "GoFault",
		TagDiagnosticStandard: "device",
		TagController:         "ControllerGoDeviceId",
		TagFaultState:         "Active",
		TagAmberWarningLamp:   "false",
		TagRedStopLamp:        "false",
		TagMalfunctionLamp:    "false",
		TagProtectWarningLamp: "false",
	}
	for tag, value := range want {
		if got := e.Tags[tag]; got != value {
			t.Errorf("%s = %q, want %q", tag, got, value)
		}
	}
	for _, tag := range []string{TagUnresolved, TagSentinelUnknown, TagFaultLampState, TagSeverityRaw, TagSeverityDerived} {
		if got, present := e.Tags[tag]; present {
			t.Errorf("%s = %q, want absent", tag, got)
		}
	}
	if string(e.Raw) != fixtures.CapturedGoFaultData {
		t.Errorf("raw is not the record as it arrived:\n%s", e.Raw)
	}
	if err := e.Validate(); err != nil {
		t.Error(err)
	}
	for _, id := range []string{"NoFailureModeId", "ControllerGoDeviceId", "FaultStatusActiveId"} {
		if n := s.getCount(id); n != 0 {
			t.Errorf("%s was looked up %d times", id, n)
		}
	}
}

// with no version the record's own bytes make the revision: a change is a new
// event, a verbatim resend is the same one.
func TestVersionlessRevisionsStayDistinct(t *testing.T) {
	changed := strings.Replace(fixtures.CapturedGoFaultData, `"count":1`, `"count":2`, 1)
	_, a := capturedServer(t, fixtures.CapturedGoFaultData, changed, fixtures.CapturedGoFaultData)

	events, _ := poll(t, a, "")
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	if events[0].EventID == events[1].EventID {
		t.Error("a changed revision reused the event_id: dedupe would swallow it")
	}
	if events[0].EventID != events[2].EventID {
		t.Error("a verbatim resend got a new event_id: dedupe would deliver it twice")
	}
}

// poll1 polls a feed that holds exactly one usable record.
// the id depends on what a record says and not on how it was serialized: key
// order and whitespace are not content, a changed value is. and raw is never
// the canonical form, only ever what arrived.
func TestHashIgnoresKeyOrderAndFormatting(t *testing.T) {
	const (
		original  = `{"id":"h","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"count":1,"faultStates":{"effectiveStatus":"FaultStatusActiveId","x":2},"redStopLamp":false}`
		reordered = `{ "redStopLamp": false, "faultStates": {"x": 2.0, "effectiveStatus": "FaultStatusActiveId"},
			"count": 1, "device": {"id": "b1"}, "dateTime": "2026-09-14T08:40:00Z", "id": "h" }`
		changed = `{"id":"h","dateTime":"2026-09-14T08:40:00Z","device":{"id":"b1"},"count":2,"faultStates":{"effectiveStatus":"FaultStatusActiveId","x":2},"redStopLamp":false}`
	)
	_, a := capturedServer(t, original, reordered, changed)
	events, _ := poll(t, a, "")
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	if events[0].EventID != events[1].EventID {
		t.Errorf("the same record with its keys reordered got a new event_id (%s, %s): a serializer change would re-fire every event",
			events[0].EventID, events[1].EventID)
	}
	if events[0].EventID == events[2].EventID {
		t.Error("a record with one changed value kept the event_id: dedupe would swallow the revision")
	}
	//the fake server compacts what it serves, so that is the form that arrived.
	//what matters is that the keys are still in the sender's order, not sorted.
	var arrived bytes.Buffer
	if err := json.Compact(&arrived, []byte(reordered)); err != nil {
		t.Fatal(err)
	}
	if string(events[1].Raw) != arrived.String() {
		t.Errorf("raw was canonicalized; it must be the record as it arrived:\n%s", events[1].Raw)
	}
}

func poll1(t *testing.T, a *Adapter) event.Event {
	t.Helper()
	events, _ := poll(t, a, "")
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	return events[0]
}
