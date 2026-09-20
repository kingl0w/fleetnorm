package geotab

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ianfrushon/fleetnorm/internal/adapter"
	"github.com/ianfrushon/fleetnorm/internal/adapter/geotab/fixtures"
	"github.com/ianfrushon/fleetnorm/internal/event"
)

// the committed fixtures must be exactly what the generator produces at the
// committed seed. without this, a generator change nobody regenerated leaves
// the two describing different things and neither says so.
func TestFixturesAreGenerated(t *testing.T) {
	files, err := fixtures.Generate(fixtures.CommittedSeed, fixtures.CommittedCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("the generator produced nothing")
	}
	for _, f := range files {
		path := filepath.Join("testdata", f.Name)
		committed, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("%s: %v (run: make fixtures)", f.Name, err)
			continue
		}
		if string(committed) != string(f.Data) {
			t.Errorf("%s is not what the generator produces at seed %d.\n"+
				"the generator and the committed fixtures have drifted; run: make fixtures",
				path, fixtures.CommittedSeed)
		}
	}

	//and nothing stale is left behind
	got, err := filepath.Glob(filepath.Join("testdata", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(files) {
		t.Errorf("testdata holds %d json files but the generator makes %d; a renamed scene left one behind", len(got), len(files))
	}
}

// the fixtures are synthetic, and a VIN that could belong to a real truck would
// undo that. real VINs never contain I, O or Q.
func TestFixtureVINsCannotBeReal(t *testing.T) {
	var ents map[string]map[string]struct {
		VIN string `json:"vehicleIdentificationNumber"`
	}
	readFixture(t, "entities.json", &ents)

	var checked int
	for id, d := range ents["Device"] {
		if d.VIN == "" {
			continue //the device with no VIN, on purpose
		}
		checked++
		if !strings.ContainsAny(d.VIN, "IOQ") {
			t.Errorf("device %s has VIN %q, which contains none of I, O or Q and so could be a real VIN", id, d.VIN)
		}
	}
	if checked == 0 {
		t.Fatal("no device VINs were checked")
	}
}

func readFixture(t *testing.T, name string, into any) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, into); err != nil {
		t.Fatalf("%s: %v", name, err)
	}
}

// fixtureFeed loads one generated GetFeed page.
func fixtureFeed(t *testing.T, name string) feedResult {
	t.Helper()
	var f feedResult
	readFixture(t, name, &f)
	if f.ToVersion == "" {
		t.Fatalf("%s has no toVersion", name)
	}
	return f
}

// fixtureEntities loads the generated reference set, flattened to the id keyed
// map the test double resolves against. the generator guarantees ids are unique
// across types.
func fixtureEntities(t *testing.T) map[string]string {
	t.Helper()
	var byType map[string]map[string]json.RawMessage
	readFixture(t, "entities.json", &byType)

	out := map[string]string{}
	for typeName, ents := range byType {
		for id, body := range ents {
			if prev, dup := out[id]; dup {
				t.Fatalf("entity id %q appears twice (%s and earlier %s)", id, typeName, prev)
			}
			out[id] = string(body)
		}
	}
	return out
}

// fixtureServer is the fake wired to the generated reference set rather than
// the hand written one.
func fixtureServer(t *testing.T, feeds ...feedResult) (*harness, *server) {
	t.Helper()
	h := newHarness(t)
	s := h.serve(defaultHost, feeds...)
	s.entities = fixtureEntities(t)
	return h, s
}

// the ordinary scene is the boring path: everything resolves and every record
// becomes an event.
func TestFixtureOrdinaryFeed(t *testing.T) {
	page := fixtureFeed(t, "feed_ordinary.json")
	h, _ := fixtureServer(t, page)

	events, cursor := poll(t, newAdapter(t, h.client), "")
	if len(events) != len(page.Data) {
		t.Fatalf("got %d events from %d records, want all of them", len(events), len(page.Data))
	}
	if string(cursor) != page.ToVersion {
		t.Errorf("cursor = %q, want the feed's toVersion %q", cursor, page.ToVersion)
	}
	for _, e := range events {
		if e.SPN == nil {
			t.Errorf("%s has no spn; every ordinary fault resolves one", e.EventID)
		}
		if e.FMI == nil {
			t.Errorf("%s has no fmi", e.EventID)
		}
		if e.VIN == "" || e.UnitID == "" {
			t.Errorf("%s has vin %q unit %q", e.EventID, e.VIN, e.UnitID)
		}
		if e.Tags[TagUnresolved] != "" {
			t.Errorf("%s has unresolved %q, want everything resolved", e.EventID, e.Tags[TagUnresolved])
		}
		if err := e.Validate(); err != nil {
			t.Errorf("%s: %v", e.EventID, err)
		}
	}
}

// the same geotab id at several versions is several events, which is the whole
// reason the version is in event_id.
func TestFixtureRevisionsFeed(t *testing.T) {
	page := fixtureFeed(t, "feed_revisions.json")
	h, _ := fixtureServer(t, page)

	events, _ := poll(t, newAdapter(t, h.client), "")
	if len(events) != len(page.Data) {
		t.Fatalf("got %d events from %d records", len(events), len(page.Data))
	}

	ids := map[string]bool{}
	bySource := map[string][]event.Event{}
	for _, e := range events {
		if ids[e.EventID] {
			t.Errorf("event_id %q is not unique across revisions", e.EventID)
		}
		ids[e.EventID] = true
		bySource[e.Tags[TagSourceID]] = append(bySource[e.Tags[TagSourceID]], e)
	}
	if len(bySource) < 2 {
		t.Fatalf("the revisions scene covers %d source ids, want at least 2", len(bySource))
	}

	var sawGrowingCount, sawDismissal bool
	for _, revs := range bySource {
		if len(revs) < 2 {
			continue
		}
		for i := 1; i < len(revs); i++ {
			if revs[i].OccurrenceCount != nil && revs[i-1].OccurrenceCount != nil &&
				*revs[i].OccurrenceCount > *revs[i-1].OccurrenceCount {
				sawGrowingCount = true
			}
		}
		for _, r := range revs {
			if r.Tags[TagDismissDateTime] != "" && r.Tags[TagDismissUser] != "" {
				sawDismissal = true
			}
		}
	}
	if !sawGrowingCount {
		t.Error("no revision showed an incrementing count")
	}
	if !sawDismissal {
		t.Error("no revision carried a dismissal")
	}
}

// every mapping path that is not the ordinary one.
func TestFixtureEdgeFeed(t *testing.T) {
	page := fixtureFeed(t, "feed_edge.json")
	h, _ := fixtureServer(t, page)

	events, _ := poll(t, newAdapter(t, h.client), "")
	if len(events) != len(page.Data) {
		t.Fatalf("got %d events from %d records; the edge scene is all valid", len(events), len(page.Data))
	}

	var (
		nonSPN, outOfRangeFMI, unresolved, vinFallback bool
		enriched, bare, oddSeverity, noLamps           bool
		noSeverity, unknownSentinel, captured, derived bool
	)
	standards := map[string]bool{}
	for _, e := range events {
		switch {
		case e.Tags[TagDiagnosticCode] != "" && e.Tags[TagDiagnosticType] != "":
			nonSPN = true
			if e.SPN != nil {
				t.Errorf("%s put a %s code in spn", e.EventID, e.Tags[TagDiagnosticType])
			}
		}
		if s := e.Tags[TagDiagnosticStandard]; s != "" {
			standards[s] = true
		}
		if e.Tags[TagSeverityAbsent] == "true" {
			want := defaultSeverity
			if e.Tags[TagSeverityDerived] == "lamp" {
				derived = true
				want = event.SeverityCritical
				if e.Tags[TagRedStopLamp] != "true" {
					t.Errorf("%s derived a severity without a red stop lamp", e.EventID)
				}
			} else {
				noSeverity = true
			}
			if e.Severity != want {
				t.Errorf("%s severity = %q, want %q", e.EventID, e.Severity, want)
			}
		}
		if e.Tags[TagSentinelUnknown] != "" {
			unknownSentinel = true
		}
		if e.Tags[TagSourceID] == "b1" && e.Tags[TagVersion] == "" {
			captured = true
		}
		if e.Tags[TagFailureModeCode] != "" {
			outOfRangeFMI = true
			if e.FMI != nil {
				t.Errorf("%s put an out-of-range code in fmi: %d", e.EventID, *e.FMI)
			}
		}
		if strings.Contains(e.Tags[TagUnresolved], "diagnostic") {
			unresolved = true
			if e.SPN != nil {
				t.Errorf("%s has an spn despite an unresolved diagnostic", e.EventID)
			}
		}
		if e.Tags[TagVINFallback] == "device_id" {
			vinFallback = true
			if e.VIN == "" {
				t.Errorf("%s fell back to the device id but has no vin", e.EventID)
			}
		}
		if e.Description != "" && e.Tags[TagRecommendation] != "" && e.Tags[TagRiskOfBreakdown] != "" {
			enriched = true
		}
		if e.Description == "" {
			bare = true
		}
		if e.Tags[TagSeverityRaw] != "" {
			oddSeverity = true
			if e.Severity != defaultSeverity {
				t.Errorf("%s severity = %q, want the documented default %q", e.EventID, e.Severity, defaultSeverity)
			}
		}
		if _, set := e.Tags[TagAmberWarningLamp]; !set {
			noLamps = true
			if e.LampStatus != "" {
				t.Errorf("%s has lamp_status %q with no lamp fields", e.EventID, e.LampStatus)
			}
		}
		if err := e.Validate(); err != nil {
			t.Errorf("%s: %v", e.EventID, err)
		}
	}

	for name, covered := range map[string]bool{
		"a diagnostic that is not an SPN": nonSPN,
		"a failure mode outside 0-31":     outOfRangeFMI,
		"an unresolved diagnostic":        unresolved,
		"a device with no VIN":            vinFallback,
		"an enriched fault":               enriched,
		"a bare fault":                    bare,
		"an unrecognized severity":        oddSeverity,
		"a fault with no lamp fields":     noLamps,
		"a fault with no severity":        noSeverity,
		"a severity raised by a lamp":     derived,
		"an unknown sentinel":             unknownSentinel,
		"the captured GoFault record":     captured,
		"a j1939 diagnostic":              standards["j1939"],
		"a j1708 diagnostic":              standards["j1708"],
		"an obd diagnostic":               standards["obd"],
		"a device diagnostic":             standards["device"],
	} {
		if !covered {
			t.Errorf("the edge scene no longer covers %s", name)
		}
	}
}

// bad records are skipped and audited, and the poll still succeeds.
func TestFixtureSkipFeed(t *testing.T) {
	page := fixtureFeed(t, "feed_skip.json")
	h, _ := fixtureServer(t, page)

	var skipped []adapter.Skipped
	a := newAdapter(t, h.client, func(o *Options) {
		o.OnSkip = func(_ context.Context, s adapter.Skipped) { skipped = append(skipped, s) }
	})

	events, cursor := poll(t, a, "")
	if len(skipped) == 0 {
		t.Fatal("the skip scene produced no skips")
	}
	if len(events) == 0 {
		t.Fatal("the skip scene produced no events; it is meant to stay under the ceiling")
	}
	if len(events)+len(skipped) != len(page.Data) {
		t.Errorf("%d events + %d skips != %d records; something went unaccounted for",
			len(events), len(skipped), len(page.Data))
	}
	if string(cursor) != page.ToVersion {
		t.Errorf("cursor = %q, want it to advance past the skips", cursor)
	}
	for _, s := range skipped {
		if !strings.HasPrefix(s.ID, "fleet-geotab:") || s.Reason == "" {
			t.Errorf("skip %+v is not named and explained", s)
		}
	}
}

// skipping most of a batch is a changed source, not noise.
func TestFixtureCeilingFeed(t *testing.T) {
	page := fixtureFeed(t, "feed_ceiling.json")
	h, _ := fixtureServer(t, page)

	var audited int
	a := newAdapter(t, h.client, func(o *Options) {
		o.OnSkip = func(context.Context, adapter.Skipped) { audited++ }
	})

	events, cursor, err := a.Poll(context.Background(), "")
	if err == nil {
		t.Fatal("the ceiling scene must fail the poll rather than look healthy")
	}
	if len(events) != 0 || cursor != "" {
		t.Errorf("got %d events and cursor %q, want none and unchanged", len(events), cursor)
	}
	if audited != 0 {
		t.Errorf("audited %d skips; a poll that made no progress reads them again", audited)
	}
}
