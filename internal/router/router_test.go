package router

import (
	"reflect"
	"testing"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/adapter/file"
	"github.com/ianfrushon/fleetnorm/internal/config"
	"github.com/ianfrushon/fleetnorm/internal/event"
)

func ptr[T any](v T) *T { return &v }

// base is a fully populated event, so every test case can mutate one thing and
// leave the rest matching.
func base() event.Event {
	at := time.Date(2026, 9, 14, 13, 4, 5, 0, time.UTC)
	return event.Event{
		SchemaVersion: event.SchemaVersion,
		EventID:       "e1",
		VIN:           "1XKYDP9X1MJ123456",
		UnitID:        "T-1102",
		OccurredAt:    at,
		ReceivedAt:    at,
		Source:        "replay",
		SourceType:    event.SourceFile,
		Severity:      event.SeverityHigh,
		SPN:           ptr(3226),
		FMI:           ptr(20),
		Tags:          map[string]string{"terminal": "chicago-yard", "derate_pending": "true"},
	}
}

func TestMatches(t *testing.T) {
	tests := []struct {
		name  string
		match config.Match
		event func(*event.Event) //optional mutation of base
		want  bool
	}{
		//a condition left out is not a condition.
		{name: "empty match is a catch-all", want: true},
		{name: "empty match still catches a bare event", want: true, event: func(e *event.Event) {
			*e = event.Event{}
		}},

		{name: "vin", match: config.Match{VIN: "1XKYDP9X1MJ123456"}, want: true},
		{name: "vin mismatch", match: config.Match{VIN: "3AKJHHDR8LSLT1234"}},
		{name: "vin is case sensitive", match: config.Match{VIN: "1xkydp9x1mj123456"}},
		{name: "vin against an event with none", match: config.Match{VIN: "1XKYDP9X1MJ123456"}, event: func(e *event.Event) {
			e.VIN = ""
		}},

		{name: "unit_id", match: config.Match{UnitID: "T-1102"}, want: true},
		{name: "unit_id mismatch", match: config.Match{UnitID: "T-1187"}},
		{name: "unit_id against an event with none", match: config.Match{UnitID: "T-1102"}, event: func(e *event.Event) {
			e.UnitID = ""
		}},

		{name: "source", match: config.Match{Source: "replay"}, want: true},
		{name: "source mismatch", match: config.Match{Source: "geotab"}},
		{name: "source_type", match: config.Match{SourceType: "file"}, want: true},
		{name: "source_type mismatch", match: config.Match{SourceType: "tsp"}},

		{name: "severity single", match: config.Match{Severity: []string{"high"}}, want: true},
		{name: "severity list matches one", match: config.Match{Severity: []string{"critical", "high"}}, want: true},
		{name: "severity list matches none", match: config.Match{Severity: []string{"info", "low"}}},
		{name: "severity is case sensitive", match: config.Match{Severity: []string{"HIGH"}}},
		{name: "empty severity list is not a condition", match: config.Match{Severity: []string{}}, want: true},

		{name: "spn exact", match: config.Match{SPN: config.SPNSet{{Lo: 3226, Hi: 3226}}}, want: true},
		{name: "spn in a list", match: config.Match{SPN: config.SPNSet{{Lo: 3216, Hi: 3216}, {Lo: 3226, Hi: 3226}}}, want: true},
		{name: "spn in no list entry", match: config.Match{SPN: config.SPNSet{{Lo: 110, Hi: 110}, {Lo: 3216, Hi: 3216}}}},
		{name: "spn in range", match: config.Match{SPN: config.SPNSet{{Lo: 3000, Hi: 4000}}}, want: true},
		{name: "spn at range low bound", match: config.Match{SPN: config.SPNSet{{Lo: 3226, Hi: 4000}}}, want: true},
		{name: "spn at range high bound", match: config.Match{SPN: config.SPNSet{{Lo: 3000, Hi: 3226}}}, want: true},
		{name: "spn just below range", match: config.Match{SPN: config.SPNSet{{Lo: 3227, Hi: 4000}}}},
		{name: "spn just above range", match: config.Match{SPN: config.SPNSet{{Lo: 3000, Hi: 3225}}}},
		{name: "spn against an event with none", match: config.Match{SPN: config.SPNSet{{Lo: 3226, Hi: 3226}}}, event: func(e *event.Event) {
			e.SPN = nil
		}},
		{name: "empty spn set is not a condition", match: config.Match{SPN: config.SPNSet{}}, want: true},
		{name: "spn zero matches zero", match: config.Match{SPN: config.SPNSet{{Lo: 0, Hi: 0}}}, want: true, event: func(e *event.Event) {
			e.SPN = ptr(0)
		}},

		{name: "fmi", match: config.Match{FMI: ptr(20)}, want: true},
		{name: "fmi mismatch", match: config.Match{FMI: ptr(4)}},
		//zero is a real FMI, and absent is not zero.
		{name: "fmi zero matches zero", match: config.Match{FMI: ptr(0)}, want: true, event: func(e *event.Event) {
			e.FMI = ptr(0)
		}},
		{name: "fmi zero does not match another value", match: config.Match{FMI: ptr(0)}},
		{name: "fmi against an event with none", match: config.Match{FMI: ptr(20)}, event: func(e *event.Event) {
			e.FMI = nil
		}},

		{name: "tag pair", match: config.Match{Tags: map[string]string{"terminal": "chicago-yard"}}, want: true},
		{name: "tag wrong value", match: config.Match{Tags: map[string]string{"terminal": "denver-yard"}}},
		{name: "tag missing key", match: config.Match{Tags: map[string]string{"trailer": "chicago-yard"}}},
		{name: "every tag pair must match", want: true, match: config.Match{Tags: map[string]string{
			"terminal": "chicago-yard", "derate_pending": "true",
		}}},
		{name: "one bad tag pair fails the rule", match: config.Match{Tags: map[string]string{
			"terminal": "chicago-yard", "derate_pending": "false",
		}}},
		//a missing key must not satisfy a rule asking for an empty value.
		{name: "empty tag value against a missing key", match: config.Match{Tags: map[string]string{"trailer": ""}}},
		{name: "empty tag value against an empty value", match: config.Match{Tags: map[string]string{"trailer": ""}}, want: true, event: func(e *event.Event) {
			e.Tags["trailer"] = ""
		}},
		{name: "tags against an event with none", match: config.Match{Tags: map[string]string{"terminal": "chicago-yard"}}, event: func(e *event.Event) {
			e.Tags = nil
		}},
		{name: "empty tag map is not a condition", match: config.Match{Tags: map[string]string{}}, want: true},

		//conditions are ANDed.
		{name: "everything specified and everything matches", want: true, match: config.Match{
			VIN: "1XKYDP9X1MJ123456", UnitID: "T-1102", Source: "replay", SourceType: "file",
			Severity: []string{"high"}, SPN: config.SPNSet{{Lo: 3000, Hi: 4000}}, FMI: ptr(20),
			Tags: map[string]string{"terminal": "chicago-yard"},
		}},
		{name: "one mismatch among many conditions", match: config.Match{
			VIN: "1XKYDP9X1MJ123456", UnitID: "T-1102", Source: "replay", SourceType: "file",
			Severity: []string{"high"}, SPN: config.SPNSet{{Lo: 3000, Hi: 4000}}, FMI: ptr(4),
			Tags: map[string]string{"terminal": "chicago-yard"},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := base()
			if tt.event != nil {
				tt.event(&e)
			}
			if got := matches(tt.match, e); got != tt.want {
				t.Errorf("matches(%+v) = %v, want %v", tt.match, got, tt.want)
			}
		})
	}
}

func TestRoute(t *testing.T) {
	tests := []struct {
		name  string
		rules []config.Rule
		want  []string
	}{
		{name: "no rules routes nowhere"},
		{
			name:  "catch-all",
			rules: []config.Rule{{Route: []string{"console"}}},
			want:  []string{"console"},
		},
		{
			name: "no rule matches",
			rules: []config.Rule{
				{Match: config.Match{Severity: []string{"critical"}}, Route: []string{"my-shop"}},
				{Match: config.Match{Source: "geotab"}, Route: []string{"console"}},
			},
		},
		{
			//not first-match-wins: a later rule still fires.
			name: "every matching rule fires",
			rules: []config.Rule{
				{Match: config.Match{Severity: []string{"high"}}, Route: []string{"my-shop"}},
				{Match: config.Match{Source: "replay"}, Route: []string{"archive"}},
				{Route: []string{"console"}},
			},
			want: []string{"my-shop", "archive", "console"},
		},
		{
			name: "a non-matching rule between two matching ones is skipped",
			rules: []config.Rule{
				{Match: config.Match{Severity: []string{"high"}}, Route: []string{"my-shop"}},
				{Match: config.Match{Severity: []string{"info"}}, Route: []string{"quiet"}},
				{Route: []string{"console"}},
			},
			want: []string{"my-shop", "console"},
		},
		{
			//named by three rules, delivered once, in the order first chosen.
			name: "destinations are deduplicated across rules",
			rules: []config.Rule{
				{Match: config.Match{Severity: []string{"high"}}, Route: []string{"my-shop", "console"}},
				{Match: config.Match{SPN: config.SPNSet{{Lo: 3226, Hi: 3226}}}, Route: []string{"my-shop"}},
				{Route: []string{"console", "my-shop"}},
			},
			want: []string{"my-shop", "console"},
		},
		{
			name:  "duplicates within one rule are deduplicated too",
			rules: []config.Rule{{Route: []string{"console", "console"}}},
			want:  []string{"console"},
		},
		{
			name: "rule order decides destination order",
			rules: []config.Rule{
				{Route: []string{"console"}},
				{Match: config.Match{Severity: []string{"high"}}, Route: []string{"my-shop"}},
			},
			want: []string{"console", "my-shop"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			//this table is about which outputs are chosen and in what order;
			//TestRouteLabels covers which rule gets the credit
			got := outputs(New(tt.rules).Route(base()))
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Route() = %v, want %v", got, tt.want)
			}
		})
	}
}

// routing the same event twice must give the same answer, and the caller's
// slice is not the router's state.
func TestRouteIsStable(t *testing.T) {
	rules := []config.Rule{
		{Match: config.Match{Severity: []string{"high"}}, Route: []string{"my-shop"}},
		{Route: []string{"console"}},
	}
	r := New(rules)
	want := []string{"my-shop", "console"}

	if got := outputs(r.Route(base())); !reflect.DeepEqual(got, want) {
		t.Fatalf("Route() = %v, want %v", got, want)
	}
	rules = append(rules, config.Rule{Route: []string{"surprise"}})
	if got := outputs(r.Route(base())); !reflect.DeepEqual(got, want) {
		t.Errorf("Route() = %v after the caller appended a rule, want %v", got, want)
	}
	if got := outputs(r.Route(base())); !reflect.DeepEqual(got, want) {
		t.Errorf("Route() = %v on a second call, want %v", got, want)
	}
}

// the shipped example config and replay file are what the quickstart shows, so
// what they do is part of the documentation.
func TestExampleConfigRoutesShippedEvents(t *testing.T) {
	t.Setenv("SHOP_SECRET", "not-a-real-secret")
	c, err := config.Load("../../configs/example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	a, err := file.New("replay", "../../testdata/events.json")
	if err != nil {
		t.Fatal(err)
	}
	events, _, err := a.Poll(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}

	want := map[string][]string{
		"replay-000001": {"console"},            //low, spn 110
		"replay-000002": {"my-shop", "console"}, //critical, spn 3226: rules 1, 2 and 3
		"replay-000003": {"my-shop", "console"}, //high, spn 3216: rules 1, 2 and 3
		"replay-000004": {"console"},            //info, no spn
	}
	if len(events) != len(want) {
		t.Fatalf("got %d events, want %d", len(events), len(want))
	}

	r := New(c.Rules)
	for _, e := range events {
		if got := outputs(r.Route(e)); !reflect.DeepEqual(got, want[e.EventID]) {
			t.Errorf("Route(%s) = %v, want %v", e.EventID, got, want[e.EventID])
		}
	}

	//the shipped rules are named, and those names are what the audit log shows
	byID := map[string][]Target{}
	for _, e := range events {
		byID[e.EventID] = r.Route(e)
	}
	wantRules := map[string][]string{
		"replay-000001": {"everything-to-console"},
		//critical: the first rule wins both outputs even though the second and
		//third also match, because the event is delivered once per output
		"replay-000002": {"urgent-to-shop", "urgent-to-shop"},
		"replay-000003": {"urgent-to-shop", "urgent-to-shop"},
		"replay-000004": {"everything-to-console"},
	}
	for id, targets := range byID {
		var got []string
		for _, t := range targets {
			got = append(got, t.Rule)
		}
		if !reflect.DeepEqual(got, wantRules[id]) {
			t.Errorf("rules for %s = %v, want %v", id, got, wantRules[id])
		}
	}
}

// outputs drops the rule labels, for the tests that are about destinations.
func outputs(targets []Target) []string {
	var out []string
	for _, t := range targets {
		out = append(out, t.Output)
	}
	return out
}

func TestRouteLabels(t *testing.T) {
	for _, tt := range []struct {
		name  string
		rules []config.Rule
		want  []Target
	}{
		{
			name:  "a named rule is credited by name",
			rules: []config.Rule{{Name: "urgent", Match: config.Match{Severity: []string{"high"}}, Route: []string{"my-shop"}}},
			want:  []Target{{Output: "my-shop", Rule: "urgent"}},
		},
		{
			name:  "an unnamed rule falls back to its position",
			rules: []config.Rule{{Route: []string{"console"}}},
			want:  []Target{{Output: "console", Rule: "rule[0]"}},
		},
		{
			name: "the position is the rule's own, not the match count",
			rules: []config.Rule{
				{Match: config.Match{Severity: []string{"info"}}, Route: []string{"never"}},
				{Route: []string{"console"}},
			},
			want: []Target{{Output: "console", Rule: "rule[1]"}},
		},
		{
			//the destination dedupe stays: one delivery, credited to whichever
			//rule put it in the queue
			name: "a shared output keeps the first matching rule",
			rules: []config.Rule{
				{Name: "first", Match: config.Match{Severity: []string{"high"}}, Route: []string{"my-shop"}},
				{Name: "second", Route: []string{"my-shop"}},
			},
			want: []Target{{Output: "my-shop", Rule: "first"}},
		},
		{
			//labels are free-form and need not be unique
			name: "two rules may share a label",
			rules: []config.Rule{
				{Name: "shop", Match: config.Match{Severity: []string{"high"}}, Route: []string{"my-shop"}},
				{Name: "shop", Route: []string{"console"}},
			},
			want: []Target{{Output: "my-shop", Rule: "shop"}, {Output: "console", Rule: "shop"}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := New(tt.rules).Route(base()); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Route() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
