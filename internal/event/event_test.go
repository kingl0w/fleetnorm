package event

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const schemaPath = "../../schema/event.schema.json"

func compileSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	f, err := os.Open(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	doc, err := jsonschema.UnmarshalJSON(f)
	if err != nil {
		t.Fatal(err)
	}
	c := jsonschema.NewCompiler()
	c.AssertFormat()
	if err := c.AddResource("event.schema.json", doc); err != nil {
		t.Fatal(err)
	}
	sch, err := c.Compile("event.schema.json")
	if err != nil {
		t.Fatal(err)
	}
	return sch
}

// every example must validate against the schema, survive a round trip through
// Event without losing anything, and pass Validate.
func TestExamples(t *testing.T) {
	sch := compileSchema(t)
	paths, err := filepath.Glob("../../schema/examples/*.json")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no examples found: %v", err)
	}
	for _, p := range paths {
		t.Run(filepath.Base(p), func(t *testing.T) {
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			var want any
			if err := json.Unmarshal(b, &want); err != nil {
				t.Fatal(err)
			}
			if err := sch.Validate(want); err != nil {
				t.Fatalf("schema: %v", err)
			}

			var e Event
			if err := json.Unmarshal(b, &e); err != nil {
				t.Fatal(err)
			}
			if err := e.Validate(); err != nil {
				t.Fatalf("Validate: %v", err)
			}

			out, err := json.Marshal(&e)
			if err != nil {
				t.Fatal(err)
			}
			var got any
			if err := json.Unmarshal(out, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("round trip lost or changed data\n got: %s\nwant: %s", out, b)
			}
		})
	}
}

// the Go type and the JSON Schema are the same contract stated twice. This
// fails when one moves without the other.
func TestSchemaDrift(t *testing.T) {
	var schema struct {
		Required   []string                   `json:"required"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	b, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &schema); err != nil {
		t.Fatal(err)
	}

	goProps := map[string]bool{} //name -> required
	rt := reflect.TypeFor[Event]()
	for i := range rt.NumField() {
		tag := rt.Field(i).Tag.Get("json")
		name, opts, _ := strings.Cut(tag, ",")
		goProps[name] = !strings.Contains(opts, "omitempty")
	}

	for name, required := range goProps {
		if _, ok := schema.Properties[name]; !ok {
			t.Errorf("Event.%s is missing from the schema's properties", name)
		}
		if required != contains(schema.Required, name) {
			t.Errorf("%s: required in Go = %v, required in schema = %v", name, required, !required)
		}
	}
	for name := range schema.Properties {
		if _, ok := goProps[name]; !ok {
			t.Errorf("schema property %s has no field on Event", name)
		}
	}
}

func TestValidate(t *testing.T) {
	at := time.Date(2026, 9, 14, 13, 4, 5, 0, time.UTC)
	base := func() Event {
		return Event{
			SchemaVersion: SchemaVersion,
			EventID:       "replay-1",
			VIN:           "1XKYDP9X1MJ123456",
			OccurredAt:    at,
			ReceivedAt:    at,
			Source:        "replay",
			SourceType:    SourceFile,
			Severity:      SeverityHigh,
		}
	}
	ptr := func(i int) *int { return &i }

	tests := []struct {
		name string
		mut  func(*Event)
		want string //substring of the expected error; "" means valid
	}{
		{name: "valid", mut: func(*Event) {}},
		{name: "valid with optionals", mut: func(e *Event) {
			e.SPN, e.FMI, e.OccurrenceCount = ptr(3226), ptr(0), ptr(1)
			e.Location = &Location{Lat: -33.8688, Lon: 151.2093}
			e.Tags = map[string]string{"terminal": "chicago-yard"}
			e.Raw = json.RawMessage(`{"faultCode":"SPN3226-FMI20"}`)
		}},
		{name: "no schema_version", mut: func(e *Event) { e.SchemaVersion = "" }, want: "schema_version"},
		{name: "no event_id", mut: func(e *Event) { e.EventID = "" }, want: "event_id"},
		{name: "no vin", mut: func(e *Event) { e.VIN = "" }, want: "vin"},
		{name: "no occurred_at", mut: func(e *Event) { e.OccurredAt = time.Time{} }, want: "occurred_at is required"},
		{name: "no received_at", mut: func(e *Event) { e.ReceivedAt = time.Time{} }, want: "received_at is required"},
		{name: "occurred_at not UTC", mut: func(e *Event) {
			e.OccurredAt = at.In(time.FixedZone("CDT", -5*60*60))
		}, want: "must be UTC"},
		{name: "no source", mut: func(e *Event) { e.Source = "" }, want: "source is required"},
		{name: "bad source_type", mut: func(e *Event) { e.SourceType = "carrier" }, want: "source_type"},
		{name: "empty source_type", mut: func(e *Event) { e.SourceType = "" }, want: "source_type"},
		{name: "bad severity", mut: func(e *Event) { e.Severity = "URGENT" }, want: "severity"},
		{name: "negative spn", mut: func(e *Event) { e.SPN = ptr(-1) }, want: "spn"},
		{name: "fmi out of range", mut: func(e *Event) { e.FMI = ptr(32) }, want: "fmi"},
		{name: "negative occurrence_count", mut: func(e *Event) { e.OccurrenceCount = ptr(-2) }, want: "occurrence_count"},
		{name: "lat out of range", mut: func(e *Event) { e.Location = &Location{Lat: 91} }, want: "lat"},
		{name: "lon out of range", mut: func(e *Event) { e.Location = &Location{Lon: -181} }, want: "lon"},
		{name: "raw not json", mut: func(e *Event) { e.Raw = json.RawMessage(`{oops`) }, want: "raw"},
	}

	sch := compileSchema(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := base()
			tt.mut(&e)
			err := e.Validate()
			switch {
			case tt.want == "" && err != nil:
				t.Fatalf("Validate() = %v, want nil", err)
			case tt.want != "" && err == nil:
				t.Fatalf("Validate() = nil, want error about %q", tt.want)
			case tt.want != "" && !strings.Contains(err.Error(), tt.want):
				t.Fatalf("Validate() = %v, want error about %q", err, tt.want)
			}
			//whatever Validate accepts, the schema must accept too.
			if err != nil {
				return
			}
			b, err := json.Marshal(&e)
			if err != nil {
				t.Fatal(err)
			}
			var inst any
			if err := json.Unmarshal(b, &inst); err != nil {
				t.Fatal(err)
			}
			if err := sch.Validate(inst); err != nil {
				t.Errorf("Validate passed but schema rejected: %v", err)
			}
		})
	}
}

func TestAnnotate(t *testing.T) {
	ptr := func(i int) *int { return &i }
	tests := []struct {
		name string
		spn  *int
		tags map[string]string
		want map[string]string
	}{
		{name: "nil spn", spn: nil, want: nil},
		{name: "below ceiling", spn: ptr(3226), want: nil},
		{name: "at ceiling", spn: ptr(maxSPN), want: nil},
		{name: "above ceiling", spn: ptr(maxSPN + 1), want: map[string]string{TagSPNOutOfRange: "true"}},
		{
			name: "above ceiling keeps existing tags",
			spn:  ptr(999999),
			tags: map[string]string{"terminal": "chicago-yard"},
			want: map[string]string{"terminal": "chicago-yard", TagSPNOutOfRange: "true"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := Event{SPN: tt.spn, Tags: tt.tags}
			e.Annotate()
			if !reflect.DeepEqual(e.Tags, tt.want) {
				t.Fatalf("Tags = %v, want %v", e.Tags, tt.want)
			}
			if tt.spn != nil && (e.SPN == nil || *e.SPN != *tt.spn) {
				t.Errorf("Annotate changed SPN: got %v, want %v", e.SPN, *tt.spn)
			}

			again := Event{SPN: tt.spn, Tags: e.Tags}
			again.Annotate()
			if !reflect.DeepEqual(again.Tags, e.Tags) {
				t.Errorf("not idempotent: second call gave %v, first gave %v", again.Tags, e.Tags)
			}
		})
	}
}

func TestValidateReservedTags(t *testing.T) {
	at := time.Date(2026, 9, 14, 13, 4, 5, 0, time.UTC)
	spn := 999999
	e := Event{
		SchemaVersion: SchemaVersion,
		EventID:       "replay-1",
		VIN:           "1XKYDP9X1MJ123456",
		OccurredAt:    at,
		ReceivedAt:    at,
		Source:        "replay",
		SourceType:    SourceFile,
		Severity:      SeverityHigh,
		SPN:           &spn,
	}

	//Validate must accept its own annotations.
	e.Annotate()
	if _, ok := e.Tags[TagSPNOutOfRange]; !ok {
		t.Fatalf("setup: Annotate did not tag, got %v", e.Tags)
	}
	if err := e.Validate(); err != nil {
		t.Errorf("Validate rejected an annotated event: %v", err)
	}

	//but not an adapter squatting on the namespace.
	for _, k := range []string{TagPrefix, TagPrefix + "made_up", TagPrefix + "spn_out_of_range_2"} {
		e.Tags = map[string]string{k: "true"}
		if err := e.Validate(); err == nil {
			t.Errorf("Validate accepted reserved tag %q, want error", k)
		} else if !strings.Contains(err.Error(), "reserved") {
			t.Errorf("Validate(%q) = %v, want an error about the reserved namespace", k, err)
		}
	}

	//Tags outside the namespace stay fine, prefix match must be on the dot.
	e.Tags = map[string]string{"terminal": "chicago-yard", "fleetnorm_shop": "true", "fleetnorm": "x"}
	if err := e.Validate(); err != nil {
		t.Errorf("Validate rejected ordinary tags: %v", err)
	}
}
