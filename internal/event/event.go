// package event is fleetnorm's normalized fault event. this type and
// schema/event.schema.json are one contract; a test fails if they drift.
package event

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// stamped on every event this build produces
const SchemaVersion = "0.1.0"

// reserved for fleetnorm's own annotations. adapters must not write it.
const TagPrefix = "fleetnorm."

const TagSPNOutOfRange = TagPrefix + "spn_out_of_range"

// every tag fleetnorm writes. adding one is a line here and a line in Annotate.
var knownTags = map[string]bool{
	TagSPNOutOfRange: true,
}

// J1939 19 bit ceiling
const maxSPN = 1<<19 - 1 //524287

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityLow      Severity = "low"
	SeverityMedium   Severity = "medium"
	SeverityHigh     Severity = "high"
	SeverityCritical Severity = "critical"
)

var severities = []Severity{SeverityInfo, SeverityLow, SeverityMedium, SeverityHigh, SeverityCritical}

type SourceType string

const (
	SourceOEM  SourceType = "oem"
	SourceTSP  SourceType = "tsp"
	SourceFile SourceType = "file"
)

var sourceTypes = []SourceType{SourceOEM, SourceTSP, SourceFile}

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

// field order follows schema/event.schema.json; TestSchemaDrift fails otherwise
type Event struct {
	SchemaVersion string     `json:"schema_version"`
	EventID       string     `json:"event_id"` // stable, unique within Source
	VIN           string     `json:"vin"`
	OccurredAt    time.Time  `json:"occurred_at"`
	ReceivedAt    time.Time  `json:"received_at"`
	Source        string     `json:"source"` // adapter name
	SourceType    SourceType `json:"source_type"`
	Severity      Severity   `json:"severity"`

	UnitID          string            `json:"unit_id,omitempty"` // fleet's own asset number
	SPN             *int              `json:"spn,omitempty"`
	FMI             *int              `json:"fmi,omitempty"`
	OccurrenceCount *int              `json:"occurrence_count,omitempty"`
	LampStatus      string            `json:"lamp_status,omitempty"`
	OdometerKM      *float64          `json:"odometer_km,omitempty"`
	Location        *Location         `json:"location,omitempty"`
	Description     string            `json:"description,omitempty"`
	Tags            map[string]string `json:"tags,omitempty"`
	Raw             json.RawMessage   `json:"raw,omitempty"` // verbatim source payload; never drop it
}

// Validate reports the first reason e is not a valid v0.1 event.
func (e *Event) Validate() error {
	if e.SchemaVersion == "" {
		return fmt.Errorf("schema_version is required")
	}
	if e.EventID == "" {
		return fmt.Errorf("event_id is required")
	}
	if e.VIN == "" {
		return fmt.Errorf("vin is required")
	}
	if err := checkTime("occurred_at", e.OccurredAt); err != nil {
		return err
	}
	if err := checkTime("received_at", e.ReceivedAt); err != nil {
		return err
	}
	if e.Source == "" {
		return fmt.Errorf("source is required")
	}
	if !contains(sourceTypes, e.SourceType) {
		return fmt.Errorf("source_type %q is not one of %v", e.SourceType, sourceTypes)
	}
	if !contains(severities, e.Severity) {
		return fmt.Errorf("severity %q is not one of %v", e.Severity, severities)
	}
	if e.SPN != nil && *e.SPN < 0 {
		return fmt.Errorf("spn must not be negative")
	}
	if e.FMI != nil && (*e.FMI < 0 || *e.FMI > 31) {
		return fmt.Errorf("fmi must be 0-31, got %d", *e.FMI)
	}
	if e.OccurrenceCount != nil && *e.OccurrenceCount < 0 {
		return fmt.Errorf("occurrence_count must not be negative")
	}
	if l := e.Location; l != nil {
		if l.Lat < -90 || l.Lat > 90 {
			return fmt.Errorf("location.lat must be -90..90, got %v", l.Lat)
		}
		if l.Lon < -180 || l.Lon > 180 {
			return fmt.Errorf("location.lon must be -180..180, got %v", l.Lon)
		}
	}
	//the prefix means nothing if an adapter can write it
	for k := range e.Tags {
		if strings.HasPrefix(k, TagPrefix) && !knownTags[k] {
			return fmt.Errorf("tag %q is in the reserved %q namespace; adapters must not write it", k, TagPrefix)
		}
	}
	if len(e.Raw) > 0 && !json.Valid(e.Raw) {
		return fmt.Errorf("raw is not valid JSON")
	}
	return nil
}

// Annotate adds fleetnorm's own tags. adapters call it before emitting. it
// mutates where Validate does not, and calling it twice changes nothing.
func (e *Event) Annotate() {
	//tag it, never clamp it: the reported number is what lets someone check
	if e.SPN != nil && *e.SPN > maxSPN {
		e.tag(TagSPNOutOfRange, "true")
	}
}

func (e *Event) tag(k, v string) {
	if e.Tags == nil {
		e.Tags = map[string]string{}
	}
	e.Tags[k] = v
}

func checkTime(field string, t time.Time) error {
	if t.IsZero() {
		return fmt.Errorf("%s is required", field)
	}
	if _, off := t.Zone(); off != 0 {
		return fmt.Errorf("%s must be UTC, got offset %ds", field, off)
	}
	return nil
}

func contains[T comparable](haystack []T, needle T) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}
