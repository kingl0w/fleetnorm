// package geotab reads FaultData from a MyGeotab GetFeed change stream.
//
// this is the first adapter reading data neither we nor the fleet owner wrote,
// so it follows the standard adapter contract rather than the file adapter's
// strict default: one unusable record is skipped and audited, and the feed keeps
// moving. read docs/adapters.md, which documents the decisions this file makes.
//
// two of those decisions are worth knowing before reading the code.
//
// GetFeed resends a record whenever it changes, with a newer version, so
// event_id is "<geotab id>:<version>": every revision is its own event and a
// dismissal or a count change survives dedupe instead of being swallowed.
//
// GetFeed with no fromVersion returns no data at all, only the newest version in
// the system, so a feed started with no seed date is silently empty forever
// while healthz stays green. seed_from is required for that reason.
package geotab

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ianfrushon/fleetnorm/internal/adapter"
	"github.com/ianfrushon/fleetnorm/internal/event"
)

// tag keys this adapter writes. they are part of the adapter's contract with
// its consumers, so they are named here once and documented in
// docs/adapters.md.
const (
	TagSourceID    = "geotab.source_id"
	TagVersion     = "geotab.version"
	TagUnresolved  = "geotab.unresolved"
	TagSeverityRaw = "geotab.severity_raw"
	TagVINFallback = "geotab.vin_fallback"

	TagAmberWarningLamp   = "geotab.amber_warning_lamp"
	TagRedStopLamp        = "geotab.red_stop_lamp"
	TagMalfunctionLamp    = "geotab.malfunction_lamp"
	TagProtectWarningLamp = "geotab.protect_warning_lamp"
	TagFaultLampState     = "geotab.fault_lamp_state"

	TagController      = "geotab.controller"
	TagClassCode       = "geotab.class_code"
	TagFaultState      = "geotab.fault_state"
	TagSourceAddress   = "geotab.source_address"
	TagDismissDateTime = "geotab.dismiss_date_time"
	TagDismissUser     = "geotab.dismiss_user"

	TagDiagnosticCode  = "geotab.diagnostic_code"
	TagDiagnosticType  = "geotab.diagnostic_type"
	TagFailureModeCode = "geotab.failure_mode_code"

	TagEffectOnComponent = "geotab.effect_on_component"
	TagRecommendation    = "geotab.recommendation"
	TagRiskOfBreakdown   = "geotab.risk_of_breakdown"
)

// entity type names as MyGeotab spells them
const (
	typeDiagnostic  = "Diagnostic"
	typeFailureMode = "FailureMode"
	typeController  = "Controller"
	typeDevice      = "Device"
)

// diagnosticSPN is the one Diagnostic type whose code is a J1939 SPN. any other
// type carries a code from a different numbering scheme, and putting it in spn
// would be a wrong number rather than a missing one.
const diagnosticSPN = "SuspectParameterNumber"

// Geotab coalesces five properties into one severity in a documented precedence
// order, so this maps that one field rather than inventing a second ranking.
// the table also lives in docs/adapters.md.
var severityMap = map[string]event.Severity{
	"Critical": event.SeverityCritical,
	"Warning":  event.SeverityMedium,
	"None":     event.SeverityInfo,
	"Unknown":  event.SeverityMedium,
}

// an unrecognized severity is not evidence the fault is minor, so it maps up
// rather than down, and the original is kept in TagSeverityRaw.
const defaultSeverity = event.SeverityMedium

const (
	DefaultServer       = "my.geotab.com"
	DefaultResultsLimit = 10000
	DefaultTimeout      = 30 * time.Second

	//GetFeed is 60/min. one poll a second is the floor the documented limit
	//allows, and the limiter enforces it regardless of what is configured.
	MinPollInterval = time.Second
)

type Options struct {
	Name     string
	Server   string
	Database string
	Username string
	Password string

	//SeedFrom returns the date a cold start reads from, given the time of the
	//poll. required: a feed with no cursor and no seed reads nothing, forever.
	SeedFrom func(now time.Time) time.Time

	ResultsLimit int
	CacheRefresh time.Duration
	Timeout      time.Duration

	OnSkip adapter.SkipFunc

	//swapped in tests; nil means one built from Timeout
	HTTPClient *http.Client
}

type Adapter struct {
	name     string
	client   *client
	resolver Resolver
	seedFrom func(now time.Time) time.Time
	limit    int
	onSkip   adapter.SkipFunc
	now      func() time.Time //swapped in tests
}

func New(o Options) (*Adapter, error) {
	switch {
	case o.Name == "":
		return nil, fmt.Errorf("geotab adapter needs a name")
	case o.Database == "":
		return nil, fmt.Errorf("geotab adapter %q: database is required", o.Name)
	case o.Username == "" || o.Password == "":
		return nil, fmt.Errorf("geotab adapter %q: credentials are required", o.Name)
	case o.SeedFrom == nil:
		return nil, fmt.Errorf("geotab adapter %q: seed_from is required, see docs/adapters.md", o.Name)
	}
	if o.Server == "" {
		o.Server = DefaultServer
	}
	if o.ResultsLimit <= 0 {
		o.ResultsLimit = DefaultResultsLimit
	}
	if o.ResultsLimit > MaxResultsLimit {
		return nil, fmt.Errorf("geotab adapter %q: results_limit must be at most %d", o.Name, MaxResultsLimit)
	}
	if o.Timeout <= 0 {
		o.Timeout = DefaultTimeout
	}
	c := newClient(o.Server, o.Database, o.Username, o.Password, o.Timeout, o.HTTPClient)
	return &Adapter{
		name:     o.Name,
		client:   c,
		resolver: newCache(c.resolve, o.CacheRefresh, DefaultCacheSize),
		seedFrom: o.SeedFrom,
		limit:    o.ResultsLimit,
		onSkip:   o.OnSkip,
		now:      func() time.Time { return time.Now().UTC() },
	}, nil
}

func (a *Adapter) Name() string { return a.name }

// String redacts the credentials, since an adapter lands in startup errors and
// in %v of anything holding it.
func (a *Adapter) String() string {
	return fmt.Sprintf("geotab.Adapter{Name:%s Server:%s Database:%s ResultsLimit:%d Credentials:%s}",
		a.name, a.client.url, a.client.creds.Database, a.limit, a.client.creds)
}

// GoString redacts too: %#v bypasses String, and that output goes into tickets.
func (a *Adapter) GoString() string { return a.String() }

// Poll reads one GetFeed page. the cursor is the feed's toVersion, stored
// verbatim: it is Geotab's bookmark, not ours to interpret.
func (a *Adapter) Poll(ctx context.Context, since adapter.Cursor) ([]event.Event, adapter.Cursor, error) {
	if err := ctx.Err(); err != nil {
		return nil, since, err
	}
	received := a.now().UTC()

	//fromDate seeds a cold start and is used only there. once a cursor exists,
	//seed_from is ignored entirely.
	var seed time.Time
	if since == "" {
		seed = a.seedFrom(received).UTC()
	}
	res, err := a.client.feed(ctx, string(since), seed, a.limit)
	if err != nil {
		return nil, since, err
	}

	records := make([]faultData, 0, len(res.Data))
	var skips []adapter.Skipped
	for i, raw := range res.Data {
		var fd faultData
		if err := json.Unmarshal(raw, &fd); err != nil {
			skips = append(skips, a.skip(i, "", fmt.Sprintf("record is not a FaultData object: %v", err)))
			continue
		}
		fd.raw = raw
		fd.index = i
		records = append(records, fd)
	}

	resolved := a.enrich(ctx, records)

	events := make([]event.Event, 0, len(records))
	for _, fd := range records {
		e, err := a.normalize(fd, resolved, received)
		if err != nil {
			skips = append(skips, a.skip(fd.index, fd.ID, err.Error()))
			continue
		}
		events = append(events, e)
	}

	//report skips only once the poll has succeeded: tripping the ceiling makes
	//no progress and these records are read again on the next tick
	if adapter.SkipCeilingExceeded(len(res.Data), len(skips)) {
		return nil, since, fmt.Errorf("geotab feed: skipped %d of %d records, which is more than this adapter will call noise: %s",
			len(skips), len(res.Data), skips[0].Reason)
	}
	for _, s := range skips {
		slog.Warn("skipping unusable record", "adapter", a.name, "id", s.ID, "reason", s.Reason)
		if a.onSkip != nil {
			a.onSkip(ctx, s)
		}
	}

	//a seeded first poll is the one whose result nobody can infer later, so it
	//says what date it used and what that produced
	if since == "" {
		slog.Info("geotab feed seeded", "adapter", a.name, "from_date", seed.Format(time.RFC3339),
			"events", len(events), "skipped", len(skips))
	}
	return events, adapter.Cursor(res.ToVersion), nil
}

// skip names a record for the audit log, by its Geotab id when it parsed and by
// its position in the page when it did not.
func (a *Adapter) skip(index int, id, reason string) adapter.Skipped {
	locator := id
	if locator == "" {
		locator = "index:" + strconv.Itoa(index)
	}
	return adapter.Skipped{Adapter: a.name, ID: adapter.SyntheticID(a.name, locator), Reason: reason}
}

// enrich resolves every referenced id in the page, one batched lookup per type.
// a lookup that fails yields an empty map for that type, which reads the same as
// an unknown id: the field goes missing and the event says so.
func (a *Adapter) enrich(ctx context.Context, records []faultData) map[string]map[string]Entity {
	ids := map[string][]string{}
	for _, fd := range records {
		for typeName, r := range map[string]ref{
			typeDiagnostic:  fd.Diagnostic,
			typeFailureMode: fd.FailureMode,
			typeController:  fd.Controller,
			typeDevice:      fd.Device,
		} {
			if r.ID != "" {
				ids[typeName] = append(ids[typeName], r.ID)
			}
		}
	}

	resolved := map[string]map[string]Entity{}
	for typeName, list := range ids {
		got, err := a.resolver.Resolve(ctx, typeName, list)
		if err != nil {
			//a lookup failure costs a field, never an event
			slog.Warn("geotab enrichment failed", "adapter", a.name, "entity", typeName, "error", err)
		}
		resolved[typeName] = got
	}
	return resolved
}

// ref is a MyGeotab id reference, which is all FaultData carries for its
// related objects.
type ref struct {
	ID string `json:"id"`
}

// faultData is one record off the feed. every field the mapping reads is here;
// everything else survives in raw.
type faultData struct {
	ID          string    `json:"id"`
	Version     string    `json:"version"`
	DateTime    time.Time `json:"dateTime"`
	Device      ref       `json:"device"`
	Diagnostic  ref       `json:"diagnostic"`
	FailureMode ref       `json:"failureMode"`
	Controller  ref       `json:"controller"`
	Count       *int      `json:"count"`
	Severity    string    `json:"severity"`

	AmberWarningLamp   *bool  `json:"amberWarningLamp"`
	RedStopLamp        *bool  `json:"redStopLamp"`
	MalfunctionLamp    *bool  `json:"malfunctionLamp"`
	ProtectWarningLamp *bool  `json:"protectWarningLamp"`
	FaultLampState     string `json:"faultLampState"`

	ClassCode     string `json:"classCode"`
	FaultState    string `json:"faultState"`
	SourceAddress *int   `json:"sourceAddress"`

	DismissDateTime *time.Time `json:"dismissDateTime"`
	DismissUser     ref        `json:"dismissUser"`

	//present only on enriched faults; absence is normal and never an error
	FaultDescription  string   `json:"faultDescription"`
	EffectOnComponent string   `json:"effectOnComponent"`
	Recommendation    string   `json:"recommendation"`
	RiskOfBreakdown   *float64 `json:"riskOfBreakdown"`

	raw   json.RawMessage //verbatim, before enrichment
	index int             //position in the page, for a synthetic id
}

func (a *Adapter) normalize(fd faultData, resolved map[string]map[string]Entity, received time.Time) (event.Event, error) {
	if fd.ID == "" {
		return event.Event{}, fmt.Errorf("record has no id")
	}
	if fd.Version == "" {
		//without a version the event_id is not unique per revision, which is the
		//whole point of it
		return event.Event{}, fmt.Errorf("record %s has no version", fd.ID)
	}

	e := event.Event{
		SchemaVersion: event.SchemaVersion,
		EventID:       fd.ID + ":" + fd.Version,
		OccurredAt:    fd.DateTime.UTC(),
		ReceivedAt:    received,
		Source:        a.name,
		SourceType:    event.SourceTSP,
		Severity:      defaultSeverity,
		Description:   fd.FaultDescription,
		Tags: map[string]string{
			TagSourceID: fd.ID,
			TagVersion:  fd.Version,
		},
		Raw: fd.raw,
	}

	if sev, ok := severityMap[fd.Severity]; ok {
		e.Severity = sev
	} else {
		e.Tags[TagSeverityRaw] = fd.Severity
	}

	var unresolved []string
	lookup := func(typeName string, r ref) (Entity, bool) {
		if r.ID == "" {
			return Entity{}, false
		}
		ent, ok := resolved[typeName][r.ID]
		if !ok {
			//named as FaultData spells the property, not as the entity type
			unresolved = append(unresolved, strings.ToLower(typeName[:1])+typeName[1:])
		}
		return ent, ok
	}

	//SPN comes from the diagnostic, and only when the diagnostic is one. a code
	//from another numbering scheme in spn would be a wrong answer, which is
	//worse than none.
	if d, ok := lookup(typeDiagnostic, fd.Diagnostic); ok && d.Code != nil {
		if d.Kind == diagnosticSPN {
			spn := *d.Code
			e.SPN = &spn
		} else {
			e.Tags[TagDiagnosticCode] = strconv.Itoa(*d.Code)
			e.Tags[TagDiagnosticType] = d.Kind
		}
	}
	//FMI comes from the failure mode. a code outside 0-31 is not an FMI, so it
	//is tagged rather than allowed to fail the whole record.
	if f, ok := lookup(typeFailureMode, fd.FailureMode); ok && f.Code != nil {
		if *f.Code >= 0 && *f.Code <= 31 {
			fmi := *f.Code
			e.FMI = &fmi
		} else {
			e.Tags[TagFailureModeCode] = strconv.Itoa(*f.Code)
		}
	}
	if c, ok := lookup(typeController, fd.Controller); ok && c.Name != "" {
		e.Tags[TagController] = c.Name
	}

	dev, ok := lookup(typeDevice, fd.Device)
	switch {
	case ok && dev.VIN != "":
		e.VIN = dev.VIN
	case fd.Device.ID != "":
		//a device with no VIN is still a truck. the id identifies it, and the
		//tag says the vin field is not really a VIN.
		e.VIN = fd.Device.ID
		e.Tags[TagVINFallback] = "device_id"
	}
	if dev.Name != "" {
		e.UnitID = dev.Name
	}

	if len(unresolved) > 0 {
		sort.Strings(unresolved)
		e.Tags[TagUnresolved] = strings.Join(unresolved, ",")
	}

	if fd.Count != nil {
		count := *fd.Count
		e.OccurrenceCount = &count
	}

	//the five lamps a technician actually reads. no schema field models them,
	//so all five are tagged under stable key names.
	setBool(e.Tags, TagAmberWarningLamp, fd.AmberWarningLamp)
	setBool(e.Tags, TagRedStopLamp, fd.RedStopLamp)
	setBool(e.Tags, TagMalfunctionLamp, fd.MalfunctionLamp)
	setBool(e.Tags, TagProtectWarningLamp, fd.ProtectWarningLamp)
	set(e.Tags, TagFaultLampState, fd.FaultLampState)

	set(e.Tags, TagClassCode, fd.ClassCode)
	set(e.Tags, TagFaultState, fd.FaultState)
	if fd.SourceAddress != nil {
		e.Tags[TagSourceAddress] = strconv.Itoa(*fd.SourceAddress)
	}
	if fd.DismissDateTime != nil && !fd.DismissDateTime.IsZero() {
		e.Tags[TagDismissDateTime] = fd.DismissDateTime.UTC().Format(time.RFC3339)
	}
	set(e.Tags, TagDismissUser, fd.DismissUser.ID)

	//enriched-fault extras: present or absent, never an empty tag
	set(e.Tags, TagEffectOnComponent, fd.EffectOnComponent)
	set(e.Tags, TagRecommendation, fd.Recommendation)
	if fd.RiskOfBreakdown != nil {
		e.Tags[TagRiskOfBreakdown] = strconv.FormatFloat(*fd.RiskOfBreakdown, 'g', -1, 64)
	}

	e.Annotate()
	if err := e.Validate(); err != nil {
		return e, fmt.Errorf("record %s: %w", fd.ID, err)
	}
	return e, nil
}

// set writes a tag only when there is something to write: an empty string tag
// says "the source reported nothing", which is a different claim from silence.
func set(tags map[string]string, key, value string) {
	if value != "" {
		tags[key] = value
	}
}

func setBool(tags map[string]string, key string, value *bool) {
	if value != nil {
		tags[key] = strconv.FormatBool(*value)
	}
}
