// package fixtures generates the synthetic MyGeotab FaultData the geotab
// adapter tests read.
//
// it is a package rather than only a command so the drift test can call it
// directly: TestFixturesAreGenerated compares Generate's output against the
// committed files, with no subprocess and no dependency on the working
// directory. cmd/genfixtures is the thin wrapper that writes them to disk.
//
// the output is deterministic. the same seed produces byte-identical files, so
// a generator change that is not regenerated fails CI instead of going
// unnoticed. adding a mapping case means adding it here and regenerating, never
// editing the JSON by hand.
//
// nothing here comes from a fleet. VINs are impossible rather than merely
// unassigned and device names are obviously synthetic. the one real thing is
// the reference data under "captured" below: four Diagnostic records and one
// FaultData record taken verbatim from a MyGeotab demo database, because the
// shapes a live server sends turned out not to be the documented ones.
package fixtures

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"time"
)

// CommittedSeed and CommittedCount are what the fixtures in testdata were
// generated with. the drift test regenerates at exactly these.
const (
	CommittedSeed  = 1
	CommittedCount = 12
)

// File is one generated fixture: the name it is written under and its bytes.
type File struct {
	Name string
	Data []byte
}

// Generate builds the whole fixture set. count is the size of the ordinary
// scene; the other scenes are fixed, since each exists to cover a specific path
// and a variable number of them would cover it no better.
func Generate(seed uint64, count int) ([]File, error) {
	if count < 1 {
		return nil, fmt.Errorf("count must be at least 1, got %d", count)
	}
	g := newGen(seed)
	scenes := []struct {
		name string
		body any
	}{
		{"entities.json", g.entities()},
		{"feed_ordinary.json", g.ordinary(count)},
		{"feed_revisions.json", g.revisions()},
		{"feed_edge.json", g.edge()},
		{"feed_skip.json", g.skip()},
		{"feed_ceiling.json", g.ceiling()},
	}

	out := make([]File, 0, len(scenes))
	for _, s := range scenes {
		b, err := json.MarshalIndent(s.body, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("%s: %w", s.name, err)
		}
		out = append(out, File{Name: s.name, Data: append(b, '\n')})
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// the reference data. small and hand written on purpose.
// ---------------------------------------------------------------------------

// a deliberately short table of well known J1939 SPNs. SAE sells the digital
// annex and this is not an attempt to reproduce it: it is enough distinct
// values to make fixtures that look like a fault feed.
var spns = []struct {
	code int
	name string
}{
	{84, "Wheel-Based Vehicle Speed"},
	{100, "Engine Oil Pressure"},
	{102, "Engine Intake Manifold #1 Pressure"},
	{110, "Engine Coolant Temperature"},
	{157, "Engine Injector Metering Rail #1 Pressure"},
	{190, "Engine Speed"},
	{639, "J1939 Network #1"},
	{1761, "Aftertreatment 1 Diesel Exhaust Fluid Tank Volume"},
	{3216, "Aftertreatment 1 Intake NOx"},
	{3226, "Aftertreatment 1 Outlet NOx"},
	{3251, "Aftertreatment 1 DPF Differential Pressure"},
	{5246, "Aftertreatment SCR Operator Inducement Severity"},
}

// J1939 failure mode identifiers. the adapter only accepts 0 to 31 into fmi, so
// outOfRangeFMI below is the value that must not get through.
var fmis = []struct {
	code int
	name string
}{
	{0, "Data valid but above normal operating range, most severe"},
	{1, "Data valid but below normal operating range, most severe"},
	{2, "Data erratic, intermittent or incorrect"},
	{3, "Voltage above normal or shorted high"},
	{4, "Voltage below normal or shorted low"},
	{5, "Current below normal or open circuit"},
	{16, "Data valid but above normal operating range, moderately severe"},
	{18, "Data valid but below normal operating range, moderately severe"},
	{20, "Data drifted high"},
	{31, "Condition exists"},
}

// a FailureMode code that is not an FMI. the adapter must tag it rather than
// put it in the fmi field, where 0 to 31 is the whole domain.
const outOfRangeFMI = 254

var controllers = []string{"Engine #1", "Transmission #1", "Brakes - System Controller", "Aftertreatment #1"}

var severities = []string{"Critical", "Warning", "None", "Unknown"}

var lampStates = []string{"Red", "Amber", "Protect", "None"}

var classCodes = []string{"Ecm", "Obd", "Proprietary"}

var faultStates = []string{"Active", "Inactive", "Pending"}

// ---------------------------------------------------------------------------
// captured. verbatim from a live MyGeotab demo database, never edited: the
// point of them is that nobody here decided what they look like. things they
// show that the entity reference does not: diagnosticType is SuspectParameter,
// controller is a bare string on one Diagnostic and an object on the next, and
// "none" is a string constant rather than null.
// ---------------------------------------------------------------------------

// the one Diagnostic whose code is an SPN
const CapturedSuspectParameter = `{"parameterGroup":"ParameterGroupNoneId","conversion":2000,"dataLength":1,"offset":0,"code":16,"controller":"ControllerNoneId","diagnosticType":"SuspectParameter","engineType":"EngineTypeGenericId","faultResetMode":"None","id":"aiDN8IHDf7EGdVvd5LDg3gg","name":"Engine fuel filter differential pressure (see also SPN 1382)","source":"SourceJ1939Id","unitOfMeasure":"UnitOfMeasurePascalsId","validLoggingPeriod":"None","isLogGuaranteedOnEstimateError":false,"version":"0000000000000eb9"}`

// diagnostics that are not SPNs. their code belongs to another numbering
// scheme, so putting it in spn would be a wrong answer rather than a missing
// one. Sid is a real vehicle fault on J1708, GoFault is the device talking
// about itself.
const (
	CapturedObdFault = `{"code":36,"controller":{"id":"ControllerObdBodyId"},"diagnosticType":"ObdFault","engineType":"EngineTypeGenericId","faultResetMode":"None","id":"aEnI-29ZA-E2gPcMfvf0XYQ","name":"ISO/SAE reserved","source":"SourceObdId","unitOfMeasure":"UnitOfMeasureNoneId","validLoggingPeriod":"None","isLogGuaranteedOnEstimateError":false,"version":"00000000000060ce"}`
	CapturedSid      = `{"code":151,"controller":{"id":"ControllerAnyId"},"diagnosticType":"Sid","engineType":"EngineTypeGenericId","faultResetMode":"None","id":"aAN6X-WwiwkGcywinKbpt3w","name":"System diagnostic code #1","source":"SourceJ1708Id","unitOfMeasure":"UnitOfMeasureNoneId","validLoggingPeriod":"None","isLogGuaranteedOnEstimateError":false,"version":"00000000000001a1"}`
	CapturedGoFault  = `{"engineType":"EngineTypeNoneId","code":466,"controller":{"id":"ControllerGoDeviceId"},"diagnosticType":"GoFault","faultResetMode":"AutoReset","id":"aysJxXoc3v0-Y6PGVSjoOxA","name":"Fault - engine hours stale","source":"SourceGeotabGoId","unitOfMeasure":"UnitOfMeasureNoneId","validLoggingPeriod":"None","isLogGuaranteedOnEstimateError":false,"version":"0000000000009938"}`
)

// the FaultData record that referenced CapturedGoFault. no severity, no
// faultLampState, no version, failureMode as a bare sentinel string, controller
// as an object holding a sentinel, and ids three characters long next to opaque
// ones: nothing about an id's length or format can be assumed.
const CapturedGoFaultData = `{"amberWarningLamp":false,"controller":{"id":"ControllerGoDeviceId"},"count":1,"dateTime":"2026-08-31T09:31:22.054Z","device":{"id":"b1C"},"diagnostic":{"id":"aysJxXoc3v0-Y6PGVSjoOxA"},"failureMode":"NoFailureModeId","faultState":"Active","faultStates":{"effectiveStatus":"FaultStatusActiveId"},"id":"b1","malfunctionLamp":false,"protectWarningLamp":false,"redStopLamp":false}`

// the device CapturedGoFaultData names. the id is the captured one; the device
// behind it is synthetic like every other.
const CapturedDeviceID = "b1C"

var capturedNonSPN = []string{CapturedObdFault, CapturedSid, CapturedGoFault}

// idOf reads the id back out of a captured record, so it is written down once.
func idOf(captured string) string {
	var e struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(captured), &e); err != nil || e.ID == "" {
		panic(fmt.Sprintf("captured record has no id: %v", err))
	}
	return e.ID
}

// ---------------------------------------------------------------------------

type gen struct {
	r       *rand.Rand
	devices []device
	version int64
	clock   time.Time
}

type device struct {
	ID   string
	Name string
	VIN  string //empty for the device that has none
}

func newGen(seed uint64) *gen {
	g := &gen{
		r:       rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		version: 0x100,
		//a fixed epoch: a fixture that moves with the wall clock is not a fixture
		clock: time.Date(2026, 9, 14, 8, 0, 0, 0, time.UTC),
	}
	for i := 1; i <= 8; i++ {
		d := device{
			ID:   fmt.Sprintf("b%04d", i),
			Name: fmt.Sprintf("unit-%04d", i),
			VIN:  fakeVIN(i),
		}
		//one device with no VIN, for the fall-back-to-device-id path
		if i == 8 {
			d.VIN = ""
		}
		g.devices = append(g.devices, d)
	}
	return g
}

// fakeVIN returns something VIN shaped that cannot be a VIN. real VINs never
// contain I, O or Q, so the O here makes it impossible rather than merely
// unassigned, and no check digit is computed.
func fakeVIN(n int) string {
	return fmt.Sprintf("FLEETNORMFAKE%04d", n)
}

// nextVersion returns the next feed version. MyGeotab versions are opaque
// strings that increase; these are hex counters, which is enough.
func (g *gen) nextVersion() string {
	g.version++
	return fmt.Sprintf("%016X", g.version)
}

// nextTime advances the clock by a seeded but bounded step, so timestamps
// increase without ever depending on when the generator ran.
func (g *gen) nextTime() string {
	g.clock = g.clock.Add(time.Duration(1+g.r.IntN(45)) * time.Minute)
	return g.clock.UTC().Format("2006-01-02T15:04:05.000Z")
}

func (g *gen) device() device { return g.devices[g.r.IntN(len(g.devices))] }

func pick[T any](r *rand.Rand, in []T) T { return in[r.IntN(len(in))] }

// ---------------------------------------------------------------------------
// output shapes
// ---------------------------------------------------------------------------

// ref is an id reference. a live server sends one as an object or as a bare
// string, so a scene can ask for either.
type ref struct {
	ID   string
	Bare bool
}

func (r ref) MarshalJSON() ([]byte, error) {
	if r.Bare {
		return json.Marshal(r.ID)
	}
	return json.Marshal(map[string]string{"id": r.ID})
}

// feed is a GetFeed response, the shape the adapter's client decodes, so a
// fixture can be served by the test double without a wrapper. a record is a
// *fault, or a json.RawMessage for a captured one that must stay verbatim.
type feed struct {
	Data      []any  `json:"data"`
	ToVersion string `json:"toVersion"`
}

// fault is one FaultData record. every field the adapter reads is here, and
// omitempty is what lets a scene leave one out on purpose.
type fault struct {
	ID          string `json:"id,omitempty"`
	Version     string `json:"version,omitempty"`
	DateTime    string `json:"dateTime,omitempty"`
	Device      *ref   `json:"device,omitempty"`
	Diagnostic  *ref   `json:"diagnostic,omitempty"`
	FailureMode *ref   `json:"failureMode,omitempty"`
	Controller  *ref   `json:"controller,omitempty"`
	Count       *int   `json:"count,omitempty"`
	Severity    string `json:"severity,omitempty"`

	AmberWarningLamp   *bool  `json:"amberWarningLamp,omitempty"`
	RedStopLamp        *bool  `json:"redStopLamp,omitempty"`
	MalfunctionLamp    *bool  `json:"malfunctionLamp,omitempty"`
	ProtectWarningLamp *bool  `json:"protectWarningLamp,omitempty"`
	FaultLampState     string `json:"faultLampState,omitempty"`

	ClassCode     string `json:"classCode,omitempty"`
	FaultState    string `json:"faultState,omitempty"`
	SourceAddress *int   `json:"sourceAddress,omitempty"`

	DismissDateTime string `json:"dismissDateTime,omitempty"`
	DismissUser     *ref   `json:"dismissUser,omitempty"`

	//enriched faults only
	FaultDescription  string   `json:"faultDescription,omitempty"`
	EffectOnComponent string   `json:"effectOnComponent,omitempty"`
	Recommendation    string   `json:"recommendation,omitempty"`
	RiskOfBreakdown   *float64 `json:"riskOfBreakdown,omitempty"`
}

// entity is a resolved FailureMode, Controller or Device. nothing was captured
// for these, so they stay as small as what the adapter reads.
type entity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Code *int   `json:"code,omitempty"`
	VIN  string `json:"vehicleIdentificationNumber,omitempty"`
}

// spnDiagnostic is a synthetic J1939 Diagnostic in the shape of
// CapturedSuspectParameter, sentinels and bare string controller included. the
// demo database only emits GO device faults, so the SPN path still needs
// invented diagnostics; what is not invented any more is what one looks like.
type spnDiagnostic struct {
	ParameterGroup string `json:"parameterGroup"`
	Code           int    `json:"code"`
	Controller     string `json:"controller"`
	DiagnosticType string `json:"diagnosticType"`
	EngineType     string `json:"engineType"`
	FaultResetMode string `json:"faultResetMode"`
	ID             string `json:"id"`
	Name           string `json:"name"`
	Source         string `json:"source"`
	UnitOfMeasure  string `json:"unitOfMeasure"`
}

// entities is the reference set the enrichment lookups resolve against, keyed
// by MyGeotab type name and then by id. encoding/json sorts map keys, so this
// serializes deterministically.
func (g *gen) entities() map[string]map[string]any {
	out := map[string]map[string]any{
		"Diagnostic":  {},
		"FailureMode": {},
		"Controller":  {},
		"Device":      {},
	}
	for _, s := range spns {
		id := spnID(s.code)
		out["Diagnostic"][id] = spnDiagnostic{
			ParameterGroup: "ParameterGroupNoneId", Code: s.code, Controller: "ControllerNoneId",
			DiagnosticType: "SuspectParameter", EngineType: "EngineTypeGenericId", FaultResetMode: "None",
			ID: id, Name: s.name, Source: "SourceJ1939Id", UnitOfMeasure: "UnitOfMeasureNoneId",
		}
	}
	for _, c := range append([]string{CapturedSuspectParameter}, capturedNonSPN...) {
		out["Diagnostic"][idOf(c)] = json.RawMessage(c)
	}
	for _, f := range fmis {
		id := fmiID(f.code)
		out["FailureMode"][id] = entity{ID: id, Name: f.name, Code: ptr(f.code)}
	}
	id := fmiID(outOfRangeFMI)
	out["FailureMode"][id] = entity{ID: id, Name: "Not an FMI", Code: ptr(outOfRangeFMI)}

	for _, name := range controllers {
		id := controllerID(name)
		out["Controller"][id] = entity{ID: id, Name: name}
	}
	for _, d := range g.devices {
		out["Device"][d.ID] = entity{ID: d.ID, Name: d.Name, VIN: d.VIN}
	}
	out["Device"][CapturedDeviceID] = entity{ID: CapturedDeviceID, Name: "unit-go-0001", VIN: fakeVIN(9001)}

	//ids must be unique across types: the test double resolves by id alone
	seen := map[string]string{}
	types := make([]string, 0, len(out))
	for t := range out {
		types = append(types, t)
	}
	sort.Strings(types)
	for _, t := range types {
		ids := make([]string, 0, len(out[t]))
		for id := range out[t] {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			if prev, dup := seen[id]; dup {
				panic(fmt.Sprintf("entity id %q used by both %s and %s", id, prev, t))
			}
			seen[id] = t
		}
	}
	return out
}

func spnID(code int) string        { return fmt.Sprintf("DiagnosticSpn%d", code) }
func fmiID(code int) string        { return fmt.Sprintf("FailureModeFmi%d", code) }
func controllerID(n string) string { return "Controller" + nonAlnum.Replace(n) }

// ---------------------------------------------------------------------------
// scenes
// ---------------------------------------------------------------------------

// ordinary is the boring path: everything resolves, nothing is unusual.
func (g *gen) ordinary(n int) feed {
	f := feed{}
	for range n {
		f.Data = append(f.Data, g.fault())
	}
	f.ToVersion = g.nextVersion()
	return f
}

// revisions is the same record resent as it changes, which is what GetFeed does
// and why the version is part of event_id. the count climbs, then the fault is
// dismissed.
func (g *gen) revisions() feed {
	f := feed{}
	base := g.fault()
	base.Count = ptr(1)

	for _, count := range []int{1, 2, 5} {
		rev := *base
		rev.Version = g.nextVersion()
		rev.DateTime = g.nextTime()
		rev.Count = ptr(count)
		f.Data = append(f.Data, &rev)
	}
	//the dismissal: a later version of a record already delivered
	dismissed := *base
	dismissed.Version = g.nextVersion()
	dismissed.DateTime = g.nextTime()
	dismissed.Count = ptr(5)
	dismissed.FaultState = "Inactive"
	dismissed.DismissDateTime = g.nextTime()
	dismissed.DismissUser = &ref{ID: "u0001"}
	f.Data = append(f.Data, &dismissed)

	//a second id at two versions, so grouping by source id has something to group
	other := g.fault()
	other.Count = ptr(1)
	second := *other
	second.Version = g.nextVersion()
	second.DateTime = g.nextTime()
	second.Count = ptr(2)
	f.Data = append(f.Data, other, &second)

	f.ToVersion = g.nextVersion()
	return f
}

// edge is every mapping path that is not the ordinary one.
func (g *gen) edge() feed {
	f := feed{}

	//diagnostics that are not SPNs: the code must not reach the spn field
	for _, c := range capturedNonSPN {
		x := g.fault()
		x.Diagnostic = &ref{ID: idOf(c)}
		f.Data = append(f.Data, x)
	}

	//the captured record, exactly as the live feed sent it
	f.Data = append(f.Data, json.RawMessage(CapturedGoFaultData))

	//references as bare strings, and "none" said with a sentinel: no fmi, no
	//controller, and no lookup for either
	sentinels := g.fault()
	sentinels.Diagnostic = &ref{ID: idOf(CapturedSuspectParameter), Bare: true}
	sentinels.Device.Bare = true
	sentinels.FailureMode = &ref{ID: "NoFailureModeId", Bare: true}
	sentinels.Controller = &ref{ID: "ControllerNoneId", Bare: true}
	f.Data = append(f.Data, sentinels)

	//a sentinel nobody has seen before: not resolved, not fatal, tagged
	unknownSentinel := g.fault()
	unknownSentinel.Controller = &ref{ID: "ControllerNotSeenBeforeId"}
	f.Data = append(f.Data, unknownSentinel)

	//no severity at all, which is what live records look like, lamps all false
	//included: the documented default, and not a downgrade
	noSeverity := g.fault()
	noSeverity.Severity = ""
	noSeverity.AmberWarningLamp, noSeverity.RedStopLamp = ptr(false), ptr(false)
	noSeverity.MalfunctionLamp, noSeverity.ProtectWarningLamp = ptr(false), ptr(false)
	f.Data = append(f.Data, noSeverity)

	//the same with the red stop lamp on, the one thing allowed to raise it
	redStop := g.fault()
	redStop.Severity = ""
	redStop.RedStopLamp = ptr(true)
	f.Data = append(f.Data, redStop)

	//a failure mode code outside 0-31
	outOfRange := g.fault()
	outOfRange.FailureMode = &ref{ID: fmiID(outOfRangeFMI)}
	f.Data = append(f.Data, outOfRange)

	//a diagnostic id nothing resolves, for the unresolved-enrichment path
	unresolved := g.fault()
	unresolved.Diagnostic = &ref{ID: "DiagnosticSpnMissingFromReferenceSet"}
	f.Data = append(f.Data, unresolved)

	//a device with no VIN: vin falls back to the device id
	noVIN := g.fault()
	noVIN.Device = &ref{ID: g.devices[len(g.devices)-1].ID}
	f.Data = append(f.Data, noVIN)

	//an enriched fault and a bare one, the same record with and without the
	//fields that only enriched faults carry
	enriched := g.fault()
	enriched.FaultDescription = "Aftertreatment 1 SCR intake NOx sensor: data drifted high"
	enriched.EffectOnComponent = "Reduced engine performance and possible derate"
	enriched.Recommendation = "Schedule service within 50 hours"
	enriched.RiskOfBreakdown = ptr(0.42)
	f.Data = append(f.Data, enriched)

	bare := g.fault()
	bare.FaultDescription = ""
	f.Data = append(f.Data, bare)

	//an unrecognized severity, for the documented default and severity_raw
	odd := g.fault()
	odd.Severity = "CatastrophicallyBad"
	f.Data = append(f.Data, odd)

	//lamp combinations: all off, all on, and the mixed case a technician
	//actually sees
	for i, lamps := range [][4]bool{
		{false, false, false, false},
		{true, true, true, true},
		{true, false, false, true},
	} {
		x := g.fault()
		x.AmberWarningLamp, x.RedStopLamp = ptr(lamps[0]), ptr(lamps[1])
		x.MalfunctionLamp, x.ProtectWarningLamp = ptr(lamps[2]), ptr(lamps[3])
		x.FaultLampState = lampStates[i%len(lampStates)]
		f.Data = append(f.Data, x)
	}

	//no lamp fields at all: absent must stay absent, not become false
	noLamps := g.fault()
	noLamps.AmberWarningLamp, noLamps.RedStopLamp = nil, nil
	noLamps.MalfunctionLamp, noLamps.ProtectWarningLamp = nil, nil
	noLamps.FaultLampState = ""
	f.Data = append(f.Data, noLamps)

	f.ToVersion = g.nextVersion()
	return f
}

// skip is mostly good records with a few the adapter cannot use, staying under
// the ceiling so the poll still succeeds.
func (g *gen) skip() feed {
	f := feed{}
	for range 9 {
		f.Data = append(f.Data, g.fault())
	}
	for _, m := range g.malformed() {
		f.Data = append(f.Data, m)
	}
	f.ToVersion = g.nextVersion()
	return f
}

// ceiling skips more than half of at least ten records, which is the point
// where skipping stops looking like noise and becomes a changed source.
func (g *gen) ceiling() feed {
	f := feed{}
	for range 2 {
		f.Data = append(f.Data, g.fault())
	}
	for range 4 {
		for _, m := range g.malformed() {
			f.Data = append(f.Data, m)
		}
	}
	f.ToVersion = g.nextVersion()
	return f
}

// malformed returns one of each way a record fails validation after mapping.
func (g *gen) malformed() []*fault {
	noID := g.fault()
	noID.ID = ""

	//a version is not required, live records have none, but a time is
	noTime := g.fault()
	noTime.DateTime = ""

	//no device means no VIN, and vin is required
	noDevice := g.fault()
	noDevice.Device = nil

	return []*fault{noID, noTime, noDevice}
}

// fault builds one ordinary record with everything resolvable.
func (g *gen) fault() *fault {
	d := g.device()
	diagnostic := idOf(CapturedSuspectParameter)
	if n := g.r.IntN(len(spns) + 1); n < len(spns) {
		diagnostic = spnID(spns[n].code)
	}
	fmi := pick(g.r, fmis)
	controller := pick(g.r, controllers)

	return &fault{
		ID:          fmt.Sprintf("a%09d", g.r.IntN(1_000_000_000)),
		Version:     g.nextVersion(),
		DateTime:    g.nextTime(),
		Device:      &ref{ID: d.ID},
		Diagnostic:  &ref{ID: diagnostic},
		FailureMode: &ref{ID: fmiID(fmi.code)},
		Controller:  &ref{ID: controllerID(controller)},
		Count:       ptr(1 + g.r.IntN(20)),
		Severity:    pick(g.r, severities),

		AmberWarningLamp:   ptr(g.r.IntN(2) == 0),
		RedStopLamp:        ptr(g.r.IntN(2) == 0),
		MalfunctionLamp:    ptr(g.r.IntN(2) == 0),
		ProtectWarningLamp: ptr(g.r.IntN(2) == 0),
		FaultLampState:     pick(g.r, lampStates),

		ClassCode:     pick(g.r, classCodes),
		FaultState:    pick(g.r, faultStates),
		SourceAddress: ptr(g.r.IntN(256)),
	}
}

func ptr[T any](v T) *T { return &v }

// nonAlnum turns a controller name into an id the way a vendor id looks: no
// spaces or punctuation.
var nonAlnum = strings.NewReplacer(" ", "", "-", "", "#", "", ".", "")
