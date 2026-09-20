package geotab

import "regexp"

// Geotab does not send null or leave a property out to say "none", "any" or
// "not a real object". it sends a string constant shaped like an id:
// NoFailureModeId, ControllerAnyId, SourceJ1939Id. none of this is in the entity
// reference; it is what a live database sends.
//
// a sentinel has nothing behind it, so the one rule that holds at every site is
// that it never goes to an enrichment Get: that is an error or a meaningless
// lookup, and either way it spends the 500/min budget. what a sentinel means
// beyond that is the site's policy, which lives where the site is read:
//
//	failureMode   NoFailureModeId is no fmi. anything else is tagged unknown.
//	controller    ControllerNoneId is no controller. the other known ones are
//	              written to geotab.controller as themselves. unknown ones are
//	              too, and are tagged unknown.
//	source        read off the resolved diagnostic and mapped by
//	              diagnosticStandards, raw when unlisted. never a reference.
//	diagnostic,   not sentinel sites, always resolved, and not an inconsistency
//	device        to fix: a live Get on Diagnostic returned real records with ids
//	              of this same shape (DiagnosticIgnitionId, DiagnosticAux3Id,
//	              DiagnosticEngineHoursStaleId) next to opaque ones. applying the
//	              rule here would silently cost every such fault its spn.
//
// engineType, unitOfMeasure, parameterGroup and faultStates.effectiveStatus
// carry sentinels as well. the adapter does not read them, so they have no
// policy and reach consumers in raw.
type sentinelKind int

const (
	notSentinel     sentinelKind = iota //an ordinary id, resolve it
	sentinelNone                        //known, and means nothing is there
	sentinelValue                       //known, and means something: keep it, never resolve it
	sentinelUnknown                     //the shape, but not in the table: never resolve it, tag it
)

// what one live database showed. it is not the whole set, which is why the
// shape alone is enough to keep an id away from Get.
var knownSentinels = map[string]sentinelKind{
	"NoFailureModeId":      sentinelNone,
	"ControllerNoneId":     sentinelNone,
	"EngineTypeNoneId":     sentinelNone,
	"UnitOfMeasureNoneId":  sentinelNone,
	"ParameterGroupNoneId": sentinelNone,

	"ControllerGoDeviceId": sentinelValue,
	"ControllerAnyId":      sentinelValue,
	"ControllerObdBodyId":  sentinelValue,
	"EngineTypeGenericId":  sentinelValue,
	"FaultStatusActiveId":  sentinelValue,
	"SourceJ1939Id":        sentinelValue,
	"SourceJ1708Id":        sentinelValue,
	"SourceObdId":          sentinelValue,
	"SourceGeotabGoId":     sentinelValue,
	"SourceSystemId":       sentinelValue,
}

// a capitalized word ending in Id. generated MyGeotab ids start with a lower
// case letter (aysJxXoc3v0-Y6PGVSjoOxA, b1C), so they never match.
var sentinelShape = regexp.MustCompile(`^[A-Z][A-Za-z0-9]*Id$`)

func classify(id string) sentinelKind {
	if kind, known := knownSentinels[id]; known {
		return kind
	}
	if sentinelShape.MatchString(id) {
		return sentinelUnknown
	}
	return notSentinel
}

// the FaultData references where a sentinel can stand in for an object
var sentinelSites = map[string]bool{typeFailureMode: true, typeController: true}

// sentinelAt classifies an id at one reference site. enrich and normalize both
// go through it, so what is skipped at lookup and what is explained on the event
// cannot disagree.
func sentinelAt(typeName, id string) sentinelKind {
	if !sentinelSites[typeName] {
		return notSentinel
	}
	return classify(id)
}
