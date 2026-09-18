// package router decides which outputs an event goes to. every matching rule
// fires, not only the first, and each destination is named once. the reference
// is docs/policy.md.
package router

import (
	"slices"

	"github.com/ianfrushon/fleetnorm/internal/config"
	"github.com/ianfrushon/fleetnorm/internal/event"
)

// Target is one output an event goes to, and the label of the rule that sent it
// there. the label is what the audit log records, so "why did this fault reach
// that party" is answerable without re-deriving the match.
type Target struct {
	Output string
	Rule   string
}

type Router struct {
	rules []config.Rule
}

// New takes rules config.Load has already validated. a rule with no label gets
// its positional one here too, so a Router built directly, as tests do, still
// names every rule.
func New(rules []config.Rule) *Router {
	cloned := slices.Clone(rules)
	for i := range cloned {
		if cloned[i].Label == "" {
			cloned[i].Label = config.RuleLabel(cloned[i].Name, i)
		}
	}
	return &Router{rules: cloned}
}

// Route returns the outputs for an event, deduplicated, in the order first
// chosen. nil means no rule matched, which the caller audits as a drop.
//
// where two rules route to the same output the event is still delivered once,
// and the target keeps the first matching rule's label: that is the rule that
// put the event in the queue.
func (r *Router) Route(e event.Event) []Target {
	var targets []Target
	for _, rule := range r.rules {
		if !matches(rule.Match, e) {
			continue
		}
		for _, name := range rule.Route {
			if !slices.ContainsFunc(targets, func(t Target) bool { return t.Output == name }) {
				targets = append(targets, Target{Output: name, Rule: rule.Label})
			}
		}
	}
	return targets
}

// conditions are ANDed, lists within one are ORed, comparisons are exact.
func matches(m config.Match, e event.Event) bool {
	switch {
	case m.VIN != "" && m.VIN != e.VIN:
		return false
	case m.UnitID != "" && m.UnitID != e.UnitID:
		return false
	case m.Source != "" && m.Source != e.Source:
		return false
	case m.SourceType != "" && m.SourceType != string(e.SourceType):
		return false
	case len(m.Severity) > 0 && !slices.Contains(m.Severity, string(e.Severity)):
		return false

	//absent is neither zero nor a wildcard: an event carrying no SPN must not
	//match a rule that asks about SPN, and the same holds for FMI
	case len(m.SPN) > 0 && (e.SPN == nil || !m.SPN.Contains(*e.SPN)):
		return false
	case m.FMI != nil && (e.FMI == nil || *m.FMI != *e.FMI):
		return false
	}

	for k, v := range m.Tags {
		//two value read on purpose: the one value form would let a missing key
		//satisfy a rule matching an empty value, since both read as ""
		if got, ok := e.Tags[k]; !ok || got != v {
			return false
		}
	}
	return true
}
