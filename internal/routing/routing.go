package routing

import (
	"fmt"
	"slices"

	"synch/internal/engine"
	"synch/internal/thread"
)

// Pair identifies a harness and one of its models.
type Pair struct {
	Harness string `json:"harness"`
	Model   string `json:"model"`
}

// Candidate binds a pair to the installed harness version and its declaration.
type Candidate struct {
	Pair           Pair
	HarnessVersion string
	Capabilities   engine.CapabilitySet
}

// Request contains the routing inputs for one assignment.
type Request struct {
	HostOS     string
	TaskKind   string
	Candidates []Candidate
	Default    Pair
	Pin        *Pair
	// SelectedHarness is the accepted replacement from thread.State.SelectedHarness.
	// Supply it when routing the assignment being restarted after harness failure.
	SelectedHarness *thread.HarnessSelection
	// Permitted checks the assignment's policy. A missing predicate permits no pair.
	Permitted func(Pair) bool
	Evidence  []Evidence
}

// Outcome distinguishes dispatchable selections from the T06 pause outcome.
type Outcome string

const (
	Selected       Outcome = "selected"
	NoEligiblePair Outcome = "no_eligible_pair"
)

// Result carries the pair and event fields for dispatch or the typed T06 outcome.
type Result struct {
	Outcome       Outcome
	Pair          Pair
	Pinned        bool
	RoutingReason string
}

// Eligible reports the value used by the thread machine's Pair hook for T05, T06, and T21.
func (r Result) Eligible() bool {
	return r.Outcome == Selected
}

// Select honors an accepted replacement or manual pin, then routes using the default and comparison evidence.
func Select(req Request) Result {
	if chosen := req.SelectedHarness; chosen != nil {
		return selectPin(req, Pair{Harness: chosen.Harness, Model: chosen.Model}, "operator-selected replacement")
	}
	if req.Pin != nil {
		return selectPin(req, *req.Pin, "operator pin")
	}

	best := bestAlternative(req)
	if best != nil {
		return Result{
			Outcome: Selected, Pair: best.Alternative,
			RoutingReason: fmt.Sprintf("better than the configured default on held-out task set %q (%s)",
				best.TaskSetID, best.TaskKind),
		}
	}
	if eligible(req, req.Default) {
		return Result{Outcome: Selected, Pair: req.Default, RoutingReason: "configured default"}
	}
	return Result{Outcome: NoEligiblePair, RoutingReason: "configured default unavailable and no eligible alternative with better evidence"}
}

// selectPin preserves an operator's choice even when dispatch must pause.
func selectPin(req Request, pair Pair, source string) Result {
	if eligible(req, pair) {
		return Result{Outcome: Selected, Pair: pair, Pinned: true, RoutingReason: source}
	}
	return Result{Outcome: NoEligiblePair, Pinned: true, RoutingReason: source + " is ineligible"}
}

// eligible checks every required qualification against a single matching declaration and applies policy.
func eligible(req Request, pair Pair) bool {
	if pair.Harness == "" || pair.Model == "" || req.HostOS == "" || req.Permitted == nil {
		return false
	}
	for _, candidate := range req.Candidates {
		caps := candidate.Capabilities
		if candidate.Pair == pair && caps.Adapter == pair.Harness && caps.OS == req.HostOS &&
			slices.Contains(caps.Models, pair.Model) && versionInRange(candidate.HarnessVersion, caps.HarnessVersions) &&
			requiredOpsQualified(caps) {
			return req.Permitted(pair)
		}
	}
	return false
}

// requiredOpsQualified checks all V1 required operations, including skill_delivery.
func requiredOpsQualified(caps engine.CapabilitySet) bool {
	for _, op := range [...]string{engine.StartOp, engine.SendOp, engine.StreamOp, engine.StopOp, engine.SkillDeliveryOp} {
		if caps.Ops[op].Status != engine.Qualified {
			return false
		}
	}
	return true
}
