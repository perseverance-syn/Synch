package engine

import (
	"errors"
	"fmt"
	"maps"
)

// Status is the qualification state for an operation.
type Status string

const (
	Qualified   Status = "qualified"
	Unavailable Status = "unavailable"
	Untested    Status = "untested"
)

const (
	StartOp         = "start"
	SendOp          = "send"
	StreamOp        = "stream"
	InterruptOp     = "interrupt"
	StopOp          = "stop"
	ResumeOp        = "native_resume"
	UsageOp         = "usage"
	InterceptionOp  = "tool_interception"
	SkillDeliveryOp = "skill_delivery"
)

// Capability records an operation's qualification state and supporting evidence.
type Capability struct {
	Status   Status `json:"status"`
	Evidence string `json:"evidence,omitempty"`
	Reason   string `json:"reason,omitempty"`
}

// CapabilitySet declares the adapter's operations for one harness version interval and OS.
type CapabilitySet struct {
	Version         string                `json:"capabilities_version"`
	Adapter         string                `json:"adapter"`
	AdapterVersion  string                `json:"adapter_version"`
	HarnessVersions string                `json:"harness_versions"`
	OS              string                `json:"os"`
	Models          []string              `json:"models"`
	Ops             map[string]Capability `json:"ops"`
	SkillDelivery   map[string]Capability `json:"skill_delivery"`
}

// Copy isolates declaration data from mutations by its callers.
func (c CapabilitySet) Copy() CapabilitySet {
	c.Ops = maps.Clone(c.Ops)
	c.SkillDelivery = maps.Clone(c.SkillDelivery)
	c.Models = append([]string{}, c.Models...)
	return c
}

// Require checks qualification and synchronously records a denied operation.
// A request with an empty identifier returns a typed validation error and
// persists nothing, because a denial fact with empty IDs is corrupt history.
// (Nonempty but malformed IDs are out of this guard's scope; the adapter's
// command validation rejects those separately.)
func (c CapabilitySet) Require(sink Sink, assignmentID, threadID, operation string) error {
	if assignmentID == "" || threadID == "" {
		field := "thread_id"
		if assignmentID == "" {
			field = "assignment_id"
		}
		return &InvalidRequestError{Field: field, Reason: "must not be empty"}
	}
	if c.Ops[operation].Status == Qualified {
		return nil
	}
	denied := fmt.Errorf("%w: %s", ErrUnavailable, operation)
	if err := sink.Persist(Fact{Type: "engine.capability_unavailable", ThreadID: threadID,
		Payload: CapabilityUnavailable{AssignmentID: assignmentID, Harness: c.Adapter, Operation: operation}}); err != nil {
		return errors.Join(denied, fmt.Errorf("persist capability denial: %w", err))
	}
	return denied
}
