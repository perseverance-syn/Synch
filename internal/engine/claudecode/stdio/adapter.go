package stdio

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"unicode/utf8"

	"synch/internal/engine"
)

// Declaration lists only untested operations until independently qualified.
func Declaration(adapter, versions string) engine.CapabilitySet {
	ops := make(map[string]engine.Capability)
	for _, op := range []string{engine.StartOp, engine.SendOp, engine.StreamOp, engine.InterruptOp,
		engine.StopOp, engine.ResumeOp, engine.UsageOp, engine.InterceptionOp, engine.SkillDeliveryOp} {
		ops[op] = engine.Capability{Status: engine.Untested}
	}
	return engine.CapabilitySet{Version: "1.0.0", Adapter: adapter, AdapterVersion: "0.1.0",
		HarnessVersions: versions, OS: runtime.GOOS, Models: []string{}, Ops: ops,
		SkillDelivery: map[string]engine.Capability{
			"injection": {Status: engine.Qualified, Reason: "adapter inserts pinned text into the initial instructions"},
			"native":    {Status: engine.Unavailable, Reason: "adapter uses assignment-scoped instruction injection"},
		}}
}

// Qualify overlays an isolated set of operation declarations for the same adapter range and OS.
func Qualify(caps engine.CapabilitySet, q *engine.CapabilitySet) (engine.CapabilitySet, error) {
	if q == nil {
		return caps, nil
	}
	if q.Adapter != caps.Adapter || q.OS != caps.OS || q.HarnessVersions != caps.HarnessVersions || q.Version != caps.Version {
		return engine.CapabilitySet{}, errors.New("qualification does not match adapter declaration")
	}
	qualified := q.Copy()
	for op, capability := range qualified.Ops {
		if _, exists := caps.Ops[op]; exists {
			caps.Ops[op] = capability
		}
	}
	if injection, ok := qualified.SkillDelivery["injection"]; ok {
		caps.SkillDelivery["injection"] = injection
	}
	caps.Models = qualified.Models
	return caps, nil
}

// Instructions validates the assignment scope and injects stable instructions, pinned skills and changing instructions in order.
func Instructions(req engine.StartRequest) (string, error) {
	if req.AssignmentID == "" || req.ThreadID == "" {
		return "", errors.New("assignment and thread IDs are required")
	}
	if err := engine.ValidateEnv(req.Env); err != nil {
		return "", err
	}
	if err := validateEndpoint(req.MCPEndpoint); err != nil {
		return "", err
	}
	var blocks []string
	for _, instruction := range req.Instructions {
		if instruction.Stable {
			blocks = append(blocks, instruction.Text)
		}
	}
	for _, skill := range req.Skills {
		digest := fmt.Sprintf("%x", sha256.Sum256([]byte(skill.Text)))
		if skill.ID == "" || !utf8.ValidString(skill.Text) ||
			(!strings.EqualFold(skill.Digest, digest) && !strings.EqualFold(skill.Digest, "sha256:"+digest)) {
			return "", fmt.Errorf("invalid pinned skill %q", skill.ID)
		}
		blocks = append(blocks, fmt.Sprintf("Skill %q (sha256:%s)\n%s", skill.ID, digest, skill.Text))
	}
	for _, instruction := range req.Instructions {
		if !instruction.Stable {
			blocks = append(blocks, instruction.Text)
		}
	}
	return strings.Join(blocks, "\n\n"), nil
}
