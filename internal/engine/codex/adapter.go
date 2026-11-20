package codex

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"unicode/utf8"

	"synch/internal/engine"
)

const harnessVersion = "0.169.0"
const versionRange = ">=0.169.0 <0.169.1"

var _ engine.EngineAdapter = (*Adapter)(nil)
var _ engine.Resumer = (*Adapter)(nil)

// --- Capability declaration (engine-adapter section 2) ---

// Adapter opens and controls one App Server process per session through an injected launcher.
type Adapter struct {
	launcher Launcher
	sink     engine.Sink
	caps     engine.CapabilitySet
}

// Declaration returns the unqualified declaration for this OS and the matrix's observed release.
func Declaration() engine.CapabilitySet {
	ops := make(map[string]engine.Capability)
	for _, op := range []string{engine.StartOp, engine.SendOp, engine.StreamOp, engine.InterruptOp,
		engine.StopOp, engine.ResumeOp, engine.UsageOp, engine.InterceptionOp, engine.SkillDeliveryOp} {
		ops[op] = engine.Capability{Status: engine.Untested}
	}
	return engine.CapabilitySet{Version: "1.0.0", Adapter: "codex", AdapterVersion: "0.1.0",
		HarnessVersions: versionRange, OS: runtime.GOOS, Models: []string{}, Ops: ops,
		SkillDelivery: map[string]engine.Capability{
			"injection": {Status: engine.Qualified, Reason: "adapter inserts pinned text into the initial instructions"},
			"native":    {Status: engine.Unavailable, Reason: "no verified per-run role-scoped skill directory"},
		}}
}

// New uses the shipped declaration unless qualification data for the same version and OS is supplied.
func New(launcher Launcher, sink engine.Sink, qualification *engine.CapabilitySet) (*Adapter, error) {
	if launcher == nil || sink == nil {
		return nil, errors.New("missing Codex launcher or event sink")
	}
	caps := Declaration()
	if qualification != nil {
		if qualification.Adapter != caps.Adapter || qualification.OS != caps.OS ||
			qualification.HarnessVersions != caps.HarnessVersions || qualification.Version != caps.Version {
			return nil, errors.New("qualification does not match the Codex adapter declaration")
		}
		caps.Ops = qualification.Copy().Ops
		caps.Models = append(caps.Models, qualification.Models...)
	}
	return &Adapter{launcher: launcher, sink: sink, caps: caps}, nil
}

// Capabilities returns an isolated copy of the declaration without running Codex.
func (a *Adapter) Capabilities(context.Context) (engine.CapabilitySet, error) {
	return a.caps.Copy(), nil
}

// --- Session admission and initial instructions (engine-adapter sections 1 and 3) ---

// Start creates a Codex thread and immediately starts its instruction turn.
func (a *Adapter) Start(ctx context.Context, req engine.StartRequest) (engine.Session, error) {
	return a.open(ctx, req, "", engine.StartOp)
}

// Resume reattaches to a saved Codex thread without firing a new turn.
// External operations still require provider reconciliation.
func (a *Adapter) Resume(ctx context.Context, ref string, req engine.StartRequest) (engine.Session, error) {
	return a.open(ctx, req, ref, engine.ResumeOp)
}

// open gates launches, checks the installed release, and prepares a session or cleans up its process.
func (a *Adapter) open(ctx context.Context, req engine.StartRequest, ref, operation string) (engine.Session, error) {
	if err := a.caps.Require(a.sink, req.AssignmentID, req.ThreadID, operation); err != nil {
		return nil, err
	}
	if req.AssignmentID == "" || req.ThreadID == "" {
		return nil, errors.New("assignment and thread IDs are required")
	}
	if err := engine.ValidateEnv(req.Env); err != nil {
		return nil, err
	}
	if operation == engine.ResumeOp && ref == "" {
		return nil, errors.New("missing Codex session reference")
	}
	prompt, err := startPrompt(req)
	if err != nil {
		return nil, err
	}
	version, err := a.launcher.Version(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, harnessFailure("probe Codex version", err)
	}
	if err := checkVersion(version); err != nil {
		return nil, harnessFailure("check Codex version", err)
	}
	transport, err := a.launcher.Launch(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, harnessFailure("launch Codex", err)
	}
	s := newSession(a, req, transport)
	if err := s.initialize(ctx, req, ref, prompt); err != nil {
		s.abort()
		<-s.done
		return nil, err
	}
	return s, nil
}

// checkVersion accepts the single stable release in the declared patch interval.
func checkVersion(output string) error {
	fields := strings.Fields(output)
	if len(fields) != 2 || (fields[0] != "codex-cli" && fields[0] != "codex") || fields[1] != harnessVersion {
		return fmt.Errorf("executable is outside the supported Codex range %s", versionRange)
	}
	return nil
}

// startPrompt orders stable instructions and verified skill text before changing instructions.
func startPrompt(req engine.StartRequest) (string, error) {
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

// harnessFailure classifies failures of the binary and protocol for the daemon.
func harnessFailure(op string, err error) error {
	return &engine.HarnessError{Operation: op, Cause: err}
}
