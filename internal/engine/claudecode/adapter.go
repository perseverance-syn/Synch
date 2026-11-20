package claudecode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"synch/internal/engine"
	"synch/internal/engine/claudecode/stdio"
	"synch/internal/execdriver"
)

const versionRange = ">=2.1.325 <2.1.326"

var _ engine.EngineAdapter = (*Adapter)(nil)
var _ engine.Resumer = (*Adapter)(nil)

// Adapter owns the configuration for one Claude Code process per assignment.
type Adapter struct {
	launcher stdio.Launcher
	sink     engine.Sink
	policy   chan<- PermissionRequest
	caps     engine.CapabilitySet
}

// Declaration reports the observed Claude Code patch and the untested operations.
func Declaration() engine.CapabilitySet { return stdio.Declaration("claude-code", versionRange) }

// New creates an adapter with an optional policy receiver. Missing decisions are denied.
func New(launcher stdio.Launcher, sink engine.Sink, policy chan<- PermissionRequest, qualification *engine.CapabilitySet) (*Adapter, error) {
	if launcher == nil || sink == nil {
		return nil, errors.New("missing Claude Code launcher or event sink")
	}
	caps, err := stdio.Qualify(Declaration(), qualification)
	if err != nil {
		return nil, err
	}
	return &Adapter{launcher: launcher, sink: sink, policy: policy, caps: caps}, nil
}

// NewDirectLauncher finds the unmodified operator-installed Claude Code executable.
func NewDirectLauncher(driver execdriver.ExecutionDriver, binary string) (*stdio.DirectLauncher, error) {
	if binary == "" {
		binary = "claude"
	}
	return stdio.NewDirectLauncher(driver, binary)
}

// Capabilities returns an isolated declaration without starting Claude Code.
func (a *Adapter) Capabilities(context.Context) (engine.CapabilitySet, error) {
	return a.caps.Copy(), nil
}

// Start initializes the control channel and sends the first assignment turn.
func (a *Adapter) Start(ctx context.Context, req engine.StartRequest) (engine.Session, error) {
	return a.open(ctx, req, "", engine.StartOp)
}

// Resume selects existing Claude history and defers current instructions until the next Send.
func (a *Adapter) Resume(ctx context.Context, ref string, req engine.StartRequest) (engine.Session, error) {
	return a.open(ctx, req, ref, engine.ResumeOp)
}

// open validates admission and the installed version before starting the interactive protocol.
func (a *Adapter) open(ctx context.Context, req engine.StartRequest, ref, op string) (engine.Session, error) {
	if err := a.caps.Require(a.sink, req.AssignmentID, req.ThreadID, op); err != nil {
		return nil, err
	}
	if op == engine.ResumeOp && ref == "" {
		return nil, errors.New("missing Claude Code session reference")
	}
	prompt, err := stdio.Instructions(req)
	if err != nil {
		return nil, err
	}
	if err := a.checkVersion(ctx, req); err != nil {
		return nil, &engine.HarnessError{Operation: "check Claude Code version", Cause: err}
	}
	args, err := launchArgs(req, ref)
	if err != nil {
		return nil, err
	}
	t, err := a.launcher.Launch(ctx, req, args...)
	if err != nil {
		return nil, &engine.HarnessError{Operation: "launch Claude Code", Cause: err}
	}
	s := &session{wire: stdio.NewSession(t, a.caps, a.sink, req), policy: a.policy, ref: ref,
		sendLane: make(chan struct{}, 1), assignmentID: req.AssignmentID, threadID: req.ThreadID}
	s.wire.Start(s.frame)
	err = s.control(ctx, "initialize")
	if err == nil {
		if ref != "" {
			s.initial = prompt
		} else if prompt != "" {
			err = s.send(ctx, prompt)
		}
	}
	if err != nil {
		s.wire.Fail("initialize", err)
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return nil, errors.Join(err, s.wire.Wait(cleanup))
	}
	return s, nil
}

// launchArgs keeps stdio permission prompts enabled and binds options that might begin with a dash.
func launchArgs(req engine.StartRequest, ref string) ([]string, error) {
	args := []string{"-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose",
		"--permission-prompt-tool", "stdio"}
	if req.Model != "" {
		args = append(args, "--model="+req.Model)
	}
	if ref != "" {
		args = append(args, "--resume="+ref)
	}
	if req.MCPEndpoint != "" {
		config, err := json.Marshal(stdio.Fields{"mcpServers": stdio.Fields{"synch": stdio.Fields{
			"type": "http", "url": req.MCPEndpoint,
		}}})
		if err != nil {
			return nil, fmt.Errorf("encode Claude MCP config: %w", err)
		}
		args = append(args, "--mcp-config", string(config))
	}
	return args, nil
}

// --- Version gate: separate the unauthenticated probe from the session ---

// checkVersion probes only --version, then accepts the stable patch observed in the matrix.
func (a *Adapter) checkVersion(ctx context.Context, req engine.StartRequest) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	// The version command needs no daemon-brokered values.
	req.Env = nil
	t, err := a.launcher.Launch(ctx, req, "--version")
	if err != nil {
		return err
	}
	type result struct {
		data []byte
		err  error
	}
	read := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(io.LimitReader(t, 1025))
		read <- result{data: data, err: err}
	}()
	defer func() { _ = t.Close() }()
	select {
	case <-ctx.Done():
		cancelErr := cancelProbe(t)
		_ = t.Close()
		<-read
		return errors.Join(ctx.Err(), cancelErr)
	case output := <-read:
		if output.err != nil || len(output.data) > 1024 {
			return errors.Join(errors.New("invalid Claude Code version output"), output.err, cancelProbe(t))
		}
		info, err := t.Wait(ctx)
		if err != nil || info.ExitCode != 0 || info.ExecutionOutcome != "exited" || !info.DescendantsCleaned {
			return errors.Join(errors.New("version probe failed for Claude Code"), err, cancelProbe(t))
		}
		if strings.Join(strings.Fields(string(output.data)), " ") != "2.1.325 (Claude Code)" {
			return fmt.Errorf("executable is outside supported Claude Code range %s", versionRange)
		}
		return nil
	}
}

// cancelProbe gives supervisor cleanup time even if the probe context expired.
func cancelProbe(t stdio.Transport) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return t.Cancel(ctx)
}
