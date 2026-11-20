package kimi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"synch/internal/engine"
	"synch/internal/engine/claudecode/stdio"
	"synch/internal/execdriver"
)

var _ engine.EngineAdapter = (*Adapter)(nil)

// Adapter configures a Kimi Code CLI ACP process and its event sink.
type Adapter struct {
	launcher stdio.Launcher
	sink     engine.Sink
	caps     engine.CapabilitySet
}

// Declaration reports the ACP compatibility floor and untested operations.
func Declaration() engine.CapabilitySet {
	// The matrix does not identify a CLI release floor, so any release routes; the ACP protocol is checked at initialize. The name is the contract's `kimi-code` harness.
	caps := stdio.Declaration("kimi-code", ">=0.0")
	caps.Ops[engine.UsageOp] = engine.Capability{Status: engine.Untested, Reason: "Kimi ACP does not report token usage"}
	return caps
}

// New constructs an adapter with optional matching qualification data.
func New(launcher stdio.Launcher, sink engine.Sink, qualification *engine.CapabilitySet) (*Adapter, error) {
	if launcher == nil || sink == nil {
		return nil, errors.New("missing Kimi launcher or event sink")
	}
	caps, err := stdio.Qualify(Declaration(), qualification)
	if err != nil {
		return nil, err
	}
	return &Adapter{launcher: launcher, sink: sink, caps: caps}, nil
}

// NewDirectLauncher finds the operator-installed Kimi Code CLI executable.
func NewDirectLauncher(driver execdriver.ExecutionDriver, binary string) (*stdio.DirectLauncher, error) {
	if binary == "" {
		binary = "kimi"
	}
	return stdio.NewDirectLauncher(driver, binary)
}

// Capabilities returns an isolated declaration without starting Kimi.
func (a *Adapter) Capabilities(context.Context) (engine.CapabilitySet, error) {
	return a.caps.Copy(), nil
}

// Start initializes ACP, opens one session, and sends the assignment as its first prompt.
func (a *Adapter) Start(ctx context.Context, req engine.StartRequest) (engine.Session, error) {
	if err := a.caps.Require(a.sink, req.AssignmentID, req.ThreadID, engine.StartOp); err != nil {
		return nil, err
	}
	prompt, err := stdio.Instructions(req)
	if err != nil {
		return nil, err
	}
	req.Workdir, err = filepath.Abs(req.Workdir)
	if err != nil {
		return nil, fmt.Errorf("resolve Kimi working directory: %w", err)
	}
	t, err := a.launcher.Launch(ctx, req, "acp")
	if err != nil {
		return nil, &engine.HarnessError{Operation: "launch Kimi ACP", Cause: err}
	}

	s := &session{wire: stdio.NewSession(t, a.caps, a.sink, req), lane: make(chan struct{}, 1)}
	s.wire.Start(s.frame)
	if err := s.initialize(ctx, req, prompt); err != nil {
		s.wire.Fail("initialize ACP", err)
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		return nil, errors.Join(err, s.wire.Wait(cleanup))
	}
	return s, nil
}

// initialize negotiates ACP v1, registers daemon MCP, and opens the requested workspace.
func (s *session) initialize(ctx context.Context, req engine.StartRequest, prompt string) error {
	params := stdio.Fields{"protocolVersion": 1, "clientCapabilities": stdio.Fields{},
		"clientInfo": stdio.Fields{"name": "synch", "version": "0.1.0"}}
	var hello struct {
		ProtocolVersion   int
		AgentCapabilities struct {
			MCPCapabilities struct{ HTTP bool }
		}
	}
	err := s.call(ctx, "initialize", params, func(raw json.RawMessage) error {
		return json.Unmarshal(raw, &hello)
	})
	if err != nil {
		return err
	}
	if hello.ProtocolVersion != 1 {
		return errors.New("unsupported ACP version reported by Kimi")
	}
	servers := []stdio.Fields{}
	if req.MCPEndpoint != "" {
		if !hello.AgentCapabilities.MCPCapabilities.HTTP {
			return errors.New("daemon HTTP MCP endpoint is unsupported by Kimi ACP")
		}
		servers = append(servers, stdio.Fields{"type": "http", "name": "synch", "url": req.MCPEndpoint, "headers": []string{}})
	}
	err = s.call(ctx, "session/new", stdio.Fields{"cwd": req.Workdir, "mcpServers": servers}, s.acceptSession)
	if err != nil {
		return err
	}
	if req.Model != "" {
		err = s.call(ctx, "session/set_config_option", stdio.Fields{
			"sessionId": s.Ref(), "configId": "model", "value": req.Model,
		}, nil)
		if err != nil {
			return err
		}
	}
	if prompt != "" {
		return s.prompt(ctx, prompt)
	}
	return nil
}

// acceptSession stores the ACP ID before reading subsequent session updates.
func (s *session) acceptSession(raw json.RawMessage) error {
	var result struct{ SessionID string }
	if err := json.Unmarshal(raw, &result); err != nil {
		return err
	}
	if result.SessionID == "" {
		return errors.New("missing session ID in Kimi ACP response")
	}
	s.mu.Lock()
	s.ref = result.SessionID
	s.mu.Unlock()
	return nil
}
