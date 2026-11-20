package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"

	"synch/internal/engine"
	"synch/internal/engine/claudecode"
	"synch/internal/engine/codex"
	"synch/internal/engine/kimi"
	"synch/internal/policy"
)

// harness is one offered engine adapter and the version routing may assume for it.
type harness struct {
	name    string
	adapter engine.EngineAdapter
	version string
}

// find returns the offered harness with this adapter name, or nil.
func (d *Daemon) find(name string) *harness {
	for _, h := range d.harnesses {
		if h.name == name {
			return h
		}
	}
	return nil
}

// offer registers an adapter under its declared name.
func (d *Daemon) offer(cfg Config, adapter engine.EngineAdapter) error {
	caps, err := adapter.Capabilities(context.Background())
	if err != nil {
		return err
	}
	version := cfg.HarnessVersions[caps.Adapter]
	if version == "" {
		version = lowerBound(caps.HarnessVersions)
	}
	d.harnesses = append(d.harnesses, &harness{name: caps.Adapter, adapter: adapter, version: version})
	return nil
}

// lowerBound extracts the oldest release a version range admits ("" when it names none).
func lowerBound(declaration string) string {
	for _, field := range strings.Fields(declaration) {
		if v, ok := strings.CutPrefix(field, ">="); ok {
			return v
		}
		if v, ok := strings.CutPrefix(field, "="); ok {
			return v
		}
		if field[0] >= '0' && field[0] <= '9' {
			return field
		}
	}
	return ""
}

// openHarnesses constructs the Codex, Claude Code and Kimi adapters from their binaries.
// A harness whose binary is absent is not offered. Launches go through the
// policy-checked Direct driver, so no adapter can start a process by itself.
func (d *Daemon) openHarnesses(cfg Config) error {
	sink := engineSink{daemon: d}
	if cfg.adapter != nil {
		adapter, err := cfg.adapter(sink)
		if err != nil {
			return err
		}
		return d.offer(cfg, adapter)
	}
	driver := policyDriver{ExecutionDriver: d.driver, daemon: d}
	qualification := func(name string) *engine.CapabilitySet {
		if q, ok := cfg.Qualifications[name]; ok {
			return &q
		}
		return nil
	}
	if path, ok := locate(cfg.CodexBinary, "codex"); ok {
		launcher, err := codex.NewDirectLauncher(driver, path)
		if err != nil {
			return err
		}
		adapter, err := codex.New(launcher, sink, qualification("codex"))
		if err != nil {
			return err
		}
		if err := d.offer(cfg, adapter); err != nil {
			return err
		}
	}
	if path, ok := locate(cfg.ClaudeBinary, "claude"); ok {
		launcher, err := claudecode.NewDirectLauncher(driver, path)
		if err != nil {
			return err
		}
		d.permits = make(chan claudecode.PermissionRequest)
		d.workers.Add(1)
		go d.answerPermissions()
		adapter, err := claudecode.New(launcher, sink, d.permits, qualification("claude-code"))
		if err != nil {
			return err
		}
		if err := d.offer(cfg, adapter); err != nil {
			return err
		}
	}
	if path, ok := locate(cfg.KimiBinary, "kimi"); ok {
		launcher, err := kimi.NewDirectLauncher(driver, path)
		if err != nil {
			return err
		}
		adapter, err := kimi.New(launcher, sink, qualification("kimi-code"))
		if err != nil {
			return err
		}
		return d.offer(cfg, adapter)
	}
	return nil
}

// locate resolves a configured or default binary name; absence means "not offered".
func locate(configured, fallback string) (string, bool) {
	name := configured
	if name == "" {
		name = fallback
	}
	path, err := exec.LookPath(name)
	if err != nil {
		slog.Info("harness binary not found; harness not offered", "binary", name)
		return "", false
	}
	return path, true
}

// answerPermissions decides each Claude Code can_use_tool request through the
// policy engine. Any error, denial or missing rule is a refusal (fail closed).
func (d *Daemon) answerPermissions() {
	defer d.workers.Done()
	for {
		select {
		case <-d.lifetime.Done():
			return
		case request := <-d.permits:
			request.Reply(d.toolAllowed(request))
		}
	}
}

// toolAllowed asks policy whether the thread may use the requested tool.
func (d *Daemon) toolAllowed(request claudecode.PermissionRequest) bool {
	d.decisionMu.Lock()
	defer d.decisionMu.Unlock()
	decision, err := d.policy.Decide(request.Context, policy.Request{
		Thread: request.ThreadID, Action: "tool.use", Target: request.ToolName,
		Machine: d.driver.Describe().Host,
	})
	if err != nil {
		slog.Warn("tool permission decision failed", "thread_id", request.ThreadID, "error", fmt.Sprint(err))
		return false
	}
	return decision.Allowed
}
