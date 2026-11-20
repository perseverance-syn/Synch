package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"synch/internal/credentials"
	"synch/internal/engine"
	"synch/internal/policy"
)

// --- Credentials: broker at use, prompt redaction and the Direct-mode limit (card 401) ---

// DirectLimitation is listed on a child's result when it ran under Direct Host access
// (spec line 74: "Direct native actions can exceed journal coverage. Show that limit").
const DirectLimitation = "direct host access: native actions can exceed journal coverage"

// ErrSecretInEnv refuses a launch whose environment carries a known secret value:
// secrets are brokered at use, never placed in engine Env (card 401 C2).
var ErrSecretInEnv = errors.New("engine environment carries a known secret value")

// UseSecret hands a thread its own secret at the moment of use. The role comes from the
// thread's facts, never from the caller: a coordinator (no managed parent) is always
// refused, and a worker reads only its thread scope after policy allows secret.use.
func (d *Daemon) UseSecret(ctx context.Context, threadID, name string) (string, error) {
	state, err := d.threads.Status(ctx, threadID)
	if err != nil {
		return "", err
	}
	caller := credentials.Caller{Role: credentials.RoleThread, Thread: threadID}
	if state.Facts.ManagedParent == "" {
		caller = credentials.Caller{Role: credentials.RoleCoordinator}
	}
	return d.creds.Use(ctx, caller, name)
}

// authorizeSecret decides a thread's secret use as the policy action secret.use on this
// machine; ask (waiting for approval) and deny both refuse.
func (d *Daemon) authorizeSecret(ctx context.Context, caller credentials.Caller, name string) error {
	d.decisionMu.Lock()
	defer d.decisionMu.Unlock()
	decision, err := d.policy.Decide(ctx, policy.Request{Thread: caller.Thread, Action: "secret.use", Target: name, Machine: d.driver.Describe().Host})
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return fmt.Errorf("%w: %s", ErrPolicyDenied, decision.Reason)
	}
	return nil
}

// redactRequest returns a copy of req whose instructions and skill texts have every known
// secret removed. An Env value holding a known secret refuses the launch instead.
func (d *Daemon) redactRequest(req engine.StartRequest) (engine.StartRequest, error) {
	red := d.creds.Redactor()
	for _, v := range req.Env {
		if _, n := red.Redact(v); n > 0 {
			return req, ErrSecretInEnv
		}
	}
	req.Instructions = slices.Clone(req.Instructions)
	for i := range req.Instructions {
		req.Instructions[i].Text = red.String(req.Instructions[i].Text)
	}
	req.Skills = slices.Clone(req.Skills)
	for i := range req.Skills {
		req.Skills[i].Text = red.String(req.Skills[i].Text)
	}
	return req, nil
}

// ranDirect reports whether any assignment of the thread was dispatched under Direct.
func (d *Daemon) ranDirect(id string) (bool, error) {
	var cursor int64
	for {
		events, err := d.store.ReadStream("thread:"+id, cursor, 256)
		if err != nil {
			return false, err
		}
		for _, e := range events {
			cursor = e.Seq
			var p struct{ Execution string }
			if e.Type == "assignment.dispatched" && json.Unmarshal(e.Payload, &p) == nil && p.Execution == "direct" {
				return true, nil
			}
		}
		if len(events) < 256 {
			return false, nil
		}
	}
}
