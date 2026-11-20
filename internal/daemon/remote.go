package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	"synch/internal/engine"
	"synch/internal/execdriver"
	"synch/internal/policy"
	"synch/internal/remote"
	"synch/internal/thread"
)

// RemoteConfig registers one SSH runner as the daemon's remote execution driver.
// A thread is bound to it when its RunnerID fact names Runner.
type RemoteConfig struct {
	Registry *remote.Registry
	Runner   string
	Broker   remote.Broker
}

// openRemote registers the remote driver under the remote execution kind.
// With no configuration nothing is created and nothing changes.
func (d *Daemon) openRemote(cfg *RemoteConfig) error {
	if cfg == nil {
		return nil
	}
	driver, err := remote.NewDriver(cfg.Registry, cfg.Runner, cfg.Broker, d.policy, d.store, filepath.Join(d.dataDir, "remote.db"))
	if err != nil {
		return err
	}
	d.remote, d.remoteID = driver, cfg.Runner
	return nil
}

// Driver returns the registered execution driver of a kind, or nil.
func (d *Daemon) Driver(kind string) execdriver.ExecutionDriver {
	switch {
	case kind == execdriver.KindDirect:
		return d.driver
	case kind == execdriver.KindRemote && d.remote != nil:
		return d.remote
	}
	return nil
}

// startRemote handles a new assignment bound to the remote runner. A runner
// that is not connected pauses the running thread (T19). A connected runner
// cannot start an engine yet: the engine launchers accept only the Direct driver.
func (d *Daemon) startRemote(ctx context.Context, req engine.StartRequest) error {
	if !d.remote.Status().Connected {
		return d.runnerDisconnected(ctx, req.ThreadID, false)
	}
	return d.startFailed(ctx, req, errors.New("engine launchers require the Direct driver; remote engine launch is unavailable"))
}

// DoFresh mediates a new approval-dependent action on the thread's runner.
// A disconnected runner pauses the thread with runner_disconnected (T19).
func (d *Daemon) DoFresh(ctx context.Context, id string, request policy.Request) (policy.Decision, error) {
	if d.remote == nil {
		return policy.Decision{}, errors.New("no remote driver is configured")
	}
	request.Thread = id
	decision, err := d.remote.DoFresh(ctx, request)
	var offline *remote.DisconnectedError
	if errors.As(err, &offline) {
		return decision, errors.Join(err, d.runnerDisconnected(ctx, id, true))
	}
	return decision, err
}

// runnerDisconnected records the guard facts for T19 and applies it to a running thread.
func (d *Daemon) runnerDisconnected(ctx context.Context, id string, fresh bool) error {
	err := d.threads.Update(ctx, id, func(f *thread.Facts) error {
		f.Connected, f.RunnerID = false, d.remoteID
		f.NeedsDispatch, f.FreshApprovalNeeded = f.NeedsDispatch || !fresh, f.FreshApprovalNeeded || fresh
		return nil
	})
	if err != nil {
		return err
	}
	if err := d.threads.Apply(ctx, id, thread.Command{Transition: "T19"}); err != nil && !errors.Is(err, thread.ErrTransition) {
		return fmt.Errorf("pause for disconnected runner: %w", err)
	}
	return nil
}
