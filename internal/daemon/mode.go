package daemon

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"synch/internal/api"
	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/queue"
	"synch/internal/thread"
)

const (
	hostMode    = "full_control"
	sandboxMode = "sandbox"
	modeTable   = `CREATE TABLE IF NOT EXISTS daemon_modes (
		thread TEXT PRIMARY KEY, active TEXT NOT NULL, pending TEXT NOT NULL, active_seq INTEGER NOT NULL)`
)

// SandboxDriver can reopen its admission gate for a later switch after a clean settle.
type SandboxDriver interface {
	execdriver.ExecutionDriver
	Reopen(threadID string) error
}

// modeState gates admission from a durable scope record, including after a restart.
type modeState struct {
	root, active, pending string
	seq                   int64
}

// modeCreation is the immutable thread.created ancestry from the event catalogue.
type modeCreation struct {
	Parent string `json:"parent_thread_id"`
}

// modeRoot walks committed creation facts to the grant-owning coordinator.
func (d *Daemon) modeRoot(id string) (string, error) {
	visited := map[string]bool{}
	for id != "" {
		if visited[id] {
			return "", errors.New("cycle in mode scope ancestry")
		}
		visited[id] = true
		events, err := d.store.ReadStream("thread:"+id, 0, 32)
		if err != nil {
			return "", err
		}
		var creation *modeCreation
		for _, e := range events {
			if e.Type != "thread.created" {
				continue
			}
			creation = new(modeCreation)
			if err := json.Unmarshal(e.Payload, creation); err != nil {
				return "", err
			}
			break
		}
		if creation == nil {
			return "", thread.ErrMissing
		}
		if creation.Parent == "" {
			return id, nil
		}
		id = creation.Parent
	}
	return "", thread.ErrMissing
}

// findMode reads the coordinator's mode for a thread within the current writer.
func (d *Daemon) findMode(ctx context.Context, tx *eventlog.Transaction, id string) (modeState, error) {
	root, err := d.modeRoot(id)
	if err != nil {
		return modeState{}, err
	}
	m := modeState{root: root, active: hostMode}
	err = tx.QueryRowContext(ctx, `SELECT active,pending,active_seq FROM daemon_modes WHERE thread=?`, root).
		Scan(&m.active, &m.pending, &m.seq)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return m, err
}

// modeFor reads the durable admission state outside an existing transaction.
func (d *Daemon) modeFor(ctx context.Context, id string) (m modeState, err error) {
	err = d.store.Write(ctx, func(tx *eventlog.Transaction) error {
		m, err = d.findMode(ctx, tx, id)
		return err
	})
	return
}

// modeMembers finds the committed descendants of root while the caller owns the writer.
// Every child creation follows its parent's creation in global event order.
func (d *Daemon) modeMembers(ctx context.Context, root string) ([]string, error) {
	ids := []string{root}
	seen := map[string]bool{root: true}
	var cursor int64
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		events, err := d.store.ReadGlobal(cursor, 256)
		if err != nil {
			return nil, err
		}
		for _, e := range events {
			cursor = e.GlobalSeq
			if e.Type != "thread.created" || seen[e.ThreadID] {
				continue
			}
			var creation modeCreation
			if err := json.Unmarshal(e.Payload, &creation); err != nil {
				return nil, err
			}
			if seen[creation.Parent] {
				seen[e.ThreadID] = true
				ids = append(ids, e.ThreadID)
			}
		}
		if len(events) < 256 {
			return ids, nil
		}
	}
}

// modeDispatchAllowed prevents T05 from racing the transaction that pauses a scope.
func (d *Daemon) modeDispatchAllowed(ctx context.Context, tx *eventlog.Transaction, s thread.State) (bool, error) {
	m, err := d.findMode(ctx, tx, s.ID)
	return m.pending == "" && (m.active != sandboxMode || d.sandbox != nil), err
}

// --- Admission: validate before acceptance and pause atomically with the receipt ---

// prepareMode verifies the capability and authority before staging durable mode changes.
func (d *Daemon) prepareMode(ctx context.Context, tx *eventlog.Transaction, c api.ModeCommand) (func(context.Context, *eventlog.Transaction) error, func(context.Context) error, error) {
	m, err := d.findMode(ctx, tx, c.Thread)
	if err != nil {
		return nil, nil, err
	}
	if err := d.validateMode(c, m); err != nil {
		return nil, nil, err
	}
	commit := func(ctx context.Context, tx *eventlog.Transaction) error {
		if m.pending != "" {
			return nil
		}
		return d.pauseMode(ctx, tx, c, m)
	}
	return commit, func(ctx context.Context) error { return d.settleMode(ctx, c, m) }, nil
}

// validateMode allows switches only on the grant-owning coordinator for the local host.
func (d *Daemon) validateMode(c api.ModeCommand, m modeState) error {
	host := d.driver.Describe()
	switch {
	case c.Target != host.Host || c.Thread != m.root || c.To != hostMode && c.To != sandboxMode:
		return &queue.Rejection{Code: "validation_failed", Detail: "mode switches require the coordinator and its local host target"}
	case m.active == c.To || m.pending != "" && m.pending != c.To:
		return &queue.Rejection{Code: "invalid_transition", Detail: "the scope is already in that mode or switching to another mode"}
	case c.To == sandboxMode && d.sandbox == nil:
		return &queue.Rejection{Code: "invalid_transition", Detail: "sandbox_unavailable: no certified Sandbox driver is configured"}
	case c.To == hostMode:
		grants, err := d.modeGrants(c.Thread, m.seq)
		if err != nil {
			return err
		}
		if len(grants) == 0 {
			return &queue.Rejection{Code: "grant_required", Detail: "a fresh operator grant is required for this host account"}
		}
	}
	return nil
}

// pauseMode records the request and applies T18 to every running scope member.
func (d *Daemon) pauseMode(ctx context.Context, tx *eventlog.Transaction, c api.ModeCommand, m modeState) error {
	ids, err := d.modeMembers(ctx, c.Thread)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO daemon_modes VALUES(?,?,?,?) ON CONFLICT(thread) DO UPDATE SET pending=excluded.pending`, c.Thread, m.active, c.To, m.seq)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := recordMode(ctx, tx, id, c, m.active, "requested"); err != nil {
			return err
		}
		err := d.threads.ApplyTx(ctx, tx, id, thread.Command{Transition: "T18"})
		if err != nil && !errors.Is(err, thread.ErrTransition) {
			return err
		}
	}
	return nil
}

// recordMode writes a catalogue v2 mode event with the authenticated operator and target.
func recordMode(ctx context.Context, tx *eventlog.Transaction, id string, c api.ModeCommand, from, phase string) (eventlog.Event, error) {
	payload := map[string]any{"actor": c.Actor, "target": c.Target, "from": from, "to": c.To, "phase": phase}
	if phase == "active" && c.To == sandboxMode {
		payload["host_processes_stopped"] = true
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return eventlog.Event{}, err
	}
	return tx.Append(ctx, eventlog.NewEvent{Stream: "thread:" + id, ThreadID: id, Type: "mode.changed", TypeVersion: 2,
		Actor: eventlog.Actor{Kind: "operator", ID: c.Actor}, Payload: raw})
}

// --- Settlement: stop work and revoke authority before publishing the active mode ---

// settleMode fences in-flight starts, settles each member, then releases T18 pauses.
func (d *Daemon) settleMode(ctx context.Context, c api.ModeCommand, m modeState) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	d.dispatchMu.Lock()
	defer d.dispatchMu.Unlock()
	ids, err := d.markModeSettling(ctx, c, m.active)
	if err != nil {
		return err
	}
	var settleErr error
	for _, id := range ids {
		settleErr = errors.Join(settleErr, d.stopModeMember(ctx, id, m.active))
	}
	if settleErr != nil {
		return settleErr
	}
	if c.To == sandboxMode {
		if err := d.revokeModeGrants(ctx, ids, c.Actor); err != nil {
			return err
		}
	} else {
		for _, id := range ids {
			if err := d.sandbox.Reopen(id); err != nil {
				return err
			}
			// Reconstruct the drained Host generation if the daemon restarted in Sandbox.
			report, err := d.driver.Settle(ctx, id)
			if err != nil {
				return err
			}
			if !report.HostProcessesStopped {
				return fmt.Errorf("prior Host process cleanup unconfirmed: %s", report.Reason)
			}
			if err := d.driver.Reopen(id); err != nil {
				return err
			}
		}
	}
	return d.activateMode(ctx, ids, c, m)
}

// markModeSettling waits for prior launch admission and records settlement in progress.
func (d *Daemon) markModeSettling(ctx context.Context, c api.ModeCommand, from string) (ids []string, err error) {
	// Spawn holds this lock from its mode check through process registration.
	d.decisionMu.Lock()
	defer d.decisionMu.Unlock()
	err = d.store.Write(ctx, func(tx *eventlog.Transaction) error {
		var err error
		ids, err = d.modeMembers(ctx, c.Thread)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if _, err := recordMode(ctx, tx, id, c, from, "settling"); err != nil {
				return err
			}
		}
		return nil
	})
	return
}

// stopModeMember settles the prior execution driver and waits for its engine session.
func (d *Daemon) stopModeMember(ctx context.Context, id, mode string) error {
	d.mu.Lock()
	a := d.sessions[id]
	d.mu.Unlock()
	if a != nil {
		a.cancel()
	}
	driver := d.Driver(execdriver.KindDirect)
	if mode == sandboxMode {
		driver = d.sandbox
	}
	if driver == nil {
		return errors.New("prior execution driver is unavailable for settlement")
	}
	var sessionErr error
	if a != nil {
		select {
		case <-a.done:
			sessionErr = a.stopErr
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	report, err := driver.Settle(ctx, id)
	if err != nil {
		return errors.Join(sessionErr, err)
	}
	if !report.HostProcessesStopped {
		return errors.Join(sessionErr, fmt.Errorf("process cleanup unconfirmed for %s: %s", id, report.Reason))
	}
	return sessionErr
}

// classifyModeMember records an interrupted assignment as lost unless its work was already classified.
func (d *Daemon) classifyModeMember(ctx context.Context, id string) error {
	s, err := d.threads.Status(ctx, id)
	if err != nil || s.Reason != "mode_switch" || s.Facts.Assignment == "" || s.Work[s.Facts.Assignment] != "" {
		return err
	}
	work := "lost"
	if s.Facts.ManagedParent == "" {
		work = "resumable"
	} // A coordinator never runs a process.
	return d.record(id, "process.exited", map[string]any{"assignment_id": s.Facts.Assignment,
		"execution_outcome": "killed", "work": work, "descendants_cleaned": true}, "")
}

// revokeModeGrants completes every matching grant revocation, including retries of incomplete revocations.
func (d *Daemon) revokeModeGrants(ctx context.Context, ids []string, actor string) error {
	for _, id := range ids {
		grants, err := d.modeGrants(id, 0)
		if err != nil {
			return err
		}
		for _, grant := range grants {
			if err := d.policy.Revoke(ctx, grant, actor); err != nil {
				return fmt.Errorf("revoke Host grant: %w", err)
			}
		}
	}
	return nil
}

// activateMode publishes the active event and admission mode together, then releases settled pauses.
func (d *Daemon) activateMode(ctx context.Context, ids []string, c api.ModeCommand, m modeState) error {
	err := d.store.Write(ctx, func(tx *eventlog.Transaction) error {
		for _, id := range ids {
			// Read committed authority while holding the writer so a concurrent issue
			// or revoke cannot change the decision before the active event commits.
			grants, err := d.modeGrants(id, 0)
			if err != nil {
				return err
			}
			if c.To == sandboxMode && len(grants) > 0 {
				return errors.New("host grants changed during settlement")
			}
			if id == c.Thread && c.To == hostMode {
				fresh, err := d.modeGrants(id, m.seq)
				if err != nil {
					return err
				}
				if len(fresh) == 0 {
					return errors.New("fresh Host grant disappeared during settlement")
				}
			}
			event, err := recordMode(ctx, tx, id, c, m.active, "active")
			if err != nil {
				return err
			}
			if id == c.Thread {
				if _, err := tx.ExecContext(ctx, `UPDATE daemon_modes SET active=?,pending='',active_seq=? WHERE thread=?`, c.To, event.GlobalSeq, id); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := d.classifyModeMember(ctx, id); err != nil {
			return err
		}
		if err := d.releaseModeMember(ctx, id, c.To); err != nil {
			return err
		}
	}
	return nil
}

// releaseModeMember supplies the settlement facts and lets T21 enforce ordinary work guards.
func (d *Daemon) releaseModeMember(ctx context.Context, id, mode string) error {
	s, err := d.threads.Status(ctx, id)
	if err != nil || s.Reason != "mode_switch" {
		return err
	}
	if err := d.threads.Update(ctx, id, func(f *thread.Facts) error {
		f.SwitchToSandbox, f.ActiveSettled = mode == sandboxMode, true
		return nil
	}); err != nil {
		return err
	}
	s, err = d.threads.Status(ctx, id)
	if err != nil || s.Reason != "mode_switch" {
		return err
	}
	return d.threads.Apply(ctx, id, thread.Command{Transition: "T21"})
}

// resumeModePauses finishes releases interrupted after mode activation committed.
func (d *Daemon) resumeModePauses(ctx context.Context) error {
	ids, err := d.threadsIn(ctx, "any", "paused")
	if err != nil {
		return err
	}
	for _, id := range ids {
		state, err := d.threads.Status(ctx, id)
		if err != nil {
			return err
		}
		if state.Reason != "mode_switch" {
			continue
		}
		mode, err := d.modeFor(ctx, id)
		if err != nil {
			return err
		}
		if mode.seq == 0 || mode.pending != "" || mode.active == sandboxMode && d.sandbox == nil {
			continue
		}
		if err := d.classifyModeMember(ctx, id); err != nil {
			return err
		}
		if err := d.releaseModeMember(ctx, id, mode.active); err != nil {
			return err
		}
	}
	return nil
}

// --- Grant reads: preserve policy ownership of issuance and revocation ---

// modeGrants returns matching own grants issued after the cutoff that still need revocation.
// For a fresh Host grant check it excludes any grant whose revocation has started.
func (d *Daemon) modeGrants(id string, cutoff int64) ([]string, error) {
	host := d.driver.Describe()
	var ids []string
	status := map[string]bool{}
	var cursor int64
	for {
		events, err := d.store.ReadStream("thread:"+id, cursor, 256)
		if err != nil {
			return nil, err
		}
		for _, e := range events {
			cursor = e.Seq
			switch e.Type {
			case "grant.issued":
				var g struct{ ID, Host, Identity string }
				if err := json.Unmarshal(e.Payload, &g); err != nil {
					return nil, err
				}
				if e.GlobalSeq > cutoff && g.Host == host.Host && g.Identity == host.Identity {
					ids = append(ids, g.ID)
					status[g.ID] = true
				}
			case "grant.revoked":
				var r struct {
					Grant   string `json:"grant_id"`
					Settled bool
				}
				if err := json.Unmarshal(e.Payload, &r); err != nil {
					return nil, err
				}
				if cutoff != 0 || r.Settled {
					delete(status, r.Grant)
				}
			}
		}
		if len(events) < 256 {
			break
		}
	}
	active := ids[:0]
	for _, id := range ids {
		if status[id] {
			active = append(active, id)
		}
	}
	return active, nil
}

// needsOwnGrant keeps existing Builders parked until the operator grants them directly
// after an upgrade. Newly delegated Builders follow ordinary Q-21 inheritance.
func (d *Daemon) needsOwnGrant(id string, m modeState) (bool, error) {
	if id == m.root || m.active != hostMode || m.seq == 0 {
		return false, nil
	}
	events, err := d.store.ReadStream("thread:"+id, 0, 1)
	if err != nil {
		return false, err
	}
	if len(events) == 1 && events[0].GlobalSeq > m.seq {
		return false, nil
	}
	grants, err := d.modeGrants(id, m.seq)
	return len(grants) == 0, err
}
