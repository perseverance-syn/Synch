package thread

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"synch/internal/eventlog"
)

// --- Checkpoint format: encode exactly the handoff fields (?4) ---

type Reference struct {
	URI    string `json:"uri"`
	Digest string `json:"digest"`
}
type Checkpoint struct {
	Version      int    `json:"checkpoint_version"`
	ThreadID     string `json:"thread_id"`
	BriefVersion int    `json:"brief_version"`
	Plan         struct {
		Revision int    `json:"revision"`
		Digest   string `json:"digest"`
	} `json:"plan"`
	Revision     string `json:"revision"`
	PendingEdits struct {
		Diff           Reference `json:"uncommitted_diff"`
		WorktreeDigest string    `json:"worktree_digest"`
	} `json:"pending_edits"`
	FailedAttempts []FailedAttempt `json:"failed_attempts"`
	OpenDecisions  []Decision      `json:"open_decisions"`
	Uncertain      []string        `json:"uncertain_external_requests"`
	Skills         []Skill         `json:"skills"`
	Handoff        []string        `json:"handoff_files"`
}
type FailedAttempt struct {
	Assignment string `json:"assignment_id"`
	Harness    string `json:"harness"`
	Model      string `json:"model"`
	Reason     string `json:"reason"`
}
type Decision struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Summary string `json:"summary"`
}
type Skill struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}

// --- Checkpoint publication: sync the file and persist its reference before T14 or T35 ---

// Rollover commits T13 before writing the checkpoint for the next session.
func (m *Machine) Rollover(ctx context.Context, dir string, cp Checkpoint, trigger string) (Reference, error) {
	if err := m.Apply(ctx, cp.ThreadID, Command{Transition: "T13"}); err != nil {
		return Reference{}, err
	}
	return m.FinishCheckpoint(ctx, dir, cp, trigger)
}

// FinishCheckpoint can retry after T13 was committed but file publication failed.
func (m *Machine) FinishCheckpoint(ctx context.Context, dir string, cp Checkpoint, trigger string) (Reference, error) {
	state, err := m.Status(ctx, cp.ThreadID)
	if err != nil {
		return Reference{}, err
	}
	if state.Phase != "rollover" && state.Reason != "harness_unavailable" {
		return Reference{}, ErrTransition
	}
	// A retry after the reference committed only owes the exit transition.
	if state.Phase == "rollover" && state.Facts.CheckpointID != "" {
		return state.Checkpoint, m.Apply(ctx, cp.ThreadID, Command{Transition: "T14"})
	}
	switch trigger {
	case "context_limit", "harness_switch", "operator", "harness_failure", "daemon_recovery":
	default:
		return Reference{}, ErrTransition
	}
	cp.Version = 1
	cp.BriefVersion = state.Facts.BriefVersion
	if state.Facts.PlanDigest != "" {
		cp.Plan.Digest = state.Facts.PlanDigest
		if state.Facts.PlanRevision != 0 {
			cp.Plan.Revision = state.Facts.PlanRevision
		}
	}
	if state.Facts.Revision != "" {
		cp.Revision = state.Facts.Revision
	}
	cp.Uncertain = []string{}
	for id, v := range state.Requests {
		if v == "" || v == "unknown" {
			cp.Uncertain = append(cp.Uncertain, id)
		}
	}
	slices.Sort(cp.Uncertain)
	if cp.FailedAttempts == nil {
		cp.FailedAttempts = []FailedAttempt{}
	}
	if cp.OpenDecisions == nil {
		cp.OpenDecisions = []Decision{}
	}
	if cp.Skills == nil {
		cp.Skills = []Skill{}
	}
	if cp.Handoff == nil {
		cp.Handoff = []string{}
	}
	raw, err := json.Marshal(cp)
	if err != nil {
		return Reference{}, err
	}
	id := eventlog.NewID()
	path, err := filepath.Abs(filepath.Join(dir, id+".json"))
	if err != nil {
		return Reference{}, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return Reference{}, err
	}
	_, err = f.Write(raw)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(path)
		return Reference{}, err
	}
	uriPath := filepath.ToSlash(path)
	if !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	ref := Reference{(&url.URL{Scheme: "file", Path: uriPath}).String(), fmt.Sprintf("sha256:%x", sha256.Sum256(raw))}
	payload, err := json.Marshal(map[string]any{"checkpoint_id": id, "checkpoint": ref, "trigger": trigger})
	if err != nil {
		return Reference{}, err
	}
	err = m.store.Write(ctx, func(tx *eventlog.Transaction) error {
		s, err := load(ctx, tx, cp.ThreadID)
		if err != nil {
			return err
		}
		// The file was written outside SQLite. Revalidate its owner before publishing the reference.
		if s.Phase != state.Phase || s.Reason != state.Reason || s.Facts.Session != state.Facts.Session || s.Facts.CheckpointID != state.Facts.CheckpointID {
			return ErrTransition
		}
		_, err = tx.Append(ctx, eventlog.NewEvent{Stream: "thread:" + s.ID, ThreadID: s.ID, Type: "rollover.checkpointed", TypeVersion: 1, Actor: eventlog.Actor{Kind: "daemon", ID: "local"}, Payload: payload})
		return err
	})
	if err != nil {
		return Reference{}, err
	}
	if state.Phase == "rollover" {
		return ref, m.Apply(ctx, cp.ThreadID, Command{Transition: "T14"})
	}
	return ref, nil
}
