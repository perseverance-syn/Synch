package thread

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"synch/internal/eventlog"
)

// Dispatch supplies the verified handoff to the new session only after T05 commits.
func (m *Machine) Dispatch(ctx context.Context, id, assignment, session string, start func(context.Context, *Checkpoint) error) error {
	var cp *Checkpoint
	started := false
	err := m.store.Write(ctx, func(tx *eventlog.Transaction) error {
		s, err := load(ctx, tx, id)
		if err != nil {
			return err
		}
		if s.Pending != nil {
			if err := m.refresh(ctx, tx, &s); err != nil {
				return err
			}
		}
		if err = m.apply(ctx, tx, &s, Command{Transition: "T05", Assignment: assignment, Session: session}); err != nil {
			return err
		}
		started = s.Phase == "running"
		if !started {
			return nil
		}
		if start == nil {
			return errors.New("missing session starter")
		}
		if s.Checkpoint.URI != "" {
			cp, err = readCheckpoint(s.Checkpoint, id)
			return err
		}
		return nil
	})
	if err != nil || !started {
		return err
	}
	return start(ctx, cp)
}

// readCheckpoint verifies the referenced file's digest and thread identity before handing it to a session.
func readCheckpoint(ref Reference, id string) (*Checkpoint, error) {
	u, err := url.Parse(ref.URI)
	if err != nil || u.Scheme != "file" || u.Host != "" {
		return nil, errors.New("invalid checkpoint file URI")
	}
	path := u.Path
	// A Windows file URI encodes the drive path with a leading slash.
	if runtime.GOOS == "windows" {
		path = strings.TrimPrefix(path, "/")
	}
	raw, err := os.ReadFile(filepath.FromSlash(path))
	if err != nil {
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	if fmt.Sprintf("sha256:%x", sha256.Sum256(raw)) != ref.Digest {
		return nil, errors.New("checkpoint digest mismatch")
	}
	var cp Checkpoint
	if err := json.Unmarshal(raw, &cp); err != nil {
		return nil, err
	}
	if cp.Version != 1 || cp.ThreadID != id {
		return nil, errors.New("checkpoint identity mismatch")
	}
	return &cp, nil
}

// HarnessFailure commits T15 and attempts the handoff checkpoint required by ?4.
func (m *Machine) HarnessFailure(ctx context.Context, dir string, cp Checkpoint) error {
	if err := m.Apply(ctx, cp.ThreadID, Command{Transition: "T15"}); err != nil {
		return err
	}
	_, err := m.FinishCheckpoint(ctx, dir, cp, "harness_failure")
	return err
}

// ClassifyWork describes the engine assignment independently of provider outcomes.
func ClassifyWork(finished, checkpointCoversInput, nativeResumeQualified bool, sessionRef string) string {
	if finished {
		return "complete"
	}
	if checkpointCoversInput || nativeResumeQualified && sessionRef != "" {
		return "resumable"
	}
	return "lost"
}

// Control admits operator actions in every phase. The caller records command-specific
// evidence (for example an external resolution) in this same transaction.
func (m *Machine) Control(ctx context.Context, tx *eventlog.Transaction, id, action string, record func() error) error {
	switch action {
	case "pause", "cancel", "resume", "revoke", "export", "decide", "resolve_external_request":
	default:
		return ErrTransition
	}
	if record != nil {
		if err := record(); err != nil {
			return err
		}
	}
	s, err := load(ctx, tx, id)
	if err != nil {
		return err
	}
	var row string
	switch {
	case action == "pause" && s.Phase == "running":
		row = "T20"
	case action == "cancel" && !terminal(s.Phase):
		row = "T30"
	case action == "resume" && s.Phase == "paused" && s.Reason != recovery:
		row = "T21"
	case action == "decide" && s.Phase == "awaiting_decision":
		row = "T11"
	default:
		return nil
	}
	err = m.apply(ctx, tx, &s, Command{Transition: row})
	if errors.Is(err, ErrTransition) && action == "resume" {
		return nil
	}
	return err
}
