package thread

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

	"synch/internal/eventlog"
)

// --- Catch-up: project persisted records off the event append path ---

// catchUp folds new event records in order and refreshes D-15 resolutions.
func catchUp(ctx context.Context, tx *eventlog.Transaction, s *State) error {
	if err := restoreDecisionHistory(ctx, tx, s); err != nil {
		return fmt.Errorf("read decision history: %w", err)
	}
	if err := catchUpEvents(ctx, tx, s); err != nil {
		return fmt.Errorf("read thread events: %w", err)
	}
	if err := resolveDecisions(ctx, tx, s); err != nil {
		return fmt.Errorf("read accepted decide receipts: %w", err)
	}
	if err := loadHarnessSelection(ctx, tx, s); err != nil {
		return fmt.Errorf("read accepted select_harness receipt: %w", err)
	}
	return nil
}

// restoreDecisionHistory replaces pre-D-15 snapshot decisions with their catalogue provenance.
func restoreDecisionHistory(ctx context.Context, tx *eventlog.Transaction, s *State) error {
	if s.CatalogueDecisions {
		return nil
	}
	s.OutOfScope = ""
	s.Decisions = make(map[string]bool)
	rows, err := tx.QueryContext(ctx, `SELECT envelope FROM events WHERE stream=? AND global_seq<=?
		AND json_extract(envelope,'$.type') IN ('plan.revised','thread.state_changed') ORDER BY global_seq`, "thread:"+s.ID, s.Projected)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var e eventlog.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		var p payload
		if json.Unmarshal(e.Payload, &p) != nil {
			continue
		}
		switch {
		case e.Type == "plan.revised" && e.TypeVersion == 2:
			projectPlanScope(s, e.ID, p)
		case e.Type == "thread.state_changed" && e.TypeVersion == 4:
			projectDecisionTransition(s, e.ID, p)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	s.CatalogueDecisions = true
	return nil
}

// catchUpEvents advances the cursor even when an event has no usable facts.
func catchUpEvents(ctx context.Context, tx *eventlog.Transaction, s *State) error {
	rows, err := tx.QueryContext(ctx, "SELECT envelope FROM events WHERE stream IN (?, 'system') AND global_seq>? ORDER BY global_seq", "thread:"+s.ID, s.Projected)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			return err
		}
		var e eventlog.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		project(s, e)
		s.Projected = e.GlobalSeq
	}
	return rows.Err()
}

// resolveDecisions closes only decisions backed by a stored accepted receipt for this thread.
func resolveDecisions(ctx context.Context, tx *eventlog.Transaction, s *State) error {
	if !openDecision(s) {
		return nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT d.decision_id FROM decision_receipts d JOIN receipts r ON r.receipt_id=d.receipt_id
 WHERE d.thread_id=? AND r.status='accepted'`, s.ID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return err
		}
		if s.Decisions[id] {
			s.Decisions[id] = false
		}
	}
	return rows.Err()
}

// openDecisionFor records the ID of the event that opened a D-15 decision.
func openDecisionFor(s *State, id string) {
	if id == "" {
		return
	}
	if s.Decisions == nil {
		s.Decisions = make(map[string]bool)
	}
	if _, seen := s.Decisions[id]; !seen {
		s.Decisions[id] = true
	}
}

var projectedVersions = map[string]int{
	"resource.pressure": 1, "runner.connection": 1, "mode.changed": 2,
	"external_request.persisted": 3, "external_request.resolved": 4, "process.exited": 1, "rollover.checkpointed": 1,
	"plan.revised": 2, "thread.state_changed": 4, "check.result": 1, "audit.status": 1, "acceptance.recorded": 1,
}

type payload map[string]json.RawMessage

// field reads an exact catalogue key and treats null or a type mismatch as absent.
func field[T any](p payload, key string) (T, bool) {
	var value T
	raw := p[key]
	if len(raw) == 0 || string(raw) == "null" || json.Unmarshal(raw, &value) != nil {
		var zero T
		return zero, false
	}
	return value, true
}

// stringField returns an empty value when the catalogue string is missing or mistyped.
func stringField(p payload, key string) string { v, _ := field[string](p, key); return v }

// --- Field mapping: consume only the facts required by the state machine ---

// project folds one catalogue event, ignoring unknown fields and malformed facts.
func project(s *State, e eventlog.Event) {
	if version, ok := projectedVersions[e.Type]; !ok || version != e.TypeVersion {
		return
	}
	var p payload
	if json.Unmarshal(e.Payload, &p) != nil {
		return
	}
	switch e.Type {
	case "resource.pressure":
		if stringField(p, "resource") != "disk" {
			return
		}
		switch stringField(p, "level") {
		case "ok":
			s.Facts.DiskPressure = false
		case "pressure":
			s.Facts.DiskPressure = true
		}
	case "runner.connection":
		projectRunner(s, p)
	case "mode.changed":
		to, phase := stringField(p, "to"), stringField(p, "phase")
		stopped, _ := field[bool](p, "host_processes_stopped")
		if slices.Contains([]string{"sandbox", "provider", "full_control"}, to) && slices.Contains([]string{"requested", "settling", "active"}, phase) {
			s.Facts.SandboxActive = to == "sandbox" && phase == "active" && stopped
		}
	case "external_request.persisted":
		if id := stringField(p, "external_request_id"); id != "" {
			if _, known := s.Requests[id]; !known {
				s.Requests[id] = ""
			}
		}
	case "external_request.resolved":
		id, res := stringField(p, "external_request_id"), stringField(p, "resolution")
		if id != "" && slices.Contains([]string{"succeeded", "failed", "not_executed", "unknown"}, res) {
			s.Requests[id] = res
		}
	case "process.exited":
		id, work := stringField(p, "assignment_id"), stringField(p, "work")
		if id == "" || !slices.Contains([]string{"complete", "resumable", "lost"}, work) {
			return
		}
		s.Work[id] = work
		if work == "lost" && !slices.Contains(s.FailedAssignments, id) {
			s.FailedAssignments = append(s.FailedAssignments, id)
		}
	case "rollover.checkpointed":
		ref, ok := field[payload](p, "checkpoint")
		id := stringField(p, "checkpoint_id")
		uri, digest := stringField(ref, "uri"), stringField(ref, "digest")
		if ok && id != "" && uri != "" && digest != "" {
			s.Facts.CheckpointID = id
			s.Checkpoint = Reference{URI: uri, Digest: digest}
		}
	case "plan.revised":
		projectPlan(s, e.ID, p)
	case "thread.state_changed":
		projectDecisionTransition(s, e.ID, p)
	case "check.result", "audit.status", "acceptance.recorded":
		projectOutcome(s, e.Type, p)
	}
}

// projectRunner distinguishes reconnection from completion of journal replay.
func projectRunner(s *State, p payload) {
	if s.Facts.RunnerID == "" || stringField(p, "runner") != s.Facts.RunnerID {
		return
	}
	switch stringField(p, "state") {
	case "disconnected":
		s.Facts.Connected = false
		s.Facts.JournalReplayed = false
	case "connected":
		s.Facts.Connected = true
		from, fromOK := field[int64](p, "replayed_from_seq")
		to, toOK := field[int64](p, "replayed_to_seq")
		s.Facts.JournalReplayed = fromOK && toOK && from >= 0 && to >= from
	}
}

// projectPlan records the revised plan and opens a D-15 decision when it extends scope.
func projectPlan(s *State, id string, p payload) {
	if revision, ok := field[int](p, "plan_revision"); ok && revision > 0 {
		s.Facts.PlanRevision = revision
	}
	if digest := stringField(p, "plan_digest"); digest != "" {
		s.Facts.PlanDigest = digest
	}
	if version, ok := field[int](p, "brief_version"); ok && version > 0 {
		s.Facts.BriefVersion = version
	}
	projectPlanScope(s, id, p)
}

// projectPlanScope derives the latest scope obligation from the plan's event ID.
func projectPlanScope(s *State, id string, p payload) {
	if within, ok := field[bool](p, "within_scope"); ok {
		s.OutOfScope = ""
		if !within {
			s.OutOfScope = id
			openDecisionFor(s, id)
		}
	}
}

// projectDecisionTransition opens the decision required by T10, T17, or T34.
func projectDecisionTransition(s *State, id string, p payload) {
	row := stringField(p, "transition")
	switch row {
	case "T10", "T17", "T34":
		r := rules[row]
		if stringField(p, "from") == r.from && stringField(p, "to") == r.to {
			openDecisionFor(s, id)
		}
	}
}

// projectOutcome updates only an outcome of the evaluated revision and its own kind.
func projectOutcome(s *State, typ string, p payload) {
	revision := stringField(p, "revision")
	if revision == "" || revision != s.Facts.Revision {
		return
	}
	status := stringField(p, "status")
	switch typ {
	case "check.result":
		if name := stringField(p, "check"); name != "" && slices.Contains([]string{"passed", "failed", "error", "partial", "not_run"}, status) {
			s.Checks[name] = status
		}
	case "audit.status":
		if slices.Contains([]string{"off", "pending", "running", "pass", "corrections_requested", "error"}, status) {
			s.Audit = status
		}
	case "acceptance.recorded":
		switch stringField(p, "decision") {
		case "accepted":
			s.AcceptedRevision = revision
		case "rejected":
			s.AcceptedRevision = ""
		}
	}
}
