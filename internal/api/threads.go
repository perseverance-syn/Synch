package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"synch/internal/eventlog"
	"synch/internal/queue"
	"synch/internal/thread"
)

const threadIndex = `CREATE TABLE IF NOT EXISTS api_threads(
 id TEXT PRIMARY KEY, project_id TEXT NOT NULL, harness TEXT NOT NULL, model TEXT NOT NULL
)`

// threadView is the public projection combining submission metadata and live machine facts.
type threadView struct {
	ID           string `json:"id"`
	ProjectID    string `json:"project_id"`
	Role         string `json:"role"`
	State        string `json:"state"`
	Depth        int    `json:"depth"`
	PauseReason  string `json:"pause_reason,omitempty"`
	BriefVersion int    `json:"brief_version,omitempty"`
	PlanDigest   string `json:"plan_digest,omitempty"`
	Revision     string `json:"revision,omitempty"`
	Harness      string `json:"harness,omitempty"`
	Model        string `json:"model,omitempty"`
	Audit        string `json:"audit_status,omitempty"`
	LastSeq      int64  `json:"last_seq"`
}

// createThread persists the coordinator and the API's submission metadata in the receipt writer.
func (s *Server) createThread(body map[string]any) (queue.Decision, error) {
	if !createThreadBody.valid(body) {
		return queue.Decision{}, invalid("invalid thread submission")
	}
	if s.pressured() {
		return queue.Decision{}, &queue.Rejection{Code: "disk_pressure"}
	}
	id := eventlog.NewID()
	review, _ := body["review_plan_first"].(bool)
	audit, _ := body["audit"].(string)
	if audit == "" {
		audit = "default"
	}
	pin, _ := body["pin"].(map[string]any)
	harness, _ := pin["harness"].(string)
	model, _ := pin["model"].(string)
	return queue.Decision{ThreadID: id, HTTPStatus: 201, Commit: func(ctx context.Context, tx *eventlog.Transaction) (any, error) {
		if _, err := tx.ExecContext(ctx, threadIndex); err != nil {
			return nil, err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO api_threads(id,project_id,harness,model) VALUES(?,?,?,?)", id, body["project_id"], harness, model); err != nil {
			return nil, err
		}
		facts := thread.Facts{BriefVersion: 1, ReviewPlanFirst: review, AuditSetting: audit}
		if err := s.config.Threads.CreateTx(ctx, tx, id, facts); err != nil {
			return nil, err
		}
		result := map[string]any{"thread_id": id, "submission": body}
		return result, nil
	}}, nil
}

// readThreadIndex reads only metadata owned by the HTTP submission surface.
func (s *Server) readThreadIndex(ctx context.Context, id, project string) ([]threadView, error) {
	views := []threadView{}
	err := s.config.Store.Write(ctx, func(tx *eventlog.Transaction) error {
		if _, err := tx.ExecContext(ctx, threadIndex); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, "SELECT id,project_id,harness,model FROM api_threads WHERE (?='' OR id=?) AND (?='' OR project_id=?) ORDER BY id", id, id, project, project)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var view threadView
			if err := rows.Scan(&view.ID, &view.ProjectID, &view.Harness, &view.Model); err != nil {
				return err
			}
			views = append(views, view)
		}
		return rows.Err()
	})
	return views, err
}

// hydrate adds fields managed by the thread machine and the persisted stream head.
func (s *Server) hydrate(ctx context.Context, v *threadView) error {
	state, err := s.config.Threads.Status(ctx, v.ID)
	if err != nil {
		return err
	}
	v.Role, v.State, v.PauseReason = "coordinator", state.Phase, state.Reason
	v.BriefVersion, v.PlanDigest, v.Revision = state.Facts.BriefVersion, state.Facts.PlanDigest, state.Facts.Revision
	v.Audit = state.Audit
	if state.SelectedHarness != nil {
		v.Harness, v.Model = state.SelectedHarness.Harness, state.SelectedHarness.Model
	}
	v.LastSeq, err = s.streamHead("thread:" + v.ID)
	return err
}

// listThreads returns coordinator submissions filtered by project and phase.
func (s *Server) listThreads(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	phase := query.Get("state")
	ctx := r.Context()
	views, err := s.readThreadIndex(ctx, "", query.Get("project_id"))
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	filtered := views[:0]
	for _, view := range views {
		if err := s.hydrate(ctx, &view); err != nil {
			writeProblem(w, newProblem(500, "internal", ""))
			return
		}
		if phase == "" || view.State == phase {
			filtered = append(filtered, view)
		}
	}
	writeJSON(w, map[string]any{"threads": filtered})
}

// getThread returns one indexed thread with its current lifecycle status.
func (s *Server) getThread(w http.ResponseWriter, r *http.Request, id string) {
	if !ulidPattern.MatchString(id) {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	ctx := r.Context()
	views, err := s.readThreadIndex(ctx, id, "")
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	if len(views) == 0 {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	err = s.hydrate(ctx, &views[0])
	switch {
	case errors.Is(err, thread.ErrMissing), errors.Is(err, sql.ErrNoRows):
		writeProblem(w, newProblem(404, "not_found", ""))
	case err != nil:
		writeProblem(w, newProblem(500, "internal", ""))
	default:
		writeJSON(w, views[0])
	}
}

// stringFieldFromRecord reads a string from a queue-generated receipt without changing its bytes.
func stringFieldFromRecord(record []byte, key string) string {
	var fields map[string]json.RawMessage
	var value string
	if json.Unmarshal(record, &fields) == nil {
		_ = json.Unmarshal(fields[key], &value)
	}
	return value
}
