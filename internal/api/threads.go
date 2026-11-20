package api

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"
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
	// WaitingFor is a derived, read-only note on a queued thread: what the operator must
	// do before the daemon can dispatch it (see Config.WaitingFor). It is not machine state.
	WaitingFor string `json:"waiting_for,omitempty"`
	// Checks groups the recorded check results of the evaluated revision by their five statuses
	// (required checks with no result count as not_run). OpenRequirements lists what is still
	// outstanding: required checks not passed and open decisions. Both are read-only.
	Checks           map[string][]string `json:"checks,omitempty"`
	OpenRequirements []openRequirement   `json:"open_requirements,omitempty"`
	LastSeq          int64               `json:"last_seq"`
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
		// The catalogue creation fact carries depth and project from the start, as
		// children.CreateRoot writes it (the daemon never backfills it).
		created, err := json.Marshal(map[string]any{
			"thread_id": id, "project_id": body["project_id"], "role": "coordinator", "review_plan_first": review, "depth": 0,
		})
		if err != nil {
			return nil, err
		}
		if _, err := tx.Append(ctx, eventlog.NewEvent{ThreadID: id, Stream: "thread:" + id, Type: "thread.created", TypeVersion: 1,
			Actor: eventlog.Actor{Kind: "daemon", ID: "local"}, Payload: created}); err != nil {
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
	if v.Role == "" {
		v.Role = "coordinator"
	}
	v.State, v.PauseReason = state.Phase, state.Reason
	v.BriefVersion, v.PlanDigest, v.Revision = state.Facts.BriefVersion, state.Facts.PlanDigest, state.Facts.Revision
	v.Audit = state.Audit
	if s.config.WaitingFor != nil && state.Phase == "queued" {
		v.WaitingFor = s.config.WaitingFor(ctx, v.ID)
	}
	v.Checks, v.OpenRequirements = checkSummary(state)
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

// managedThreadView reads project, role and depth from the durable child creation fact.
func (s *Server) managedThreadView(id string) (threadView, error) {
	var cursor int64
	for {
		events, err := s.config.Store.ReadStream("thread:"+id, cursor, 32)
		if err != nil {
			return threadView{}, err
		}
		for _, e := range events {
			cursor = e.Seq
			if e.Type != "thread.created" || e.TypeVersion != 1 {
				continue
			}
			var created struct {
				Project string `json:"project_id"`
				Parent  string `json:"parent_thread_id"`
				Role    string `json:"role"`
				Depth   int    `json:"depth"`
			}
			if err := json.Unmarshal(e.Payload, &created); err != nil {
				return threadView{}, err
			}
			if created.Parent != "" {
				return threadView{ID: id, ProjectID: created.Project, Role: created.Role, Depth: created.Depth}, nil
			}
		}
		if len(events) < 32 {
			return threadView{}, thread.ErrMissing
		}
	}
}

// getThread returns one coordinator or managed child with its current lifecycle status.
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
	var view threadView
	if len(views) > 0 {
		view = views[0]
	} else {
		view, err = s.managedThreadView(id)
	}
	if err == nil {
		err = s.hydrate(ctx, &view)
	}
	switch {
	case errors.Is(err, thread.ErrMissing), errors.Is(err, sql.ErrNoRows):
		writeProblem(w, newProblem(404, "not_found", ""))
	case err != nil:
		writeProblem(w, newProblem(500, "internal", ""))
	default:
		writeJSON(w, view)
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

// openRequirement is one outstanding item on a thread: a required check or an open decision.
type openRequirement struct {
	Kind   string `json:"kind"` // check or decision
	ID     string `json:"id"`
	Status string `json:"status,omitempty"`
}

var checkStatuses = []string{"passed", "failed", "error", "partial", "not_run"}

// checkSummary projects machine state into the optional checks and open_requirements fields.
func checkSummary(s thread.State) (map[string][]string, []openRequirement) {
	required := s.Facts.RequiredChecks
	if len(s.Checks) == 0 && len(required) == 0 && len(s.Decisions) == 0 {
		return nil, nil
	}
	groups := map[string][]string{}
	for _, status := range checkStatuses {
		groups[status] = []string{}
	}
	names := slices.Sorted(maps.Keys(s.Checks))
	for _, name := range names {
		groups[s.Checks[name]] = append(groups[s.Checks[name]], name)
	}
	var open []openRequirement
	for _, name := range required {
		status := s.Checks[name]
		if status == "" {
			groups["not_run"] = append(groups["not_run"], name)
		}
		if status != "passed" {
			open = append(open, openRequirement{Kind: "check", ID: name, Status: cmp.Or(status, "not_run")})
		}
	}
	for _, id := range slices.Sorted(maps.Keys(s.Decisions)) {
		if s.Decisions[id] {
			open = append(open, openRequirement{Kind: "decision", ID: id, Status: "open"})
		}
	}
	return groups, open
}
