package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"synch/internal/children"
	"synch/internal/eventlog"
)

// eventRecorder keeps the managed-child records (decision 06) in the shared
// event log as child.assignment_recorded, child.result_recorded and
// audit.setting_recorded on the child's own stream. Records are immutable:
// the first one wins and an identical retry is a no-op. Lookups only read
// committed events, so they are safe inside the event writer.
type eventRecorder struct {
	store *eventlog.Store

	mu          sync.Mutex
	byID        map[string]string // assignment ID to child thread ID
	assignments map[string]children.Assignment
	results     map[string]children.Result
}

// newEventRecorder returns a recorder over the daemon's event store.
func newEventRecorder(store *eventlog.Store) *eventRecorder {
	return &eventRecorder{
		store: store, byID: make(map[string]string),
		assignments: make(map[string]children.Assignment), results: make(map[string]children.Result),
	}
}

// RecordAssignment validates and appends the assignment on the child's stream.
func (r *eventRecorder) RecordAssignment(ctx context.Context, a children.Assignment) error {
	if err := a.Validate(); err != nil {
		return err
	}
	raw, err := a.Payload()
	if err != nil {
		return err
	}
	var prior children.Assignment
	found, err := r.find(a.ChildThreadID, "child.assignment_recorded", &prior)
	if err != nil {
		return err
	}
	if found {
		old, err := prior.Payload()
		if err != nil || !bytes.Equal(old, raw) {
			return errors.New("child already has a different assignment")
		}
		return nil
	}
	if err := r.append(ctx, a.ParentThreadID, a.ChildThreadID, "child.assignment_recorded", raw); err != nil {
		return err
	}
	r.mu.Lock()
	r.byID[a.AssignmentID] = a.ChildThreadID
	r.mu.Unlock()
	return nil
}

// RecordResult validates and appends the child's result unless one exists.
func (r *eventRecorder) RecordResult(ctx context.Context, res children.Result) error {
	if err := res.Validate(); err != nil {
		return err
	}
	raw, err := res.Payload()
	if err != nil {
		return err
	}
	var prior children.Result
	found, err := r.find(res.ChildThreadID, "child.result_recorded", &prior)
	if err != nil || found {
		return err
	}
	a, err := r.Assignment(ctx, res.ChildThreadID)
	if err != nil {
		return err
	}
	return r.append(ctx, a.ParentThreadID, res.ChildThreadID, "child.result_recorded", raw)
}

// RecordAuditSetting appends the selected audit setting to the thread's stream.
func (r *eventRecorder) RecordAuditSetting(ctx context.Context, a children.AuditSetting) error {
	if err := a.Validate(); err != nil {
		return err
	}
	raw, err := a.Payload()
	if err != nil {
		return err
	}
	return r.append(ctx, a.ThreadID, a.ThreadID, "audit.setting_recorded", raw)
}

// Assignment reads the assignment recorded for a child thread.
func (r *eventRecorder) Assignment(_ context.Context, child string) (children.Assignment, error) {
	r.mu.Lock()
	a, ok := r.assignments[child]
	r.mu.Unlock()
	if ok {
		return a, nil
	}
	found, err := r.find(child, "child.assignment_recorded", &a)
	if err != nil {
		return a, err
	}
	if !found {
		return a, fmt.Errorf("assignment for %s: %w", child, children.ErrRecordNotFound)
	}
	r.mu.Lock()
	r.assignments[child], r.byID[a.AssignmentID] = a, child
	r.mu.Unlock()
	return a, nil
}

// AssignmentByID finds an assignment by its own ID, scanning the log on a cache miss.
// ponytail: a linear scan of the global log on a miss; index by assignment ID if dependencies get common.
func (r *eventRecorder) AssignmentByID(ctx context.Context, id string) (children.Assignment, error) {
	r.mu.Lock()
	child, ok := r.byID[id]
	r.mu.Unlock()
	if ok {
		return r.Assignment(ctx, child)
	}
	var cursor int64
	for {
		events, err := r.store.ReadGlobal(cursor, 256)
		if err != nil {
			return children.Assignment{}, err
		}
		for _, e := range events {
			cursor = e.GlobalSeq
			var a children.Assignment
			if e.Type == "child.assignment_recorded" && json.Unmarshal(e.Payload, &a) == nil && a.AssignmentID == id {
				r.mu.Lock()
				r.byID[id] = a.ChildThreadID
				r.mu.Unlock()
				return a, nil
			}
		}
		if len(events) < 256 {
			return children.Assignment{}, fmt.Errorf("assignment %s: %w", id, children.ErrRecordNotFound)
		}
	}
}

// Result reads the result recorded for a child thread.
func (r *eventRecorder) Result(_ context.Context, child string) (children.Result, error) {
	r.mu.Lock()
	res, ok := r.results[child]
	r.mu.Unlock()
	if ok {
		return res, nil
	}
	found, err := r.find(child, "child.result_recorded", &res)
	if err != nil {
		return res, err
	}
	if !found {
		return res, fmt.Errorf("result for %s: %w", child, children.ErrRecordNotFound)
	}
	r.mu.Lock()
	r.results[child] = res
	r.mu.Unlock()
	return res, nil
}

// find decodes the first event of a type on a thread's stream.
func (r *eventRecorder) find(thread, typ string, into any) (bool, error) {
	const page = 256
	var cursor int64
	for {
		events, err := r.store.ReadStream("thread:"+thread, cursor, page)
		if err != nil {
			return false, err
		}
		for _, e := range events {
			cursor = e.Seq
			if e.Type == typ && e.TypeVersion == 1 {
				return true, json.Unmarshal(e.Payload, into)
			}
		}
		if len(events) < page {
			return false, nil
		}
	}
}

// append writes one coordinator-attributed record on a thread's stream.
func (r *eventRecorder) append(ctx context.Context, coordinator, thread, typ string, payload []byte) error {
	return r.store.Write(ctx, func(tx *eventlog.Transaction) error {
		_, err := tx.Append(ctx, eventlog.NewEvent{
			ThreadID: thread, Stream: "thread:" + thread, Type: typ, TypeVersion: 1,
			Actor: eventlog.Actor{Kind: "coordinator", ID: coordinator}, Payload: payload,
		})
		return err
	})
}
