package children

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"sync"
)

// ErrRecordNotFound indicates that an assignment or result has not been recorded.
var ErrRecordNotFound = errors.New("child record not found")

// ulidPattern is the catalogue's ULID shape (26 Crockford base32 characters).
var ulidPattern = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)

// Assignment describes one managed plan step and its required evidence.
type Assignment struct {
	ParentThreadID      string   `json:"parent_thread_id"`
	ChildThreadID       string   `json:"child_thread_id"`
	AssignmentID        string   `json:"assignment_id"`
	Purpose             string   `json:"purpose"`
	Role                string   `json:"role"`
	RoleReason          string   `json:"role_reason"`
	InputRevision       string   `json:"input_revision,omitempty"`
	Target              string   `json:"target,omitempty"`
	AllowedCapabilities []string `json:"allowed_capabilities"`
	ExpectedEvidence    []string `json:"expected_evidence"`
	Dependencies        []string `json:"dependencies,omitempty"` // Assignment IDs of already-created siblings.
}

// Validate checks assignment identity, routing rationale, input, and list entries.
func (a Assignment) Validate() error {
	for _, id := range []string{a.ParentThreadID, a.ChildThreadID, a.AssignmentID} {
		if !ulidPattern.MatchString(id) {
			return fmt.Errorf("assignment ID %q is not a ULID", id)
		}
	}
	if a.ParentThreadID == a.ChildThreadID {
		return errors.New("assignment requires distinct parent and child IDs")
	}
	switch a.Role {
	case "builder", "ops", "security", "auditor":
	default:
		return fmt.Errorf("invalid child role %q", a.Role)
	}
	if blank(a.Purpose) || blank(a.RoleReason) || blank(a.InputRevision) && blank(a.Target) || len(a.ExpectedEvidence) == 0 {
		return errors.New("assignment requires purpose, role reason, input revision or target, and expected evidence")
	}
	if !validEntries(a.AllowedCapabilities, a.ExpectedEvidence, a.Dependencies) {
		return errors.New("assignment lists must contain nonblank entries")
	}
	if !unique(a.AllowedCapabilities) || !unique(a.ExpectedEvidence) {
		return errors.New("allowed capabilities and expected evidence must be unique")
	}
	seen := make(map[string]bool)
	for _, dep := range a.Dependencies {
		if !ulidPattern.MatchString(dep) || dep == a.AssignmentID || seen[dep] {
			return fmt.Errorf("invalid or duplicate sibling dependency %q", dep)
		}
		seen[dep] = true
	}
	return nil
}

// Check is one named check and, outside the passed group, why it is there.
type Check struct {
	Check  string `json:"check"`
	Reason string `json:"reason,omitempty"`
}

// checkGroups lists the catalogue's five result groups.
var checkGroups = []string{"passed", "failed", "error", "partial", "not_run"}

// Result collects the final outcome and the evidence delivered by a child.
type Result struct {
	ChildThreadID    string             `json:"child_thread_id"`
	AssignmentID     string             `json:"assignment_id"`
	Outcome          string             `json:"outcome"`
	Revision         string             `json:"revision"`
	Checks           map[string][]Check `json:"checks"` // Group to checks; all five groups present.
	Limitations      []string           `json:"limitations"`
	OpenRequirements []string           `json:"open_requirements"`
}

// Validate requires IDs, an allowed outcome, a revision and catalogue-shaped checks.
func (r Result) Validate() error {
	if !ulidPattern.MatchString(r.ChildThreadID) || !ulidPattern.MatchString(r.AssignmentID) {
		return errors.New("result requires ULID child and assignment IDs")
	}
	switch r.Outcome {
	case "completed", "failed", "cancelled":
	default:
		return fmt.Errorf("invalid result outcome %q", r.Outcome)
	}
	if blank(r.Revision) {
		return errors.New("result requires a revision")
	}
	if !validEntries(r.Limitations, r.OpenRequirements) {
		return errors.New("result lists must contain nonblank entries")
	}
	if !unique(r.Limitations) || !unique(r.OpenRequirements) {
		return errors.New("result limitations and open requirements must be unique")
	}
	for status := range r.Checks {
		if !slices.Contains(checkGroups, status) {
			return fmt.Errorf("invalid check group %q", status)
		}
	}
	seen := make(map[string]bool)
	for _, group := range checkGroups {
		entries, ok := r.Checks[group]
		if !ok {
			return fmt.Errorf("result checks missing group %q", group)
		}
		for _, e := range entries {
			if blank(e.Check) || seen[e.Check] {
				return fmt.Errorf("invalid or duplicate result check %q", e.Check)
			}
			if group != "passed" && blank(e.Reason) {
				return fmt.Errorf("check %q in group %q requires a reason", e.Check, group)
			}
			seen[e.Check] = true
		}
	}
	return nil
}

// AuditSetting records the selected audit setting and the reason for it.
type AuditSetting struct {
	ThreadID string `json:"thread_id"`
	Setting  string `json:"setting"`
	Reason   string `json:"reason"`
}

// Validate checks the audit selection and its rationale.
func (a AuditSetting) Validate() error {
	if !ulidPattern.MatchString(a.ThreadID) || blank(a.Reason) {
		return errors.New("audit setting requires a ULID thread ID and a reason")
	}
	switch a.Setting {
	case "off", "level1", "level2", "level3":
		return nil
	}
	return fmt.Errorf("invalid audit setting %q", a.Setting)
}

// Payload encodes the catalogue child.assignment_recorded payload; required
// lists are emitted as empty arrays, never null.
func (a Assignment) Payload() ([]byte, error) {
	a = a.clone()
	if a.AllowedCapabilities == nil {
		a.AllowedCapabilities = []string{}
	}
	return json.Marshal(a)
}

// Payload encodes the catalogue child.result_recorded payload; all five check
// groups and both lists are emitted as arrays, never null.
func (r Result) Payload() ([]byte, error) {
	r = r.clone()
	if r.Checks == nil {
		r.Checks = map[string][]Check{}
	}
	for _, g := range checkGroups {
		if r.Checks[g] == nil {
			r.Checks[g] = []Check{}
		}
	}
	if r.Limitations == nil {
		r.Limitations = []string{}
	}
	if r.OpenRequirements == nil {
		r.OpenRequirements = []string{}
	}
	return json.Marshal(r)
}

// Payload encodes the catalogue audit.setting_recorded payload.
func (a AuditSetting) Payload() ([]byte, error) { return json.Marshal(a) }

// blank reports whether text is empty after trimming whitespace.
func blank(text string) bool { return strings.TrimSpace(text) == "" }

// unique reports whether a list has no repeated entries.
func unique(list []string) bool {
	seen := make(map[string]bool, len(list))
	for _, s := range list {
		if seen[s] {
			return false
		}
		seen[s] = true
	}
	return true
}

// validEntries reports whether all list entries contain nonblank text.
func validEntries(lists ...[]string) bool {
	for _, list := range lists {
		if slices.ContainsFunc(list, blank) {
			return false
		}
	}
	return true
}

// --- Recording: keep D-27 records behind an injectable storage boundary ---

// Recorder stores validated records and exposes assignment and result lookups
// for the dependency gate. Lookups can run inside the event writer and must
// never acquire that writer. Identical writes must be safe to retry.
type Recorder interface {
	RecordAssignment(context.Context, Assignment) error
	RecordResult(context.Context, Result) error
	RecordAuditSetting(context.Context, AuditSetting) error
	Assignment(context.Context, string) (Assignment, error)
	AssignmentByID(context.Context, string) (Assignment, error)
	Result(context.Context, string) (Result, error)
}

// MemoryRecorder is a concurrency-safe Recorder for tests. Its zero value is ready.
type MemoryRecorder struct {
	mu          sync.RWMutex
	assignments map[string]Assignment
	results     map[string]Result
	audits      map[string]AuditSetting
}

// RecordAssignment validates and stores an immutable copy keyed by child thread.
func (m *MemoryRecorder) RecordAssignment(_ context.Context, a Assignment) error {
	if err := a.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return storeRecord(&m.assignments, a.ChildThreadID, a.clone())
}

// Assignment returns a copy of the stored child assignment.
func (m *MemoryRecorder) Assignment(_ context.Context, childID string) (Assignment, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.assignments[childID]
	if !ok {
		return Assignment{}, ErrRecordNotFound
	}
	return a.clone(), nil
}

// RecordResult validates and stores an immutable copy of a child's result.
func (m *MemoryRecorder) RecordResult(_ context.Context, r Result) error {
	if err := r.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return storeRecord(&m.results, r.ChildThreadID, r.clone())
}

// Result returns a copy of the stored child result.
func (m *MemoryRecorder) Result(_ context.Context, childID string) (Result, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.results[childID]
	if !ok {
		return Result{}, ErrRecordNotFound
	}
	return r.clone(), nil
}

// RecordAuditSetting validates and stores the latest setting for a thread.
func (m *MemoryRecorder) RecordAuditSetting(_ context.Context, a AuditSetting) error {
	if err := a.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.audits == nil {
		m.audits = make(map[string]AuditSetting)
	}
	m.audits[a.ThreadID] = a
	return nil
}

// AuditSetting returns the current stored setting and rationale for tests.
func (m *MemoryRecorder) AuditSetting(threadID string) (AuditSetting, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	a, ok := m.audits[threadID]
	if !ok {
		return AuditSetting{}, ErrRecordNotFound
	}
	return a, nil
}

// storeRecord allows retries while rejecting rewrites of an immutable record.
func storeRecord[T any](records *map[string]T, key string, value T) error {
	if old, ok := (*records)[key]; ok && !reflect.DeepEqual(old, value) {
		return fmt.Errorf("child record %s already exists with different content", key)
	}
	if *records == nil {
		*records = make(map[string]T)
	}
	(*records)[key] = value
	return nil
}

// clone copies assignment slices so caller mutations cannot rewrite the record.
func (a Assignment) clone() Assignment {
	a.AllowedCapabilities = slices.Clone(a.AllowedCapabilities)
	a.ExpectedEvidence = slices.Clone(a.ExpectedEvidence)
	a.Dependencies = slices.Clone(a.Dependencies)
	return a
}

// clone copies result collections so caller mutations cannot rewrite evidence.
func (r Result) clone() Result {
	r.Checks = maps.Clone(r.Checks)
	for status, checks := range r.Checks {
		r.Checks[status] = slices.Clone(checks)
	}
	r.Limitations = slices.Clone(r.Limitations)
	r.OpenRequirements = slices.Clone(r.OpenRequirements)
	return r
}

// AssignmentByID returns a copy of the assignment with the given assignment ID.
func (m *MemoryRecorder) AssignmentByID(_ context.Context, assignmentID string) (Assignment, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, a := range m.assignments {
		if a.AssignmentID == assignmentID {
			return a.clone(), nil
		}
	}
	return Assignment{}, ErrRecordNotFound
}
