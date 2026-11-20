package children

import (
	"context"
	"errors"
	"slices"
	"testing"

	"synch/internal/eventlog"
	"synch/internal/thread"
)

// resultFor builds a valid result with all five empty check groups.
func resultFor(a Assignment, outcome string) Result {
	return Result{ChildThreadID: a.ChildThreadID, AssignmentID: a.AssignmentID, Outcome: outcome, Revision: "rev",
		Checks: map[string][]Check{"passed": nil, "failed": nil, "error": nil, "partial": nil, "not_run": nil}}
}

// dependentOn creates a queued child that depends on a fresh sibling and returns both assignments.
func dependentOn(t *testing.T, c *Coordinator, recorder *MemoryRecorder, root string) (dep, dependent Assignment) {
	t.Helper()
	first := childThread(t, c, root)
	dep, err := recorder.Assignment(t.Context(), first)
	must(t, err)
	dependent = assignment(root)
	dependent.Dependencies = []string{dep.AssignmentID}
	must(t, c.CreateChild(t.Context(), dependent, thread.Facts{}))
	queue(t, c, dependent.ChildThreadID)
	return dep, dependent
}

// tryDispatch dispatches and reports the error and whether the starter ran.
func tryDispatch(c *Coordinator, id string) (bool, error) {
	started := false
	err := c.Dispatch(context.Background(), id, eventlog.NewID(), eventlog.NewID(), func(context.Context, *thread.Checkpoint) error {
		started = true
		return nil
	})
	return started, err
}

// TestFailedDependencyBlocksDependent proves C5/L1 a failed or cancelled dependency never starts the dependent and fails it with a typed error.
func TestFailedDependencyBlocksDependent(t *testing.T) {
	for _, outcome := range []string{"failed", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			c, _, recorder := fixture(t, Limits{MaxLocal: 2}, nil)
			dep, dependent := dependentOn(t, c, recorder, rootThread(t, c))
			must(t, c.RecordResult(t.Context(), resultFor(dep, outcome)))
			started, err := tryDispatch(c, dependent.ChildThreadID)
			var de *DependencyError
			if !errors.As(err, &de) || de.Dependency != dep.AssignmentID || started {
				t.Fatalf("dispatch error = %v, started = %v", err, started)
			}
			assertPhase(t, c, dependent.ChildThreadID, "queued", false)
			r, err := recorder.Result(t.Context(), dependent.ChildThreadID)
			must(t, err)
			if r.Outcome != "failed" || len(r.OpenRequirements) != 1 {
				t.Fatalf("dependent result = %+v", r)
			}
		})
	}
}

// TestEndedDependencyWithoutResultBlocksDependent proves L1/O3 a terminal dependency with no result does not leave the dependent queued forever.
func TestEndedDependencyWithoutResultBlocksDependent(t *testing.T) {
	c, _, recorder := fixture(t, Limits{MaxLocal: 2}, nil)
	dep, dependent := dependentOn(t, c, recorder, rootThread(t, c))
	must(t, c.Cancel(t.Context(), dep.ChildThreadID))
	started, err := tryDispatch(c, dependent.ChildThreadID)
	var de *DependencyError
	if !errors.As(err, &de) || started {
		t.Fatalf("dispatch error = %v, started = %v", err, started)
	}
	if _, err := recorder.Result(t.Context(), dependent.ChildThreadID); err != nil {
		t.Fatalf("dependent has no failed result: %v", err)
	}
}

// TestDependenciesAreAssignmentIDs proves L4 dependencies resolve by sibling assignment ID, not thread ID.
func TestDependenciesAreAssignmentIDs(t *testing.T) {
	c, _, recorder := fixture(t, Limits{}, nil)
	root := rootThread(t, c)
	first := childThread(t, c, root)
	dep, err := recorder.Assignment(t.Context(), first)
	must(t, err)
	byThread := assignment(root)
	byThread.Dependencies = []string{first}
	reject(t, c.CreateChild(t.Context(), byThread, thread.Facts{}))
	other, err := recorder.Assignment(t.Context(), childThread(t, c, childThread(t, c, root)))
	must(t, err)
	foreign := assignment(root)
	foreign.Dependencies = []string{other.AssignmentID}
	reject(t, c.CreateChild(t.Context(), foreign, thread.Facts{}))
	ok := assignment(root)
	ok.Dependencies = []string{dep.AssignmentID}
	must(t, c.CreateChild(t.Context(), ok, thread.Facts{}))
	got, err := recorder.AssignmentByID(t.Context(), ok.AssignmentID)
	must(t, err)
	if !slices.Equal(got.Dependencies, ok.Dependencies) {
		t.Fatalf("dependencies = %v", got.Dependencies)
	}
}

// TestResultValidation proves L2, L3 and L5 outcome enum, check groups and required revision.
func TestResultValidation(t *testing.T) {
	good := resultFor(assignment(eventlog.NewID()), "completed")
	must(t, good.Validate())
	for name, mutate := range map[string]func(*Result){
		"bogus outcome":     func(r *Result) { r.Outcome = "bogus" },
		"empty outcome":     func(r *Result) { r.Outcome = "" },
		"empty revision":    func(r *Result) { r.Revision = " " },
		"missing group":     func(r *Result) { delete(r.Checks, "partial") },
		"unknown group":     func(r *Result) { r.Checks["skipped"] = nil },
		"failed no reason":  func(r *Result) { r.Checks["failed"] = []Check{{Check: "t"}} },
		"error no reason":   func(r *Result) { r.Checks["error"] = []Check{{Check: "t"}} },
		"partial no reason": func(r *Result) { r.Checks["partial"] = []Check{{Check: "t"}} },
		"not_run no reason": func(r *Result) { r.Checks["not_run"] = []Check{{Check: "t"}} },
		"duplicate check":   func(r *Result) { r.Checks["passed"] = []Check{{Check: "t"}, {Check: "t"}} },
		"nil checks":        func(r *Result) { r.Checks = nil },
		"non ULID child":    func(r *Result) { r.ChildThreadID = "abc" },
	} {
		r := good.clone()
		mutate(&r)
		if r.Validate() == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	for _, o := range []string{"completed", "failed", "cancelled"} {
		r := good.clone()
		r.Outcome = o
		must(t, r.Validate())
	}
	r := good.clone()
	r.Checks["passed"] = []Check{{Check: "build"}}
	r.Checks["failed"] = []Check{{Check: "test", Reason: "red"}}
	must(t, r.Validate())
}

// TestAuditSettingAndAssignmentIDs proves L6 setting enum, ULID IDs and unique capabilities.
func TestAuditSettingAndAssignmentIDs(t *testing.T) {
	id := eventlog.NewID()
	for _, s := range []string{"off", "level1", "level2", "level3"} {
		must(t, AuditSetting{ThreadID: id, Setting: s, Reason: "why"}.Validate())
	}
	for _, s := range []string{"default", "on", ""} {
		reject(t, AuditSetting{ThreadID: id, Setting: s, Reason: "why"}.Validate())
	}
	reject(t, AuditSetting{ThreadID: id, Setting: "off"}.Validate())
	reject(t, AuditSetting{ThreadID: "x", Setting: "off", Reason: "why"}.Validate())
	a := assignment(id)
	must(t, a.Validate())
	a.AllowedCapabilities = []string{"workspace.read", "workspace.read"}
	reject(t, a.Validate())
	a = assignment(id)
	a.ChildThreadID = "not-a-ulid"
	reject(t, a.Validate())
	a = assignment(id)
	a.AssignmentID = "I" + a.AssignmentID[1:] // Crockford excludes I.
	reject(t, a.Validate())
}
