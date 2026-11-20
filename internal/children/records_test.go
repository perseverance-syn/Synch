package children

import (
	"context"
	"slices"
	"testing"

	"synch/internal/thread"
)

// TestValidatedRecordsUseRecorder proves C4 persists the three valid Go records through the injected Recorder.
func TestValidatedRecordsUseRecorder(t *testing.T) {
	c, store, recorder := fixture(t, Limits{}, nil)
	a := assignment(rootThread(t, c))
	a.AllowedCapabilities = []string{"workspace.read"}
	a.Target, a.InputRevision = "working directory", ""
	bad := a
	bad.Role = "coordinator"
	reject(t, c.CreateChild(t.Context(), bad, thread.Facts{}))
	bad = a
	bad.RoleReason = " "
	reject(t, c.CreateChild(t.Context(), bad, thread.Facts{}))
	must(t, c.CreateChild(t.Context(), a, thread.Facts{}))
	a.AllowedCapabilities[0] = "workspace.write"
	got, err := recorder.Assignment(t.Context(), a.ChildThreadID)
	must(t, err)
	if got.RoleReason != a.RoleReason || !slices.Equal(got.AllowedCapabilities, []string{"workspace.read"}) {
		t.Fatalf("assignment = %+v", got)
	}

	r := Result{
		ChildThreadID: a.ChildThreadID, AssignmentID: a.AssignmentID, Outcome: "completed", Revision: "rev",
		Checks: map[string][]Check{
			"passed": {{Check: "build"}}, "failed": {{"test", "red"}}, "error": {{"vet", "crash"}}, "partial": {{"race", "one pkg"}}, "not_run": {{"lint", "absent"}},
		},
		Limitations: []string{"Windows only"}, OpenRequirements: []string{"Cross-platform review"},
	}
	badResult := r.clone()
	badResult.Checks["skipped"] = []Check{{"security", "x"}}
	reject(t, c.RecordResult(t.Context(), badResult))
	must(t, c.RecordResult(t.Context(), r))
	result, err := recorder.Result(t.Context(), a.ChildThreadID)
	must(t, err)
	result.Checks["passed"][0].Check = "changed"
	result, err = recorder.Result(t.Context(), a.ChildThreadID)
	must(t, err)
	if result.Revision != r.Revision || !slices.Equal(result.Checks["passed"], []Check{{Check: "build"}}) ||
		!slices.Equal(result.Limitations, r.Limitations) || !slices.Equal(result.OpenRequirements, r.OpenRequirements) {
		t.Fatalf("result = %+v", result)
	}
	setting := AuditSetting{ThreadID: a.ChildThreadID, Setting: "level1", Reason: "Enabled by the operator"}
	badSetting := setting
	badSetting.Reason = ""
	reject(t, c.RecordAuditSetting(t.Context(), badSetting))
	must(t, c.RecordAuditSetting(t.Context(), setting))
	audit, err := recorder.AuditSetting(a.ChildThreadID)
	must(t, err)
	if audit != setting {
		t.Fatalf("audit = %+v, want %+v", audit, setting)
	}
	events, err := store.ReadStream("thread:"+a.ChildThreadID, 0, 10)
	must(t, err)
	if len(events) != 1 || events[0].Type != "thread.created" {
		t.Fatalf("child stream = %+v", events)
	}
}

// TestRecorderFailureKeepsChildUnavailable proves C4 a failed assignment write cannot create a schedulable child.
func TestRecorderFailureKeepsChildUnavailable(t *testing.T) {
	c, _, recorder := fixture(t, Limits{}, nil)
	a := assignment(rootThread(t, c))
	c.recorder = failingRecorder{MemoryRecorder: recorder}
	reject(t, c.CreateChild(t.Context(), a, thread.Facts{}))
	_, err := c.Threads().Status(t.Context(), a.ChildThreadID)
	reject(t, err)
	c.recorder = recorder
	must(t, c.CreateChild(t.Context(), a, thread.Facts{}))
}

type failingRecorder struct{ *MemoryRecorder }

// RecordAssignment returns the injected write failure without storing the assignment.
func (failingRecorder) RecordAssignment(context.Context, Assignment) error { return ErrRecordNotFound }

// reject stops the test when an invalid record was accepted.
func reject(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error")
	}
}
