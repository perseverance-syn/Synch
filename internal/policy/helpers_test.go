package policy

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"synch/internal/eventlog"
)

// --- Test fixtures: fake daemon state, engine setup and event inspection ---

const (
	rootID  = "01JD3Q8Z7Y6X5W4V3T2S1R0QPN"
	childID = "01JD3Q8Z7Y6X5W4V3T2S1R0QPM"
	grandID = "01JD3Q8Z7Y6X5W4V3T2S1R0QPK"
	otherID = "01JD3Q8Z7Y6X5W4V3T2S1R0QPJ"
	rev1    = "5977ec186c62ce66b2e1e48051bda521bb7f6b9f"
	rev2    = "6977ec186c62ce66b2e1e48051bda521bb7f6b9f"
)

var (
	t0    = time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	wd1   = "sha256:" + strings.Repeat("e", 64)
	wd2   = "sha256:" + strings.Repeat("f", 64)
	plan1 = "sha256:" + strings.Repeat("4", 64)
	plan2 = "sha256:" + strings.Repeat("5", 64)
)

// fakeState is an in-memory State whose fields tests mutate between calls.
type fakeState struct {
	threads  map[string]ThreadInfo
	modes    map[string]string // thread -> access mode; default provider
	files    []*File
	cur      Current
	vms      map[string]bool   // thread+"/"+vm
	accepted map[string]string // thread -> accepted revision
	settle   func(ctx context.Context, id string, threads []string) error
	dbPath   string // event log file, for tests that hold its write lock
	onCurr   func() // called at the start of every Current call, if set
	now      time.Time
}

// Thread returns the stored thread or an error.
func (f *fakeState) Thread(id string) (ThreadInfo, error) {
	t, ok := f.threads[id]
	if !ok {
		return ThreadInfo{}, errors.New("no such thread")
	}
	return t, nil
}

// AccessMode returns the configured mode, provider by default.
func (f *fakeState) AccessMode(thread, _ string) (string, error) {
	if m, ok := f.modes[thread]; ok {
		return m, nil
	}
	return "provider", nil
}

// Policy returns the current files.
func (f *fakeState) Policy(string) ([]*File, error) { return f.files, nil }

// Current returns the current digests, after the onCurr hook if one is set.
func (f *fakeState) Current(string) (Current, error) {
	if f.onCurr != nil {
		f.onCurr()
	}
	return f.cur, nil
}

// OwnsVM reports configured ownership.
func (f *fakeState) OwnsVM(thread, vm string) (bool, error) { return f.vms[thread+"/"+vm], nil }

// MergeAccepted reports whether this exact revision was accepted.
func (f *fakeState) MergeAccepted(thread, revision string) (bool, error) {
	return f.accepted[thread] == revision, nil
}

// Settle calls the configured settler, if any.
func (f *fakeState) Settle(ctx context.Context, id string, threads []string) error {
	if f.settle != nil {
		return f.settle(ctx, id, threads)
	}
	return nil
}

// fixture builds an engine over a fresh event log with root, child and
// grandchild threads (managed, created at t0+1m and t0+2m) and one sibling.
func fixture(t *testing.T, files ...string) (*Engine, *fakeState, *eventlog.Store) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "events.db")
	log, err := eventlog.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	st := &fakeState{
		threads: map[string]ThreadInfo{
			rootID:  {ID: rootID, Role: "builder", Managed: true, CreatedAt: t0.Add(-time.Hour), Children: []string{childID}},
			childID: {ID: childID, Parent: rootID, Role: "builder", Managed: true, CreatedAt: t0.Add(time.Minute), Children: []string{grandID}},
			grandID: {ID: grandID, Parent: childID, Role: "builder", Managed: true, CreatedAt: t0.Add(2 * time.Minute)},
			otherID: {ID: otherID, Role: "builder", Managed: true, CreatedAt: t0},
		},
		modes:    map[string]string{},
		cur:      Current{Head: rev1, PlanDigest: plan1, WorktreeDigest: wd1},
		vms:      map[string]bool{},
		accepted: map[string]string{},
		now:      t0,
		dbPath:   dbPath,
	}
	for _, f := range files {
		st.files = append(st.files, mustLoad(t, f))
	}
	return New(log, st, func() time.Time { return st.now }), st, log
}

// --- Rule files and event inspection ---

// originOf returns a trusted origin matching the file's declared source.
func originOf(data []byte) Origin {
	var head struct {
		Source string `json:"source"`
	}
	_ = json.Unmarshal(data, &head)
	switch head.Source {
	case "project":
		return Origin{Kind: OriginProject, Project: "proj-1", Revision: rev1}
	case "role", "skill":
		return Origin{Kind: OriginPack, PackFile: "pack.json", Digest: "sha256:" + strings.Repeat("a", 64)}
	}
	return Origin{Kind: OriginStore}
}

// mustLoad loads a rule file from its trusted origin or fails the test.
func mustLoad(t *testing.T, src string) *File {
	t.Helper()
	f, err := Load([]byte(src), originOf([]byte(src)))
	if err != nil {
		t.Fatalf("load %s: %v", src, err)
	}
	return f
}

// events returns the decoded payloads of a thread's events of one type.
func events(t *testing.T, log *eventlog.Store, thread, typ string) []map[string]any {
	t.Helper()
	all, err := log.ReadStream("thread:"+thread, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]any
	for _, e := range all {
		if e.Type == typ {
			var p map[string]any
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatal(err)
			}
			out = append(out, p)
		}
	}
	return out
}

// ptr returns a pointer to v.
func ptr[T any](v T) *T { return &v }
