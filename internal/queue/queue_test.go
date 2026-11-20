package queue

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"

	"os"
	"path/filepath"
	"sync/atomic"
	"synch/internal/eventlog"
	"synch/internal/thread"
	"testing"
)

// --- Admission: exercise the shared vectors and malformed input (AC8) ---

// TestDigestVectors verifies AC8 against every contract vector for admission and canonical digest.
func TestDigestVectors(t *testing.T) {
	raw, err := os.ReadFile("../../contracts/tests/digest-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var data struct {
		Vectors []struct {
			Name, Method, Route string
			Text                string            `json:"body_text"`
			Hex                 string            `json:"body_hex"`
			Params              map[string]string `json:"path_params"`
			Expect              struct{ Digest, Reject string }
		}
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		t.Fatal(err)
	}
	for _, v := range data.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			b := []byte(v.Text)
			if v.Hex != "" {
				var err error
				b, err = hex.DecodeString(v.Hex)
				if err != nil {
					t.Fatal(err)
				}
			}
			d, err := PayloadDigest(v.Method, v.Route, v.Params, b)
			if v.Expect.Reject != "" {
				if !errors.Is(err, Detail(v.Expect.Reject)) {
					t.Fatalf("got %v", err)
				}
			} else if err != nil || d != v.Expect.Digest {
				t.Fatal(d, err)
			}
		})
	}
}

// TestAdmissionDetails verifies AC8 detail codes for malformed input and nested duplicate names.
func TestAdmissionDetails(t *testing.T) {
	for _, tc := range []struct {
		body string
		want Detail
	}{{"{", MalformedJSON}, {"[]", BodyMustBeObject}, {"\ufeff{}", InvalidUTF8}, {`{"n":{"a":1,"\u0061":2}}`, DuplicateMember}, {`{"\udc00":1}`, InvalidUnicode}} {
		_, err := Admit("/v1/threads", []byte(tc.body))
		if !errors.Is(err, tc.want) {
			t.Fatal(tc, err)
		}
	}
}

// --- Receipts: verify replay, isolation, conflict, and atomicity (AC9) ---

// openStore opens an SQLite event store for receipt tests.
func openStore(t *testing.T, path string) *eventlog.Store {
	t.Helper()
	s, err := eventlog.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestConcurrentReceiptsAndConflict verifies AC9 by serializing the same key and preserving the receipt on conflict.
func TestConcurrentReceiptsAndConflict(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "q.db"))
	t.Cleanup(func() { _ = s.Close() })
	r := Request{Caller: "caller", ID: "same", Method: "POST", Route: "/v1/threads", Body: []byte("{}")}
	var calls atomic.Int32
	id := eventlog.NewID()
	apply := func(context.Context, *eventlog.Transaction, map[string]any) (Decision, error) {
		calls.Add(1)
		return Decision{ThreadID: id, HTTPStatus: 201}, nil
	}
	type result struct {
		r Response
		e error
	}
	ch := make(chan result, 2)
	start := make(chan struct{})
	for range 2 {
		go func() { <-start; v, err := Execute(t.Context(), s, r, apply); ch <- result{v, err} }()
	}
	close(start)
	a, b := <-ch, <-ch
	if a.e != nil || b.e != nil || a.r.Replayed == b.r.Replayed || !bytes.Equal(a.r.Body, b.r.Body) || calls.Load() != 1 {
		t.Fatal(a, b, calls.Load())
	}
	r.Body = []byte(`{"x":1}`)
	_, err := Execute(t.Context(), s, r, apply)
	var c *Conflict
	if !errors.As(err, &c) || c.ReceiptID != a.r.ReceiptID || c.StoredPayloadDigest != a.r.PayloadDigest || calls.Load() != 1 {
		t.Fatal(c, err)
	}
}

// TestRestartReplaysAtomicCancel verifies AC9 worked example C, including rollback and byte-for-byte replay across reopen.
func TestRestartReplaysAtomicCancel(t *testing.T) {
	path := filepath.Join(t.TempDir(), "q.db")
	s := openStore(t, path)
	id := eventlog.NewID()
	m, err := thread.Open(t.Context(), s, thread.Hooks{ConfirmGone: func(context.Context, string) (bool, error) { return true, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Create(t.Context(), id, thread.Facts{}); err != nil {
		t.Fatal(err)
	}
	r := Request{Caller: "operator:alice", ID: "c-2", Method: "POST", Route: "/v1/threads/{thread_id}/commands", PathParams: map[string]string{"thread_id": id}, Body: []byte(`{"command":"cancel"}`)}
	crash := true
	apply := func(context.Context, *eventlog.Transaction, map[string]any) (Decision, error) {
		return Decision{ThreadID: id, HTTPStatus: 202, Commit: func(ctx context.Context, tx *eventlog.Transaction) (any, error) {
			if err := m.ApplyTx(ctx, tx, id, thread.Command{Transition: "T30"}); err != nil {
				return nil, err
			}
			if crash {
				return nil, errors.New("precommit failure")
			}
			return nil, nil
		}}, nil
	}
	if _, err := Execute(t.Context(), s, r, apply); err == nil {
		t.Fatal("committed failed transaction")
	}
	if head, err := s.Head(); err != nil || head != 0 {
		t.Fatal(head, err)
	}
	crash = false
	first, err := Execute(t.Context(), s, r, apply)
	if err != nil || first.Replayed {
		t.Fatal(first, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, path)
	defer func() { _ = s.Close() }()
	replay, err := Execute(t.Context(), s, r, nil)
	if err != nil || !replay.Replayed || !bytes.Equal(first.Body, replay.Body) || first.ContentType != replay.ContentType || first.HTTPStatus != replay.HTTPStatus {
		t.Fatal(replay, err)
	}
	if head, err := s.Head(); err != nil || head != 2 {
		t.Fatal(head, err)
	}
}

// TestStoredRejection verifies AC9 by replaying a deterministic rejection after its first application.
func TestStoredRejection(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "r.db"))
	defer func() { _ = s.Close() }()
	r := Request{Caller: "a", ID: "1", Method: "POST", Route: "/v1/threads", Body: []byte("{}")}
	a, err := Execute(t.Context(), s, r, func(context.Context, *eventlog.Transaction, map[string]any) (Decision, error) {
		return Decision{}, &Rejection{Code: "invalid_transition"}
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := Execute(t.Context(), s, r, nil)
	if err != nil || !b.Replayed || !bytes.Equal(a.Body, b.Body) {
		t.Fatal(b, err)
	}
	r.Body = []byte("{")
	_, err = Execute(t.Context(), s, r, nil)
	if !errors.Is(err, MalformedJSON) {
		t.Fatal(err)
	}
}

// TestNullAdmittedThenRejectedAndCallerIsolation verifies AC8 explicit null identity and AC9 caller-scoped receipts.
func TestNullAdmittedThenRejectedAndCallerIsolation(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "queue.db"))
	defer func() { _ = s.Close() }()
	r := Request{Caller: "alice", ID: "c1", Method: "POST", Command: "createThread", Route: "/v1/threads", Body: []byte(`{"pin":null}`)}
	rejected, err := Execute(t.Context(), s, r, nil)
	if err != nil || rejected.HTTPStatus != 400 || rejected.Status != "rejected" {
		t.Fatal(rejected, err)
	}
	record, err := Lookup(t.Context(), s, r.Caller, r.ID)
	if err != nil || !bytes.Equal(record, rejected.Record) || !bytes.Contains(record, []byte(`"problem":`)) {
		t.Fatal(string(record), err)
	}
	r.Body = []byte("{}")
	_, err = Execute(t.Context(), s, r, nil)
	var conflict *Conflict
	if !errors.As(err, &conflict) {
		t.Fatal(err)
	}
	r.Caller = "bob"
	accepted, err := Execute(t.Context(), s, r, func(context.Context, *eventlog.Transaction, map[string]any) (Decision, error) {
		return Decision{ThreadID: eventlog.NewID(), HTTPStatus: 201}, nil
	})
	if err != nil || accepted.Replayed || accepted.ReceiptID == rejected.ReceiptID {
		t.Fatal(accepted, err)
	}
}

// TestRejectedCommandRollsBackEvents verifies AC9 by rolling back events while preserving a deterministic rejection.
func TestRejectedCommandRollsBackEvents(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "queue.db"))
	defer func() { _ = s.Close() }()
	id := eventlog.NewID()
	r := Request{Caller: "alice", ID: "c2", Method: "POST", Route: "/v1/threads/{thread_id}/commands", PathParams: map[string]string{"thread_id": id}, Body: []byte(`{"command":"accept"}`)}
	rejected, err := Execute(t.Context(), s, r, func(context.Context, *eventlog.Transaction, map[string]any) (Decision, error) {
		return Decision{ThreadID: id, HTTPStatus: 202, Commit: func(context.Context, *eventlog.Transaction) (any, error) { return nil, thread.ErrTransition }}, nil
	})
	if err != nil || rejected.HTTPStatus != 409 {
		t.Fatal(rejected, err)
	}
	if head, err := s.Head(); err != nil || head != 0 {
		t.Fatal(head, err)
	}
	replayed, err := Execute(t.Context(), s, r, nil)
	if err != nil || !replayed.Replayed || !bytes.Equal(replayed.Body, rejected.Body) {
		t.Fatal(replayed, err)
	}
}
