package api

import (
	"context"
	"testing"

	"synch/internal/eventlog"
	"synch/internal/queue"
)

// TestChangeModeHook proves authenticated admission, typed refusal and once-only post-commit settlement.
func TestChangeModeHook(t *testing.T) {
	s, store, _ := fixture(t)
	id := create(t, s, "create")
	path := "/v1/threads/" + id + "/commands"
	body := `{"command":"change_mode","target":"host-a","to":"sandbox"}`
	var prepared, committed, settled int
	s.config.ChangeMode = func(_ context.Context, _ *eventlog.Transaction, c ModeCommand) (func(context.Context, *eventlog.Transaction) error, func(context.Context) error, error) {
		prepared++
		if c != (ModeCommand{Actor: testCaller, Thread: id, Target: "host-a", To: "sandbox"}) {
			t.Fatalf("command: %+v", c)
		}
		if prepared == 1 {
			return nil, nil, &queue.Rejection{Code: "invalid_transition", Detail: "sandbox_unavailable"}
		}
		return func(context.Context, *eventlog.Transaction) error {
				committed++
				return nil
			}, func(ctx context.Context) error {
				settled++
				_, err := queue.Lookup(ctx, store, testCaller, "accepted")
				if err != nil {
					t.Fatalf("settlement ran before the receipt committed: %v", err)
				}
				return nil
			}, nil
	}
	refused := request(t, s, "POST", path, testToken, "refused", body)
	if refused.Code != 409 || decode[problem](t, refused.Body.Bytes()).Detail != "sandbox_unavailable" {
		t.Fatalf("refusal: %d %s", refused.Code, refused.Body)
	}
	for _, replay := range []string{"false", "true"} {
		w := request(t, s, "POST", path, testToken, "accepted", body)
		if w.Code != 202 || w.Header().Get(replayedHeader) != replay {
			t.Fatalf("acceptance replay %s: %d %v %s", replay, w.Code, w.Header(), w.Body)
		}
	}
	if prepared != 2 || committed != 1 || settled != 1 {
		t.Fatalf("prepared %d, committed %d, settled %d", prepared, committed, settled)
	}
}
