package cli

import (
	"context"
	"strings"
	"testing"

	"synch/internal/api"
	"synch/internal/thread"
)

// TestThreadsShowAndListNameTheGrantCommand proves card 113 G2: a queued thread
// that waits for the operator's grant says so in plain words with the exact
// command, in `threads show` and `threads list`; --json stays the raw body.
func TestThreadsShowAndListNameTheGrantCommand(t *testing.T) {
	var machine *thread.Machine
	f := newCLIFixture(t, func(c *api.Config) {
		machine = c.Threads
		c.WaitingFor = func(_ context.Context, id string) string {
			return "waiting for an operator grant: run `synch grants create " + id + " --host h --identity i`"
		}
	})
	id := f.createThread("waiting", "needs a grant")
	if err := machine.Apply(t.Context(), id, thread.Command{Transition: "T01"}); err != nil {
		t.Fatal(err)
	}
	want := "thread " + id + " is waiting for an operator grant: run `synch grants create " + id + " --host h --identity i`"
	for _, args := range [][]string{{"threads", "show", id}, {"threads", "list"}} {
		code, out, errOut := f.run(f.authenticated(args...)...)
		if code != 0 || !strings.Contains(out, want) {
			t.Fatalf("%v = %d, %s, %s\nwant a line %q", args, code, out, errOut, want)
		}
	}
	code, out, errOut := f.run(f.authenticated(append([]string{"--json"}, "threads", "show", id)...)...)
	if code != 0 || strings.Contains(out, "is waiting for") || !strings.Contains(out, `"waiting_for"`) {
		t.Fatalf("--json show = %d, %s, %s", code, out, errOut)
	}
}
