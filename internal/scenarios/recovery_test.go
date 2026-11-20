package scenarios

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"synch/internal/cli"
)

// TestCloseDesktopAndRecoverThreadInCLI checks API creation and CLI history replay.
func TestCloseDesktopAndRecoverThreadInCLI(t *testing.T) {
	_, base, _ := startDaemon(t)
	raw := apiCall(t, "POST", base+"/v1/threads", `{"project_id":"local","brief":"recovery fixture","audit":"off"}`, "create-s1")
	var created struct {
		ThreadID string `json:"thread_id"`
	}
	if err := json.Unmarshal(raw, &created); err != nil || created.ThreadID == "" {
		t.Fatalf("create: %s %v", raw, err)
	}
	stream := "thread:" + created.ThreadID
	raw = apiCall(t, "GET", base+"/v1/streams/"+stream+"/events/page", "", "")
	var page struct {
		Events []struct {
			ID  string `json:"id"`
			Seq int64  `json:"seq"`
		} `json:"events"`
		Head int64 `json:"head"`
	}
	if err := json.Unmarshal(raw, &page); err != nil || len(page.Events) == 0 {
		t.Fatalf("page: %s %v", raw, err)
	}
	var out, diagnostics bytes.Buffer
	env := func(key string) string {
		if key == "SYNCH_TOKEN" {
			return "scenario-token"
		}
		return ""
	}
	code := cli.Main([]string{"--server", base, "--json", "events", "follow", stream, "--until", fmt.Sprint(page.Head)}, &out, &diagnostics, env)
	if code != 0 {
		t.Fatalf("CLI: %d %s", code, diagnostics.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != len(page.Events) {
		t.Fatalf("history count: %d vs %d: %s", len(lines), len(page.Events), out.String())
	}
	for i, line := range lines {
		var got struct {
			ID  string `json:"id"`
			Seq int64  `json:"seq"`
		}
		if err := json.Unmarshal([]byte(line), &got); err != nil || got.ID != page.Events[i].ID || got.Seq != int64(i+1) {
			t.Fatalf("history[%d]: %s %v", i, line, err)
		}
	}
	t.Log("partial: API-to-CLI fixture passed; needs 601 authenticated harness")
}
