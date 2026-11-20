package mcp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.Header.Get("Authorization") != "Bearer caller" {
			t.Errorf("bad request: %s", r.Method)
		}
		switch r.URL.Path {
		case "/v1/threads/thread1":
			_, _ = fmt.Fprint(w, `{"state":"queued","waiting_for":"operator","checks":{"passed":["build"],"failed":[],"error":[],"partial":[],"not_run":["test"]},"open_requirements":[{"kind":"check","id":"test","status":"not_run"}]}`)
		case "/v1/outputs/out_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/search":
			if r.URL.Query().Get("thread") != "thread1" || r.URL.Query().Get("pattern") != "FAIL & more" || r.URL.Query().Get("limit") == "" {
				t.Errorf("bad query: %s", r.URL.RawQuery)
			}
			_, _ = fmt.Fprint(w, `{"ref":"out_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","matches":[{"line":7,"text":"FAIL excerpt"}],"truncated":false}`)
		default:
			t.Errorf("unexpected path: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	input := strings.Join([]string{`{"jsonrpc":"2.0","id":1,"method":"initialize"}`, `{"jsonrpc":"2.0","id":2,"method":"tools/list"}`, `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"thread_status","arguments":{}}}`, `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"search_output","arguments":{"reference":"out_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pattern":"FAIL & more"}}}`, `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"request_approval"}}`, `not json`, `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"bogus"}}`, `{"jsonrpc":"2.0","id":7,"method":"ping"}`, `{"jsonrpc":"2.0","id":8,"method":"shutdown"}`}, "\n") + "\n"
	var out bytes.Buffer
	if err := Serve(strings.NewReader(input), &out, Config{Server: api.URL, Token: "caller", ThreadID: "thread1"}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 9 {
		t.Fatal(out.String())
	}
	var list struct {
		Result struct {
			Tools []struct {
				Name string `json:"name"`
			} `json:"tools"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(lines[1]), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Result.Tools) != 2 || list.Result.Tools[0].Name != "thread_status" || list.Result.Tools[1].Name != "search_output" {
		t.Fatal(lines[1])
	}
	for _, s := range []string{"queued", "operator", "passed", "failed", "error", "partial", "not_run", "open_requirements"} {
		if !strings.Contains(lines[2], s) {
			t.Fatalf("missing %s: %s", s, lines[2])
		}
	}
	if !strings.Contains(lines[3], "FAIL excerpt") || !strings.Contains(lines[3], "out_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") || !strings.Contains(lines[4], "-32602") || !strings.Contains(lines[5], "-32700") || !strings.Contains(lines[6], "-32602") {
		t.Fatal(out.String())
	}
}

func TestSearchOwnershipAndCap(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("pattern") == "private" {
			http.NotFound(w, r)
			return
		}
		matches := make([]map[string]any, 100)
		for i := range matches {
			matches[i] = map[string]any{"line": i + 1, "text": strings.Repeat("x", 512)}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ref": "out_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "matches": matches, "truncated": false})
	}))
	defer api.Close()
	input := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search_output","arguments":{"reference":"out_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pattern":"private"}}}` + "\n" + `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_output","arguments":{"reference":"out_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pattern":"x"}}}` + "\n"
	var out bytes.Buffer
	if err := Serve(strings.NewReader(input), &out, Config{Server: api.URL, Token: "caller", ThreadID: "thread1"}); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], `"isError":true`) || !strings.Contains(lines[0], "404") {
		t.Fatal(out.String())
	}
	if len(lines[1]) > 17000 || !strings.Contains(lines[1], "out_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa") || !strings.Contains(lines[1], `truncated\":true`) {
		t.Fatalf("cap: %d %s", len(lines[1]), lines[1][:100])
	}
}
