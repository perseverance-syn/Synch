package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// scannedRoute is one method/path pair found by the plain line scan.
type scannedRoute struct{ method, path string }

// scanRoutes reads openapi.yaml without a YAML library: path keys sit at a
// two-space indent and their methods at four spaces.
func scanRoutes(t *testing.T) []scannedRoute {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "contracts", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var routes []scannedRoute
	path := ""
	for _, line := range strings.Split(string(raw), "\n") {
		switch {
		case strings.HasPrefix(line, "  /") && strings.HasSuffix(line, ":"):
			path = strings.TrimSuffix(strings.TrimPrefix(line, "  "), ":")
		case line == "  components:":
			path = ""
		case path != "" && (strings.HasPrefix(line, "    get:") || strings.HasPrefix(line, "    post:")):
			method := strings.ToUpper(strings.TrimSuffix(strings.TrimSpace(line), ":"))
			routes = append(routes, scannedRoute{method: method, path: path})
		}
	}
	if len(routes) != 13 {
		t.Fatalf("route scan found %d routes, want the contract's 13", len(routes))
	}
	return routes
}

// TestCommandsReachEveryRoute proves C1: every path and method in openapi.yaml
// is covered by a CLI command, and each command reaches its route on the real
// in-process server.
func TestCommandsReachEveryRoute(t *testing.T) {
	f := newCLIFixture(t)
	routes := scanRoutes(t)
	threadID := f.createThread("cov-create", "coverage thread")

	commands := map[scannedRoute][]string{
		{"GET", "/v1/health"}:                        f.authenticated("--json", "health"),
		{"GET", "/v1/threads"}:                       f.authenticated("--json", "threads", "list"),
		{"POST", "/v1/threads"}:                      f.authenticated("--json", "threads", "create", "--project", "cov", "--brief", "route coverage", "--request-id", "cov-post-threads"),
		{"GET", "/v1/threads/{thread_id}"}:           f.authenticated("--json", "threads", "show", threadID),
		{"POST", "/v1/threads/{thread_id}/commands"}: f.authenticated("--json", "thread", "cmd", threadID, "pause", "--request-id", "cov-commands"),
		{"POST", "/v1/threads/{thread_id}/approvals"}: f.authenticated("--json", "approvals", "create", threadID,
			"--worktree-digest", "sha256:"+strings.Repeat("a", 64), "--target", "/workspace",
			"--actions", "git.status,fs.write", "--expires-at", "2027-01-01T00:00:00Z",
			"--revision", strings.Repeat("b", 40), "--request-id", "cov-approvals"),
		{"POST", "/v1/threads/{thread_id}/grants"}: f.authenticated("--json", "grants", "create", threadID,
			"--host", "host-a", "--identity", "runner:r1", "--request-id", "cov-grants"),
		{"POST", "/v1/grants/{grant_id}/revoke"}:    f.authenticated("--json", "grants", "revoke", "grant-1", "--request-id", "cov-revoke"),
		{"GET", "/v1/receipts/{request_id}"}:        f.authenticated("--json", "receipt", "cov-post-threads"),
		{"GET", "/v1/streams/{stream}/events"}:      f.authenticated("--json", "events", "follow", "all", "--until", "1"),
		{"GET", "/v1/streams/{stream}/events/page"}: f.authenticated("--json", "events", "page", "all"),
	}
	covered := map[scannedRoute]bool{}
	for _, route := range routes {
		// Driver listing is API-only; evidence export has dedicated integration tests.
		if route.path == "/v1/execution-drivers" || route.path == "/v1/threads/{thread_id}/evidence" {
			continue
		}
		if _, ok := commands[route]; !ok {
			t.Fatalf("no CLI command covers %s %s", route.method, route.path)
		}
		covered[route] = true
	}
	for route := range commands {
		if !covered[route] {
			t.Fatalf("command table covers a route the scan did not find: %s %s", route.method, route.path)
		}
	}

	for _, route := range routes {
		if !covered[route] {
			continue
		}
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			args := commands[route]
			code, out, errOut := f.run(args...)
			if code != 0 {
				t.Fatalf("command for %s %s = %d, %s, %s", route.method, route.path, code, out, errOut)
			}
			for _, evidence := range routeEvidence(route) {
				if !strings.Contains(out, evidence) {
					t.Fatalf("output for %s %s lacks %q:\n%s", route.method, route.path, evidence, out)
				}
			}
		})
	}
}

// routeEvidence names the response markers that prove the route was reached,
// as opposed to a 404 from a wrong path or a 401 from bad authentication.
func routeEvidence(route scannedRoute) []string {
	switch route.path + " " + route.method {
	case "/v1/health GET":
		return []string{`"status":"ok"`}
	case "/v1/threads GET":
		return []string{`"threads":[`}
	case "/v1/threads POST", "/v1/threads/{thread_id}/approvals POST", "/v1/threads/{thread_id}/grants POST":
		return []string{`"http_status":201`, `"status":"accepted"`}
	case "/v1/threads/{thread_id} GET":
		return []string{`"state":"draft"`}
	case "/v1/threads/{thread_id}/commands POST", "/v1/grants/{grant_id}/revoke POST":
		return []string{`"http_status":202`}
	case "/v1/receipts/{request_id} GET":
		return []string{`"receipt_id":"`, `"request_id":"cov-post-threads"`}
	case "/v1/streams/{stream}/events GET":
		return []string{`"global_seq":1`}
	case "/v1/streams/{stream}/events/page GET":
		return []string{`"events":[`, `"log_epoch":"`}
	default:
		return nil
	}
}
