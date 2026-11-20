package api

import (
	"bufio"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var contractPath = regexp.MustCompile(`^  (/[^ ]+):$`)
var contractMethod = regexp.MustCompile(`^    (get|put|post|delete|options|head|patch|trace):$`)

// openAPIRoutes scans paths and methods from the frozen contract without a YAML dependency.
func openAPIRoutes(t *testing.T) map[string]bool {
	t.Helper()
	f, err := os.Open(filepath.Join("..", "..", "contracts", "api", "openapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	scanner := bufio.NewScanner(f)
	paths := false
	path := ""
	pairs := make(map[string]bool)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "paths:" {
			paths = true
			continue
		}
		if !paths {
			continue
		}
		if line != "" && line[0] != ' ' {
			break
		}
		if match := contractPath.FindStringSubmatch(line); match != nil {
			path = match[1]
		}
		if match := contractMethod.FindStringSubmatch(line); match != nil {
			pairs[strings.ToUpper(match[1])+" "+path] = true
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return pairs
}

// TestOpenAPIRoutes proves AC3 registers every contract method and path and rejects extra methods and paths.
func TestOpenAPIRoutes(t *testing.T) {
	s, _, _ := fixture(t)
	contract := openAPIRoutes(t)
	if len(contract) == 0 || len(contract) != len(routes) {
		t.Fatalf("contract %v vs routes %v", contract, routes)
	}
	fill := strings.NewReplacer("{thread_id}", "01JD3Q8Z7Y6X5W4V3T2S1R0QPN", "{grant_id}", "grant", "{request_id}", "request", "{ref}", "out_00000000000000000000000000000000", "{stream}", "system")
	for _, entry := range routes {
		key := entry.method + " " + entry.path
		if !contract[key] {
			t.Fatalf("extra or duplicate route %s", key)
		}
		delete(contract, key)
		path := fill.Replace(entry.path)
		if entry.operation == "streamEvents" {
			path += "?after=1"
		}
		expected := http.StatusOK
		switch entry.operation {
		case "streamEvents":
			expected = http.StatusConflict
		case "searchOutput":
			expected = http.StatusBadRequest
		case "getThread", "getReceipt", "getEvidence", "getDiff":
			expected = http.StatusNotFound
		}
		if entry.method == http.MethodPost {
			expected = http.StatusBadRequest
		}
		got := request(t, s, entry.method, path, testToken, "", "")
		if got.Code != expected {
			t.Fatalf("%s: %d %s", key, got.Code, got.Body)
		}
		if entry.method == http.MethodPost {
			requireProblem(t, got, 400, "request_id_required", "")
		}
		refused := request(t, s, "PATCH", path, testToken, "", "")
		requireProblem(t, refused, 405, "method_not_allowed", "")
		if allow := refused.Header().Get("Allow"); !strings.Contains(allow, entry.method) {
			t.Fatalf("Allow for %s: %q", key, allow)
		}
	}
	if len(contract) != 0 {
		t.Fatalf("missing routes: %v", contract)
	}
	requireProblem(t, request(t, s, "GET", "/v1/unknown", testToken, "", ""), 404, "not_found", "")
	refused := request(t, s, "HEAD", "/v1/health", testToken, "", "")
	requireProblem(t, refused, 405, "method_not_allowed", "")
	if refused.Header().Get("Allow") != http.MethodGet {
		t.Fatalf("health Allow: %q", refused.Header().Get("Allow"))
	}
}
