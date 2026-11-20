package api

import (
	"strings"
	"testing"
)

func TestExecutionDriversEndpoint(t *testing.T) {
	s, _, _ := fixture(t)
	s.config.ExecutionDrivers = func() []ExecutionDriver {
		return []ExecutionDriver{{Kind: "direct", Description: "host", Status: "ready"}}
	}
	path := "/v1/execution-drivers"
	requireProblem(t, request(t, s, "GET", path, "", "", ""), 401, "unauthorized", "")
	got := request(t, s, "GET", path, testToken, "", "")
	if got.Code != 200 || !strings.Contains(got.Body.String(), `"kind":"direct"`) {
		t.Fatalf("%d %s", got.Code, got.Body)
	}
}
