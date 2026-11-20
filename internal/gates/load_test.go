package gates

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// gateFile encodes a gate file for tests, embedding threshold raw JSON.
func gateFile(t *testing.T, checks ...Check) []byte {
	t.Helper()
	raw, err := json.Marshal(GateFile{Version: GateVersion, Checks: checks})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// rawCheck builds one check with a raw threshold member.
func rawCheck(id, format string, threshold string) Check {
	check := Check{ID: id, Command: []string{"tool", "run"}, Report: &ReportSpec{Path: id + ".out", Format: ReportFormat(format)}}
	if threshold != "" {
		check.Threshold = rawJSON(threshold)
	}
	return check
}

// rawJSON decodes a JSON object fragment into a raw message.
func rawJSON(object string) *json.RawMessage {
	raw := json.RawMessage(object)
	return &raw
}

// TestLoadAcceptsWellFormedGateFiles proves G1: a valid gate file loads with
// its order, thresholds and required flags intact, and the digest is the
// sha256 of the exact bytes.
func TestLoadAcceptsWellFormedGateFiles(t *testing.T) {
	data := gateFile(t,
		rawCheck("unit", string(FormatGoTestJSON), `{"max_failures": 3}`),
		Check{ID: "lint", Command: []string{"lint"}, Required: true},
	)
	file, err := Load(data)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if file.Version != GateVersion || len(file.Checks) != 2 {
		t.Fatalf("file = %+v", file)
	}
	if file.Checks[0].maxAllowed != 3 {
		t.Fatalf("unit threshold = %d, want 3", file.Checks[0].maxAllowed)
	}
	if !file.Checks[1].Required || file.Checks[1].maxAllowed != 0 {
		t.Fatalf("lint check = %+v", file.Checks[1])
	}
	if got, want := Digest(data), digestHex(data); got != want {
		t.Fatalf("Digest = %s, want %s", got, want)
	}
}

// digestHex computes the config digest independently of the gates package.
func digestHex(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", sum)
}

// TestLoadRejectsInvalidGateFiles proves G1: unknown fields at every level,
// duplicate and empty ids, empty argv, unknown formats, foreign threshold
// keys, negative values, wrong versions and trailing content are load errors.
func TestLoadRejectsInvalidGateFiles(t *testing.T) {
	tests := []struct {
		name   string
		data   string
		wantIn string
	}{
		{"unknown top-level field", `{"version":1,"checks":[{"id":"a","command":["t"]}],"extra":1}`, "unknown field"},
		{"unknown check field", `{"version":1,"checks":[{"id":"a","command":["t"],"shell":true}]}`, "unknown field"},
		{"unknown report field", `{"version":1,"checks":[{"id":"a","command":["t"],"report":{"path":"r","format":"junit-xml","extra":1}}]}`, "unknown field"},
		{"duplicate ids", gateJSONTwoChecks("dup", "dup"), "duplicate id"},
		{"empty id", `{"version":1,"checks":[{"id":"","command":["t"]}]}`, "id is required"},
		{"empty command", `{"version":1,"checks":[{"id":"a","command":[]}]}`, "must not be empty"},
		{"empty argv element", `{"version":1,"checks":[{"id":"a","command":["t",""]}]}`, "empty elements"},
		{"empty version argv", `{"version":1,"checks":[{"id":"a","command":["t"],"version_command":[""]}]}`, "empty elements"},
		{"unknown format", `{"version":1,"checks":[{"id":"a","command":["t"],"report":{"path":"r","format":"cobertura"}}]}`, "unknown report format"},
		{"foreign threshold key", `{"version":1,"checks":[{"id":"a","command":["t"],"report":{"path":"r","format":"junit-xml"},"threshold":{"max_issues":1}}]}`, "unknown field"},
		{"negative threshold", `{"version":1,"checks":[{"id":"a","command":["t"],"report":{"path":"r","format":"junit-xml"},"threshold":{"max_failures":-1}}]}`, "negative"},
		{"negative timeout", `{"version":1,"checks":[{"id":"a","command":["t"],"timeout":-5}]}`, "negative"},
		{"wrong version", `{"version":2,"checks":[{"id":"a","command":["t"]}]}`, "unsupported gate file version"},
		{"missing version", `{"checks":[{"id":"a","command":["t"]}]}`, "unsupported gate file version"},
		{"empty checks", `{"version":1,"checks":[]}`, "no checks"},
		{"trailing content", `{"version":1,"checks":[{"id":"a","command":["t"]}]} {"version":1}`, "trailing content"},
		{"invalid json", `{`, "parse gate file"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := Load([]byte(test.data))
			if err == nil {
				t.Fatal("Load accepted an invalid gate file")
			}
			if !strings.Contains(err.Error(), test.wantIn) {
				t.Fatalf("Load = %v, want it to mention %q", err, test.wantIn)
			}
		})
	}
}

// gateJSONTwoChecks builds a gate file with two same-named checks.
func gateJSONTwoChecks(first, second string) string {
	return `{"version":1,"checks":[{"id":"` + first + `","command":["t"]},{"id":"` + second + `","command":["t"]}]}`
}
