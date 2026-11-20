package gates

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// --- Schema checks: the record matches the contract ---

// schemaPayloadProperties reads the e_check_result payload property names and
// the check_status enum straight from the contract schema.
func schemaPayloadProperties(t *testing.T) (map[string]bool, []string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "contracts", "events", "catalogue.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Defs map[string]struct {
			Enum       []string `json:"enum"`
			Properties struct {
				Payload struct {
					Properties map[string]json.RawMessage `json:"properties"`
					Required   []string                   `json:"required"`
				} `json:"payload"`
			} `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("decode catalogue schema: %v", err)
	}
	definition, ok := schema.Defs["e_check_result"]
	if !ok {
		t.Fatal("catalogue schema lacks e_check_result")
	}
	names := make(map[string]bool, len(definition.Properties.Payload.Properties))
	for name := range definition.Properties.Payload.Properties {
		names[name] = true
	}
	var statuses []string
	if statusDef, ok := schema.Defs["check_status"]; ok {
		statuses = statusDef.Enum
	}
	return names, statuses
}

// TestResultRecordMatchesCheckResultSchema proves G5: the result record's JSON
// field names are exactly e_check_result's payload members, the report
// reference carries uri and digest, the status vocabulary matches
// check_status, and config_digest is a sha256 ref.
func TestResultRecordMatchesCheckResultSchema(t *testing.T) {
	properties, statuses := schemaPayloadProperties(t)
	full := Result{
		Check:        "unit",
		Status:       StatusPassed,
		Reason:       "unused for passed",
		Revision:     "rev-1",
		ConfigDigest: "sha256:" + strings.Repeat("a", 64),
		ToolVersions: map[string]string{"tool": "v1"},
		Report:       &ReportRef{URI: "report.xml", Digest: "sha256:" + strings.Repeat("b", 64)},
	}
	raw, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	// additionalProperties: false: the record's members must be exactly the
	// schema's payload property names.
	if len(decoded) != len(properties) {
		t.Fatalf("record members %v, schema properties %v", keysOf(decoded), properties)
	}
	for name := range decoded {
		if !properties[name] {
			t.Fatalf("record member %q is not in e_check_result", name)
		}
		delete(properties, name)
	}
	if len(properties) != 0 {
		t.Fatalf("schema properties missing from the record: %v", properties)
	}
	report, ok := decoded["report"].(map[string]any)
	if !ok || len(report) != 2 || report["uri"] != full.Report.URI || report["digest"] != full.Report.Digest {
		t.Fatalf("report ref = %v", decoded["report"])
	}
	if !strings.HasPrefix(full.ConfigDigest, "sha256:") || len(full.ConfigDigest) != len("sha256:")+64 {
		t.Fatalf("config_digest shape = %q", full.ConfigDigest)
	}
	if len(statuses) != 5 {
		t.Fatalf("check_status enum = %v", statuses)
	}
	for _, status := range statuses {
		switch Status(status) {
		case StatusPassed, StatusFailed, StatusError, StatusPartial, StatusNotRun:
		default:
			t.Fatalf("status %q is missing from the package constants", status)
		}
	}
}

// --- Runner reason checks: every non-passed record explains itself ---

// TestRunnerResultsCarryReasons proves G5: every non-passed record the runner
// produces carries a non-empty reason, and passed records omit it.
func TestRunnerResultsCarryReasons(t *testing.T) {
	dir := t.TempDir()
	reportPath := filepath.Join(dir, "r.xml")
	reportBody := "<testsuite name=\"s\" tests=\"1\"><testcase name=\"a\"><failure message=\"x\"/></testcase></testsuite>"
	if err := os.WriteFile(reportPath, []byte(reportBody), 0600); err != nil {
		t.Fatal(err)
	}
	runner := newTestRunner(&mapExecutor{outcomes: map[string]struct {
		code int
		err  error
	}{
		"t timeout": {0, &TimeoutError{Seconds: 3}},
		"t missing": {0, nil},
		"t passing": {0, nil},
		"t failing": {1, nil},
	}})
	results, err := runner.Run(context.Background(), gateFile(t,
		Check{ID: "timeout", Command: []string{"t", "timeout"}, Report: &ReportSpec{Path: "r.xml", Format: FormatJUnitXML}},
		Check{ID: "missing", Command: []string{"t", "missing"}, Report: &ReportSpec{Path: "absent.xml", Format: FormatJUnitXML}},
		Check{ID: "passing", Command: []string{"t", "passing"}},
		Check{ID: "failing", Command: []string{"t", "failing"}, Required: true},
		Check{ID: "skipped", Command: []string{"t", "skipped"}},
	), RunInput{Dir: "rev-1"})
	if err != nil {
		t.Fatal(err)
	}
	want := []Status{StatusError, StatusError, StatusPassed, StatusFailed, StatusNotRun}
	for i, result := range results {
		if result.Status != want[i] {
			t.Fatalf("result %d status = %s, want %s (%+v)", i, result.Status, want[i], result)
		}
		if result.Status == StatusPassed && result.Reason != "" {
			t.Fatalf("passed record %d carries a reason: %+v", i, result)
		}
		if result.Status != StatusPassed && strings.TrimSpace(result.Reason) == "" {
			t.Fatalf("record %d with status %s lacks a reason: %+v", i, result.Status, result)
		}
	}
}

// --- Reuse: stored records of every status ---

// TestReuseOfErroredRecords proves G6 applies to every status: a stored error
// record with matching inputs is reused without running.
func TestReuseOfErroredRecords(t *testing.T) {
	file := gateFile(t, Check{ID: "a", Command: []string{"a"}})
	executor := &scriptedExecutor{err: errors.New("boom")}
	runner := newTestRunner(executor)
	first, err := runner.Run(context.Background(), file, RunInput{Dir: "rev-1"})
	if err != nil {
		t.Fatal(err)
	}
	if first[0].Status != StatusError {
		t.Fatalf("first run = %+v", first[0])
	}
	again := &scriptedExecutor{code: 0}
	reused, err := newTestRunner(again).Run(context.Background(), file, RunInput{Dir: "rev-1", Stored: map[string]Result{"a": first[0]}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(reused[0], first[0]) || len(again.runs) != 0 {
		t.Fatalf("errored record was not reused: %+v, runs %v", reused[0], again.runs)
	}
}

// keysOf lists a map's keys as a sorted slice for stable failure messages.
func keysOf(value map[string]any) []string {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
