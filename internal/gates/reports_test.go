package gates

import (
	"strings"
	"testing"
)

// TestJUnitParserCountsAndItems proves G3: the junit parser returns the
// failure count and one item per test case with its outcome and message.
func TestJUnitParserCountsAndItems(t *testing.T) {
	report := `<testsuites name="all">
  <testsuite name="pkg-a">
    <testcase name="ok" classname="a"/><testcase name="bad" classname="a">
      <failure message="boom">stack</failure></testcase>
    <testcase name="hmm" classname="a"><error message="e"/></testcase>
    <testcase name="later" classname="a"><skipped message="why"/></testcase>
  </testsuite>
  <testsuite name="pkg-b"><testcase name="fine" classname="b"/></testsuite>
</testsuites>`
	parsed, err := parseReport(FormatJUnitXML, []byte(report))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Unreadable != 0 || len(parsed.Items) != 5 {
		t.Fatalf("parsed = %+v", parsed)
	}
	failures := 0
	for _, item := range parsed.Items {
		switch item.Name {
		case "bad":
			if item.Status != StatusFailed || item.Detail != "boom" {
				t.Fatalf("item bad = %+v", item)
			}
			failures++
		case "hmm":
			if item.Status != StatusFailed || item.Detail != "e" {
				t.Fatalf("item hmm = %+v", item)
			}
			failures++
		case "later":
			if item.Status != StatusSkipped {
				t.Fatalf("item later = %+v", item)
			}
		case "ok", "fine":
			if item.Status != StatusPassed {
				t.Fatalf("item %s = %+v", item.Name, item)
			}
		default:
			t.Fatalf("unexpected item %+v", item)
		}
	}
	if failures != 2 {
		t.Fatalf("failure count = %d, want 2", failures)
	}
}

// TestGoTestParserCountsAndItems proves G3: the go-test-json parser returns
// per-test outcomes, treats a package-level fail as a failure, and ignores
// non-outcome events.
func TestGoTestParserCountsAndItems(t *testing.T) {
	report := "{\"Action\":\"start\",\"Package\":\"pkg\"}\n" +
		"{\"Action\":\"run\",\"Package\":\"pkg\",\"Test\":\"TestA\"}\n" +
		"{\"Action\":\"output\",\"Package\":\"pkg\",\"Test\":\"TestA\",\"Output\":\"--- FAIL\\n\"}\n" +
		"{\"Action\":\"fail\",\"Package\":\"pkg\",\"Test\":\"TestA\"}\n" +
		"{\"Action\":\"pass\",\"Package\":\"pkg\",\"Test\":\"TestB\"}\n" +
		"{\"Action\":\"skip\",\"Package\":\"pkg\",\"Test\":\"TestC\"}\n" +
		"{\"Action\":\"fail\",\"Package\":\"pkg\"}\n"
	parsed, err := parseReport(FormatGoTestJSON, []byte(report))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Unreadable != 0 || len(parsed.Items) != 4 {
		t.Fatalf("parsed = %+v", parsed)
	}
	if parsed.Items[0].Status != StatusFailed || parsed.Items[0].Name != "pkg/TestA" {
		t.Fatalf("item 0 = %+v", parsed.Items[0])
	}
	if parsed.Items[1].Status != StatusPassed || parsed.Items[2].Status != StatusSkipped {
		t.Fatalf("items 1-2 = %+v %+v", parsed.Items[1], parsed.Items[2])
	}
	if parsed.Items[3].Name != "pkg/" || parsed.Items[3].Status != StatusFailed || parsed.Items[3].Detail != "package-level failure" {
		t.Fatalf("package-level item = %+v", parsed.Items[3])
	}
	failures := 0
	for _, item := range parsed.Items {
		if item.Status == StatusFailed {
			failures++
		}
	}
	if failures != 2 {
		t.Fatalf("failure count = %d, want 2", failures)
	}
}

// TestGolangciParserCountsAndItems proves G3: the golangci-lint parser returns
// one failed item per issue with linter, position and text.
func TestGolangciParserCountsAndItems(t *testing.T) {
	report := `[{"FromLinter":"revive","Text":"unused parameter","Pos":{"Filename":"a.go","Line":3,"Column":7}},
 {"FromLinter":"govet","Text":"shadow","Pos":{"Filename":"b.go","Line":9}}]`
	parsed, err := parseReport(FormatGolangciLint, []byte(report))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Unreadable != 0 || len(parsed.Items) != 2 {
		t.Fatalf("parsed = %+v", parsed)
	}
	if parsed.Items[0].Name != "revive: a.go:3" || parsed.Items[0].Status != StatusFailed || parsed.Items[0].Detail != "unused parameter" {
		t.Fatalf("item 0 = %+v", parsed.Items[0])
	}
	if parsed.Items[1].Name != "govet: b.go:9" {
		t.Fatalf("item 1 = %+v", parsed.Items[1])
	}
}

// TestSarifParserCountsAndItems proves G3: the sarif parser returns one failed
// item per result across every run, with the driver and rule names.
func TestSarifParserCountsAndItems(t *testing.T) {
	report := `{"version":"2.1.0","runs":[
 {"tool":{"driver":{"name":"scanner-a"}},"results":[
   {"ruleId":"R1","level":"error","message":{"text":"first"}},
   {"ruleId":"R2","level":"warning","message":{"text":"second"}}]},
 {"tool":{"driver":{"name":"scanner-b"}},"results":[
   {"ruleId":"R3","message":{"text":"third"}}]}]}`
	parsed, err := parseReport(FormatSarif, []byte(report))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Unreadable != 0 || len(parsed.Items) != 3 {
		t.Fatalf("parsed = %+v", parsed)
	}
	if parsed.Items[0].Name != "scanner-a/R1" || parsed.Items[0].Detail != "first" || parsed.Items[0].Status != StatusFailed {
		t.Fatalf("item 0 = %+v", parsed.Items[0])
	}
	if parsed.Items[2].Name != "scanner-b/R3" {
		t.Fatalf("item 2 = %+v", parsed.Items[2])
	}
}

// TestParseReportUnknownFormat proves G3's dispatch rejects unknown formats,
// which keeps an unsupported report from ever producing a pass.
func TestParseReportUnknownFormat(t *testing.T) {
	_, err := parseReport(ReportFormat("cobertura"), []byte("<coverage/>"))
	if err == nil || !strings.Contains(err.Error(), "unsupported report format") {
		t.Fatalf("parseReport = %v", err)
	}
}
