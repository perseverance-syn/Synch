package gates

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
)

// --- Report parsers: junit-xml, go-test-json, golangci-lint-json, sarif ---

// ReportItem is one readable entry of a report: a test case, a test, a lint
// issue or a SARIF result. Status is passed, failed or skipped for test-like
// formats and failed for finding-like formats, where every entry is a failure.
type ReportItem struct {
	Name   string
	Status Status
	Detail string
}

// parsedReport is the outcome of parsing one report document. Unreadable
// counts entries that were present but could not be decoded; Readable counts
// elements that decoded correctly, including entries and structurally valid
// runs. A report with unreadable entries and nothing readable is malformed;
// with something readable alongside them it is only partial.
type parsedReport struct {
	Items      []ReportItem
	Readable   int
	Unreadable int
}

// parseReport parses one report document with the format's parser. A returned
// error means the document itself is malformed or unsupported, which is an
// error status and never a pass.
func parseReport(format ReportFormat, data []byte) (parsedReport, error) {
	switch format {
	case FormatJUnitXML:
		return parseJUnitXML(data)
	case FormatGoTestJSON:
		return parseGoTestJSON(data)
	case FormatGolangciLint:
		return parseGolangciLint(data)
	case FormatSarif:
		return parseSarif(data)
	default:
		return parsedReport{}, fmt.Errorf("unsupported report format %q", format)
	}
}

// --- junit-xml ---

// junitSuite mirrors the parts of a <testsuite> element the runner needs.
type junitSuite struct {
	Name      string       `xml:"name,attr"`
	TestCases []junitCase  `xml:"testcase"`
	Suites    []junitSuite `xml:"testsuite"`
}

// junitCase mirrors one <testcase> element; failure, error and skipped mark
// the outcome, and their absence means the case passed.
type junitCase struct {
	Name    string `xml:"name,attr"`
	Failure *struct {
		Message string `xml:"message,attr"`
		Text    string `xml:",chardata"`
	} `xml:"failure"`
	Error *struct {
		Message string `xml:"message,attr"`
		Text    string `xml:",chardata"`
	} `xml:"error"`
	Skipped *struct {
		Message string `xml:"message,attr"`
	} `xml:"skipped"`
}

// parseJUnitXML parses a junit report. The root must be <testsuite> or
// <testsuites> and carry a name; a document with no test cases is an error,
// because a gate over zero tests verifies nothing; a test case without a name
// is an unreadable entry.
func parseJUnitXML(data []byte) (parsedReport, error) {
	var root struct {
		XMLName xml.Name
		junitSuite
	}
	dec := xml.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&root); err != nil {
		return parsedReport{}, fmt.Errorf("invalid junit XML: %w", err)
	}
	if err := junitTrailing(dec); err != nil {
		return parsedReport{}, err
	}
	switch root.XMLName.Local {
	case "testsuite", "testsuites":
	default:
		return parsedReport{}, fmt.Errorf("wrong junit root element <%s>, want <testsuite> or <testsuites>", root.XMLName.Local)
	}
	if root.Name == "" {
		return parsedReport{}, errors.New("junit root suite is missing the required name attribute")
	}
	var parsed parsedReport
	var walk func(suite junitSuite)
	walk = func(suite junitSuite) {
		for _, testCase := range suite.TestCases {
			item, ok := junitItem(testCase)
			if !ok {
				parsed.Unreadable++
				continue
			}
			parsed.Readable++
			parsed.Items = append(parsed.Items, item)
		}
		for _, nested := range suite.Suites {
			walk(nested)
		}
	}
	walk(root.junitSuite)
	if len(parsed.Items)+parsed.Unreadable == 0 {
		return parsedReport{}, errors.New("junit report contains no test cases")
	}
	return parsed, nil
}

// junitTrailing rejects anything after the root element except whitespace,
// comments and processing instructions, so a valid root followed by garbage
// is not a valid report.
func junitTrailing(dec *xml.Decoder) error {
	for {
		tok, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("invalid junit XML after the root element: %w", err)
		}
		switch t := tok.(type) {
		case xml.Comment, xml.ProcInst:
		case xml.CharData:
			if len(bytes.TrimSpace(t)) > 0 {
				return errors.New("invalid junit XML: content after the root element")
			}
		default:
			return errors.New("invalid junit XML: content after the root element")
		}
	}
}

// junitItem converts one test case, reporting false when the case has no name
// and therefore cannot be identified.
func junitItem(testCase junitCase) (ReportItem, bool) {
	if testCase.Name == "" {
		return ReportItem{}, false
	}
	item := ReportItem{Name: testCase.Name, Status: StatusPassed}
	if testCase.Failure != nil || testCase.Error != nil {
		item.Status = StatusFailed
		if testCase.Failure != nil {
			item.Detail = firstNonEmpty(testCase.Failure.Message, testCase.Failure.Text)
		} else {
			item.Detail = firstNonEmpty(testCase.Error.Message, testCase.Error.Text)
		}
	} else if testCase.Skipped != nil {
		item.Status = StatusSkipped
		item.Detail = testCase.Skipped.Message
	}
	return item, true
}

// StatusSkipped marks a test the tool did not run; it is an item status, not
// a check status.
const StatusSkipped = "skipped"

// --- go-test-json (go test -json) ---

// goTestEvent mirrors one line of go test -json output.
type goTestEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test"`
	Output  string `json:"Output"`
}

// parseGoTestJSON parses go test -json lines. A line that is not valid JSON,
// or an event without Action or Package, is an unreadable entry; an unknown
// action rejects the whole report. Only pass, fail and skip events are
// outcomes, and a fail event without a Test name is a package-level failure
// such as a build error. A package that reports events but never a
// package-level outcome was cut off, so the stream counts one unreadable
// entry for it and the result is partial at best.
func parseGoTestJSON(data []byte) (parsedReport, error) {
	var parsed parsedReport
	seen := map[string]bool{}
	finished := map[string]bool{}
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var event goTestEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			// A line-based format has no document to be malformed: an
			// undecodable line is an unreadable entry like any other.
			parsed.Unreadable++
			continue
		}
		switch event.Action {
		case "build-output", "build-fail":
			// Build events carry an ImportPath, not a Package, and no outcome.
			parsed.Readable++
			continue
		case "start", "run", "pause", "cont", "output", "bench", "pass", "fail", "skip":
		case "":
			parsed.Unreadable++
			continue
		default:
			return parsedReport{}, fmt.Errorf("go-test-json report has unknown action %q", event.Action)
		}
		if event.Package == "" {
			parsed.Unreadable++
			continue
		}
		parsed.Readable++
		seen[event.Package] = true
		switch event.Action {
		case "pass", "fail", "skip":
			status := StatusPassed
			switch event.Action {
			case "fail":
				status = StatusFailed
			case "skip":
				status = StatusSkipped
			}
			if event.Test == "" {
				finished[event.Package] = true
			}
			item := ReportItem{Name: event.Package + "/" + event.Test, Status: status, Detail: event.Output}
			if event.Test == "" && event.Action == "fail" {
				item.Detail = "package-level failure"
			}
			parsed.Items = append(parsed.Items, item)
		default:
			// start, run, pause, cont, output and bench lines carry no outcome.
		}
	}
	for pkg := range seen {
		if !finished[pkg] {
			parsed.Unreadable++
		}
	}
	if len(parsed.Items)+parsed.Unreadable == 0 {
		return parsedReport{}, errors.New("go-test-json report contains no events")
	}
	return parsed, nil
}

// --- golangci-lint-json ---

// golangciIssue mirrors one issue object of golangci-lint's JSON output.
type golangciIssue struct {
	FromLinter string `json:"FromLinter"`
	Text       string `json:"Text"`
	Pos        struct {
		Filename string `json:"Filename"`
		Line     int    `json:"Line"`
	} `json:"Pos"`
}

// parseGolangciLint parses golangci-lint's JSON document. v2 writes an object
// whose Issues member is the issue array (empty when the run is clean); a bare
// array is accepted too. JSON null, a missing or null Issues member, or any
// other root is an error. An element that is not an issue object, or lacks the
// linter name or the text, is an unreadable entry.
func parseGolangciLint(data []byte) (parsedReport, error) {
	raw := bytes.TrimSpace(data)
	if len(raw) > 0 && raw[0] == '{' {
		var doc map[string]json.RawMessage // exact member names, unlike a struct decode
		if err := json.Unmarshal(raw, &doc); err != nil {
			return parsedReport{}, fmt.Errorf("invalid golangci-lint JSON: %w", err)
		}
		raw = bytes.TrimSpace(doc["Issues"])
	}
	if len(raw) == 0 || raw[0] != '[' {
		return parsedReport{}, errors.New("golangci-lint report has no Issues array")
	}
	var elements []json.RawMessage
	if err := json.Unmarshal(raw, &elements); err != nil {
		return parsedReport{}, fmt.Errorf("invalid golangci-lint issues array: %w", err)
	}
	var parsed parsedReport
	for _, element := range elements {
		var issue golangciIssue
		if err := json.Unmarshal(element, &issue); err != nil || issue.FromLinter == "" || issue.Text == "" {
			parsed.Unreadable++
			continue
		}
		parsed.Readable++
		parsed.Items = append(parsed.Items, ReportItem{
			Name:   fmt.Sprintf("%s: %s:%d", issue.FromLinter, issue.Pos.Filename, issue.Pos.Line),
			Status: StatusFailed,
			Detail: issue.Text,
		})
	}
	return parsed, nil
}

// --- sarif ---

// sarifVersion is the only SARIF version the parser accepts.
const sarifVersion = "2.1.0"

// sarifReport mirrors the parts of a SARIF log the runner needs: the version,
// every run's tool driver name and each run's results.
type sarifReport struct {
	Version string `json:"version"`
	Runs    []struct {
		Tool struct {
			Driver struct {
				Name string `json:"name"`
			} `json:"driver"`
		} `json:"tool"`
		Results []struct {
			RuleID  string `json:"ruleId"`
			Level   string `json:"level"`
			Message struct {
				Text string `json:"text"`
			} `json:"message"`
		} `json:"results"`
	} `json:"runs"`
}

// parseSarif parses a SARIF log. The version member must be exactly 2.1.0 and the
// runs array is required; a run without a tool driver name and a result without a rule ID
// or message text are unreadable entries. Zero results is a clean report.
func parseSarif(data []byte) (parsedReport, error) {
	var report sarifReport
	if err := json.Unmarshal(data, &report); err != nil {
		return parsedReport{}, fmt.Errorf("invalid SARIF JSON: %w", err)
	}
	if report.Version != sarifVersion {
		return parsedReport{}, fmt.Errorf("sarif version %q is not supported, want %s", report.Version, sarifVersion)
	}
	if report.Runs == nil {
		return parsedReport{}, errors.New("sarif report is missing the required runs array")
	}
	var parsed parsedReport
	for _, run := range report.Runs {
		if run.Tool.Driver.Name == "" {
			parsed.Unreadable++
			continue
		}
		parsed.Readable++
		for _, result := range run.Results {
			if result.RuleID == "" || result.Message.Text == "" {
				parsed.Unreadable++
				continue
			}
			parsed.Readable++
			parsed.Items = append(parsed.Items, ReportItem{
				Name:   run.Tool.Driver.Name + "/" + result.RuleID,
				Status: StatusFailed,
				Detail: result.Message.Text,
			})
		}
	}
	return parsed, nil
}

// firstNonEmpty returns the first value that is not empty.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
