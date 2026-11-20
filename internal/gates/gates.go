package gates

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// --- Gate file: strict decoding of the ordered check definitions ---

// GateVersion is the only gate file version this package accepts.
const GateVersion = 1

// ReportFormat names one supported report parser.
type ReportFormat string

// The formats with parsers in this package. An unknown format is a load error
// and, if a caller assembles a check without Load, an error result at run time.
const (
	FormatJUnitXML     ReportFormat = "junit-xml"
	FormatGoTestJSON   ReportFormat = "go-test-json"
	FormatGolangciLint ReportFormat = "golangci-lint-json"
	FormatSarif        ReportFormat = "sarif"
)

// GateFile is the decoded .synch/gates.json document.
type GateFile struct {
	Version int     `json:"version"`
	Checks  []Check `json:"checks"`
}

// Check is one ordered gate check.
type Check struct {
	ID             string           `json:"id"`
	Command        []string         `json:"command"`
	Workdir        string           `json:"workdir,omitempty"`
	Timeout        int64            `json:"timeout,omitempty"`
	VersionCommand []string         `json:"version_command,omitempty"`
	Report         *ReportSpec      `json:"report,omitempty"`
	Threshold      *json.RawMessage `json:"threshold,omitempty"`
	Required       bool             `json:"required"`

	// maxAllowed is the format-specific failure count the threshold permits;
	// zero when no threshold is declared, so any failure fails the check.
	maxAllowed int
}

// ReportSpec points at the report file a check writes and its format.
type ReportSpec struct {
	Path   string       `json:"path"`
	Format ReportFormat `json:"format"`
}

// Load decodes a gate file strictly: unknown JSON fields at any level, an
// unsupported version, duplicate or empty check ids, empty argv, an unknown
// report format, a threshold key the format does not define, or a negative
// timeout are load errors. It returns the parsed file with the resolved
// per-format threshold.
func Load(data []byte) (GateFile, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var file GateFile
	if err := dec.Decode(&file); err != nil {
		return GateFile{}, fmt.Errorf("parse gate file: %w", err)
	}
	if err := trailingContent(dec); err != nil {
		return GateFile{}, err
	}
	if file.Version != GateVersion {
		return GateFile{}, fmt.Errorf("unsupported gate file version %d, want %d", file.Version, GateVersion)
	}
	if len(file.Checks) == 0 {
		return GateFile{}, errors.New("gate file has no checks")
	}
	seen := map[string]bool{}
	for i := range file.Checks {
		check := &file.Checks[i]
		if check.ID == "" {
			return GateFile{}, fmt.Errorf("check %d: id is required", i)
		}
		if seen[check.ID] {
			return GateFile{}, fmt.Errorf("check %q: duplicate id", check.ID)
		}
		seen[check.ID] = true
		if err := validateArgv(check.Command, check.ID, "command"); err != nil {
			return GateFile{}, err
		}
		if check.VersionCommand != nil {
			// An absent version command is fine; a present-but-empty one is not.
			if err := validateArgv(check.VersionCommand, check.ID, "version_command"); err != nil {
				return GateFile{}, err
			}
		}
		if check.Timeout < 0 {
			return GateFile{}, fmt.Errorf("check %q: timeout must not be negative", check.ID)
		}
		if check.Report == nil {
			continue
		}
		if check.Report.Path == "" {
			return GateFile{}, fmt.Errorf("check %q: report path is required", check.ID)
		}
		max, err := check.threshold(check.Report.Format)
		if err != nil {
			return GateFile{}, fmt.Errorf("check %q: %v", check.ID, err)
		}
		check.maxAllowed = max
	}
	return file, nil
}

// trailingContent rejects anything after the JSON document, such as truncated
// concatenations or appended data.
func trailingContent(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("trailing content after gate file: %w", err)
	}
	return nil
}

// validateArgv requires a non-empty argv without empty elements.
func validateArgv(argv []string, id, field string) error {
	if len(argv) == 0 {
		return fmt.Errorf("check %q: %s must not be empty", id, field)
	}
	for _, element := range argv {
		if element == "" {
			return fmt.Errorf("check %q: %s must not contain empty elements", id, field)
		}
	}
	return nil
}

// --- Threshold decoding: per-format limits on failures ---

// threshold decodes the format-specific threshold strictly and returns the
// maximum failure count it allows. A missing threshold allows zero.
func (c *Check) threshold(format ReportFormat) (int, error) {
	failures := func(name string, raw *json.RawMessage) (int, error) {
		if raw == nil {
			return 0, nil
		}
		spec := struct {
			MaxFailures *int `json:"max_failures"`
		}{}
		if err := strictDecode(*raw, &spec); err != nil {
			return 0, fmt.Errorf("threshold: %w", err)
		}
		if spec.MaxFailures == nil {
			return 0, nil
		}
		if *spec.MaxFailures < 0 {
			return 0, fmt.Errorf("threshold: %s must not be negative", name)
		}
		return *spec.MaxFailures, nil
	}
	switch format {
	case FormatJUnitXML, FormatGoTestJSON:
		return failures("max_failures", c.Threshold)
	case FormatGolangciLint:
		if c.Threshold == nil {
			return 0, nil
		}
		spec := struct {
			MaxIssues *int `json:"max_issues"`
		}{}
		if err := strictDecode(*c.Threshold, &spec); err != nil {
			return 0, fmt.Errorf("threshold: %w", err)
		}
		if spec.MaxIssues == nil {
			return 0, nil
		}
		if *spec.MaxIssues < 0 {
			return 0, errors.New("threshold: max_issues must not be negative")
		}
		return *spec.MaxIssues, nil
	case FormatSarif:
		if c.Threshold == nil {
			return 0, nil
		}
		spec := struct {
			MaxResults *int `json:"max_results"`
		}{}
		if err := strictDecode(*c.Threshold, &spec); err != nil {
			return 0, fmt.Errorf("threshold: %w", err)
		}
		if spec.MaxResults == nil {
			return 0, nil
		}
		if *spec.MaxResults < 0 {
			return 0, errors.New("threshold: max_results must not be negative")
		}
		return *spec.MaxResults, nil
	default:
		return 0, fmt.Errorf("unknown report format %q", format)
	}
}

// strictDecode decodes raw into value, rejecting unknown members and any
// trailing content after the object.
func strictDecode(raw json.RawMessage, value any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(value); err != nil {
		return err
	}
	var extra any
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing content in threshold")
	}
	return nil
}

// --- Digest: the exact-bytes config digest used for evidence reuse ---

// Digest returns the config digest of the raw gate file bytes: sha256 over
// exactly the bytes on disk, so any edit invalidates recorded evidence.
func Digest(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", sum)
}
