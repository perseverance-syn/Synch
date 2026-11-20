package scanner

import (
	"encoding/json"
	"strings"

	"synch/internal/findings"
)

type trivyDocument struct {
	SchemaVersion int
	ArtifactName  string
	ArtifactType  string
	Results       []trivyResult
}

type trivyResult struct {
	Target            string
	Class             string
	Vulnerabilities   []trivyVulnerability
	Misconfigurations []trivyMisconfiguration
	Secrets           []json.RawMessage
	Licenses          []json.RawMessage
	CustomResources   []json.RawMessage
}

type trivyVulnerability struct {
	VulnerabilityID, PkgName, InstalledVersion, FixedVersion string
	Title, Severity, PrimaryURL                              string
	PkgIdentifier                                            struct{ PURL string }
}

type trivyMisconfiguration struct {
	ID, Title, Message, Severity, PrimaryURL, Status string
	CauseMetadata                                    struct{ StartLine, EndLine int }
}

// trivyReport validates the supported filesystem schema and normalizes redacted entries.
func (d *Driver) trivyReport(output []byte) ([]byte, []findings.Finding, error) {
	value, err := decodeValue(output)
	if err != nil {
		return nil, nil, err
	}
	safe, err := d.redactJSON(value)
	if err != nil {
		return nil, nil, err
	}
	report, err := json.Marshal(safe)
	if err != nil {
		return nil, nil, ErrReport
	}
	var doc trivyDocument
	if json.Unmarshal(report, &doc) != nil || doc.SchemaVersion == 0 || doc.ArtifactName == "" || doc.ArtifactType == "" {
		return nil, nil, ErrReport
	}
	if doc.SchemaVersion != 2 || doc.ArtifactType != "filesystem" {
		return nil, nil, ErrUnsupportedReport
	}
	var normalized []findings.Finding
	for _, result := range doc.Results {
		entries, err := result.normalize()
		if err != nil {
			return nil, nil, err
		}
		normalized = append(normalized, entries...)
	}
	return report, normalized, nil
}

// normalize translates all supported Trivy entries and rejects incomplete sections.
func (r trivyResult) normalize() ([]findings.Finding, error) {
	if r.Target == "" || r.Class == "" {
		return nil, ErrReport
	}
	if len(r.Secrets) != 0 || len(r.Licenses) != 0 || len(r.CustomResources) != 0 {
		return nil, ErrUnsupportedReport
	}
	switch r.Class {
	case "os-pkgs", "lang-pkgs", "config":
	default:
		return nil, ErrUnsupportedReport
	}
	var normalized []findings.Finding
	for _, v := range r.Vulnerabilities {
		severity, err := trivySeverity(v.Severity)
		if err != nil || v.VulnerabilityID == "" || v.PkgName == "" {
			return nil, ErrReport
		}
		normalized = append(normalized, findings.Finding{
			Rule:     findings.Rule{ID: v.VulnerabilityID, Title: v.Title, URL: v.PrimaryURL},
			Severity: severity, NativeSeverity: v.Severity,
			Component: findings.Component{Type: "package", Name: v.PkgName, Version: v.InstalledVersion, FixedVersion: v.FixedVersion, PURL: v.PkgIdentifier.PURL},
			Location:  findings.Location{Path: r.Target},
			Evidence:  findings.Evidence{Summary: firstText(v.Title, v.VulnerabilityID)},
		})
	}
	for _, m := range r.Misconfigurations {
		switch m.Status {
		case "PASS", "EXCEPTION":
			continue
		case "FAIL":
		default:
			return nil, ErrReport
		}
		severity, err := trivySeverity(m.Severity)
		start, end := m.CauseMetadata.StartLine, m.CauseMetadata.EndLine
		if err != nil || m.ID == "" || start < 0 || end < 0 || (end > 0 && end < start) {
			return nil, ErrReport
		}
		normalized = append(normalized, findings.Finding{
			Rule:     findings.Rule{ID: m.ID, Title: m.Title, URL: m.PrimaryURL},
			Severity: severity, NativeSeverity: m.Severity,
			Component: findings.Component{Type: "config", Name: r.Target},
			Location:  findings.Location{Path: r.Target, StartLine: start, EndLine: end},
			Evidence:  findings.Evidence{Summary: firstText(m.Message, m.Title, m.ID)},
		})
	}
	return normalized, nil
}

// trivySeverity accepts only the native severity vocabulary in section 4.
func trivySeverity(native string) (findings.Severity, error) {
	severity := findings.Severity(strings.ToLower(native))
	if severity.Rank() < 0 || severity == findings.Info || native != strings.ToUpper(native) {
		return "", ErrReport
	}
	return severity, nil
}

// firstText selects the most descriptive populated evidence field.
func firstText(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
