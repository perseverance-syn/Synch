package findings

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/big"
	"strconv"
	"time"
)

// --- Finding types and severity ---

// Finding matches the v2 scanner finding schema.
type Finding struct {
	FindingVersion int          `json:"finding_version"`
	ID             string       `json:"id"`
	Fingerprint    string       `json:"fingerprint"`
	Tool           Tool         `json:"tool"`
	Rule           Rule         `json:"rule"`
	Severity       Severity     `json:"severity"`
	NativeSeverity string       `json:"native_severity,omitempty"`
	Component      Component    `json:"component"`
	Location       Location     `json:"location"`
	Evidence       Evidence     `json:"evidence"`
	Revision       string       `json:"revision"`
	Provenance     []Provenance `json:"provenance"`
	Triage         *Triage      `json:"triage,omitempty"`
}

type Tool struct {
	Name         string `json:"name"`
	Version      string `json:"version"`
	BinarySHA256 string `json:"binary_sha256"`
	DBVersion    string `json:"db_version,omitempty"`
}

type Rule struct {
	ID    string `json:"id"`
	Title string `json:"title,omitempty"`
	URL   string `json:"url,omitempty"`
}

type Severity string

const (
	Unknown  Severity = "unknown"
	Info     Severity = "info"
	Low      Severity = "low"
	Medium   Severity = "medium"
	High     Severity = "high"
	Critical Severity = "critical"
)

// Rank orders supported severities for threshold evaluation.
func (s Severity) Rank() int {
	switch s {
	case Unknown:
		return 0
	case Info:
		return 1
	case Low:
		return 2
	case Medium:
		return 3
	case High:
		return 4
	case Critical:
		return 5
	default:
		return -1
	}
}

type Component struct {
	Type         string `json:"type"`
	Name         string `json:"name"`
	Version      string `json:"version,omitempty"`
	PURL         string `json:"purl,omitempty"`
	FixedVersion string `json:"fixed_version,omitempty"`
}

type Location struct {
	Path       string `json:"path"`
	StartLine  int    `json:"start_line,omitempty"`
	EndLine    int    `json:"end_line,omitempty"`
	Commit     string `json:"commit,omitempty"`
	ImageLayer string `json:"image_layer,omitempty"`
}

type Evidence struct {
	Summary         string    `json:"summary"`
	SnippetRedacted string    `json:"snippet_redacted,omitempty"`
	Report          ReportRef `json:"report"`
}

type ReportRef struct {
	URI      string `json:"uri"`
	Digest   string `json:"digest"`
	Redacted bool   `json:"redacted"`
}

type Provenance struct {
	ScanID   string    `json:"scan_id"`
	Tool     string    `json:"tool"`
	Report   ReportRef `json:"report"`
	NativeID string    `json:"native_id,omitempty"`
}

// --- Recorded payload and sealing ---

// RecordedPayload is the scanner branch of the finding.recorded v2 payload.
type RecordedPayload struct {
	Source  string  `json:"source"`
	Finding Finding `json:"finding"`
}

// Recorded returns the v2 scanner event payload for a normalized finding.
func (f Finding) Recorded() RecordedPayload {
	return RecordedPayload{Source: "scanner", Finding: f}
}

// Seal assigns the schema version, a ULID, and the contract fingerprint.
func (f *Finding) Seal() {
	f.FindingVersion = 2
	f.ID = newID()
	f.Fingerprint = Fingerprint(f.Tool.Name, f.Rule.ID, f.Component, f.Location)
}

// --- Identity: canonical fingerprints and ULIDs (scanner-driver section 4) ---

// Fingerprint hashes the fixed JCS object specified by scanner-driver section 4.
func Fingerprint(tool, rule string, component Component, location Location) string {
	data := []byte(`{"component":{"name":`)
	data = appendJSONString(data, component.Name)
	data = append(data, `,"type":`...)
	data = appendJSONString(data, component.Type)
	data = append(data, `,"version":`...)
	data = appendJSONString(data, component.Version)
	data = append(data, `},"location":{"path":`...)
	data = appendJSONString(data, location.Path)
	data = append(data, `,"start_line":`...)
	data = strconv.AppendInt(data, int64(location.StartLine), 10)
	data = append(data, `},"rule":`...)
	data = appendJSONString(data, rule)
	data = append(data, `,"tool":`...)
	data = appendJSONString(data, tool)
	data = append(data, '}')
	return Digest(data)
}

// appendJSONString applies JCS string escaping, including literal Unicode and HTML characters.
func appendJSONString(dst []byte, value string) []byte {
	const hex = "0123456789abcdef"
	dst = append(dst, '"')
	for _, char := range value {
		switch char {
		case '"', '\\':
			dst = append(dst, '\\', byte(char))
		case '\b':
			dst = append(dst, `\b`...)
		case '\f':
			dst = append(dst, `\f`...)
		case '\n':
			dst = append(dst, `\n`...)
		case '\r':
			dst = append(dst, `\r`...)
		case '\t':
			dst = append(dst, `\t`...)
		default:
			if char < 0x20 {
				dst = append(dst, '\\', 'u', '0', '0', hex[char>>4], hex[char&15])
			} else {
				dst = append(dst, string(char)...)
			}
		}
	}
	return append(dst, '"')
}

// Digest formats a SHA-256 digest as used for fingerprints and reports.
func Digest(data []byte) string {
	return fmt.Sprintf("sha256:%x", sha256.Sum256(data))
}

// newID encodes 48 bits of Unix milliseconds and 80 random bits as a ULID.
func newID() string {
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	var bits [16]byte
	binary.BigEndian.PutUint64(bits[:8], uint64(time.Now().UnixMilli())<<16)
	_, _ = rand.Read(bits[6:])
	value := new(big.Int).SetBytes(bits[:])
	base, digit := big.NewInt(32), new(big.Int)
	var id [26]byte
	for i := len(id) - 1; i >= 0; i-- {
		value.QuoRem(value, base, digit)
		id[i] = alphabet[digit.Int64()]
	}
	return string(id[:])
}
