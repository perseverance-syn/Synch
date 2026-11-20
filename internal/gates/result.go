package gates

import (
	"crypto/sha256"
	"fmt"
)

// --- check.result records (catalogue.schema.json $defs/e_check_result) ---

// Status is one check status of $defs/check_status.
type Status string

// The five statuses a check result may carry. A malformed or unsupported
// report never produces StatusPassed.
const (
	StatusPassed  Status = "passed"
	StatusFailed  Status = "failed"
	StatusError   Status = "error"
	StatusPartial Status = "partial"
	StatusNotRun  Status = "not_run"
)

// Result is one check.result payload record. The field set is exactly
// e_check_result's payload members (additionalProperties false); the daemon
// owns the event envelope, including stream and thread_id.
type Result struct {
	Check        string            `json:"check"`
	Status       Status            `json:"status"`
	Reason       string            `json:"reason,omitempty"`
	Revision     string            `json:"revision"`
	ConfigDigest string            `json:"config_digest,omitempty"`
	ToolVersions map[string]string `json:"tool_versions,omitempty"`
	Report       *ReportRef        `json:"report,omitempty"`
}

// ReportRef points at a report file: its path and the sha256 of its bytes.
type ReportRef struct {
	URI    string `json:"uri"`
	Digest string `json:"digest"`
}

// reportRef builds the report reference from the file's bytes. An empty byte
// slice has a digest too: the sha256 of nothing, which still identifies the
// missing content and keeps the reference honest.
func reportRef(uri string, data []byte) *ReportRef {
	sum := sha256.Sum256(data)
	return &ReportRef{URI: uri, Digest: fmt.Sprintf("sha256:%x", sum)}
}

// skipReason is the contract's exact reason for a check an earlier required
// failure prevented from running.
const skipReason = "skipped: earlier required check failed"

// validStored reports whether a stored record is well formed: a status from
// the contract's vocabulary, and a reason whenever the status is not passed.
func validStored(stored Result) bool {
	switch stored.Status {
	case StatusPassed:
		return true
	case StatusFailed, StatusError, StatusPartial, StatusNotRun:
		return stored.Reason != ""
	default:
		return false
	}
}

// reusable reports whether a stored result may stand in for a fresh run: it
// must be a valid record of this check, and the source revision, the gate file
// digest and the probed tool versions must all match exactly, otherwise the
// evidence is stale and the check reruns.
func reusable(stored Result, id, revision, digest string, versions map[string]string) bool {
	if stored.Check != id || !validStored(stored) {
		return false
	}
	if stored.Revision != revision || stored.ConfigDigest != digest {
		return false
	}
	if len(stored.ToolVersions) != len(versions) {
		return false
	}
	for tool, version := range versions {
		if stored.ToolVersions[tool] != version {
			return false
		}
	}
	return true
}

// notRunResult is the record for a check that never started, with the reason
// it did not: an earlier required failure or an interrupted run.
func notRunResult(check, revision, digest, reason string) Result {
	return Result{
		Check:        check,
		Status:       StatusNotRun,
		Reason:       reason,
		Revision:     revision,
		ConfigDigest: digest,
	}
}
