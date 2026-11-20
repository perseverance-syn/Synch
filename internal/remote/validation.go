package remote

import (
	"bytes"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"regexp"
	"slices"

	"synch/internal/eventlog"
)

var (
	runnerID          = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	ulid              = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	digest            = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	ruleID            = regexp.MustCompile(`^(operator|project|role|skill|builtin):[^# ]+#[a-z0-9][a-z0-9._-]*$`)
	errInvalidRecord  = errors.New("invalid runner journal record")
	errInvalidPayload = errors.New("invalid runner journal payload")
)

// field distinguishes an absent member from a provided zero value.
type field[T any] struct {
	value   T
	present bool
}

// UnmarshalJSON decodes an explicitly provided field and rejects JSON null.
func (f *field[T]) UnmarshalJSON(raw []byte) error {
	if bytes.Equal(raw, []byte("null")) {
		return errInvalidPayload
	}
	f.present = true
	return decodeStrict(raw, &f.value)
}

// decodeStrict rejects unknown and duplicate members and requires exact JSON member names.
func decodeStrict(raw []byte, into any) error {
	return jsonv2.Unmarshal(raw, into, jsonv2.RejectUnknownMembers(true))
}

// recordShape wraps the optional redaction count so explicit null is rejected.
type recordShape struct {
	Version     int             `json:"runner_event_version"`
	Runner      string          `json:"runner"`
	SourceSeq   int64           `json:"source_seq"`
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	TypeVersion int             `json:"type_version"`
	OccurredAt  json.RawMessage `json:"occurred_at"`
	ThreadID    string          `json:"thread_id"`
	Redactions  field[int]      `json:"redactions"`
	Payload     json.RawMessage `json:"payload"`
}

// decodeRecord verifies the wire shape before converting to the event log's public type.
func decodeRecord(raw []byte) (eventlog.RunnerRecord, error) {
	var shape recordShape
	var record eventlog.RunnerRecord
	if err := decodeStrict(raw, &shape); err != nil {
		return record, errInvalidRecord
	}
	if err := json.Unmarshal(raw, &record); err != nil {
		return record, errInvalidRecord
	}
	return record, validateRecord(record)
}

// validateRecord enforces the runner envelope, type version and catalogue payload before acknowledgement.
func validateRecord(record eventlog.RunnerRecord) error {
	if record.RunnerEventVersion != 2 || !runnerID.MatchString(record.Runner) || record.SourceSeq < 1 ||
		!ulid.MatchString(record.ID) || !ulid.MatchString(record.ThreadID) || record.OccurredAt.IsZero() || record.Redactions < 0 {
		return errInvalidRecord
	}
	var payload interface{ valid() bool }
	version := 1
	switch record.Type {
	case "engine.output":
		payload = &outputPayload{}
	case "engine.usage":
		payload = &usagePayload{}
	case "engine.capability_unavailable":
		payload = &capabilityPayload{}
	case "process.exited":
		payload = &exitPayload{}
	case "check.result":
		payload = &checkPayload{}
	case "policy.decided":
		payload, version = &policyPayload{}, 2
	default:
		return errInvalidRecord
	}
	if record.TypeVersion != version {
		return errInvalidRecord
	}
	if err := decodeStrict(record.Payload, payload); err != nil || !payload.valid() {
		return fmt.Errorf("%s: %w", record.Type, errInvalidPayload)
	}
	return nil
}

// --- Payloads emitted by the supervisor (event catalogue §3) ---

type artifactPayload struct {
	URI    string `json:"uri"`
	Digest string `json:"digest"`
}

// valid checks the nonempty URI and canonical SHA-256 digest.
func (p artifactPayload) valid() bool {
	return p.URI != "" && digest.MatchString(p.Digest)
}

type outputPayload struct {
	Assignment string                 `json:"assignment_id"`
	Channel    string                 `json:"channel"`
	Chunk      field[string]          `json:"chunk"`
	Full       field[artifactPayload] `json:"full_output"`
	Truncated  field[bool]            `json:"truncated"`
}

// valid checks the byte cap, channel and artifact required for truncated output.
func (p *outputPayload) valid() bool {
	return ulid.MatchString(p.Assignment) && slices.Contains([]string{"stdout", "stderr", "message", "tool_call", "tool_result"}, p.Channel) &&
		p.Chunk.present && len(p.Chunk.value) <= 65536 && (!p.Truncated.value || p.Full.present) && (!p.Full.present || p.Full.value.valid())
}

type exitPayload struct {
	Assignment string      `json:"assignment_id"`
	Outcome    string      `json:"execution_outcome"`
	Work       string      `json:"work"`
	Code       field[int]  `json:"exit_code"`
	Cleaned    field[bool] `json:"descendants_cleaned"`
}

// valid checks the assignment and the execution and work outcome enums.
func (p *exitPayload) valid() bool {
	return ulid.MatchString(p.Assignment) &&
		slices.Contains([]string{"exited", "killed", "stalled", "lost_on_reboot", "cancelled"}, p.Outcome) &&
		slices.Contains([]string{"complete", "resumable", "lost"}, p.Work)
}

// --- Other locally observable facts (event catalogue §3) ---

type usagePayload struct {
	Assignment string        `json:"assignment_id"`
	Reported   field[bool]   `json:"reported"`
	Input      field[uint64] `json:"input_tokens"`
	Output     field[uint64] `json:"output_tokens"`
	Elapsed    field[uint64] `json:"elapsed_ms"`
}

// valid requires a reporting flag and forbids token counts if usage was not reported.
func (p *usagePayload) valid() bool {
	return ulid.MatchString(p.Assignment) && p.Reported.present && (p.Reported.value || !p.Input.present && !p.Output.present)
}

type capabilityPayload struct {
	Assignment string        `json:"assignment_id"`
	Harness    field[string] `json:"harness"`
	Operation  field[string] `json:"operation"`
	Fallback   field[string] `json:"fallback"`
}

// valid checks the assignment and presence of the required strings.
func (p *capabilityPayload) valid() bool {
	return ulid.MatchString(p.Assignment) && p.Harness.present && p.Operation.present
}

type checkPayload struct {
	Check    string                          `json:"check"`
	Status   string                          `json:"status"`
	Revision string                          `json:"revision"`
	Reason   field[string]                   `json:"reason"`
	Config   field[string]                   `json:"config_digest"`
	Tools    field[map[string]field[string]] `json:"tool_versions"`
	Report   field[artifactPayload]          `json:"report"`
}

// valid enforces a reason for checks that did not pass and validates optional evidence.
func (p *checkPayload) valid() bool {
	return p.Check != "" && p.Revision != "" && slices.Contains([]string{"passed", "failed", "error", "partial", "not_run"}, p.Status) &&
		(p.Status == "passed" && !p.Reason.present || p.Reason.value != "") &&
		(!p.Config.present || digest.MatchString(p.Config.value)) && (!p.Report.present || p.Report.value.valid())
}

type policyPayload struct {
	Action   string        `json:"action"`
	Machine  string        `json:"machine"`
	Decision string        `json:"decision"`
	Outcome  string        `json:"outcome"`
	Rule     field[string] `json:"rule_id"`
	Target   field[string] `json:"target"`
	Approval field[string] `json:"approval_id"`
	Grant    field[string] `json:"grant_id"`
	Command  field[string] `json:"command"`
}

// valid checks the decision and outcome enums and the optional qualified rule ID.
func (p *policyPayload) valid() bool {
	return p.Action != "" && p.Machine != "" && slices.Contains([]string{"allow", "deny", "ask"}, p.Decision) &&
		slices.Contains([]string{"executed", "blocked", "waiting_approval", "failed"}, p.Outcome) &&
		(!p.Rule.present || ruleID.MatchString(p.Rule.value))
}
