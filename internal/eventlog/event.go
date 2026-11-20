package eventlog

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
	"unicode/utf8"
)

type Actor struct {
	Kind    string `json:"kind"`
	ID      string `json:"id"`
	Machine string `json:"machine,omitempty"`
}
type Source struct {
	Runner string `json:"runner"`
	Seq    int64  `json:"seq"`
}
type NewEvent struct {
	ID          string          `json:"id"`
	Stream      string          `json:"stream"`
	Type        string          `json:"type"`
	TypeVersion int             `json:"type_version"`
	Actor       Actor           `json:"actor"`
	ThreadID    string          `json:"thread_id,omitempty"`
	RequestID   string          `json:"request_id,omitempty"`
	CausationID string          `json:"causation_id,omitempty"`
	OccurredAt  *time.Time      `json:"occurred_at,omitempty"`
	Payload     json.RawMessage `json:"payload"`
}
type Event struct {
	NewEvent
	EnvelopeVersion int       `json:"envelope_version"`
	Seq             int64     `json:"seq"`
	GlobalSeq       int64     `json:"global_seq"`
	RecordedAt      time.Time `json:"recorded_at"`
	Source          *Source   `json:"source,omitempty"`
	Redactions      int       `json:"redactions"`
}
type RunnerRecord struct {
	RunnerEventVersion int             `json:"runner_event_version"`
	Runner             string          `json:"runner"`
	SourceSeq          int64           `json:"source_seq"`
	ID                 string          `json:"id"`
	Type               string          `json:"type"`
	TypeVersion        int             `json:"type_version"`
	OccurredAt         time.Time       `json:"occurred_at"`
	ThreadID           string          `json:"thread_id"`
	Redactions         int             `json:"redactions,omitempty"`
	Payload            json.RawMessage `json:"payload"`
}

type Redactor func(json.RawMessage) (json.RawMessage, int, error)

var (
	ulid      = regexp.MustCompile("^[0-9A-HJKMNP-TV-Z]{26}$")
	eventType = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)
	requestID = regexp.MustCompile("^[A-Za-z0-9._:-]{1,128}$")
	runnerID  = regexp.MustCompile("^[A-Za-z0-9._-]{1,64}$")
)

// validate checks the envelope and its stream binding before persistence.
func (e NewEvent) validate(ingested bool) error {
	if !ulid.MatchString(e.ID) || !eventType.MatchString(e.Type) || e.TypeVersion < 1 {
		return fmt.Errorf("invalid event id, type or type_version")
	}
	if e.CausationID != "" && !ulid.MatchString(e.CausationID) || e.RequestID != "" && !requestID.MatchString(e.RequestID) {
		return fmt.Errorf("invalid correlation fields")
	}
	system := e.Type == "resource.pressure" || e.Type == "runner.connection"
	if (e.Stream == "system") != system || !system && (e.Stream != "thread:"+e.ThreadID || !ulid.MatchString(e.ThreadID)) || e.ThreadID != "" && !ulid.MatchString(e.ThreadID) {
		return fmt.Errorf("invalid stream binding for %s", e.Type)
	}
	switch e.Actor.Kind {
	case "runner":
		if !ingested || e.OccurredAt == nil {
			return fmt.Errorf("runner requires a journal record")
		}
	case "operator", "daemon", "coordinator", "agent", "driver", "client":
	default:
		return fmt.Errorf("unknown actor kind")
	}
	if e.Actor.ID == "" {
		return fmt.Errorf("missing actor id")
	}
	return validatePayload(e.Type, e.Payload)
}

// validatePayload checks JSON object payloads and the size and reference rules for engine output.
func validatePayload(typ string, p json.RawMessage) error {
	var fields map[string]json.RawMessage
	if !utf8.Valid(p) || json.Unmarshal(p, &fields) != nil || fields == nil {
		return fmt.Errorf("payload must be a UTF-8 JSON object")
	}
	if typ != "engine.output" {
		return nil
	}
	var chunk *string
	if json.Unmarshal(fields["chunk"], &chunk) != nil || chunk == nil || len(*chunk) > 65536 {
		return fmt.Errorf("missing or oversized output chunk")
	}
	var truncated bool
	if raw, ok := fields["truncated"]; ok {
		if err := json.Unmarshal(raw, &truncated); err != nil {
			return fmt.Errorf("decode truncated flag: %w", err)
		}
	}
	if truncated {
		var ref map[string]string
		if json.Unmarshal(fields["full_output"], &ref) != nil || ref["uri"] == "" || ref["digest"] == "" {
			return fmt.Errorf("truncated output requires full_output uri and digest")
		}
	}
	return nil
}
