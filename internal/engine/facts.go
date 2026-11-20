package engine

// OutputCap is the producer-enforced maximum number of UTF-8 bytes in a chunk.
const OutputCap = 65536

// Sink synchronously persists schema-version-1 facts. Implementations may be called concurrently.
type Sink interface{ Persist(Fact) error }

// Fact carries a catalogue event and its thread scope to a daemon-owned sink.
type Fact struct {
	Type     string
	ThreadID string
	Payload  any
}

// Output is the engine.output payload. Producers split content at UTF-8 boundaries.
type Output struct {
	AssignmentID string `json:"assignment_id"`
	Channel      string `json:"channel"`
	Chunk        string `json:"chunk"`
}

// UsageReport is the engine.usage payload. Omitted token fields mean no report.
type UsageReport struct {
	AssignmentID string `json:"assignment_id"`
	Reported     bool   `json:"reported"`
	InputTokens  *int64 `json:"input_tokens,omitempty"`
	OutputTokens *int64 `json:"output_tokens,omitempty"`
}

// CapabilityUnavailable is the engine.capability_unavailable payload.
type CapabilityUnavailable struct {
	AssignmentID string `json:"assignment_id"`
	Harness      string `json:"harness"`
	Operation    string `json:"operation"`
}
