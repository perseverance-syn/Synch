package claudecode

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"

	"synch/internal/engine"
)

type envelope struct {
	Type      string
	Subtype   string
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
	UUID      string
	Request   json.RawMessage
	Response  json.RawMessage
	Message   json.RawMessage
	Usage     json.RawMessage
	IsError   bool `json:"is_error"`
}

// --- Stream dispatch: scope ordinary records to the initialized native session ---

// frame dispatches control traffic before any session-output state checks.
func (s *session) frame(raw []byte) error {
	var header struct {
		Type      string
		RequestID string `json:"request_id"`
		Request   json.RawMessage
	}
	if err := json.Unmarshal(raw, &header); err != nil {
		return err
	}
	if header.Type == "control_request" {
		return s.answerPermission(header.RequestID, header.Request)
	}

	var msg envelope
	if err := json.Unmarshal(raw, &msg); err != nil {
		return err
	}
	switch msg.Type {
	case "control_response":
		return s.controlResponse(msg.Response)
	case "system":
		if msg.Subtype == "init" {
			return s.initSession(msg.SessionID)
		}
	case "result", "assistant", "user":
		s.mu.Lock()
		current := s.initialized && s.ref == msg.SessionID
		s.mu.Unlock()
		if !current {
			return nil
		}
		if msg.Type == "result" {
			return s.result(msg)
		}
		return s.message(msg.Type, msg.Message)
	case "":
		return errors.New("missing Claude Code stream message type")
	}
	return nil
}

// initSession sets the native reference and rejects a conflicting init.
func (s *session) initSession(ref string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ref == "" || (s.ref != "" && s.ref != ref) {
		return errors.New("inconsistent session ID in Claude Code init")
	}
	s.ref = ref
	s.initialized = true
	return nil
}

// --- Output and usage: publish completed message blocks and track turn results ---

// message emits only assistant text and actual tool blocks, without replaying user text or partial deltas.
func (s *session) message(kind string, raw json.RawMessage) error {
	var msg struct{ Content json.RawMessage }
	if err := json.Unmarshal(raw, &msg); err != nil {
		return err
	}
	// Claude may echo a string user message as well as arrays carrying tool results.
	if kind == "user" && bytes.HasPrefix(bytes.TrimSpace(msg.Content), []byte("\"")) {
		return nil
	}
	var blocks []json.RawMessage
	if err := json.Unmarshal(msg.Content, &blocks); err != nil {
		return err
	}
	if blocks == nil {
		return errors.New("missing content blocks in Claude Code message")
	}
	for _, body := range blocks {
		var block struct {
			Type string
			Text *string
		}
		if err := json.Unmarshal(body, &block); err != nil {
			return err
		}
		var channel, chunk string
		switch {
		case kind == "assistant" && block.Type == "text":
			if block.Text == nil {
				return errors.New("missing text in Claude Code text block")
			}
			channel, chunk = "message", *block.Text
		case kind == "assistant" && block.Type == "tool_use":
			channel, chunk = "tool_call", string(body)
		case kind == "user" && block.Type == "tool_result":
			channel, chunk = "tool_result", string(body)
		case block.Type == "":
			return errors.New("missing type in Claude Code content block")
		}
		if err := s.wire.Output(channel, chunk); err != nil {
			return err
		}
	}
	return nil
}

// result closes the current turn and accumulates one usage report per result.
func (s *session) result(msg envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active || (msg.UUID != "" && msg.UUID == s.lastResult) {
		return nil
	}
	// The SDK documents error_during_execution as the normal interrupted result.
	interrupted := s.interrupting && msg.Subtype == "error_during_execution"
	s.active, s.interrupting = false, false
	s.lastResult = msg.UUID
	if msg.IsError && !interrupted {
		return errors.New("failed turn reported by Claude Code")
	}
	if len(msg.Usage) == 0 || bytes.Equal(msg.Usage, []byte("null")) {
		return nil
	}
	var usage struct {
		Input  *int64 `json:"input_tokens"`
		Output *int64 `json:"output_tokens"`
	}
	if err := json.Unmarshal(msg.Usage, &usage); err != nil {
		return err
	}
	if usage.Input == nil || usage.Output == nil || *usage.Input < 0 || *usage.Output < 0 ||
		*usage.Input > math.MaxInt64-s.usage.InputTokens || *usage.Output > math.MaxInt64-s.usage.OutputTokens {
		return errors.New("invalid Claude Code token totals")
	}
	s.usage = engine.Usage{Reported: true, InputTokens: s.usage.InputTokens + *usage.Input,
		OutputTokens: s.usage.OutputTokens + *usage.Output}
	return nil
}
