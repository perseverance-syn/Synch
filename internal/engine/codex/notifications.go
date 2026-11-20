package codex

import (
	"encoding/json"
	"errors"
	"time"
	"unicode/utf8"

	"synch/internal/engine"
)

// --- Thread and turn state (engine-adapter section 1) ---

type notification struct {
	ThreadID, TurnID, ItemID string
	Delta                    *string
	Turn                     struct {
		ID, Status string
	}
	Item       json.RawMessage
	TokenUsage struct {
		Total struct {
			InputTokens, OutputTokens *int64
		}
	}
}

type itemState struct {
	kind     string
	streamed bool
	finished bool
}

// notification handles only assignment-related events and ignores unrelated methods and threads.
func (s *session) notification(method string, raw json.RawMessage) error {
	switch method {
	case "turn/started", "turn/completed", "item/started", "item/completed",
		"item/agentMessage/delta", "thread/tokenUsage/updated":
	default:
		return nil
	}
	var n notification
	if err := json.Unmarshal(raw, &n); err != nil {
		return harnessFailure("decode "+method, err)
	}
	if n.ThreadID == "" {
		return harnessFailure("decode "+method, errors.New("missing thread ID"))
	}
	event, err := s.applyNotification(method, n)
	if err != nil || event.Channel == "" {
		return err
	}
	return s.output(event.Channel, event.Chunk)
}

// applyNotification updates scoped state without holding the lock during persistence.
func (s *session) applyNotification(method string, n notification) (engine.EngineEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n.ThreadID != s.ref {
		return engine.EngineEvent{}, nil
	}
	switch method {
	case "turn/started", "turn/completed":
		return engine.EngineEvent{}, s.turnNotification(method, n)
	case "thread/tokenUsage/updated":
		return engine.EngineEvent{}, s.recordUsage(n)
	default:
		if n.TurnID == "" {
			return engine.EngineEvent{}, harnessFailure("decode "+method, errors.New("missing turn ID"))
		}
		if n.TurnID != s.turnID {
			return engine.EngineEvent{}, nil
		}
		return s.itemNotification(method, n)
	}
}

// turnNotification tracks the active turn even if notifications arrive before its RPC reply.
func (s *session) turnNotification(method string, n notification) error {
	id := n.Turn.ID
	if id == "" {
		return harnessFailure("decode "+method, errors.New("missing turn ID"))
	}
	if method == "turn/started" {
		if s.starting && s.turnID == "" && s.completedID != id {
			s.turnID = id
		}
		return nil
	}
	if id != s.turnID {
		return nil
	}
	s.turnID, s.completedID = "", id
	clear(s.items)
	switch n.Turn.Status {
	case "completed", "interrupted":
		return nil
	case "failed":
		return harnessFailure("run Codex turn", errors.New("server reported a failed turn"))
	default:
		return harnessFailure("decode "+method, errors.New("unknown terminal turn status"))
	}
}

// recordUsage saves the cumulative totals even when completion precedes the usage update.
func (s *session) recordUsage(n notification) error {
	if n.TurnID == "" {
		return harnessFailure("decode Codex token usage", errors.New("missing turn ID"))
	}
	if n.TurnID != s.turnID && n.TurnID != s.completedID {
		return nil
	}
	total := n.TokenUsage.Total
	if total.InputTokens == nil || total.OutputTokens == nil || *total.InputTokens < 0 || *total.OutputTokens < 0 {
		return harnessFailure("decode Codex token usage", errors.New("missing or negative token totals"))
	}
	s.usage = engine.Usage{Reported: true, InputTokens: *total.InputTokens, OutputTokens: *total.OutputTokens}
	return nil
}

// --- Ordered output and persistence (engine-adapter section 4) ---

// itemNotification maps agent deltas and tool lifecycle records to engine events.
func (s *session) itemNotification(method string, n notification) (engine.EngineEvent, error) {
	if method == "item/agentMessage/delta" {
		if n.ItemID == "" || n.Delta == nil {
			return engine.EngineEvent{}, harnessFailure("decode agent message", errors.New("missing item ID or delta"))
		}
		state := s.items[n.ItemID]
		if state.finished {
			return engine.EngineEvent{}, nil
		}
		state.streamed = true
		s.items[n.ItemID] = state
		return engine.EngineEvent{Channel: "message", Chunk: *n.Delta}, nil
	}
	var item struct {
		ID, Type string
		Text     *string
	}
	if err := json.Unmarshal(n.Item, &item); err != nil {
		return engine.EngineEvent{}, harnessFailure("decode Codex item", err)
	}
	if item.ID == "" || item.Type == "" {
		return engine.EngineEvent{}, harnessFailure("decode Codex item", errors.New("missing item ID or type"))
	}
	state := s.items[item.ID]
	if state.finished {
		return engine.EngineEvent{}, nil
	}
	if method == "item/started" {
		if state.kind != "" {
			return engine.EngineEvent{}, nil
		}
		state.kind = item.Type
		s.items[item.ID] = state
		if toolItem(item.Type) {
			return engine.EngineEvent{Channel: "tool_call", Chunk: string(n.Item)}, nil
		}
		return engine.EngineEvent{}, nil
	}
	state.finished = true
	s.items[item.ID] = state
	switch {
	case toolItem(item.Type) && state.kind == item.Type:
		return engine.EngineEvent{Channel: "tool_result", Chunk: string(n.Item)}, nil
	case item.Type == "agentMessage" && !state.streamed && item.Text != nil:
		return engine.EngineEvent{Channel: "message", Chunk: *item.Text}, nil
	default:
		return engine.EngineEvent{}, nil
	}
}

// toolItem identifies documented observable tool items without mediating their execution.
func toolItem(kind string) bool {
	switch kind {
	case "commandExecution", "fileChange", "mcpToolCall", "dynamicToolCall",
		"collabToolCall", "webSearch", "imageView":
		return true
	default:
		return false
	}
}

// output preserves all content in UTF-8 chunks and persists each chunk before
// publication. Delivery blocks when the consumer falls behind, so the pipe's
// backpressure reaches the harness instead of failing the session at a lag.
func (s *session) output(channel, chunk string) error {
	for len(chunk) > 0 {
		end := min(len(chunk), engine.OutputCap)
		for end < len(chunk) && !utf8.RuneStart(chunk[end]) {
			end--
		}
		piece := chunk[:end]
		chunk = chunk[end:]
		payload := engine.Output{AssignmentID: s.assignmentID, Channel: channel, Chunk: piece}
		if err := s.persist("engine.output", payload); err != nil {
			return err
		}
		if s.adapter.caps.Ops[engine.StreamOp].Status != engine.Qualified {
			continue
		}
		s.deliver(engine.EngineEvent{Channel: channel, Chunk: piece})
	}
	return nil
}

// deliver hands one persisted event to the consumer. While the stream holds
// eventBuffer events it waits, so a slow consumer delays delivery and the
// pipe's backpressure reaches the harness instead of losing output or ending
// the session. The reader is the only sender until finish, so the last slot
// stays free for the terminal event. Once shutdown starts (quit) the event is
// not handed over: it is already persisted, and shutdown must not wait for
// the consumer.
func (s *session) deliver(ev engine.EngineEvent) {
	if len(s.events) >= eventBuffer {
		// ponytail: 1 ms poll while the consumer lags; a channel cannot wait
		// for "one slot below capacity", and lag is the rare case.
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for len(s.events) >= eventBuffer {
			select {
			case <-s.quit:
				return
			case <-tick.C:
			}
		}
	}
	select {
	case <-s.quit:
	default:
		s.events <- ev
	}
}
