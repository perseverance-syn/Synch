package kimi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"synch/internal/engine/claudecode/stdio"
)

type envelope struct {
	Version string `json:"jsonrpc"`
	ID      json.RawMessage
	Method  string
	Params  json.RawMessage
	Result  json.RawMessage
	Error   *struct{ Code int }
}

// request constructs one ACP JSON-RPC frame with a client-side correlation ID.
func request(method string, params stdio.Fields) func(string) stdio.Fields {
	return func(id string) stdio.Fields {
		return stdio.Fields{"jsonrpc": "2.0", "id": id, "method": method, "params": params}
	}
}

// call waits for the corresponding ACP result before continuing initialization.
func (s *session) call(ctx context.Context, method string, params stdio.Fields, apply func(json.RawMessage) error) error {
	return s.calls.Call(ctx, s.wire, request(method, params), apply)
}

// frame routes JSON-RPC replies, notifications and reverse requests.
func (s *session) frame(raw []byte) error {
	var frame envelope
	if err := json.Unmarshal(raw, &frame); err != nil {
		return err
	}
	if frame.Version != "2.0" {
		return errors.New("invalid Kimi JSON-RPC version")
	}
	switch {
	case frame.Method != "" && len(frame.ID) != 0:
		return s.reverse(frame)
	case frame.Method == "session/update":
		return s.update(frame.Params)
	case frame.Method != "":
		return nil
	default:
		return s.response(frame)
	}
}

// response validates an ACP reply without relaying provider error text into logs.
func (s *session) response(frame envelope) error {
	var id string
	if err := json.Unmarshal(frame.ID, &id); err != nil || id == "" {
		return errors.New("invalid Kimi JSON-RPC response ID")
	}
	if frame.Error != nil && len(frame.Result) == 0 {
		return s.calls.Reply(id, nil, fmt.Errorf("ACP request rejected by Kimi (code %d)", frame.Error.Code))
	}
	if frame.Error != nil || !bytes.HasPrefix(bytes.TrimSpace(frame.Result), []byte("{")) {
		return errors.New("invalid Kimi JSON-RPC result")
	}
	return s.calls.Reply(id, frame.Result, nil)
}

// reverse cancels permissions and rejects other client requests without executing any tool.
func (s *session) reverse(frame envelope) error {
	var id any
	decoder := json.NewDecoder(bytes.NewReader(frame.ID))
	decoder.UseNumber()
	if err := decoder.Decode(&id); err != nil {
		return err
	}
	switch id.(type) {
	case string, json.Number:
	default:
		return errors.New("invalid Kimi reverse request ID")
	}
	reply := stdio.Fields{"jsonrpc": "2.0", "id": frame.ID}
	if frame.Method == "session/request_permission" {
		reply["result"] = stdio.Fields{"outcome": stdio.Fields{"outcome": "cancelled"}}
	} else {
		reply["error"] = stdio.Fields{"code": -32601, "message": "Client method unavailable"}
	}
	return s.wire.Write(s.wire.Context(), reply)
}

// update maps text and tool updates for the active ACP session to persisted engine output.
func (s *session) update(raw json.RawMessage) error {
	var params struct {
		SessionID string
		Update    json.RawMessage
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return err
	}
	if params.SessionID == "" || params.SessionID != s.Ref() {
		return nil
	}
	var update struct {
		SessionUpdate string
		Content       json.RawMessage
	}
	if err := json.Unmarshal(params.Update, &update); err != nil {
		return err
	}
	switch update.SessionUpdate {
	case "agent_message_chunk":
		// Message content is a block. Tool updates use arrays of tool content.
		var content struct {
			Type string
			Text *string
		}
		if err := json.Unmarshal(update.Content, &content); err != nil {
			return err
		}
		if content.Type == "text" {
			if content.Text == nil {
				return errors.New("missing text in Kimi ACP text update")
			}
			return s.wire.Output("message", *content.Text)
		}
	case "tool_call":
		return s.wire.Output("tool_call", string(params.Update))
	case "tool_call_update":
		return s.wire.Output("tool_result", string(params.Update))
	case "":
		return errors.New("missing variant in Kimi ACP update")
	}
	return nil
}
