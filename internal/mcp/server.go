package mcp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Config identifies one thread and its caller credential.
type Config struct{ Server, Token, ThreadID string }

type request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
type response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}
type tool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	InputSchema any    `json:"inputSchema"`
}

// Serve reads newline-delimited JSON-RPC messages until EOF or shutdown.
func Serve(in io.Reader, out io.Writer, cfg Config) error {
	if cfg.Server == "" {
		cfg.Server = "http://127.0.0.1:7420"
	}
	u, err := url.Parse(cfg.Server)
	if err != nil || u.Scheme != "http" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return errors.New("MCP server address must be a local HTTP URL")
	}
	if cfg.Token == "" || cfg.ThreadID == "" || strings.ContainsAny(cfg.ThreadID, "/\\") {
		return errors.New("SYNCH_TOKEN and SYNCH_THREAD_ID are required")
	}
	c := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	enc := json.NewEncoder(out)
	for scanner.Scan() {
		var req request
		var resp response
		resp.JSONRPC = "2.0"
		resp.ID = json.RawMessage("null")
		if json.Unmarshal(scanner.Bytes(), &req) != nil {
			resp.Error = &rpcError{-32700, "Parse error"}
		} else if req.JSONRPC != "2.0" || req.Method == "" {
			resp.Error = &rpcError{-32600, "Invalid Request"}
		} else {
			if len(req.ID) > 0 {
				resp.ID = req.ID
			}
			resp.Result, resp.Error = handle(c, cfg, req)
			if len(req.ID) == 0 {
				continue
			}
		}
		if err := enc.Encode(resp); err != nil {
			return err
		}
		if req.Method == "shutdown" {
			return nil
		}
	}
	return scanner.Err()
}

func handle(c *http.Client, cfg Config, req request) (any, *rpcError) {
	switch req.Method {
	case "initialize":
		return map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]string{"name": "synch", "version": "1"}}, nil
	case "ping", "shutdown":
		return map[string]any{}, nil
	case "tools/list":
		return map[string]any{"tools": tools()}, nil
	case "tools/call":
		var p struct {
			Name      string          `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if json.Unmarshal(req.Params, &p) != nil || p.Name == "" {
			return nil, &rpcError{-32602, "Invalid params"}
		}
		switch p.Name {
		case "thread_status":
			status, err := getStatus(c, cfg)
			if err != nil {
				return toolError("Thread status unavailable"), nil
			}
			raw, _ := json.Marshal(status)
			safe := strings.ReplaceAll(string(raw), cfg.Token, "[redacted]")
			wrapped, _ := json.Marshal(content(safe, false))
			if len(wrapped) > 16000 {
				return toolError("Status exceeds output limit; no searchable status reference is available"), nil
			}
			return content(safe, false), nil
		case "search_output":
			var args struct {
				Reference string `json:"reference"`
				Pattern   string `json:"pattern"`
			}
			if json.Unmarshal(p.Arguments, &args) != nil || args.Reference == "" || args.Pattern == "" {
				return nil, &rpcError{-32602, "reference and pattern are required"}
			}
			result, err := search(c, cfg, args.Reference, args.Pattern)
			if err != nil {
				return toolError(err.Error()), nil
			}
			return content(result, false), nil
		default:
			return nil, &rpcError{-32602, "Unknown tool"}
		}
	default:
		return nil, &rpcError{-32601, "Method not found"}
	}
}

type statusView struct {
	State            string              `json:"state"`
	WaitingFor       string              `json:"waiting_for"`
	Checks           map[string][]string `json:"checks"`
	OpenRequirements []struct {
		Kind   string `json:"kind"`
		ID     string `json:"id"`
		Status string `json:"status,omitempty"`
	} `json:"open_requirements"`
}

func getStatus(c *http.Client, cfg Config) (statusView, error) {
	var view statusView
	req, err := http.NewRequest(http.MethodGet, strings.TrimRight(cfg.Server, "/")+"/v1/threads/"+url.PathEscape(cfg.ThreadID), nil)
	if err != nil {
		return view, err
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	resp, err := c.Do(req)
	if err != nil {
		return view, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return view, fmt.Errorf("status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil || len(data) > 65536 {
		return view, errors.New("invalid status")
	}
	err = json.Unmarshal(data, &view)
	if view.Checks == nil {
		view.Checks = make(map[string][]string)
	}
	for _, name := range []string{"passed", "failed", "error", "partial", "not_run"} {
		if view.Checks[name] == nil {
			view.Checks[name] = []string{}
		}
	}
	if view.OpenRequirements == nil {
		view.OpenRequirements = []struct {
			Kind   string `json:"kind"`
			ID     string `json:"id"`
			Status string `json:"status,omitempty"`
		}{}
	}
	return view, err
}

var outputRef = regexp.MustCompile(`^out_[0-9a-f]{32}$`)

type searchResult struct {
	Ref     string `json:"ref"`
	Matches []struct {
		Line int    `json:"line"`
		Text string `json:"text"`
	} `json:"matches"`
	Truncated bool `json:"truncated"`
}

func search(c *http.Client, cfg Config, ref, pattern string) (string, error) {
	if !outputRef.MatchString(ref) {
		return "", errors.New("invalid output reference")
	}
	q := url.Values{"thread": {cfg.ThreadID}, "pattern": {pattern}, "limit": {"20"}}
	endpoint := strings.TrimRight(cfg.Server, "/") + "/v1/outputs/" + url.PathEscape(ref) + "/search?" + q.Encode()
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return "", errors.New("invalid search request")
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Token)
	resp, err := c.Do(req)
	if err != nil {
		return "", errors.New("search unavailable")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("search failed (HTTP %d)", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", errors.New("search response unavailable")
	}
	var result searchResult
	if json.Unmarshal(data, &result) != nil || result.Ref != ref {
		return "", errors.New("invalid search response")
	}
	if result.Matches == nil {
		result.Matches = make([]struct {
			Line int    `json:"line"`
			Text string `json:"text"`
		}, 0)
	}
	result.Ref = strings.ReplaceAll(result.Ref, cfg.Token, "[redacted]")
	for i := range result.Matches {
		result.Matches[i].Text = strings.ReplaceAll(result.Matches[i].Text, cfg.Token, "[redacted]")
	}
	for {
		raw, _ := json.Marshal(result)
		wrapped, _ := json.Marshal(content(string(raw), false))
		if len(wrapped) <= 16000 {
			return string(raw), nil
		}
		if len(result.Matches) == 0 {
			return "", errors.New("search reference exceeds output limit")
		}
		result.Matches = result.Matches[:len(result.Matches)-1]
		result.Truncated = true
	}
}

func tools() []tool {
	schema := map[string]any{"type": "object", "properties": map[string]any{}}
	return []tool{{"thread_status", "Read the current thread status.", schema}, {"search_output", "Search retained output for matching excerpts.", map[string]any{"type": "object", "properties": map[string]any{"reference": map[string]string{"type": "string"}, "pattern": map[string]string{"type": "string"}}, "required": []string{"reference", "pattern"}}}}
}

func content(s string, failed bool) any {
	return map[string]any{"content": []map[string]string{{"type": "text", "text": s}}, "isError": failed}
}
func toolError(s string) any { return content(s, true) }
