package remote

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"synch/internal/execdriver"
)

const subsystem = "synch-runner-v1"
const agentRequest = "auth-agent-req@openssh.com"
const agentChannel = "auth-agent@openssh.com"
const wireTimeout = 30 * time.Second
const wireMessageLimit = 1 << 20

// Broker supplies SSH authentication and a separately scoped agent when forwarding is granted.
type Broker interface {
	Agent(context.Context, Target) (agent.Agent, error)
	ForwardingAgent(context.Context, Target, string) (agent.Agent, error)
}

// ForwardingGrant verifies an explicit agent-forwarding grant for the thread and target.
type ForwardingGrant interface {
	AllowsAgentForwarding(context.Context, Target, string) (bool, error)
}

type wireRequest struct {
	Operation    string               `json:"operation"`
	Thread       string               `json:"thread,omitempty"`
	Run          string               `json:"run,omitempty"`
	Spec         *execdriver.ProcSpec `json:"spec,omitempty"`
	After        int64                `json:"after,omitempty"`
	Offset       int64                `json:"offset,omitempty"`
	Data         []byte               `json:"data,omitempty"`
	Protocol     string               `json:"protocol,omitempty"`
	ForwardAgent bool                 `json:"forward_agent,omitempty"`
}

type wireResponse struct {
	Runner  string                   `json:"runner"`
	Error   string                   `json:"error,omitempty"`
	Records []json.RawMessage        `json:"records,omitempty"`
	Head    *int64                   `json:"head,omitempty"`
	Settle  *execdriver.SettleReport `json:"settle,omitempty"`
	Data    []byte                   `json:"data,omitempty"`
	EOF     bool                     `json:"eof,omitempty"`
	Written int                      `json:"written,omitempty"`
}

// --- Pinned transport and broker authentication ---

// dialSSH authenticates through the broker after the server proves its registered host key.
func dialSSH(ctx context.Context, target Target, broker Broker) (*ssh.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, wireTimeout)
	defer cancel()
	var dialer net.Dialer
	raw, err := dialer.DialContext(ctx, "tcp", target.address())
	if err != nil {
		return nil, fmt.Errorf("dial SSH target: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()
	config := &ssh.ClientConfig{
		User: target.User,
		HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error {
			actual := ssh.FingerprintSHA256(key)
			if subtle.ConstantTimeCompare([]byte(target.Fingerprint), []byte(actual)) != 1 {
				return &HostKeyReviewError{Runner: target.Runner, Expected: target.Fingerprint, Presented: actual}
			}
			return nil
		},
		Auth: []ssh.AuthMethod{ssh.PublicKeysCallback(func() ([]ssh.Signer, error) {
			auth, err := broker.Agent(ctx, target)
			if err != nil || auth == nil {
				return nil, errors.New("SSH authentication broker unavailable")
			}
			return auth.Signers()
		})},
	}
	conn, channels, requests, err := ssh.NewClientConn(raw, target.address(), config)
	if err != nil {
		_ = raw.Close()
		return nil, fmt.Errorf("authenticate SSH target: %w", err)
	}
	if !stop() {
		_ = conn.Close()
		return nil, ctx.Err()
	}
	return ssh.NewClient(conn, channels, requests), nil
}

// --- Runner subsystem requests ---

// callSSH opens one request session. A forwarded session stays open for the dispatched process.
func callSSH(ctx context.Context, client *ssh.Client, runner string, req wireRequest) (wireResponse, *ssh.Session, error) {
	ctx, cancel := context.WithTimeout(ctx, wireTimeout)
	defer cancel()
	// Close the transport on expiry so a silent peer cannot keep channel reads blocked.
	// The runner owns dispatched work independently of this transport.
	stop := context.AfterFunc(ctx, func() { _ = client.Close() })
	defer stop()
	session, err := client.NewSession()
	if err != nil {
		return wireResponse{}, nil, errors.Join(err, ctx.Err())
	}
	response, err := exchange(session, runner, req)
	if !stop() {
		err = errors.Join(err, ctx.Err())
	}
	if err != nil || !req.ForwardAgent || response.Error != "" {
		_ = session.Close()
		return response, nil, err
	}
	return response, session, nil
}

// exchange submits one request and verifies that the pinned target reports the registered runner ID.
func exchange(session *ssh.Session, runner string, req wireRequest) (wireResponse, error) {
	var res wireResponse
	stdin, err := session.StdinPipe()
	if err != nil {
		return res, err
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		return res, err
	}
	if req.ForwardAgent {
		if err := agent.RequestAgentForwarding(session); err != nil {
			return res, fmt.Errorf("request scoped agent forwarding: %w", err)
		}
	}
	if err := session.RequestSubsystem(subsystem); err != nil {
		return res, fmt.Errorf("request runner subsystem: %w", err)
	}
	if err := json.NewEncoder(stdin).Encode(req); err != nil {
		return res, fmt.Errorf("send runner request: %w", err)
	}
	if !req.ForwardAgent {
		if err := stdin.Close(); err != nil {
			return res, fmt.Errorf("finish SSH request: %w", err)
		}
	}
	res, err = decodeResponse(stdout)
	if err != nil {
		return res, err
	}
	if res.Runner != runner {
		return wireResponse{}, errRunnerIdentity
	}
	return res, nil
}

// decodeResponse bounds how much a pinned peer can make the daemon read for one response.
func decodeResponse(stdout io.Reader) (wireResponse, error) {
	var res wireResponse
	if err := json.NewDecoder(io.LimitReader(stdout, wireMessageLimit)).Decode(&res); err != nil {
		return wireResponse{}, fmt.Errorf("receive runner response: %w", err)
	}
	return res, nil
}

var errRunnerIdentity = errors.New("SSH runner identity mismatch")
