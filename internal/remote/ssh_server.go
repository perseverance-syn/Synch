package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"

	"synch/internal/execdriver"
)

// --- Authenticated sessions and forwarding admission ---

const serverReadTimeout = 2 * time.Second

// ServeSSH serves one transport using the supplied SSH authentication and operation allowlist.
// Run it on an accepted TCP connection for each client while keeping the Runner itself alive.
func (r *Runner) ServeSSH(raw net.Conn, config *ssh.ServerConfig, operations []string) error {
	defer func() { _ = raw.Close() }()
	if config == nil || config.NoClientAuth {
		return errors.New("runner SSH requires authentication")
	}
	if err := raw.SetDeadline(time.Now().Add(serverReadTimeout)); err != nil {
		return fmt.Errorf("set SSH handshake deadline: %w", err)
	}
	conn, channels, requests, err := ssh.NewServerConn(raw, config)
	if err != nil {
		return fmt.Errorf("accept runner SSH connection: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if err := raw.SetDeadline(time.Time{}); err != nil {
		return fmt.Errorf("clear SSH handshake deadline: %w", err)
	}
	go ssh.DiscardRequests(requests)
	var sessions sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	for offer := range channels {
		if offer.ChannelType() != "session" {
			_ = offer.Reject(ssh.UnknownChannelType, "unsupported SSH channel")
			continue
		}
		ch, reqs, err := offer.Accept()
		if err != nil {
			continue
		}
		sessions.Go(func() { r.serveSession(ctx, conn, ch, reqs, operations) })
	}
	cancel()
	sessions.Wait()
	return nil
}

// serveSession accepts only the runner subsystem and optional forwarding on that session.
func (r *Runner) serveSession(ctx context.Context, conn *ssh.ServerConn, ch ssh.Channel, reqs <-chan *ssh.Request, operations []string) {
	defer func() { _ = ch.Close() }()
	var forwarded bool
	for req := range reqs {
		switch req.Type {
		case agentRequest:
			forwarded = slices.Contains(operations, OpAgentForwarding)
			_ = req.Reply(forwarded, nil)
		case "subsystem":
			var name struct{ Name string }
			allowed := ssh.Unmarshal(req.Payload, &name) == nil && name.Name == subsystem
			if req.Reply(allowed, nil) == nil && allowed {
				go ssh.DiscardRequests(reqs)
				r.serveRequest(ctx, conn, ch, operations, forwarded)
			}
			return
		default:
			_ = req.Reply(false, nil)
		}
	}
}

// serveRequest bounds and decodes a request before dispatching it, and never exposes process errors.
func (r *Runner) serveRequest(ctx context.Context, conn *ssh.ServerConn, ch ssh.Channel, operations []string, forwarded bool) {
	var req wireRequest
	decoder := json.NewDecoder(io.LimitReader(ch, wireMessageLimit))
	decoder.DisallowUnknownFields()
	// Stop the read timer before dispatch so a forwarded session keeps its original lifetime.
	timer := time.AfterFunc(serverReadTimeout, func() { _ = ch.Close() })
	readErr := decoder.Decode(&req)
	if !timer.Stop() || readErr != nil {
		return
	}
	res := wireResponse{Runner: r.id}
	var err error
	switch {
	case !slices.Contains(operations, req.Operation):
		err = ErrOperationDenied
	case req.ForwardAgent && (req.Operation != OpSpawn || !forwarded):
		err = ErrOperationDenied
	default:
		var socket string
		if req.ForwardAgent {
			var release func()
			socket, release, err = forwardSocket(conn)
			if err == nil {
				defer release()
			}
		}
		if err == nil {
			err = r.handle(ctx, req, socket, &res)
		}
	}
	res.Error = wireError(err)
	if json.NewEncoder(ch).Encode(res) == nil && req.ForwardAgent && err == nil {
		// Stay available until the daemon closes this grant's session or the SSH transport drops.
		_, _ = io.Copy(io.Discard, ch)
	}
}

// --- Journal and owned-process operations ---

// handle dispatches a registered runner operation to the process supervisor or journal.
func (r *Runner) handle(ctx context.Context, req wireRequest, socket string, res *wireResponse) error {
	switch req.Operation {
	case OpReplay:
		if req.After < 0 {
			return ErrOperationDenied
		}
		records, head, err := readReplayPage(r.db, r.id, req.After)
		res.Records, res.Head = records, &head
		return err
	case OpSpawn:
		if req.Spec == nil {
			return ErrOperationDenied
		}
		return r.spawn(*req.Spec, socket)
	case OpSettle:
		if !ulid.MatchString(req.Thread) {
			return ErrOperationDenied
		}
		report, err := r.settle(ctx, req.Thread)
		res.Settle = &report
		return err
	case OpCancel, OpProgress, OpProtocol:
		run, err := r.lookupRun(req.Thread, req.Run)
		if err != nil {
			return err
		}
		switch req.Operation {
		case OpCancel:
			return run.proc.Cancel(ctx)
		case OpProgress:
			run.proc.Progress()
			return nil
		default:
			return run.handleProtocol(req, res)
		}
	default:
		return ErrOperationDenied
	}
}

// handleProtocol moves one bounded range of bytes without coupling the process to SSH.
func (run *ownedRun) handleProtocol(req wireRequest, res *wireResponse) error {
	capture := run.protocol
	if capture == nil {
		return ErrOperationDenied
	}
	var err error
	switch req.Protocol {
	case "read":
		if req.Offset < 0 {
			return ErrOperationDenied
		}
		res.Data, res.EOF, err = capture.read(req.Offset)
	case "write":
		if len(req.Data) > protocolChunk {
			return ErrOperationDenied
		}
		res.Written, err = capture.stdin.Write(req.Data)
	case "close":
		err = capture.stdin.Close()
	default:
		return ErrOperationDenied
	}
	return err
}

// wireError maps only stable error codes onto the SSH wire so sensitive details stay local.
func wireError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrOperationDenied):
		return "denied"
	case errors.Is(err, ErrDispatchRecorded):
		return "reserved"
	case errors.Is(err, execdriver.ErrThreadSettling):
		return "settling"
	default:
		return "failed"
	}
}

// responseError maps wire error codes back to admission and dispatch errors.
func responseError(code string) error {
	switch code {
	case "":
		return nil
	case "denied":
		return ErrOperationDenied
	case "reserved":
		return ErrDispatchRecorded
	case "settling":
		return execdriver.ErrThreadSettling
	default:
		return errors.New("remote runner operation failed")
	}
}
