package remote

import (
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

var errAgentMutation = errors.New("forwarded SSH agent is read-only")

type signingAgent struct{ agent.Agent }

// Add refuses to add private keys to the forwarded broker.
func (signingAgent) Add(agent.AddedKey) error { return errAgentMutation }

// Remove refuses to remove a key from the forwarded broker.
func (signingAgent) Remove(ssh.PublicKey) error { return errAgentMutation }

// RemoveAll refuses to clear the forwarded broker's keys.
func (signingAgent) RemoveAll() error { return errAgentMutation }

// Lock refuses to alter the broker's lock state.
func (signingAgent) Lock([]byte) error { return errAgentMutation }

// Unlock refuses to alter the broker's lock state.
func (signingAgent) Unlock([]byte) error { return errAgentMutation }

// SignWithFlags preserves standard signature algorithm negotiation when the broker supports it.
func (a signingAgent) SignWithFlags(key ssh.PublicKey, data []byte, flags agent.SignatureFlags) (*ssh.Signature, error) {
	if extended, ok := a.Agent.(agent.ExtendedAgent); ok {
		return extended.SignWithFlags(key, data, flags)
	}
	if flags != 0 {
		return nil, errors.New("forwarded broker does not support signature flags")
	}
	return a.Sign(key, data)
}

// Extension denies agent-specific operations outside listing and signing.
func (signingAgent) Extension(string, []byte) ([]byte, error) {
	return nil, agent.ErrExtensionUnsupported
}

// forwardSocket exposes only the granted session's brokered agent to the spawned process.
func forwardSocket(conn *ssh.ServerConn) (string, func(), error) {
	dir, err := os.MkdirTemp("", "synch-agent-")
	if err != nil {
		return "", nil, fmt.Errorf("create agent socket directory: %w", err)
	}
	path := filepath.Join(dir, "agent")
	listener, err := net.Listen("unix", path)
	if err != nil {
		_ = os.Remove(dir)
		return "", nil, fmt.Errorf("listen on forwarded agent socket: %w", err)
	}
	go serveForwardedAgent(listener, conn)
	return path, func() {
		_ = listener.Close()
		_ = os.Remove(path)
		_ = os.Remove(dir)
	}, nil
}

// serveForwardedAgent opens an SSH agent channel for each client of the private socket.
func serveForwardedAgent(listener net.Listener, conn *ssh.ServerConn) {
	for {
		local, err := listener.Accept()
		if err != nil {
			return
		}
		go proxyAgent(local, conn)
	}
}

// proxyAgent transports agent messages without storing or interpreting private key material.
func proxyAgent(local net.Conn, conn *ssh.ServerConn) {
	defer func() { _ = local.Close() }()
	upstream, requests, err := conn.OpenChannel(agentChannel, nil)
	if err != nil {
		return
	}
	defer func() { _ = upstream.Close() }()
	go ssh.DiscardRequests(requests)
	go func() {
		_, _ = io.Copy(upstream, local)
		_ = upstream.CloseWrite()
	}()
	_, _ = io.Copy(local, upstream)
}
