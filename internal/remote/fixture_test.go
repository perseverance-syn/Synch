package remote

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/policy"
	"synch/internal/supervisor"
)

type testBroker struct {
	auth, forward              agent.Agent
	authCalls, forwardingCalls atomic.Int64
}

// Agent provides an in-memory authentication agent for the loopback transport.
func (b *testBroker) Agent(context.Context, Target) (agent.Agent, error) {
	b.authCalls.Add(1)
	return b.auth, nil
}

// ForwardingAgent provides a different keyring for a granted process.
func (b *testBroker) ForwardingAgent(context.Context, Target, string) (agent.Agent, error) {
	b.forwardingCalls.Add(1)
	return b.forward, nil
}

type testPolicy struct{ calls atomic.Int64 }

// Decide records that admission occurred and invokes Start inside the decision.
func (p *testPolicy) Decide(_ context.Context, req policy.Request) (policy.Decision, error) {
	p.calls.Add(1)
	if req.Start != nil {
		if err := req.Start(); err != nil {
			return policy.Decision{}, err
		}
	}
	return policy.Decision{Allowed: true}, nil
}

type testGrant bool

// AllowsAgentForwarding supplies the requested forwarding decision for a test assignment.
func (g testGrant) AllowsAgentForwarding(context.Context, Target, string) (bool, error) {
	return bool(g), nil
}

// --- Loopback runner, host keys, and transport control ---

type remoteFixture struct {
	runner      *Runner
	driver      *Driver
	registry    *Registry
	log         *eventlog.Store
	ledger      string
	broker      *testBroker
	policy      *testPolicy
	target      Target
	listener    net.Listener
	operations  []string
	mu          sync.Mutex
	config      *ssh.ServerConfig
	connections map[net.Conn]struct{}
	group       sync.WaitGroup
}

// openFixture starts a supervisor-backed runner and SSH server on loopback.
func openFixture(t *testing.T) *remoteFixture {
	t.Helper()
	root := t.TempDir()
	cfg := supervisor.Config{DataDir: filepath.Join(root, "runner"), Threshold: func(uint64) uint64 { return 0 }}
	r, err := OpenRunner("test-runner", cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	log, err := eventlog.Open(filepath.Join(root, "events.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = log.Close() })
	auth, authKey := makeAgent(t)
	forwarded, _ := makeAgent(t)
	broker := &testBroker{auth: auth, forward: forwarded}
	decider := &testPolicy{}
	registry := &Registry{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	operations := []string{OpSpawn, OpReplay, OpSettle, OpProgress, OpCancel, OpProtocol, OpAgentForwarding, "remote.approve"}
	f := &remoteFixture{
		runner: r, log: log, registry: registry, broker: broker, policy: decider, listener: listener, operations: operations,
		ledger: filepath.Join(root, "dispatch.sqlite"), connections: make(map[net.Conn]struct{}),
	}
	f.target = Target{Runner: r.id, Host: "127.0.0.1", Port: uint16(listener.Addr().(*net.TCPAddr).Port), User: "operator", Operations: operations}
	f.target.Fingerprint = f.rotateHost(t, authKey)
	if err := registry.Register(f.target); err != nil {
		t.Fatal(err)
	}
	f.driver, err = NewDriver(registry, r.id, broker, decider, log, f.ledger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.driver.Close() })
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.accept()
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done
		f.cutNetwork()
		f.group.Wait()
	})
	return f
}

// makeAgent creates an isolated in-memory keyring and returns its public identity.
func makeAgent(t *testing.T) (agent.Agent, ssh.PublicKey) {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ring := agent.NewKeyring()
	if err := ring.Add(agent.AddedKey{PrivateKey: private}); err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatal(err)
	}
	return ring, signer.PublicKey()
}

// rotateHost changes the host key presented to future SSH handshakes.
func (f *remoteFixture) rotateHost(t *testing.T, authKey ssh.PublicKey) string {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(private)
	if err != nil {
		t.Fatal(err)
	}
	config := &ssh.ServerConfig{PublicKeyCallback: func(meta ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
		if meta.User() != "operator" || !slices.Equal(key.Marshal(), authKey.Marshal()) {
			return nil, errors.New("unregistered test identity")
		}
		return nil, nil
	}}
	config.AddHostKey(signer)
	f.mu.Lock()
	f.config = config
	f.mu.Unlock()
	return ssh.FingerprintSHA256(signer.PublicKey())
}

// accept runs the SSH endpoint for each accepted test transport.
func (f *remoteFixture) accept() {
	for {
		c, err := f.listener.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		config := f.config
		f.connections[c] = struct{}{}
		f.mu.Unlock()
		f.group.Go(func() {
			_ = f.runner.ServeSSH(c, config, f.operations)
			f.mu.Lock()
			delete(f.connections, c)
			f.mu.Unlock()
		})
	}
}

// cutNetwork closes active TCP transports while leaving the runner and listener alive.
func (f *remoteFixture) cutNetwork() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for c := range f.connections {
		_ = c.Close()
	}
}

// --- Portable test process and synchronization ---

// testContext bounds SSH and process operations so test failures do not hang.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// childSpec constructs a portable test assignment executable on Windows and Unix.
func childSpec(t *testing.T, mode, gate string) execdriver.ProcSpec {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	return execdriver.ProcSpec{ID: eventlog.NewID(), ThreadID: testThread, Path: executable,
		Args: []string{"-test.run=^TestRunnerChild$", "--", mode, gate},
		Env:  append(os.Environ(), "SYNCH_REMOTE_TEST_CHILD=1", "SSH_AUTH_SOCK=forbidden"), StallTimeout: 15 * time.Second}
}

// TestRunnerChild supplies deterministic S2 and S4 child behavior under the real supervisor.
func TestRunnerChild(t *testing.T) {
	if os.Getenv("SYNCH_REMOTE_TEST_CHILD") != "1" {
		return
	}
	index := slices.Index(os.Args, "--")
	if index < 0 || index+2 >= len(os.Args) {
		t.Fatal("missing child arguments")
	}
	mode, gate := os.Args[index+1], os.Args[index+2]
	switch mode {
	case "gate":
		fmt.Println("online")
		deadline := time.Now().Add(10 * time.Second)
		for {
			if _, err := os.Stat(gate); err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("gate timed out")
			}
			time.Sleep(10 * time.Millisecond)
		}
		fmt.Println("offline-finished")
	case "agent":
		socket := os.Getenv("SSH_AUTH_SOCK")
		if socket == "" {
			fmt.Println("agent=disabled")
			return
		}
		conn, err := net.Dial("unix", socket)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close() }()
		keys, err := agent.NewClient(conn).List()
		if err != nil || len(keys) != 1 {
			t.Fatalf("scoped agent keys: %v, %v", keys, err)
		}
		fmt.Printf("agent=%s\n", ssh.FingerprintSHA256(keys[0]))
	case "protocol":
		if _, err := io.Copy(os.Stdout, os.Stdin); err != nil {
			t.Fatal(err)
		}
		// The test harness would otherwise append PASS to the raw protocol bytes.
		os.Exit(0)
	default:
		t.Fatal("unexpected child mode")
	}
}

// waitFor polls an observed local or daemon condition until its context expires.
func waitFor(t *testing.T, ctx context.Context, detail string, ready func() bool) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("wait for %s: %v", detail, ctx.Err())
		}
	}
}
