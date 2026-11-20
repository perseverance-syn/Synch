package daemon

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"synch/internal/api"
	"synch/internal/children"
	"synch/internal/engine"
	"synch/internal/engine/claudecode"
	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/gitops"
	"synch/internal/policy"
	"synch/internal/remote"
	"synch/internal/routing"
	"synch/internal/supervisor"
	"synch/internal/thread"
)

// Config supplies daemon storage, local authentication and launcher configuration.
type Config struct {
	DataDir     string
	Listen      string
	PolicyStore string
	Tokens      api.TokenVerifier
	// Binary paths default to codex, claude and kimi on PATH. A binary that
	// cannot be found means that harness is not offered to routing.
	CodexBinary, ClaudeBinary, KimiBinary string
	// DefaultHarness and DefaultModel form the configured default pair.
	DefaultHarness, DefaultModel string
	// TaskKind labels routing comparisons; it defaults to "coding".
	TaskKind string
	// Qualifications supplies operation qualification per adapter name
	// ("codex", "claude-code", "kimi-code"). Without it every operation is untested.
	Qualifications map[string]engine.CapabilitySet
	// HarnessVersions overrides the version given to routing per adapter name.
	// By default routing uses the lower bound of the declared version range,
	// and the adapter verifies the installed release when it starts a session.
	HarnessVersions map[string]string
	// Evidence is recorded decision 11 comparison data for routing.
	Evidence []routing.Evidence
	// Projects maps an API project_id to its checkout and gate file.
	Projects map[string]ProjectConfig
	// Remote registers the remote execution driver when set.
	Remote *RemoteConfig
	// ChildMaxRuntime is the daemon-wide longest a managed child's engine session may
	// run, as a Go duration (default 2h); "0" or "off" disables the budget. A project's
	// max_runtime wins. ChildLoopRepeats is the same for how many consecutive identical
	// tool calls with no other output count as a loop (default 5, at least 2).
	ChildMaxRuntime, ChildLoopRepeats Setting
	// ChildLimits overrides the Q-27 child limits (depth 3, concurrency 4, attempts 3); zero keeps each default.
	ChildLimits children.Limits

	adapter   func(engine.Sink) (engine.EngineAdapter, error)
	freeSpace func(string) (free, total uint64, err error)
	poll      time.Duration
	retryBase time.Duration
	noLoop    bool
}

// Daemon owns the components sharing one durable event store.
type Daemon struct {
	store        *eventlog.Store
	threads      *thread.Machine
	children     *children.Coordinator
	recorder     *eventRecorder
	created      map[string]time.Time // creation times of managed children, for grant inheritance
	settled      map[string]bool      // ended children whose result is recorded
	unrunnable   map[string]bool      // children being abandoned, which the pair guard refuses
	policy       *policy.Engine
	harnesses    []*harness
	driver       *execdriver.Direct
	remote       *remote.Driver
	remoteID     string
	permits      chan claudecode.PermissionRequest
	defaults     routing.Pair
	taskKind     string
	routeData    []routing.Evidence
	projects     map[string]ProjectConfig
	poll         time.Duration
	defaultWatch watchLimits
	projectWatch map[string]watchLimits
	noLoop       bool
	failed       map[string]*dispatchFailure
	verifying    map[string]bool
	retryBase    time.Duration
	worktrees    map[string]string
	supervisor   *supervisor.Supervisor
	git          *gitops.Service
	rules        []*policy.File
	dataDir      string
	host         string
	port         string
	tokens       api.TokenVerifier
	pressure     atomic.Bool

	mu         sync.Mutex
	server     *http.Server
	listenAddr net.Addr
	closed     bool
	closeOnce  sync.Once
	closeErr   error
	lifetime   context.Context
	cancel     context.CancelFunc
	workers    sync.WaitGroup
	sessions   map[string]*assignment

	recovering map[string]bool
	confirmed  bool

	evidenceMu sync.RWMutex
	decisionMu sync.Mutex
	evidence   map[string]policy.Current
}

// --- Startup: recover all components before exposing the listener ---

// New opens storage and recovers policy, threads, and supervised runs in order.
func New(cfg Config) (daemon *Daemon, err error) {
	if cfg.DataDir == "" || cfg.Tokens == nil {
		return nil, errors.New("daemon data directory and token verifier are required")
	}
	if cfg.Listen == "" {
		cfg.Listen = "127.0.0.1:" + api.DefaultPort
	}
	host, port, err := net.SplitHostPort(cfg.Listen)
	if err != nil {
		return nil, fmt.Errorf("parse listen address: %w", err)
	}
	if host != "127.0.0.1" && host != "::1" {
		return nil, &api.NonLoopbackError{Host: host}
	}
	dataDir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve data directory: %w", err)
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	store, err := eventlog.Open(filepath.Join(dataDir, "events.db"))
	if err != nil {
		return nil, fmt.Errorf("open event store: %w", err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	d := &Daemon{
		failed: make(map[string]*dispatchFailure), settled: make(map[string]bool), created: make(map[string]time.Time), unrunnable: make(map[string]bool), verifying: make(map[string]bool), worktrees: make(map[string]string),
		store: store, dataDir: dataDir, host: host, port: port, tokens: cfg.Tokens,
		lifetime: lifetime, cancel: cancel, sessions: make(map[string]*assignment),
		recovering: make(map[string]bool), evidence: make(map[string]policy.Current),
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, d.Close())
		}
	}()
	if err := d.configure(lifetime, cfg); err != nil {
		return nil, err
	}
	return d, nil
}

// configure wires components around the shared store and refuses partial recovery.
func (d *Daemon) configure(ctx context.Context, cfg Config) error {
	var err error
	if cfg.PolicyStore != "" {
		raw, err := os.ReadFile(cfg.PolicyStore)
		if err != nil {
			return fmt.Errorf("read policy store: %w", err)
		}
		file, err := policy.Load(raw, policy.Origin{Kind: policy.OriginStore})
		if err != nil {
			return fmt.Errorf("load policy store: %w", err)
		}
		d.rules = []*policy.File{file}
	}
	d.policy = policy.New(d.store, policyState{daemon: d}, time.Now)
	if err := d.policy.Recover(); err != nil {
		return fmt.Errorf("recover policy engine: %w", err)
	}
	d.recorder = newEventRecorder(d.store)
	d.children, err = children.Open(ctx, d.store, cfg.ChildLimits, d.threadHooks(), d.recorder)
	if err != nil {
		return fmt.Errorf("open thread machine: %w", err)
	}
	d.threads = d.children.Threads()
	if err := d.threads.Restart(ctx); err != nil {
		return fmt.Errorf("restart thread machine: %w", err)
	}
	d.supervisor, err = supervisor.New(supervisor.Config{DataDir: d.dataDir, FreeSpace: cfg.freeSpace}, processSink{daemon: d})
	if err != nil {
		return fmt.Errorf("open supervisor: %w", err)
	}
	if err := d.recoverProcesses(ctx); err != nil {
		return fmt.Errorf("recover supervisor: %w", err)
	}
	d.driver, err = execdriver.NewDirect(d.supervisor, grantProof{})
	if err != nil {
		return fmt.Errorf("open Direct driver: %w", err)
	}
	d.git = gitops.NewWithEvidence(d.decideGit, nil)
	d.defaults = routing.Pair{Harness: cfg.DefaultHarness, Model: cfg.DefaultModel}
	d.taskKind, d.routeData, d.projects, d.poll, d.noLoop = cfg.TaskKind, cfg.Evidence, cfg.Projects, cfg.poll, cfg.noLoop
	if d.taskKind == "" {
		d.taskKind = "coding"
	}
	if d.poll <= 0 {
		d.poll = 200 * time.Millisecond
	}
	if d.defaultWatch, d.projectWatch, err = resolveWatch(cfg); err != nil {
		return err
	}
	if d.retryBase = cfg.retryBase; d.retryBase <= 0 {
		d.retryBase = time.Second
	}
	if err := d.openHarnesses(cfg); err != nil {
		return fmt.Errorf("open engine adapters: %w", err)
	}
	if err := d.openRemote(cfg.Remote); err != nil {
		return fmt.Errorf("open remote driver: %w", err)
	}
	return nil
}

// --- Server lifecycle: stop admission before draining work and storage ---

// Run exposes the recovered daemon until ctx is canceled or Close is called.
func (d *Daemon) Run(ctx context.Context) error {
	d.mu.Lock()
	if d.closed || d.server != nil {
		d.mu.Unlock()
		return errors.New("daemon is closed or already serving")
	}
	listener, err := api.Listen(ctx, d.host, d.port)
	if err != nil {
		d.mu.Unlock()
		return errors.Join(fmt.Errorf("listen on loopback: %w", err), d.Close())
	}
	port := fmt.Sprint(listener.Addr().(*net.TCPAddr).Port)
	server := api.New(api.Config{
		Store: d.store, Threads: d.threads, Tokens: d.tokens, Policy: d.policy, Port: port, WaitingFor: d.waitingFor,
		DiskPressure: d.pressure.Load,
	}).HTTPServer()
	httpCtx, stopHTTP := context.WithCancel(context.Background())
	server.BaseContext = func(net.Listener) context.Context { return httpCtx }
	server.RegisterOnShutdown(stopHTTP)
	d.server, d.listenAddr = server, listener.Addr()
	if !d.noLoop {
		d.workers.Add(1)
		go d.dispatchLoop()
	}
	d.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = d.Close() })
	defer stop()
	serveErr := server.Serve(listener)
	if errors.Is(serveErr, http.ErrServerClosed) {
		serveErr = nil
	}
	return errors.Join(serveErr, d.Close())
}

// Close stops the API, settles workers and supervised processes, then closes storage.
func (d *Daemon) Close() error {
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		server := d.server
		d.mu.Unlock()
		if server != nil {
			// Active SSE handlers observe cancellation and release their store reads.
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			if err := server.Shutdown(ctx); err != nil {
				d.closeErr = errors.Join(err, server.Close())
			}
			cancel()
		}
		d.cancel()
		d.workers.Wait()
		if d.remote != nil {
			d.closeErr = errors.Join(d.closeErr, d.remote.Close())
		}
		if d.supervisor != nil {
			d.closeErr = errors.Join(d.closeErr, d.supervisor.Close())
		}
		d.closeErr = errors.Join(d.closeErr, d.store.Close())
	})
	return d.closeErr
}
