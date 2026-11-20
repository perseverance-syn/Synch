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
	"synch/internal/engine"
	"synch/internal/engine/codex"
	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/gitops"
	"synch/internal/policy"
	"synch/internal/supervisor"
	"synch/internal/thread"
)

// Config supplies daemon storage, local authentication and launcher configuration.
type Config struct {
	DataDir     string
	Listen      string
	PolicyStore string
	Tokens      api.TokenVerifier
	// CodexBinary defaults to codex. An unqualified adapter cannot start it.
	CodexBinary string
	adapter     func(engine.Sink) (engine.EngineAdapter, error)
	freeSpace   func(string) (free, total uint64, err error)
}

// Daemon owns the components sharing one durable event store.
type Daemon struct {
	store      *eventlog.Store
	threads    *thread.Machine
	policy     *policy.Engine
	adapter    engine.EngineAdapter
	driver     *execdriver.Direct
	supervisor *supervisor.Supervisor
	git        *gitops.Service
	rules      []*policy.File
	dataDir    string
	host       string
	port       string
	tokens     api.TokenVerifier
	pressure   atomic.Bool

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
	d.threads, err = thread.Open(ctx, d.store, d.threadHooks())
	if err != nil {
		return fmt.Errorf("open thread machine: %w", err)
	}
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
	d.adapter, err = d.newAdapter(cfg)
	if err != nil {
		return fmt.Errorf("open engine adapter: %w", err)
	}
	return nil
}

// newAdapter uses the capability-closed Codex declaration unless a test injects a fake.
func (d *Daemon) newAdapter(cfg Config) (engine.EngineAdapter, error) {
	sink := engineSink{daemon: d}
	if cfg.adapter != nil {
		return cfg.adapter(sink)
	}
	binary := cfg.CodexBinary
	if binary == "" {
		binary = "codex"
	}
	launcher := deferredLauncher{open: sync.OnceValues(func() (*codex.DirectLauncher, error) {
		return codex.NewDirectLauncher(policyDriver{ExecutionDriver: d.driver, daemon: d}, binary)
	})}
	return codex.New(launcher, sink, nil)
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
		Store: d.store, Threads: d.threads, Tokens: d.tokens, Policy: d.policy, Port: port,
		DiskPressure: d.pressure.Load,
	}).HTTPServer()
	httpCtx, stopHTTP := context.WithCancel(context.Background())
	server.BaseContext = func(net.Listener) context.Context { return httpCtx }
	server.RegisterOnShutdown(stopHTTP)
	d.server, d.listenAddr = server, listener.Addr()
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
		if d.supervisor != nil {
			d.closeErr = errors.Join(d.closeErr, d.supervisor.Close())
		}
		d.closeErr = errors.Join(d.closeErr, d.store.Close())
	})
	return d.closeErr
}
