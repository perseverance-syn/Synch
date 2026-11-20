package remote

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"

	"synch/internal/eventlog"
	"synch/internal/execdriver"
	"synch/internal/policy"
)

var (
	errOffline           = errors.New("runner connection unavailable")
	ErrDispatchUncertain = errors.New("remote dispatch may have started")
)

const pollInterval = 100 * time.Millisecond
const spawnAction = "remote.spawn"

// Decider is the policy engine boundary for new remote actions.
type Decider interface {
	Decide(context.Context, policy.Request) (policy.Decision, error)
}

// DispatchOptions carries policy approval and explicit forwarding authority for one assignment.
type DispatchOptions struct {
	ApprovalID string
	Forwarding ForwardingGrant
}

// RunnerStatus reports whether the runner is ready for new work and why it lost readiness.
type RunnerStatus struct {
	Connected, Degraded bool
	Error               error
}

// Driver dispatches to one registered runner and ingests its journal into the daemon log.
type Driver struct {
	registry *Registry
	runner   string
	broker   Broker
	policy   Decider
	log      *eventlog.Store
	ledger   *sql.DB

	lifecycle sync.Mutex
	replayMu  sync.Mutex
	mu        sync.Mutex
	link      *runnerLink
	closed    bool
	status    RunnerStatus
	procs     map[string]*remoteProc
	shutdown  chan struct{}
}

type runnerLink struct {
	target   Target
	client   *ssh.Client
	ctx      context.Context
	cancel   context.CancelFunc
	cursor   int64                    // guarded by replayMu
	forwards map[*ssh.Client]struct{} // guarded by Driver.mu
}

var _ execdriver.ExecutionDriver = (*Driver)(nil)

// NewDriver opens the daemon ledger used to authorize replay and prevent repeated dispatch.
func NewDriver(registry *Registry, runner string, broker Broker, decider Decider, log *eventlog.Store, ledgerPath string) (*Driver, error) {
	if registry == nil || broker == nil || decider == nil || log == nil || !runnerID.MatchString(runner) {
		return nil, errors.New("remote driver requires a registry, runner, SSH broker, policy, and event log")
	}
	db, err := openStorage(ledgerPath)
	if err != nil {
		return nil, err
	}
	return &Driver{registry: registry, runner: runner, broker: broker, policy: decider, log: log, ledger: db, procs: make(map[string]*remoteProc), shutdown: make(chan struct{})}, nil
}

// Kind returns the registered remote execution kind.
func (*Driver) Kind() string { return execdriver.KindRemote }

// Describe returns the target account and the runner journal evidence source.
func (d *Driver) Describe() execdriver.DriverInfo {
	target, _ := d.registry.Lookup(d.runner)
	return execdriver.DriverInfo{Label: "Remote: " + d.runner, Host: target.Host, Identity: target.User, EvidenceIntegrity: "runner journal, replayed on reconnect"}
}

// Status returns a snapshot of the connection and its replay health.
func (d *Driver) Status() RunnerStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.status
}

// Close shuts down the daemon connection and closes its dispatch ledger.
func (d *Driver) Close() error {
	d.lifecycle.Lock()
	defer d.lifecycle.Unlock()
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	close(d.shutdown)
	d.mu.Unlock()
	d.disconnect()
	d.replayMu.Lock()
	defer d.replayMu.Unlock()
	return d.ledger.Close()
}

// --- Connections and admission (execution-driver §3, T19) ---

// Reconnect authenticates the pinned target and replays its backlog before reopening admission.
func (d *Driver) Reconnect(ctx context.Context) error {
	d.lifecycle.Lock()
	defer d.lifecycle.Unlock()
	d.disconnect()
	d.mu.Lock()
	closed := d.closed
	d.mu.Unlock()
	if closed {
		return errOffline
	}
	target, ok := d.registry.Lookup(d.runner)
	if !ok {
		err := &HostKeyReviewError{Runner: d.runner}
		d.setStatus(err)
		return err
	}
	if !target.permits(OpReplay) {
		d.setStatus(ErrOperationDenied)
		return ErrOperationDenied
	}
	client, err := dialSSH(ctx, target, d.broker)
	if err != nil {
		d.setStatus(err)
		return err
	}
	linkCtx, cancel := context.WithCancel(context.Background())
	link := &runnerLink{client: client, target: target, ctx: linkCtx, cancel: cancel, forwards: make(map[*ssh.Client]struct{})}
	if err := d.recoverJournal(ctx, link); err != nil {
		cancel()
		_ = client.Close()
		d.setStatus(err)
		return err
	}
	d.mu.Lock()
	d.link = link
	d.status = RunnerStatus{Connected: true}
	d.mu.Unlock()
	go d.follow(link)
	return nil
}

// Disconnect drops the daemon transport without cancelling runner-owned work.
func (d *Driver) Disconnect() {
	d.lifecycle.Lock()
	defer d.lifecycle.Unlock()
	d.disconnect()
}

// disconnect drops the active transport while the lifecycle lock is held.
func (d *Driver) disconnect() {
	d.mu.Lock()
	link := d.link
	d.mu.Unlock()
	if link != nil {
		d.drop(link, errOffline)
	}
}

// setStatus records a disconnected connection attempt and flags protocol violations.
func (d *Driver) setStatus(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.status = RunnerStatus{Degraded: d.status.Degraded || degrades(err), Error: err}
}

// drop removes only the failed connection and releases its forwarded agent channels.
func (d *Driver) drop(link *runnerLink, err error) {
	d.mu.Lock()
	if d.link != link {
		d.mu.Unlock()
		return
	}
	d.link = nil
	d.status = RunnerStatus{Degraded: d.status.Degraded || degrades(err), Error: err}
	forwards := link.forwards
	link.forwards = nil
	d.mu.Unlock()
	link.cancel()
	_ = link.client.Close()
	for client := range forwards {
		_ = client.Close()
	}
}

// connection checks that the active SSH transport is ready and permits the operation.
func (d *Driver) connection(operation string, admission bool) (*runnerLink, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.link == nil {
		if admission {
			return nil, &DisconnectedError{Runner: d.runner}
		}
		return nil, errOffline
	}
	if !d.link.target.permits(operation) {
		return nil, ErrOperationDenied
	}
	return d.link, nil
}

// DoFresh mediates a new approval-dependent action only while its target is connected.
func (d *Driver) DoFresh(ctx context.Context, request policy.Request) (policy.Decision, error) {
	link, err := d.connection(request.Action, true)
	if err != nil {
		return policy.Decision{}, err
	}
	return d.decide(ctx, link, request, request.Start)
}

// decide checks that the same ready connection is still in place inside policy's dispatch boundary.
func (d *Driver) decide(ctx context.Context, link *runnerLink, req policy.Request, start func() error) (policy.Decision, error) {
	req.Target, req.Machine, req.Identity = d.runner, link.target.Host, link.target.User
	req.Start = func() error {
		d.mu.Lock()
		active := d.link == link
		d.mu.Unlock()
		if !active {
			return &DisconnectedError{Runner: d.runner}
		}
		if start != nil {
			return start()
		}
		return nil
	}
	return d.policy.Decide(ctx, req)
}

// call sends a registered operation over the currently active SSH transport.
func (d *Driver) call(ctx context.Context, link *runnerLink, req wireRequest) (wireResponse, error) {
	res, _, err := callSSH(ctx, link.client, d.runner, req)
	if err != nil {
		if ctx.Err() == nil {
			d.drop(link, err)
		}
		return res, err
	}
	return res, responseError(res.Error)
}

// --- Dispatch and settlement (decisions 15 and 18) ---

// Spawn implements the execution driver with agent forwarding disabled by default.
func (d *Driver) Spawn(ctx context.Context, spec execdriver.ProcSpec) (execdriver.Proc, error) {
	return d.Dispatch(ctx, spec, DispatchOptions{})
}

// Dispatch admits a new assignment, persists it, and submits it at most once.
func (d *Driver) Dispatch(ctx context.Context, spec execdriver.ProcSpec, opts DispatchOptions) (execdriver.Proc, error) {
	link, err := d.connection(OpSpawn, true)
	if err != nil {
		return nil, err
	}
	if !ulid.MatchString(spec.ID) || !ulid.MatchString(spec.ThreadID) || spec.Path == "" {
		return nil, errors.New("invalid remote process specification")
	}
	if spec.ProtocolIO && !link.target.permits(OpProtocol) {
		return nil, ErrOperationDenied
	}
	request := policy.Request{Thread: spec.ThreadID, Action: spawnAction, ApprovalID: opts.ApprovalID, Args: append([]string{spec.Path}, spec.Args...)}
	if spec.Dir != "" {
		request.Paths = []string{spec.Dir}
	}
	proc := newRemoteProc(d, spec.ThreadID, spec.ID, spec.ProtocolIO)
	decision, err := d.decide(ctx, link, request, func() error { return d.dispatch(ctx, link, spec, opts, proc) })
	if err != nil {
		return nil, err
	}
	if !decision.Allowed {
		return nil, ErrOperationDenied
	}
	return proc, nil
}

// dispatch reserves the assignment before sending and retains it if the outcome is uncertain.
func (d *Driver) dispatch(ctx context.Context, link *runnerLink, spec execdriver.ProcSpec, opts DispatchOptions, proc *remoteProc) error {
	client := link.client
	var scoped agent.Agent
	if opts.Forwarding != nil {
		forward, broker, err := d.forwardingClient(ctx, link, spec.ThreadID, opts.Forwarding)
		if err != nil {
			return err
		}
		client, scoped = forward, broker
	}
	if err := d.rememberDispatch(ctx, link, proc); err != nil {
		if client != link.client {
			d.releaseForward(link, client)
		}
		return err
	}
	if scoped != nil {
		if err := agent.ForwardToAgent(client, signingAgent{Agent: scoped}); err != nil {
			d.releaseForward(link, client)
			return fmt.Errorf("enable scoped SSH forwarding: %w", err)
		}
	}
	req := wireRequest{Operation: OpSpawn, Spec: &spec, ForwardAgent: client != link.client}
	res, _, err := callSSH(ctx, client, d.runner, req)
	if err == nil {
		err = responseError(res.Error)
		if err == nil && req.ForwardAgent {
			proc.onExit(func() { d.releaseForward(link, client) })
		}
	} else {
		if !req.ForwardAgent && ctx.Err() == nil {
			d.drop(link, err)
		}
		err = errors.Join(ErrDispatchUncertain, err)
	}
	if err != nil && req.ForwardAgent {
		d.releaseForward(link, client)
	}
	return err
}

// rememberDispatch reserves the ID and publishes its handle atomically with concurrent attachment.
func (d *Driver) rememberDispatch(ctx context.Context, link *runnerLink, proc *remoteProc) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.link != link || d.closed {
		return &DisconnectedError{Runner: d.runner}
	}
	if err := reserve(ctx, d.ledger, d.runner, proc.id, proc.thread); err != nil {
		return err
	}
	d.procs[proc.id] = proc
	return nil
}

// Settle stops new assignments on the runner and confirms whether process trees were removed.
func (d *Driver) Settle(ctx context.Context, thread string) (execdriver.SettleReport, error) {
	link, err := d.connection(OpSettle, false)
	if err != nil {
		return execdriver.SettleReport{}, err
	}
	res, err := d.call(ctx, link, wireRequest{Operation: OpSettle, Thread: thread})
	if res.Settle == nil {
		return execdriver.SettleReport{}, err
	}
	return *res.Settle, err
}

// forwardingClient verifies a request grant and prepares a dedicated transport without exposing the agent yet.
func (d *Driver) forwardingClient(ctx context.Context, link *runnerLink, thread string, grant ForwardingGrant) (*ssh.Client, agent.Agent, error) {
	if !link.target.permits(OpAgentForwarding) {
		return nil, nil, ErrOperationDenied
	}
	allowed, err := grant.AllowsAgentForwarding(ctx, link.target, thread)
	if err != nil {
		return nil, nil, fmt.Errorf("check forwarding grant: %w", err)
	}
	if !allowed {
		return nil, nil, ErrOperationDenied
	}
	client, err := dialSSH(ctx, link.target, d.broker)
	if err != nil {
		return nil, nil, err
	}
	scoped, err := d.broker.ForwardingAgent(ctx, link.target, thread)
	if err != nil || scoped == nil {
		_ = client.Close()
		return nil, nil, errors.New("scoped SSH forwarding broker unavailable")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.link != link {
		_ = client.Close()
		return nil, nil, &DisconnectedError{Runner: d.runner}
	}
	link.forwards[client] = struct{}{}
	return client, scoped, nil
}

// releaseForward removes and closes a completed assignment's dedicated forwarding transport.
func (d *Driver) releaseForward(link *runnerLink, client *ssh.Client) {
	d.mu.Lock()
	// After drop, the replaced map belongs to the disconnect path.
	delete(link.forwards, client)
	d.mu.Unlock()
	_ = client.Close()
}
