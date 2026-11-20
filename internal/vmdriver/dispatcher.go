package vmdriver

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"sync"
	"time"
)

// --- Types and construction ---

// Command is one requested provider mutation.
type Command struct {
	Endpoint  string
	Thread    string
	Operation Operation
	VM        string
	Base      string
	Spec      VMSpec
	Snapshot  string
}

// resourceKey identifies one serialized resource on one endpoint.
type resourceKey struct{ endpoint, resource string }

// queue is a one-slot gate with a user count for cleanup.
type queue struct {
	ready chan struct{}
	users int
}

// Dispatcher is the single mutation coordinator for one driver and journal.
type Dispatcher struct {
	driver  VMDriver
	journal Journal
	auth    Authorizer
	mu      sync.Mutex
	queues  map[resourceKey]*queue
}

// NewDispatcher wires the single dispatch coordinator for a provider driver.
func NewDispatcher(driver VMDriver, journal Journal, auth Authorizer) *Dispatcher {
	return &Dispatcher{driver: driver, journal: journal, auth: auth, queues: make(map[resourceKey]*queue)}
}

// Dispatch serializes a mutation, records it before sending, and pauses on any
// outcome the provider has yet to establish. The returned ID enables recovery.
func (d *Dispatcher) Dispatch(ctx context.Context, cmd Command) (Resolution, error) {
	matrix, err := d.driver.Capabilities(ctx)
	if err != nil {
		return Resolution{}, err
	}
	return d.dispatch(ctx, cmd, matrix)
}

// --- Dispatch ---

// dispatch uses the selected matrix for transport admission and recovery evidence.
func (d *Dispatcher) dispatch(ctx context.Context, cmd Command, matrix MatrixRow) (Resolution, error) {
	resource := cmd.VM
	switch cmd.Operation {
	case CreateOp:
		resource = cmd.Spec.Name
		if cmd.Base == "" {
			return Resolution{}, ErrInvalid
		}
	case StartOp, StopOp, DeleteOp:
	case SnapshotOp, RestoreOp:
		if !matrix.Allows(cmd.Operation) {
			return Resolution{}, ErrUnavailable
		}
		if cmd.Snapshot == "" {
			return Resolution{}, ErrInvalid
		}
	default:
		return Resolution{}, ErrInvalid
	}
	if cmd.Thread == "" || cmd.Endpoint == "" || resource == "" {
		return Resolution{}, ErrInvalid
	}
	if validator, ok := d.driver.(CommandValidator); ok {
		if err := validator.ValidateCommand(cmd); err != nil {
			return Resolution{}, err
		}
	}
	release, err := d.acquire(ctx, resourceKey{cmd.Endpoint, resource})
	if err != nil {
		return Resolution{}, err
	}
	defer release()

	limits, err := d.authorize(ctx, cmd, resource)
	if err != nil {
		return Resolution{}, err
	}
	record, err := newRequest(cmd, matrix.Driver, resource)
	if err != nil {
		return Resolution{}, err
	}
	if err := d.journal.Begin(ctx, record, limits); err != nil {
		return Resolution{}, err
	}

	task, dispatchErr := d.send(ctx, record.Request, cmd)
	// Commit any known result even if the caller disconnected after dispatch.
	saveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if task.ID != "" {
		record.Task = task
		binding := TaskBinding{ID: record.Request.ID, Endpoint: cmd.Endpoint, TaskID: task.ID}
		if err := d.journal.Bind(saveCtx, cmd.Thread, binding); err != nil {
			return resolution(record, Unknown, None), errors.Join(ErrReconciliation, err)
		}
	}
	switch {
	case errors.Is(dispatchErr, ErrRejected), IsPreDispatch(dispatchErr):
		// A refusal raised before any mutation was sent settles as not_executed.
		r := resolution(record, NotExecuted, Response)
		r.Note = dispatchErr.Error()
		return d.save(saveCtx, record, r, "")
	case errors.Is(dispatchErr, ErrProvider):
		return d.save(saveCtx, record, resolution(record, Failed, Response), "")
	case dispatchErr == nil && task.ID == "" && cmd.Operation != CreateOp:
		return d.save(saveCtx, record, resolution(record, Succeeded, Response), "")
	default:
		r, err := d.observe(saveCtx, record, matrix)
		if err != nil && dispatchErr != nil {
			return r, errors.Join(err, dispatchErr)
		}
		return r, err
	}
}

// authorize combines the injected policy decision with ownership, mode and limits.
func (d *Dispatcher) authorize(ctx context.Context, cmd Command, resource string) (Limits, error) {
	owned := true
	if cmd.Operation != CreateOp {
		ownership, err := d.journal.Ownership(ctx, cmd.Endpoint, resource)
		if err != nil {
			return Limits{}, err
		}
		owned = ownership.CanMutate(cmd.Endpoint, resource, cmd.Thread)
	}
	permit, err := d.auth.Authorize(ctx, cmd.Thread, cmd.Endpoint, cmd.Operation, resource, owned)
	if err != nil {
		return Limits{}, err
	}
	if !owned {
		return Limits{}, ErrNotOwner
	}
	if !permit.Allowed || (cmd.Operation == CreateOp &&
		permit.Mode != "provider" && (permit.Mode != "sandbox" || !permit.Disposable)) {
		return Limits{}, ErrDenied
	}
	if cmd.Operation == CreateOp && (permit.Limits.Endpoint <= 0 || permit.Limits.Thread <= 0) {
		return Limits{}, ErrLimit
	}
	return permit.Limits, nil
}

// send routes one persisted operation to the backend without retrying.
func (d *Dispatcher) send(ctx context.Context, req ExternalRequest, cmd Command) (TaskRef, error) {
	switch req.Operation {
	case CreateOp:
		return d.driver.CreateFromBase(ctx, req, cmd.Base, cmd.Spec, cmd.Thread)
	case StartOp:
		return d.driver.Start(ctx, req, cmd.VM)
	case StopOp:
		return d.driver.Stop(ctx, req, cmd.VM)
	case DeleteOp:
		return d.driver.Delete(ctx, req, cmd.VM)
	case SnapshotOp:
		return d.driver.Snapshot(ctx, req, cmd.VM, cmd.Snapshot)
	case RestoreOp:
		return d.driver.Restore(ctx, req, cmd.VM, cmd.Snapshot)
	default:
		return TaskRef{}, ErrInvalid
	}
}

// --- Serialization and request identity (vm-driver §3) ---

// acquire waits for the preceding mutation or reconciliation on this VM.
func (d *Dispatcher) acquire(ctx context.Context, key resourceKey) (func(), error) {
	d.mu.Lock()
	q := d.queues[key]
	if q == nil {
		q = &queue{ready: make(chan struct{}, 1)}
		d.queues[key] = q
	}
	q.users++
	d.mu.Unlock()

	select {
	case q.ready <- struct{}{}:
		return func() {
			<-q.ready
			d.release(key, q)
		}, nil
	case <-ctx.Done():
		d.release(key, q)
		return nil, ctx.Err()
	}
}

// release reclaims an idle queue while holding the map lock.
func (d *Dispatcher) release(key resourceKey, q *queue) {
	d.mu.Lock()
	defer d.mu.Unlock()
	q.users--
	if q.users == 0 {
		delete(d.queues, key)
	}
}

// newRequest creates a fresh ULID and hashes the transport parameters.
func newRequest(cmd Command, driver, resource string) (RequestRecord, error) {
	id, err := newULID()
	if err != nil {
		return RequestRecord{}, err
	}
	params, err := json.Marshal(cmd)
	if err != nil {
		return RequestRecord{}, err
	}
	digest := sha256.Sum256(params)
	return RequestRecord{Thread: cmd.Thread, Request: ExternalRequest{
		ID: id, Driver: driver, Endpoint: cmd.Endpoint, Operation: cmd.Operation,
		Resource: resource, ParamsDigest: "sha256:" + hex.EncodeToString(digest[:]),
	}}, nil
}

// newULID combines a 48-bit millisecond timestamp with 80 bits of entropy.
func newULID() (string, error) {
	var raw [16]byte
	stamp := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		raw[i] = byte(stamp)
		stamp >>= 8
	}
	if _, err := rand.Read(raw[6:]); err != nil {
		return "", err
	}
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	value := new(big.Int).SetBytes(raw[:])
	var encoded [26]byte
	for i := len(encoded) - 1; i >= 0; i-- {
		encoded[i] = alphabet[value.Uint64()&31]
		value.Rsh(value, 5)
	}
	return string(encoded[:]), nil
}
