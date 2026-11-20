package vmdriver

import (
	"context"
	"time"
)

// --- Journal events, outcomes and records ---

// Journal event names and schema versions.
const (
	EventPersisted   = "external_request.persisted"
	EventTaskBound   = "external_request.task_bound"
	EventResolved    = "external_request.resolved"
	PersistedVersion = 3
	TaskBoundVersion = 1
	ResolvedVersion  = 4
)

// Outcome is the settled result of an external request.
type Outcome string

// Via names the evidence that settled a request.
type Via string

// Outcomes and evidence sources.
const (
	Succeeded          Outcome = "succeeded"
	Failed             Outcome = "failed"
	NotExecuted        Outcome = "not_executed"
	Unknown            Outcome = "unknown"
	Response           Via     = "response"
	ProviderTask       Via     = "provider_task"
	ProviderTaskAbsent Via     = "provider_task_absent"
	ResultingResource  Via     = "resulting_resource"
	Operator           Via     = "operator"
	None               Via     = "none"
)

// TaskBinding ties a request to the provider task it was accepted as.
type TaskBinding struct {
	ID       string `json:"external_request_id"`
	Endpoint string `json:"endpoint"`
	TaskID   string `json:"provider_task_id"`
}

// AbsenceLookup records an authoritative token query that found nothing.
type AbsenceLookup struct {
	ClientToken string    `json:"client_token"`
	QueriedAt   time.Time `json:"queried_at"`
}

// Resolution is one recorded observation about a request.
type Resolution struct {
	ID         string            `json:"external_request_id"`
	Operation  Operation         `json:"operation"`
	Outcome    Outcome           `json:"resolution"`
	Via        Via               `json:"via"`
	TaskID     string            `json:"provider_task_id,omitempty"`
	Resource   *ResourceEvidence `json:"resource_evidence,omitempty"`
	Absence    *AbsenceLookup    `json:"absence_lookup,omitempty"`
	OperatorID string            `json:"operator,omitempty"`
	Note       string            `json:"note,omitempty"`
}

// RequestRecord is a persisted request with its task and observation history.
type RequestRecord struct {
	Request ExternalRequest
	Thread  string
	Task    TaskRef
	History []Resolution
}

// Limits caps VMs per endpoint and per thread.
type Limits struct{ Endpoint, Thread int }

// Journal is the daemon's durable transaction boundary. Begin atomically checks
// G.1 for the thread and endpoint/resource, rejects duplicate IDs, rechecks
// ownership and claims create capacity (counting pending creates) before
// appending persisted. Writers use one Dispatcher per driver and journal.
// Resolve atomically appends all observations, creates ownership for a
// successful create with a known VM ref or retires it for a successful delete.
// Recovery must preserve later assignments and deletions. An unresolved
// create gates its known VM ref as well as its intended name. A successful
// create without a VM ref keeps its capacity claim until ownership can be
// associated. Load supplies committed history on restart.
type Journal interface {
	Ownership(context.Context, string, string) (Ownership, error)
	Begin(context.Context, RequestRecord, Limits) error
	Bind(context.Context, string, TaskBinding) error
	Resolve(context.Context, string, []Resolution, *Ownership) error
	Load(context.Context, string) (RequestRecord, error)
}

// Permit is the policy decision for one operation.
type Permit struct {
	Allowed    bool // Includes a satisfied ask.
	Mode       string
	Disposable bool
	Limits     Limits
}

// Authorizer receives the exact ownership decision so it can record a
// policy.decided deny with not_owner even if policy would otherwise allow.
type Authorizer interface {
	Authorize(ctx context.Context, thread, endpoint string, op Operation, resource string, owned bool) (Permit, error)
}
