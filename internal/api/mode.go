package api

import (
	"context"

	"synch/internal/eventlog"
)

// ModeCommand identifies the authenticated operator and execution target for a switch.
type ModeCommand struct {
	Actor, Thread, Target, To string
}

// ChangeMode prepares a switch within the receipt writer. Commit stages its durable
// effects in the acceptance transaction. AfterCommit settles outside the writer.
type ChangeMode func(context.Context, *eventlog.Transaction, ModeCommand) (commit func(context.Context, *eventlog.Transaction) error, afterCommit func(context.Context) error, err error)
