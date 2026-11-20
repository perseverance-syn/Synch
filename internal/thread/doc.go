// Package thread owns durable thread transitions, dispatch guards, process
// recovery, and rollover handoff under thread-state-machine.md ??1?6 and
// decisions D-08 and D-09. It consumes command and outcome records. Receipt
// storage, policy evaluation, routing, and process supervision live outside
// this package.

package thread
