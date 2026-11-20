// Package api implements the loopback Synch v1 HTTP surface in api/openapi.yaml,
// including caller authentication, receipts.md request ordering, and event
// paging and SSE replay. Queue owns receipt persistence, thread owns lifecycle
// transitions, and eventlog owns event storage and ordering. Policy evaluation
// and keychain token storage are supplied by the daemon.
package api
