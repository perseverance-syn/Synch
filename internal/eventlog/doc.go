// Package eventlog owns SQLite storage, ordering, and backup for the event
// envelopes in the event catalogue and replay of runner journal records. It
// supplies the atomic writer required by receipts.md ?1 and ?5. Command
// admission, transition rules, and runner authorization belong to callers.

package eventlog
