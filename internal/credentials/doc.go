// Package credentials stores Synch secrets, brokers them at the moment of use and
// redacts every known value. It implements spec "Policy and credentials" (line 74):
// "Keep user and thread credentials separate. Store secrets in the OS keychain with
// an age-encrypted fallback. Broker secrets at use and redact known values from
// supported inputs, outputs, storage and exports", plus lines 34/102 (the
// coordinator never receives infrastructure credentials) and line 66 (drivers hold
// infrastructure credentials).
//
// It owns three things:
//
//   - Store: secrets kept in the OS keychain (Windows Credential Manager through
//     advapi32, macOS `security`, Linux `secret-tool` through an injected command
//     runner), each under a Scope: the user scope or one thread's scope. Select picks
//     the OS keychain or, when none is available, the fallback store. The fallback is
//     a stub that always fails until the age-encrypted store is approved (card 401 C5).
//   - Broker: the only reader of secrets. Callers never choose a scope: a thread
//     caller reads only its own thread scope, a driver caller only the user scope,
//     and a coordinator caller is always refused. Adapters satisfy ci.TokenBroker,
//     vmdriver.Broker and remote.Broker with driver identity, so infrastructure
//     credentials stay inside daemon-side drivers. Every value handed out is first
//     registered with the Redactor.
//   - Redactor: every known value, and its base64, URL-encoded and JSON-escaped
//     forms, replaced longest-first by "[REDACTED]" in strings, streams (retained
//     output, exports) and event payloads (the eventlog.Redactor hook).
//
// It does not decide policy (the daemon wires secret.use through policy), does not
// write events and does not hand-roll encryption.
package credentials
