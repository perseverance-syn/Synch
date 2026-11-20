# DRAFT — owner acceptance required

Use the Go 1.27 toolchain. CI reads the version from `go.mod` using `actions/setup-go` with `go-version-file: go.mod`.

Keep CLI dispatch in `cmd/synch` and implementation in focused packages under `internal/`.
Wrap errors with operation context using `%w`. Use sentinel errors for conditions callers can handle with `errors.Is`. Return errors across package boundaries and keep panics from crossing them.

Use structured logging with `log/slog`. Write diagnostics to stderr and exclude credentials from logs.

Run `go build ./...`, `go test ./... -race`, `go vet ./...` and `golangci-lint run` at the repository root. In `desktop/`, run `npm ci`, `npm run typecheck` and `npm run build`. Use `npm start` to open the desktop.

## Code comments (owner rule, 2026-11-18)
- Every package has a `doc.go` whose package comment states the package's job: what it owns, what it does not, and which contract sections it implements.
- Every function and method, exported **and** unexported, has a doc comment saying what it does, and why when that is not obvious (contract rule, invariant, failure it prevents). Start with the name, Go style: `// Append persists events in one transaction and ...`.
- Files longer than about 150 lines are split into sections with a header comment naming the section's job, e.g. `// --- Recovery: reclassify interrupted work after a daemon restart (thread-state-machine §3) ---`.
- Non-obvious blocks inside a function (locking order, retry bounds, Windows-specific calls) get a one-line comment on why.
- Tests: each test function has a one-line comment naming the acceptance criterion or contract rule it proves.
- Reviewers check this rule; missing comments are a required change.

Name branches `feat/NNN-slug`. Format commit subjects as `<area>: <summary> (card NNN)`.
