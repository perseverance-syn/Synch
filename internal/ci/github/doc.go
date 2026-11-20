// Package github implements ci.Provider for GitHub Actions over the REST API
// (workflow dispatch, workflow runs, jobs, job logs, cancel). It owns the
// GitHub request shapes and the mapping of status/conclusion to ci.Verdict
// (ci-driver.md §4). It does not own the dispatcher, redaction or result
// binding (package ci) and never holds a token: it asks the injected broker.
//
// Contract sections implemented: ci-driver.md §§1-2, 4-6. GitHub returns no run
// on dispatch, so the run is found by the workflow input synch_token, which
// the repository workflow must echo as run-name.
package github
