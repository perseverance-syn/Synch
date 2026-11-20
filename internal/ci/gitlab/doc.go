// Package gitlab implements ci.Provider for GitLab CI over the REST API v4
// (create pipeline, list pipelines and variables, pipeline jobs, job trace,
// cancel). It owns the GitLab request shapes and the mapping of pipeline and
// job statuses to ci.Verdict (ci-driver.md §4). It does not own the
// dispatcher, redaction or result binding (package ci) and never holds a
// token: it asks the injected broker per call.
//
// Contract sections implemented: ci-driver.md §§1-2, 4-6. The request token
// travels as the pipeline variable SYNCH_TOKEN and is found again by reading
// pipeline variables.
package gitlab
