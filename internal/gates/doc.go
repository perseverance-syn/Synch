// Package gates owns the local gate runner: the gate file format, the ordered
// execution of checks through an injected executor, the strict report parsers
// for junit-xml, go-test-json, golangci-lint-json and sarif, and the
// check.result records of contracts/events/catalogue.schema.json
// ($defs/e_check_result). It implements the spec's local half of "Keep ordered
// checks, thresholds and report parsers in the repository": checks run here,
// and CI integrations (GitHub Actions, GitLab CI, Jenkins) are later cards
// that reuse the same gate file and parsers, so the format stays CI-neutral.
//
// The daemon owns the event envelope (stream, thread_id, actor); this package
// produces only the check.result payload records. A malformed or unsupported
// report can never produce passed: malformed reports yield error, partly
// readable reports yield partial, and the stricter of exit code and report
// outcome wins.
//
// The gate file is JSON at .synch/gates.json (default location; callers pass
// the bytes). One complete example:
//
//	{
//	  "version": 1,
//	  "checks": [
//	    {
//	      "id": "unit-tests",
//	      "command": ["sh", "-c", "go test -json ./... > test-report.json"],
//	      "workdir": "server",
//	      "timeout": 300,
//	      "version_command": ["go", "version"],
//	      "report": {"path": "test-report.json", "format": "go-test-json"},
//	      "threshold": {"max_failures": 0},
//	      "required": true
//	    },
//	    {
//	      "id": "lint",
//	      "command": ["golangci-lint", "run", "--output.json.path", "lint.json"],
//	      "version_command": ["golangci-lint", "--version"],
//	      "report": {"path": "lint.json", "format": "golangci-lint-json"},
//	      "threshold": {"max_issues": 0},
//	      "required": false
//	    },
//	    {
//	      "id": "sast",
//	      "command": ["sast-scanner", "--sarif", "out", "sast.sarif"],
//	      "report": {"path": "sast.sarif", "format": "sarif"},
//	      "threshold": {"max_results": 0},
//	      "required": false
//	    }
//	  ]
//	}
//
// A command is an argv with no shell, so the go-test example names its shell
// to redirect go test's stdout into the declared report path. The runner reads
// the revision from the checkout (git rev-parse HEAD in the working
// directory), deletes the report path before launching each command, and
// treats a missing, oversized (over 16 MiB) or invalid report as error. A
// command that panics, is killed or is cancelled is error with a reason, and a
// timeout kills the whole process tree.
//
// Rules, all enforced at load time as errors: unknown JSON fields anywhere,
// duplicate check ids, empty id, empty command or version_command argv or an
// empty argv element, an unknown report format, a threshold key the format
// does not define, a negative threshold or timeout, a missing gate version
// other than 1, and an empty checks list. The timeout is seconds; report
// thresholds default to zero allowed failures or issues, so a failing report
// fails its check even when the command exited 0.
package gates
