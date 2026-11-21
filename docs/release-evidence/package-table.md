# Race package table

`go test ./... -race -count=1`; not run denotes a package with no test files.

| Package | Status | Output |
|---|---|---|
| synch/cmd/synch | passed | 1.317s |
| synch/internal/api | passed | 36.029s |
| synch/internal/children | passed | 20.596s |
| synch/internal/ci | passed | 1.515s |
| synch/internal/ci/github | passed | 1.427s |
| synch/internal/ci/gitlab | passed | 1.493s |
| synch/internal/ci/jenkins | passed | 2.774s |
| synch/internal/cli | passed | 15.067s |
| synch/internal/context | passed | 5.562s |
| synch/internal/credentials | passed | 1.654s |
| synch/internal/daemon | passed | 111.381s |
| synch/internal/engine | passed | 1.527s |
| synch/internal/engine/claudecode | passed | 8.741s |
| synch/internal/engine/claudecode/stdio | not run | [no test files] |
| synch/internal/engine/claudecode/stdio/fixturetest | not run | [no test files] |
| synch/internal/engine/codex | passed | 9.221s |
| synch/internal/engine/kimi | passed | 1.424s |
| synch/internal/eventlog | passed | 57.143s |
| synch/internal/evidence | passed | 1.676s |
| synch/internal/execdriver | passed | 5.792s |
| synch/internal/findings | passed | 1.306s |
| synch/internal/gates | passed | 7.897s |
| synch/internal/gitops | passed | 98.219s |
| synch/internal/mcp | passed | 1.419s |
| synch/internal/policy | passed | 101.533s |
| synch/internal/queue | passed | 9.674s |
| synch/internal/remote | passed | 33.630s |
| synch/internal/roles | passed | 1.464s |
| synch/internal/routing | passed | 1.366s |
| synch/internal/sandbox | passed | 1.399s |
| synch/internal/sandbox/sandboxtest | not run | [no test files] |
| synch/internal/scanner | passed | 14.180s |
| synch/internal/scenarios | passed | 54.556s |
| synch/internal/supervisor | passed | 6.881s |
| synch/internal/thread | passed | 39.691s |
| synch/internal/vmdriver | passed | 2.039s |
| synch/internal/vmdriver/esxi | not run | [no test files] |
| synch/internal/vmdriver/hyperv | passed | 1.322s |
| synch/internal/vmdriver/proxmox | not run | [no test files] |
| synch/internal/vmdriver/virtualbox | passed | 1.231s |
| synch/internal/vmdriver/workstation | passed | 1.413s |
