// Package main owns entry-point dispatch for Synch's one-binary runtime.
// The daemon package owns task state, process supervision and the local API.
//
// The daemon is configured by environment variables. Only SYNCH_TOKEN is required:
//
//	SYNCH_TOKEN             bearer token for local clients (required)
//	SYNCH_DATA_DIR          data directory (default: <user config dir>/synch)
//	SYNCH_LISTEN            loopback listen address (default 127.0.0.1 and the API port)
//	SYNCH_POLICY_STORE      path to the trusted operator policy file
//	SYNCH_CODEX_BIN         Codex executable (default: codex on PATH)
//	SYNCH_CLAUDE_BIN        Claude Code executable (default: claude on PATH)
//	SYNCH_KIMI_BIN          Kimi Code executable (default: kimi on PATH)
//	SYNCH_DEFAULT_HARNESS   configured default harness: codex, claude-code or kimi
//	SYNCH_DEFAULT_MODEL     configured default model for that harness
//	SYNCH_PROJECTS          JSON object mapping an API project_id to its checkout,
//	                        e.g. {"app":{"dir":"/src/app","gate_file":".synch/gates.json"}}
//	SYNCH_CHILD_MAX_RUNTIME longest a managed child's engine session may run, a Go duration such as
//	                        2h or 45m (default 2h); 0 or off disables the budget. Past it the
//	                        session is stopped and the child pauses (T15)
//	SYNCH_CHILD_LOOP_REPEATS consecutive identical tool calls with no other output that count as a
//	                        loop (default 5, at least 2); 0 or off disables it. Handled like the budget
//
// Each SYNCH_PROJECTS entry may override both for that project, in the same formats:
//
//	{"app":{"dir":"/src/app","max_runtime":"45m","loop_repeats":8}}
//
// Precedence is project, then the SYNCH_CHILD_* default, then the built-in 2h and 5.
// A value that is not valid in either place stops the daemon from starting.
//
// A harness whose executable cannot be found is not offered to routing. With the
// shipped capability declarations every operation is untested, so a daemon given
// only SYNCH_TOKEN pauses every thread with no_eligible_pair rather than run it.
package main
