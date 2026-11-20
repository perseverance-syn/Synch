package cli

import (
	"flag"
	"fmt"
	"io"
)

// Exit codes returned by Main, documented in the package comment.
const (
	exitOK          = 0
	exitUsage       = 1
	exitProblem     = 2
	exitUnreachable = 3
)

// globals are the options accepted anywhere in the argument list.
type globals struct {
	json        bool
	server      string
	tokenFile   string
	requestID   string
	allowRemote bool
	configPath  string
}

// globalValueFlags maps the value-taking global options to their destination.
func globalValueFlags(g *globals) map[string]*string {
	return map[string]*string{
		"--server":     &g.server,
		"--token-file": &g.tokenFile,
		"--request-id": &g.requestID,
		"--config":     &g.configPath,
	}
}

// parseGlobals strips the global options from anywhere in the argument list,
// leaving the subcommand and its arguments for dispatch.
func parseGlobals(args []string) (*globals, []string) {
	g := &globals{}
	values := globalValueFlags(g)
	rest := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		switch {
		case arg == "--json":
			g.json = true
		case arg == "--allow-remote":
			g.allowRemote = true
		case values[arg] != nil && i+1 < len(args):
			*values[arg] = args[i+1]
			i++
		default:
			rest = append(rest, arg)
		}
	}
	return g, rest
}

// Main runs one CLI invocation against the local API and returns the exit code.
func Main(args []string, stdout, stderr io.Writer, env func(string) string) int {
	g, rest := parseGlobals(args)
	if len(rest) == 0 {
		printUsage(stderr)
		return exitUsage
	}
	out := &printer{stdout: stdout, stderr: stderr, json: g.json}
	switch rest[0] {
	case "health":
		return runHealth(g, env, out, rest[1:])
	case "threads":
		return runThreads(g, env, out, rest[1:])
	case "thread":
		return runThreadCommand(g, env, out, rest[1:])
	case "approvals":
		return runApprovals(g, env, out, rest[1:])
	case "grants":
		return runGrants(g, env, out, rest[1:])
	case "receipt":
		return runReceipt(g, env, out, rest[1:])
	case "evidence":
		return runEvidence(g, env, out, rest[1:])
	case "events":
		return runEvents(g, env, out, rest[1:])
	case "setup":
		return runSetup(g, env, out, rest[1:])
	default:
		out.diag("unknown command %q\n\n", rest[0])
		printUsage(stderr)
		return exitUsage
	}
}

// clientFor builds an authenticated client or reports a local configuration
// error. Every subcommand except setup needs it.
func clientFor(g *globals, env func(string) string) (*client, error) {
	cfg, err := resolveConfig(g, env)
	if err != nil {
		return nil, err
	}
	return newClient(cfg), nil
}

// diag writes a diagnostic to stderr, ignoring write failures because the
// exit code already reports the outcome.
func (p *printer) diag(format string, args ...any) {
	_, _ = fmt.Fprintf(p.stderr, format, args...)
}

// localError prints a configuration or argument failure and returns the usage exit code.
func localError(out *printer, err error) int {
	out.diag("%v\n", err)
	return exitUsage
}

// serverError prints a problem the server answered and returns the problem exit code.
func serverError(out *printer, resp *response) int {
	p := resp.problem
	if p == nil {
		out.diag("server returned an unparseable error\n")
		return exitProblem
	}
	if p.Detail != "" {
		out.diag("error %d %s: %s\n", resp.status, p.Code, p.Detail)
		return exitProblem
	}
	out.diag("error %d %s\n", resp.status, p.Code)
	return exitProblem
}

// printUsage lists the commands; every route of openapi.yaml is reachable through one.
func printUsage(w io.Writer) {
	_, _ = fmt.Fprint(w, `Usage: synch [global options] <command> [arguments]

Global options (accepted anywhere): --json --server URL --token-file PATH
                --request-id ID --allow-remote --config PATH

Commands:
  health                                     daemon health and log position
  threads list [--project ID] [--state S]    list threads
  threads create --project ID --brief TEXT   submit a task (creates a thread)
        [--criteria A,B] [--review-plan-first] [--audit LEVEL]
        [--pin-harness H --pin-model M]
  threads show THREAD-ID                     show one thread
  thread cmd THREAD-ID COMMAND [FIELD=VALUE...]   send a thread command
        commands: pause resume cancel approve_plan select_harness decide
        accept reject change_mode resolve_external_request
  approvals create THREAD-ID [flags]         record a bounded approval
        --worktree-digest D --target T --actions A,B --expires-at RFC3339
        (--revision R | --plan-digest D) [--rollback C] [--irreversible-effects A,B]
  grants create THREAD-ID --host H --identity I   grant Full Control
  grants revoke GRANT-ID                     revoke a grant (no body)
  receipt REQUEST-ID                         look up one of your receipts
  evidence export THREAD-ID --out PATH       export journal evidence
  events page STREAM [--after N] [--limit N] page persisted events
  events follow STREAM [--until N]           follow the live SSE stream
  setup [--config PATH] [--server URL] [--token-file PATH]   write the config file and check the daemon
`)
}

// newFlagSet builds a subcommand flag set for its own options only; the
// global options are stripped by parseGlobals before dispatch.
func newFlagSet(name string, out *printer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(out.stderr)
	return fs
}
