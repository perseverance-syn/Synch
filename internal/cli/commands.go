package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// --- Shared command plumbing ---

// callContext bounds one ordinary request; streams set their own deadlines.
func callContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 30*time.Second)
}

// splitFlags moves flag arguments (and their values) ahead of the positional
// arguments, so operators may write flags after positionals as well. An
// unknown option is an argument error.
func splitFlags(fs *flag.FlagSet, args []string) ([]string, error) {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "--") {
			positional = append(positional, arg)
			continue
		}
		name, _, hasValue := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
		entry := fs.Lookup(name)
		if entry == nil {
			return nil, fmt.Errorf("unknown option --%s", name)
		}
		flags = append(flags, arg)
		boolFlag, isBool := entry.Value.(interface{ IsBoolFlag() bool })
		if isBool && boolFlag.IsBoolFlag() || hasValue {
			continue
		}
		if i+1 >= len(args) {
			return nil, fmt.Errorf("option --%s needs a value", name)
		}
		i++
		flags = append(flags, args[i])
	}
	return append(flags, positional...), nil
}

// unreachable reports a failed connection with the transport exit code.
func unreachable(out *printer, err error) int {
	out.diag("%v\n", err)
	return exitUnreachable
}

// printResult renders a successful GET response or fails on a write error.
func printResult(out *printer, resp *response) int {
	if err := out.rawBody(resp); err != nil {
		return localError(out, err)
	}
	return exitOK
}

// mutate posts one command body with the caller's request ID and prints the
// receipt. Stored rejections and conflicts surface as problem errors.
func mutate(g *globals, env func(string) string, out *printer, path string, body any) int {
	c, err := clientFor(g, env)
	if err != nil {
		return localError(out, err)
	}
	var raw []byte
	if body != nil {
		if raw, err = encodeBody(body); err != nil {
			return localError(out, err)
		}
	}
	ctx, cancel := callContext()
	defer cancel()
	requestID := g.requestID
	if requestID == "" {
		requestID = newRequestID()
	}
	resp, err := c.post(ctx, path, raw, requestID)
	if err != nil {
		return unreachable(out, err)
	}
	if !resp.ok() {
		// A stored rejection carries a receipt: report its id and replay flag
		// alongside the problem code (receipts.md section 5). Pre-receipt
		// errors and conflicts carry no receipt and print only the code.
		if resp.receiptID != "" {
			out.diag("receipt %s replayed %s\n", resp.receiptID, replayedText(resp.replayed))
		}
		return serverError(out, resp)
	}
	if err := out.receipt(resp); err != nil {
		return localError(out, err)
	}
	return exitOK
}

// requireArgs reports the expected positional argument count.
func requireArgs(out *printer, args []string, want int, what string) error {
	if len(args) != want {
		return fmt.Errorf("%s takes %d argument(s), got %d", what, want, len(args))
	}
	return nil
}

// --- health ---

// runHealth calls getHealth and prints the daemon health document.
func runHealth(g *globals, env func(string) string, out *printer, args []string) int {
	fs := newFlagSet("health", out)
	split, err := splitFlags(fs, args)
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if err := requireArgs(out, fs.Args(), 0, "health"); err != nil {
		return localError(out, err)
	}
	c, err := clientFor(g, env)
	if err != nil {
		return localError(out, err)
	}
	ctx, cancel := callContext()
	defer cancel()
	resp, err := c.get(ctx, "/v1/health")
	if err != nil {
		return unreachable(out, err)
	}
	if !resp.ok() {
		return serverError(out, resp)
	}
	return printResult(out, resp)
}

// --- threads ---

// runThreads dispatches the threads subcommands.
func runThreads(g *globals, env func(string) string, out *printer, args []string) int {
	if len(args) == 0 {
		return localError(out, errors.New("threads needs a subcommand: list, create, or show"))
	}
	switch args[0] {
	case "list":
		return runThreadsList(g, env, out, args[1:])
	case "create":
		return runThreadsCreate(g, env, out, args[1:])
	case "show":
		return runThreadsShow(g, env, out, args[1:])
	default:
		return localError(out, fmt.Errorf("unknown threads subcommand %q", args[0]))
	}
}

// runThreadsList calls listThreads with optional filters.
func runThreadsList(g *globals, env func(string) string, out *printer, args []string) int {
	fs := newFlagSet("threads list", out)
	project := fs.String("project", "", "filter by project ID")
	state := fs.String("state", "", "filter by thread state")
	split, err := splitFlags(fs, args)
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if err := requireArgs(out, fs.Args(), 0, "threads list"); err != nil {
		return localError(out, err)
	}
	c, err := clientFor(g, env)
	if err != nil {
		return localError(out, err)
	}
	query := url.Values{}
	if *project != "" {
		query.Set("project_id", *project)
	}
	if *state != "" {
		query.Set("state", *state)
	}
	path := "/v1/threads"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	ctx, cancel := callContext()
	defer cancel()
	resp, err := c.get(ctx, path)
	if err != nil {
		return unreachable(out, err)
	}
	if !resp.ok() {
		return serverError(out, resp)
	}
	if err := out.printThreadRows(resp); err != nil {
		return localError(out, err)
	}
	return exitOK
}

// runThreadsCreate submits a task through createThread.
func runThreadsCreate(g *globals, env func(string) string, out *printer, args []string) int {
	fs := newFlagSet("threads create", out)
	project := fs.String("project", "", "project ID")
	brief := fs.String("brief", "", "task brief")
	criteria := fs.String("criteria", "", "comma-separated acceptance criteria")
	reviewFirst := fs.Bool("review-plan-first", false, "review the plan before execution")
	audit := fs.String("audit", "", "audit level: default, off, level1, level2, level3")
	pinHarness := fs.String("pin-harness", "", "pin the harness: codex, claude-code, or kimi-code")
	pinModel := fs.String("pin-model", "", "pin the model")
	split, err := splitFlags(fs, args)
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if err := requireArgs(out, fs.Args(), 0, "threads create"); err != nil {
		return localError(out, err)
	}
	if *project == "" || *brief == "" {
		return localError(out, errors.New("threads create requires --project and --brief"))
	}
	if (*pinHarness == "") != (*pinModel == "") {
		return localError(out, errors.New("--pin-harness and --pin-model must be given together"))
	}
	body := map[string]any{"project_id": *project, "brief": *brief}
	if list := splitCSV(*criteria); list != nil {
		body["criteria"] = list
	}
	if *reviewFirst {
		body["review_plan_first"] = true
	}
	if *audit != "" {
		body["audit"] = *audit
	}
	if *pinHarness != "" {
		body["pin"] = map[string]any{"harness": *pinHarness, "model": *pinModel}
	}
	return mutate(g, env, out, "/v1/threads", body)
}

// runThreadsShow calls getThread for one thread.
func runThreadsShow(g *globals, env func(string) string, out *printer, args []string) int {
	fs := newFlagSet("threads show", out)
	split, err := splitFlags(fs, args)
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if err := requireArgs(out, fs.Args(), 1, "threads show THREAD-ID"); err != nil {
		return localError(out, err)
	}
	c, err := clientFor(g, env)
	if err != nil {
		return localError(out, err)
	}
	ctx, cancel := callContext()
	defer cancel()
	resp, err := c.get(ctx, "/v1/threads/"+url.PathEscape(fs.Arg(0)))
	if err != nil {
		return unreachable(out, err)
	}
	if !resp.ok() {
		return serverError(out, resp)
	}
	return printResult(out, resp)
}

// --- thread cmd ---

// threadCommandSpecs mirrors the ThreadCommand schema: closed bodies with the
// members each command requires or optionally accepts.
var threadCommandSpecs = map[string]struct {
	required []string
	optional []string
}{
	"pause":                    {},
	"resume":                   {},
	"cancel":                   {},
	"approve_plan":             {required: []string{"plan_digest"}},
	"select_harness":           {required: []string{"harness", "model"}},
	"decide":                   {required: []string{"decision_id", "choice"}, optional: []string{"note"}},
	"accept":                   {required: []string{"revision"}, optional: []string{"note"}},
	"reject":                   {required: []string{"revision"}, optional: []string{"note"}},
	"change_mode":              {required: []string{"target", "to"}},
	"resolve_external_request": {required: []string{"external_request_id", "resolution", "note"}},
}

// buildThreadCommand assembles the ThreadCommand body from FIELD=VALUE pairs.
func buildThreadCommand(command string, fields []string) (map[string]any, error) {
	spec, ok := threadCommandSpecs[command]
	if !ok {
		return nil, fmt.Errorf("unknown thread command %q", command)
	}
	allowed := map[string]bool{}
	for _, name := range append(append([]string{}, spec.required...), spec.optional...) {
		allowed[name] = true
	}
	body := map[string]any{"command": command}
	for _, field := range fields {
		name, value, ok := strings.Cut(field, "=")
		if !ok || !allowed[name] {
			return nil, fmt.Errorf("invalid field %q for thread command %s", field, command)
		}
		if _, seen := body[name]; seen {
			return nil, fmt.Errorf("duplicate field %q for thread command %s", name, command)
		}
		body[name] = value
	}
	for _, name := range spec.required {
		if _, ok := body[name]; !ok {
			return nil, fmt.Errorf("thread command %s requires %s=VALUE", command, name)
		}
	}
	return body, nil
}

// runThreadCommand sends one command through sendThreadCommand.
func runThreadCommand(g *globals, env func(string) string, out *printer, args []string) int {
	if len(args) == 0 || args[0] != "cmd" {
		return localError(out, errors.New("thread needs the cmd subcommand: thread cmd THREAD-ID COMMAND [FIELD=VALUE...]"))
	}
	fs := newFlagSet("thread cmd", out)
	split, err := splitFlags(fs, args[1:])
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if fs.NArg() < 2 {
		return localError(out, errors.New("thread cmd takes THREAD-ID and COMMAND, then FIELD=VALUE pairs"))
	}
	threadID, command := fs.Arg(0), fs.Arg(1)
	body, err := buildThreadCommand(command, fs.Args()[2:])
	if err != nil {
		return localError(out, err)
	}
	return mutate(g, env, out, "/v1/threads/"+url.PathEscape(threadID)+"/commands", body)
}

// --- approvals ---

// runApprovals dispatches approvals subcommands.
func runApprovals(g *globals, env func(string) string, out *printer, args []string) int {
	if len(args) == 0 || args[0] != "create" {
		return localError(out, errors.New("approvals needs the create subcommand: approvals create THREAD-ID [flags]"))
	}
	fs := newFlagSet("approvals create", out)
	worktreeDigest := fs.String("worktree-digest", "", "sha256 digest of the worktree state")
	target := fs.String("target", "", "approval target")
	actions := fs.String("actions", "", "comma-separated exact action names")
	expiresAt := fs.String("expires-at", "", "expiry as an RFC 3339 timestamp")
	revision := fs.String("revision", "", "approved revision (or --plan-digest)")
	planDigest := fs.String("plan-digest", "", "approved plan digest (or --revision)")
	rollback := fs.String("rollback", "", "rollback command for operational actions")
	irreversible := fs.String("irreversible-effects", "", "comma-separated irreversible effects")
	split, err := splitFlags(fs, args[1:])
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if err := requireArgs(out, fs.Args(), 1, "approvals create THREAD-ID"); err != nil {
		return localError(out, err)
	}
	if *worktreeDigest == "" || *target == "" || *actions == "" || *expiresAt == "" {
		return localError(out, errors.New("approvals create requires --worktree-digest, --target, --actions and --expires-at"))
	}
	if (*revision == "") == (*planDigest == "") {
		return localError(out, errors.New("approvals create takes exactly one of --revision or --plan-digest"))
	}
	if _, err := time.Parse(time.RFC3339, *expiresAt); err != nil {
		return localError(out, fmt.Errorf("--expires-at is not an RFC 3339 timestamp: %v", err))
	}
	body := map[string]any{
		"worktree_digest": *worktreeDigest, "target": *target,
		"actions": splitCSV(*actions), "expires_at": *expiresAt,
	}
	if *revision != "" {
		body["revision"] = *revision
	} else {
		body["plan_digest"] = *planDigest
	}
	if *rollback != "" {
		body["rollback"] = *rollback
	}
	if list := splitCSV(*irreversible); list != nil {
		body["irreversible_effects"] = list
	}
	return mutate(g, env, out, "/v1/threads/"+url.PathEscape(fs.Arg(0))+"/approvals", body)
}

// --- grants ---

// runGrants dispatches the grants subcommands.
func runGrants(g *globals, env func(string) string, out *printer, args []string) int {
	if len(args) == 0 {
		return localError(out, errors.New("grants needs a subcommand: create or revoke"))
	}
	switch args[0] {
	case "create":
		return runGrantsCreate(g, env, out, args[1:])
	case "revoke":
		return runGrantsRevoke(g, env, out, args[1:])
	default:
		return localError(out, fmt.Errorf("unknown grants subcommand %q", args[0]))
	}
}

// runGrantsCreate issues a Full Control grant through createGrant.
func runGrantsCreate(g *globals, env func(string) string, out *printer, args []string) int {
	fs := newFlagSet("grants create", out)
	host := fs.String("host", "", "host the grant covers")
	identity := fs.String("identity", "", "identity the grant covers")
	split, err := splitFlags(fs, args)
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if err := requireArgs(out, fs.Args(), 1, "grants create THREAD-ID"); err != nil {
		return localError(out, err)
	}
	if *host == "" || *identity == "" {
		return localError(out, errors.New("grants create requires --host and --identity"))
	}
	body := map[string]any{"host": *host, "identity": *identity}
	return mutate(g, env, out, "/v1/threads/"+url.PathEscape(fs.Arg(0))+"/grants", body)
}

// runGrantsRevoke revokes a grant; the route takes no body.
func runGrantsRevoke(g *globals, env func(string) string, out *printer, args []string) int {
	fs := newFlagSet("grants revoke", out)
	split, err := splitFlags(fs, args)
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if err := requireArgs(out, fs.Args(), 1, "grants revoke GRANT-ID"); err != nil {
		return localError(out, err)
	}
	return mutate(g, env, out, "/v1/grants/"+url.PathEscape(fs.Arg(0))+"/revoke", nil)
}

// --- receipt ---

// runReceipt looks up one of the caller's stored receipts.
func runReceipt(g *globals, env func(string) string, out *printer, args []string) int {
	fs := newFlagSet("receipt", out)
	split, err := splitFlags(fs, args)
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if err := requireArgs(out, fs.Args(), 1, "receipt REQUEST-ID"); err != nil {
		return localError(out, err)
	}
	c, err := clientFor(g, env)
	if err != nil {
		return localError(out, err)
	}
	ctx, cancel := callContext()
	defer cancel()
	resp, err := c.get(ctx, "/v1/receipts/"+url.PathEscape(fs.Arg(0)))
	if err != nil {
		return unreachable(out, err)
	}
	if !resp.ok() {
		return serverError(out, resp)
	}
	return printResult(out, resp)
}

// splitCSV splits a comma-separated flag value, or reports nil when absent.
func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			kept = append(kept, trimmed)
		}
	}
	return kept
}
