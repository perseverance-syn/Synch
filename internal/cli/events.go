package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// --- events page: gap fills, resync and the CLI's batched reads ---

// streamPattern mirrors openapi.yaml's Stream parameter.
var streamPattern = regexp.MustCompile(`^(all|system|thread:[0-9A-HJKMNP-TV-Z]{26})$`)

// pageLimit matches the API's pageEvents maximum page size.
const pageLimit = 1000

// runEvents dispatches the events subcommands.
func runEvents(g *globals, env func(string) string, out *printer, args []string) int {
	if len(args) == 0 {
		return localError(out, errors.New("events needs a subcommand: page or follow"))
	}
	switch args[0] {
	case "page":
		return runEventsPage(g, env, out, args[1:])
	case "follow":
		return runEventsFollow(g, env, out, args[1:])
	default:
		return localError(out, fmt.Errorf("unknown events subcommand %q", args[0]))
	}
}

// requireStream validates the stream argument locally before any request.
func requireStream(out *printer, value string) error {
	if !streamPattern.MatchString(value) {
		return fmt.Errorf("stream must be all, system, or thread:<ulid>, got %q", value)
	}
	return nil
}

// runEventsPage calls pageEvents for one stream.
func runEventsPage(g *globals, env func(string) string, out *printer, args []string) int {
	fs := newFlagSet("events page", out)
	after := fs.Int64("after", 0, "cursor to read after")
	limit := fs.Int64("limit", 0, "page size between 1 and 1000")
	split, err := splitFlags(fs, args)
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if err := requireArgs(out, fs.Args(), 1, "events page STREAM"); err != nil {
		return localError(out, err)
	}
	if err := requireStream(out, fs.Arg(0)); err != nil {
		return localError(out, err)
	}
	query := url.Values{}
	if *after > 0 {
		query.Set("after", strconv.FormatInt(*after, 10))
	}
	if *limit > 0 {
		query.Set("limit", strconv.FormatInt(*limit, 10))
	}
	path := "/v1/streams/" + fs.Arg(0) + "/events/page"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	c, err := clientFor(g, env)
	if err != nil {
		return localError(out, err)
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
	if g.json {
		return printResult(out, resp)
	}
	return printPageTable(out, resp, fs.Arg(0))
}

// printPageTable renders a page as a header plus one row per event. The row
// cursor is the stream's own cursor: global_seq for the all stream, seq for
// every other stream (streamEvents contract).
func printPageTable(out *printer, resp *response, stream string) int {
	var page struct {
		Events []struct {
			Seq        int64  `json:"seq"`
			GlobalSeq  int64  `json:"global_seq"`
			Type       string `json:"type"`
			RecordedAt string `json:"recorded_at"`
		} `json:"events"`
		Head     int64  `json:"head"`
		LogEpoch string `json:"log_epoch"`
	}
	if err := json.Unmarshal(resp.body, &page); err != nil {
		return printResult(out, resp)
	}
	var text strings.Builder
	fmt.Fprintf(&text, "log_epoch %s head %d\n", page.LogEpoch, page.Head)
	for _, event := range page.Events {
		cursor := event.GlobalSeq
		if stream != "all" {
			cursor = event.Seq
		}
		fmt.Fprintf(&text, "%d\t%s\t%s\n", cursor, event.Type, event.RecordedAt)
	}
	if _, err := fmt.Fprint(out.stdout, text.String()); err != nil {
		return localError(out, err)
	}
	return exitOK
}

// --- events follow: the streamEvents SSE client contract ---

// follow signals carry the follower's control flow between frames and runs.
var (
	errReconnect = errors.New("reconnect from the last contiguous cursor")
	errResync    = errors.New("resync from cursor 0")
	errDone      = errors.New("follow reached --until")
	errProblem   = errors.New("server answered a problem")
)

// follower keeps the stream state: the last contiguous received cursor L and
// the log epoch that cursor belongs to.
type follower struct {
	c      *client
	out    *printer
	stream string
	until  int64
	// cursor is the last contiguous received cursor; 0 means full history.
	cursor int64
	epoch  string
}

// runEventsFollow streams one stream until --until is reached or the output
// fails, resuming and resyncing per the streamEvents rules.
func runEventsFollow(g *globals, env func(string) string, out *printer, args []string) int {
	fs := newFlagSet("events follow", out)
	until := fs.Int64("until", 0, "exit after delivering a frame with cursor >= N")
	split, err := splitFlags(fs, args)
	if err != nil {
		return localError(out, err)
	}
	if err := fs.Parse(split); err != nil {
		return exitUsage
	}
	if err := requireArgs(out, fs.Args(), 1, "events follow STREAM"); err != nil {
		return localError(out, err)
	}
	if err := requireStream(out, fs.Arg(0)); err != nil {
		return localError(out, err)
	}
	if *until < 0 {
		return localError(out, errors.New("--until must be a nonnegative cursor"))
	}
	c, err := clientFor(g, env)
	if err != nil {
		return localError(out, err)
	}
	f := &follower{c: c, out: out, stream: fs.Arg(0), until: *until}
	if err := f.run(context.Background()); err != nil {
		if errors.Is(err, errProblem) {
			return exitProblem
		}
		if errors.Is(err, errRedirect) {
			return unreachable(out, err)
		}
		return localError(out, err)
	}
	return exitOK
}

// run connects, replays and follows the live stream, reconnecting after every
// server close until the follow ends. Connection failures are retried with a
// bounded backoff, because a stopped daemon is expected to come back.
func (f *follower) run(ctx context.Context) error {
	const minBackoff, maxBackoff = 100 * time.Millisecond, 2 * time.Second
	backoff := minBackoff
	for {
		resp, problem, err := f.c.openStream(ctx, f.stream, f.cursor)
		if err != nil {
			if !errors.Is(err, errUnreachable) {
				return err
			}
			backoff = sleepBackoff(ctx, backoff, maxBackoff)
			continue
		}
		if problem != nil {
			// openStream has already closed a non-200 response body.
			if problem.Code == "cursor_ahead" {
				// L is beyond the server's head: drop all state and rebuild
				// from cursor 0 through pageEvents before continuing live.
				if err := f.resync(ctx); errors.Is(err, errDone) {
					return nil
				} else if errors.Is(err, errUnreachable) {
					backoff = sleepBackoff(ctx, backoff, maxBackoff)
					continue
				} else if err != nil {
					return err
				}
				backoff = minBackoff
				continue
			}
			f.printProblem(problem.Status, problem)
			return errProblem
		}
		err = f.consume(ctx, resp.Body)
		_ = resp.Body.Close()
		switch {
		case errors.Is(err, errWrite):
			// A closed output ends the follow; nothing else does.
			return err
		case errors.Is(err, errDone):
			return nil
		case errors.Is(err, errResync):
			// The hello reported a different log lineage: rebuild from 0.
			if err := f.resync(ctx); errors.Is(err, errDone) {
				return nil
			} else if errors.Is(err, errUnreachable) {
				backoff = sleepBackoff(ctx, backoff, maxBackoff)
				continue
			} else if err != nil {
				return err
			}
			backoff = minBackoff
			continue
		case err == nil || errors.Is(err, errReconnect):
			// The server closed the stream cleanly: reconnect with our own L.
			backoff = minBackoff
			continue
		default:
			// Any other read failure is a dropped connection: reconnect.
			backoff = sleepBackoff(ctx, backoff, maxBackoff)
			continue
		}
	}
}

// sleepBackoff waits before the next attempt and grows the backoff bound.
func sleepBackoff(ctx context.Context, backoff, max time.Duration) time.Duration {
	timer := time.NewTimer(backoff)
	<-timer.C
	timer.Stop()
	return min(2*backoff, max)
}

// printProblem reports a problem answer with its stable code.
func (f *follower) printProblem(status int, p *problem) {
	if p.Detail != "" {
		f.out.diag("error %d %s: %s\n", status, p.Code, p.Detail)
		return
	}
	f.out.diag("error %d %s\n", status, p.Code)
}

// resync drops all stream state and rebuilds from cursor 0 through pageEvents
// up to the head, as the streamEvents contract requires, before continuing live.
func (f *follower) resync(ctx context.Context) error {
	f.cursor, f.epoch = 0, ""
	for {
		resp, err := f.c.get(ctx, fmt.Sprintf("/v1/streams/%s/events/page?after=%d&limit=%d", f.stream, f.cursor, pageLimit))
		if err != nil {
			return err
		}
		if !resp.ok() {
			if p := resp.problem; p != nil {
				f.printProblem(resp.status, p)
				return errProblem
			}
			return fmt.Errorf("resync page failed with status %d", resp.status)
		}
		var page struct {
			Events   []json.RawMessage `json:"events"`
			Head     int64             `json:"head"`
			LogEpoch string            `json:"log_epoch"`
		}
		if err := json.Unmarshal(resp.body, &page); err != nil {
			return fmt.Errorf("decode resync page: %w", err)
		}
		f.epoch = page.LogEpoch
		for _, raw := range page.Events {
			var event struct {
				Seq       int64  `json:"seq"`
				GlobalSeq int64  `json:"global_seq"`
				Type      string `json:"type"`
			}
			if err := json.Unmarshal(raw, &event); err != nil {
				return fmt.Errorf("decode resync event: %w", err)
			}
			cursor := event.GlobalSeq
			if f.stream != "all" {
				cursor = event.Seq
			}
			// Unknown types advance the cursor but are never printed.
			if catalogueEventTypes[event.Type] {
				if err := f.out.event(cursor, event.Type, raw); err != nil {
					return err
				}
			}
			f.cursor = cursor
			if f.until > 0 && cursor >= f.until {
				return errDone
			}
		}
		if len(page.Events) < pageLimit {
			return nil
		}
	}
}

// catalogueEventTypes are the event types of contracts/events/catalogue
// v4.1.0 (TestCatalogueEventTypesMatchContract keeps the list in step). The streamEvents contract requires clients to ignore events of
// unknown type while still advancing their cursor past them.
var catalogueEventTypes = map[string]bool{
	"acceptance.recorded": true, "approval.recorded": true, "approval.voided": true, "approval.declined": true,
	"assignment.delivered": true, "assignment.dispatched": true, "audit.status": true,
	"brief.version_accepted": true, "check.result": true, "engine.capability_unavailable": true,
	"engine.output": true, "engine.usage": true, "external_request.persisted": true,
	"external_request.resolved": true, "external_request.task_bound": true, "finding.recorded": true,
	"grant.issued": true, "grant.revoked": true, "mode.changed": true, "plan.revised": true,
	"policy.decided": true, "process.exited": true, "request.accepted": true,
	"resource.pressure": true, "rollover.checkpointed": true, "runner.connection": true,
	"thread.created": true, "thread.state_changed": true, "child.assignment_recorded": true,
	"child.result_recorded": true, "audit.setting_recorded": true,
}

// consume reads one SSE response until the server closes it or a frame ends
// the follow. Each frame is dispatched in wire order.
func (f *follower) consume(ctx context.Context, body io.Reader) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	frame := sseFrame{}
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			if err := f.dispatch(frame); err != nil {
				return err
			}
			frame = sseFrame{}
		case strings.HasPrefix(line, ":"):
			// Heartbeat comments carry no state.
		case strings.HasPrefix(line, "id: "):
			frame.id = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "event: "):
			frame.event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			frame.data = append(frame.data, strings.TrimPrefix(line, "data: "))
		default:
			// Unknown field lines are ignored per the SSE grammar.
		}
	}
	// EOF is the normal end of a closed stream: the caller reconnects.
	return scanner.Err()
}

// sseFrame collects one frame's fields before dispatch.
type sseFrame struct {
	id, event string
	data      []string
}

// dispatch handles one complete frame per the streamEvents rules: hello
// carries the log epoch, overflow closes the stream for reconnect with our
// own cursor, and an event whose cursor is not L+1 is discarded before
// reconnecting.
func (f *follower) dispatch(frame sseFrame) error {
	switch frame.event {
	case "":
		return nil
	case "hello":
		var hello struct {
			LogEpoch string `json:"log_epoch"`
			Head     int64  `json:"head"`
		}
		if err := json.Unmarshal([]byte(strings.Join(frame.data, "\n")), &hello); err != nil {
			return fmt.Errorf("decode hello frame: %w", err)
		}
		if f.epoch != "" && hello.LogEpoch != f.epoch {
			return errResync
		}
		f.epoch = hello.LogEpoch
		return nil
	case "overflow":
		// The server never says where to resume: reconnect with our own L.
		return errReconnect
	default:
		cursor, err := strconv.ParseInt(frame.id, 10, 64)
		if err != nil || cursor != f.cursor+1 {
			return errReconnect
		}
		f.cursor = cursor
		// Unknown event types advance the cursor but are not printed
		// (streamEvents contract).
		if !catalogueEventTypes[frame.event] {
			if f.until > 0 && cursor >= f.until {
				return errDone
			}
			return nil
		}
		if err := f.out.event(cursor, frame.event, []byte(strings.Join(frame.data, "\n"))); err != nil {
			return err
		}
		if f.until > 0 && cursor >= f.until {
			return errDone
		}
		return nil
	}
}

// openStream opens the SSE endpoint with the resume header when the follower
// has state, and classifies non-200 answers as problems.
func (c *client) openStream(ctx context.Context, stream string, after int64) (*http.Response, *problem, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/v1/streams/"+stream+"/events", nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "text/event-stream")
	if after > 0 {
		req.Header.Set("Last-Event-ID", strconv.FormatInt(after, 10))
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, transportError(err)
	}
	if resp.StatusCode == http.StatusOK {
		return resp, nil, nil
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: read error response: %v", errUnreachable, err)
	}
	out := &response{status: resp.StatusCode, body: data, contentType: resp.Header.Get("Content-Type")}
	if strings.HasPrefix(out.contentType, "application/problem+json") {
		var p problem
		if json.Unmarshal(data, &p) == nil && p.Code != "" {
			return nil, &p, nil
		}
	}
	return nil, nil, &unclassifiedError{status: resp.StatusCode, contentType: out.contentType, body: strings.TrimSpace(string(data))}
}
