package cli

import (
	"encoding/json"
	"errors"
	"fmt"

	"synch/internal/credentials"
	"synch/internal/eventlog"
	"synch/internal/evidence"
)

// runEvidence exports authenticated journal pages into a local evidence bundle.
func runEvidence(g *globals, env func(string) string, out *printer, args []string) int {
	if len(args) == 0 || args[0] != "export" {
		return localError(out, errors.New("evidence needs export"))
	}
	fs := newFlagSet("evidence export", out)
	dest := fs.String("out", "", "output directory")
	split, err := splitFlags(fs, args[1:])
	if err != nil {
		return localError(out, err)
	}
	if err = fs.Parse(split); err != nil {
		return exitUsage
	}
	if err = requireArgs(out, fs.Args(), 1, "evidence export THREAD-ID --out PATH"); err != nil {
		return localError(out, err)
	}
	if *dest == "" {
		return localError(out, errors.New("--out is required"))
	}
	stream := "thread:" + fs.Arg(0)
	if err = requireStream(out, stream); err != nil {
		return localError(out, err)
	}
	c, err := clientFor(g, env)
	if err != nil {
		return localError(out, err)
	}
	ctx, cancel := callContext()
	defer cancel()
	var events []eventlog.Event
	var cursor int64
	for {
		resp, err := c.get(ctx, fmt.Sprintf("/v1/streams/%s/events/page?after=%d&limit=%d", stream, cursor, pageLimit))
		if err != nil {
			return unreachable(out, err)
		}
		if !resp.ok() {
			return serverError(out, resp)
		}
		var page struct {
			Events []eventlog.Event `json:"events"`
		}
		if err = json.Unmarshal(resp.body, &page); err != nil {
			return localError(out, err)
		}
		events = append(events, page.Events...)
		if len(page.Events) < pageLimit {
			break
		}
		next := page.Events[len(page.Events)-1].Seq
		if next <= cursor {
			return localError(out, errors.New("evidence: event cursor did not advance"))
		}
		cursor = next
	}
	red := credentials.NewRedactor()
	if len(c.token) >= credentials.MinSecretLen {
		if err = red.Add(c.token); err != nil {
			return localError(out, err)
		}
	}
	if err = evidence.Export(*dest, fs.Arg(0), events, red); err != nil {
		return localError(out, err)
	}
	_, _ = fmt.Fprintln(out.stdout, *dest)
	return exitOK
}
