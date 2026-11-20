package cli

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

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
	resp, err := c.get(ctx, "/v1/threads/"+fs.Arg(0)+"/evidence")
	if err != nil {
		return unreachable(out, err)
	}
	if resp.ok() {
		if err := extractEvidence(*dest, resp.body, resp.manifestDigest); err != nil {
			return localError(out, err)
		}
		_, _ = fmt.Fprintln(out.stdout, *dest)
		return exitOK
	}
	if resp.status != 404 && resp.status != 405 {
		return serverError(out, resp)
	}
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

// extractEvidence validates archive paths and verifies both manifest and transport digest.
func extractEvidence(dest string, data []byte, digest string) (err error) {
	z, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return err
	}
	if err = os.Mkdir(dest, 0700); err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dest)
		}
	}()
	var total uint64
	for _, f := range z.File {
		if !filepath.IsLocal(f.Name) || strings.Contains(f.Name, `\`) || f.FileInfo().IsDir() || !f.Mode().IsRegular() {
			return errors.New("evidence: unsafe archive entry")
		}
		if f.UncompressedSize64 > (64<<20)-total {
			return errors.New("evidence: archive too large")
		}
		total += f.UncompressedSize64
		path := filepath.Join(dest, filepath.FromSlash(f.Name))
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			return err
		}
		var in io.ReadCloser
		in, err = f.Open()
		if err != nil {
			return err
		}
		var out *os.File
		out, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			_ = in.Close()
			return err
		}
		_, err = io.Copy(out, io.LimitReader(in, int64(f.UncompressedSize64)+1))
		closeIn := in.Close()
		closeOut := out.Close()
		if err != nil {
			return err
		}
		if closeIn != nil {
			return closeIn
		}
		if closeOut != nil {
			return closeOut
		}
	}
	b, err := os.ReadFile(filepath.Join(dest, "manifest.json"))
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	if digest != fmt.Sprintf("sha256:%x", sum) {
		return errors.New("evidence: manifest header mismatch")
	}
	return evidence.Verify(dest)
}
