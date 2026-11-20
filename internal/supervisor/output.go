package supervisor

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"
)

const OutputCap = 65536

type stream struct {
	p                    *Proc
	name                 string
	file, reader, writer *os.File
}

// --- Pipe and artifact ownership (execution-driver ?2) ---

// openStream opens a child output pipe and a raw artifact for one stream.
func (p *Proc) openStream(name string) (*stream, error) {
	f, err := os.CreateTemp(p.s.cfg.DataDir, name+"-*")
	if err != nil {
		return nil, fmt.Errorf("create %s artifact: %w", name, err)
	}
	r, w, err := os.Pipe()
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("pipe %s: %w", name, err)
	}
	return &stream{p, name, f, r, w}, nil
}

// discard releases the pipe and removes its artifact when spawn fails.
func (s *stream) discard() {
	_ = s.reader.Close()
	_ = s.writer.Close()
	_ = s.file.Close()
	_ = os.Remove(s.file.Name())
}

// --- Persist before live delivery (execution-driver ?2) ---

// failOutput records a capture error and closes live subscribers.
func (p *Proc) failOutput(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.outputErr = errors.Join(p.outputErr, err)
	for ch := range p.subs {
		close(ch)
		delete(p.subs, ch)
	}
}

// publish serializes sink writes and forwards each chunk only after persistence succeeds.
func (p *Proc) publish(c OutputChunk) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.outputErr != nil {
		return
	}
	// Keep the output lock across persistence and forwarding so streams share the same failure gate.
	if err := p.s.sink.Emit(Fact{Type: "engine.output", RunID: p.ID(), ThreadID: p.spec.ThreadID, Output: &c}); err != nil {
		p.outputErr = fmt.Errorf("persist %s: %w", c.Stream, err)
		for ch := range p.subs {
			close(ch)
			delete(p.subs, ch)
		}
		return
	}
	for ch := range p.subs {
		select {
		case ch <- c:
		default:
			close(ch)
			delete(p.subs, ch)
		}
	}
}

// --- Capture and UTF-8 output limits (event catalogue ?1.7, D-07) ---

// drain copies raw output to disk and publishes previews with verifiable references at each cut.
func (s *stream) drain() {
	defer s.p.readers.Done()
	defer func() { _ = s.reader.Close() }()
	digest := sha256.New()
	dst := io.MultiWriter(s.file, digest)
	buf := make([]byte, 32<<10)
	var carry []byte
	remaining := OutputCap
	var end int64
	var preview strings.Builder
	var fileErr error
	for {
		n, err := s.reader.Read(buf)
		if n > 0 {
			s.p.Progress()
			if fileErr == nil {
				_, fileErr = dst.Write(buf[:n])
				end += int64(n)
			}
		}
		data := append(carry, buf[:n]...)
		cut := false
		flush := func() {
			if fileErr == nil && (preview.Len() > 0 || cut) {
				c := OutputChunk{Stream: s.name, Chunk: preview.String(), Truncated: cut}
				if cut {
					// Flush the measured prefix before publishing a digest that can be checked immediately.
					if fileErr = s.file.Sync(); fileErr != nil {
						return
					}
					path, e := filepath.Abs(s.file.Name())
					if e != nil {
						fileErr = e
						return
					}
					c.FullOutput = &Artifact{artifactURI(path, end), "sha256:" + hex.EncodeToString(digest.Sum(nil))}
				}
				s.p.publish(c)
			}
			preview.Reset()
			cut = false
		}
		for len(data) > 0 {
			if err == nil && !utf8.FullRune(data) {
				break
			}
			r, size := utf8.DecodeRune(data)
			width := utf8.RuneLen(r)
			if r == '\n' {
				if cut {
					flush()
				}
				remaining = OutputCap
			}
			if width > remaining {
				cut = true
				remaining = 0
			} else {
				if preview.Len()+width > OutputCap {
					flush()
				}
				preview.WriteRune(r)
				if r != '\n' {
					remaining -= width
				}
			}
			data = data[size:]
		}
		carry = append(carry[:0], data...)
		// Retain the boundary rune(s) until the next rune resolves whether
		// this line ends exactly at the cap or its preview needs a cut flag.
		if cut || err != nil || remaining >= utf8.UTFMax {
			flush()
		} else if preview.Len() > utf8.UTFMax {
			text := preview.String()
			_, last := utf8.DecodeLastRuneInString(text)
			preview.Reset()
			preview.WriteString(text[:len(text)-last])
			flush()
			preview.WriteString(text[len(text)-last:])
		}
		if err != nil {
			if err != io.EOF {
				fileErr = errors.Join(fileErr, err)
			}
			break
		}
	}
	fileErr = errors.Join(fileErr, s.file.Close())
	if fileErr != nil {
		s.p.failOutput(fmt.Errorf("capture %s: %w", s.name, fileErr))
		return
	}
}

// artifactURI builds a file URI naming the append-only prefix through the exclusive byte offset.
func artifactURI(path string, end int64) string {
	path = filepath.ToSlash(path)
	u := url.URL{Scheme: "file", Path: path}
	if strings.HasPrefix(path, "//") {
		u.Host, u.Path, _ = strings.Cut(path[2:], "/")
		u.Path = "/" + u.Path
	} else if !strings.HasPrefix(path, "/") {
		u.Path = "/" + path
	}
	u.Fragment = fmt.Sprintf("bytes=0-%d", end)
	return u.String()
}
