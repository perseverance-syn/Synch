package context

import (
	"bufio"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"
)

// DefaultCap is the largest excerpt handed to an agent unless the daemon configures another.
const DefaultCap = 16 << 10

// maxMatchText bounds one reported line so a minified blob cannot fill a result.
const maxMatchText = 512

// refPattern is the only shape of reference Search accepts, which keeps a reference
// from naming any path outside the store.
var refPattern = regexp.MustCompile(`^out_[0-9a-f]{32}$`)

// Store retains full tool and check output on disk and hands agents capped excerpts.
type Store struct {
	dir string
	cap int
}

// Excerpt is what an agent receives in place of the full output.
type Excerpt struct {
	// Ref names the retained full output for Search.
	Ref string
	// Text is at most the store's cap: the whole output when it fits, else head, matching lines and tail.
	Text      string
	Truncated bool
	// Bytes is the size of the retained output and Digest its sha256.
	Bytes  int64
	Digest string
}

// Match is one line of a retained output that matched a search.
type Match struct {
	Line int
	Text string
}

// NewStore creates the retention directory. A cap of zero or less selects DefaultCap.
func NewStore(dir string, capBytes int) (*Store, error) {
	if capBytes <= 0 {
		capBytes = DefaultCap
	}
	if capBytes < 1024 {
		return nil, errors.New("output cap must be at least 1024 bytes")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create output store: %w", err)
	}
	return &Store{dir: dir, cap: capBytes}, nil
}

// path maps a validated reference to its file.
func (s *Store) path(ref string) string { return filepath.Join(s.dir, ref+".out") }

// Retain streams r to disk in full (never held in memory), then returns the capped excerpt:
// head, the lines containing any keyword (case-insensitive) and tail, in output order.
func (s *Store) Retain(r io.Reader, keywords ...string) (Excerpt, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return Excerpt{}, err
	}
	ref := "out_" + hex.EncodeToString(id[:])
	f, err := os.OpenFile(s.path(ref), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return Excerpt{}, fmt.Errorf("retain output: %w", err)
	}
	defer func() { _ = f.Close() }()
	sum := sha256.New()
	size, err := io.Copy(io.MultiWriter(f, sum), r)
	if err == nil {
		err = f.Sync()
	}
	if err != nil {
		_ = os.Remove(s.path(ref))
		return Excerpt{}, fmt.Errorf("retain output: %w", err)
	}
	ex := Excerpt{Ref: ref, Bytes: size, Digest: fmt.Sprintf("sha256:%x", sum.Sum(nil))}
	if size <= int64(s.cap) {
		raw := make([]byte, size)
		if _, err := f.ReadAt(raw, 0); err != nil && !errors.Is(err, io.EOF) {
			return Excerpt{}, err
		}
		ex.Text = strings.ToValidUTF8(string(raw), "�")
		return ex, nil
	}
	ex.Truncated = true
	ex.Text, err = s.excerpt(f, size, ref, keywords)
	return ex, err
}

// excerpt builds the capped text: 3/8 head, 3/8 tail and the rest for matching lines.
func (s *Store) excerpt(f *os.File, size int64, ref string, keywords []string) (string, error) {
	part := int64(s.cap) * 3 / 8
	head, tail := make([]byte, part), make([]byte, part)
	if _, err := f.ReadAt(head, 0); err != nil {
		return "", err
	}
	if _, err := f.ReadAt(tail, size-part); err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	budget := s.cap - 2*int(part) - 256 // 256 bytes are kept for the markers below
	var hits strings.Builder
	var lower []string
	for _, k := range keywords {
		if k != "" {
			lower = append(lower, strings.ToLower(k))
		}
	}
	if len(lower) > 0 && budget > 0 {
		err := scanLines(f, func(n int, offset int64, line string) bool {
			if offset < part || offset >= size-part {
				return true // already visible in the head or tail
			}
			l := strings.ToLower(line)
			for _, k := range lower {
				if strings.Contains(l, k) {
					entry := fmt.Sprintf("L%d: %s\n", n, clip(line, maxMatchText))
					if hits.Len()+len(entry) > budget {
						return false
					}
					hits.WriteString(entry)
					break
				}
			}
			return true
		})
		if err != nil {
			return "", err
		}
	}
	header := ""
	if hits.Len() > 0 {
		header = "[matching lines]\n"
	}
	text := fmt.Sprintf("%s\n[... output truncated: %d bytes retained as %s; search it by that reference ...]\n%s%s\n[... end of retained excerpt ...]\n%s",
		valid(head), size, ref, header, hits.String(), valid(tail))
	if len(text) > s.cap {
		text = strings.ToValidUTF8(text[:s.cap], "")
	}
	return text, nil
}

// valid drops a UTF-8 sequence cut in half by a byte boundary.
func valid(b []byte) string { return strings.ToValidUTF8(string(b), "") }

// clip shortens a line to n bytes on a rune boundary.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "..."
}

// scanLines calls fn with the 1-based number, byte offset and text of each line, stopping
// when fn returns false. Only the start of a very long line is kept for matching.
func scanLines(f *os.File, fn func(n int, offset int64, line string) bool) error {
	br := bufio.NewReaderSize(io.NewSectionReader(f, 0, 1<<62), 64<<10)
	var offset int64
	for n := 1; ; n++ {
		start := offset
		var line []byte
		for {
			chunk, more, err := br.ReadLine()
			if err != nil {
				if errors.Is(err, io.EOF) && len(line) == 0 && offset == start {
					return nil
				}
				if !errors.Is(err, io.EOF) {
					return err
				}
				break
			}
			offset += int64(len(chunk))
			if len(line) < 4*maxMatchText {
				line = append(line, chunk...)
			}
			if !more {
				break
			}
		}
		offset++ // the newline ReadLine consumed (a CRLF only shifts the head/tail boundary by a byte)
		if !fn(n, start, string(line)) {
			return nil
		}
	}
}

// Search returns up to limit lines of a retained output matching the regular expression
// (Go RE2, so a hostile pattern cannot run long). A limit of zero or less means 50.
func (s *Store) Search(ref, pattern string, limit int) ([]Match, error) {
	if !refPattern.MatchString(ref) {
		return nil, fmt.Errorf("invalid output reference %q", ref)
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, fmt.Errorf("search pattern: %w", err)
	}
	if limit <= 0 {
		limit = 50
	}
	f, err := os.Open(s.path(ref))
	if err != nil {
		return nil, fmt.Errorf("open retained output: %w", err)
	}
	defer func() { _ = f.Close() }()
	var out []Match
	err = scanLines(f, func(n int, _ int64, line string) bool {
		if re.MatchString(line) {
			out = append(out, Match{Line: n, Text: clip(line, maxMatchText)})
		}
		return len(out) < limit
	})
	return out, err
}
