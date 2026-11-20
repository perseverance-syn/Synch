package credentials

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"slices"
	"strings"
	"sync"
)

// --- Known values and their encoded forms (spec line 74: redact known values) ---

// Redacted replaces every occurrence of a known value.
const Redacted = "[REDACTED]"

// Redactor holds every value the broker handed out or a caller registered as known, and
// replaces each of them, longest first, in strings, streams and event payloads. It is
// safe for concurrent use.
type Redactor struct {
	mu    sync.RWMutex
	forms map[string]bool
	m     *matcher // rebuilt on Add; nil while nothing is known
}

// NewRedactor returns a redactor that knows no value yet.
func NewRedactor() *Redactor { return &Redactor{forms: make(map[string]bool)} }

// Add registers secret values. A value shorter than MinSecretLen is refused, because
// redacting it would shred ordinary text and leaving it would leak it.
func (r *Redactor) Add(values ...string) error {
	for _, v := range values {
		if len(v) < MinSecretLen {
			return fmt.Errorf("%w: a known value must be at least %d bytes", ErrInvalid, MinSecretLen)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	grew := false
	for _, v := range values {
		for _, f := range encodedForms(v) {
			if !r.forms[f] {
				r.forms[f], grew = true, true
			}
		}
	}
	if grew {
		r.m = newMatcher(r.forms)
	}
	return nil
}

// encodedForms lists the forms a value takes in supported inputs and outputs: plain, the
// four base64 alphabets, URL query and path escaping, JSON string escaping and, for a
// multi-line value such as a PEM key, each long line on its own.
// ponytail: base64 is matched only when the value is encoded on its own (offset 0 mod 3);
// a value embedded mid-way in a larger base64 blob is not recognised.
func encodedForms(v string) []string {
	forms := []string{v,
		base64.StdEncoding.EncodeToString([]byte(v)), base64.RawStdEncoding.EncodeToString([]byte(v)),
		base64.URLEncoding.EncodeToString([]byte(v)), base64.RawURLEncoding.EncodeToString([]byte(v)),
		url.QueryEscape(v), url.PathEscape(v)}
	if raw, err := json.Marshal(v); err == nil {
		forms = append(forms, string(raw[1:len(raw)-1]))
	}
	var plain bytes.Buffer
	enc := json.NewEncoder(&plain)
	enc.SetEscapeHTML(false)
	if enc.Encode(v) == nil {
		s := strings.TrimSpace(plain.String())
		forms = append(forms, s[1:len(s)-1])
	}
	if strings.Contains(v, "\n") {
		for _, line := range strings.Split(v, "\n") {
			line = strings.TrimSpace(line)
			if len(line) >= 16 && !strings.HasPrefix(line, "-----") {
				forms = append(forms, line)
			}
		}
	}
	return forms
}

// Values returns every known form, longest first, for redactors that take a list
// (ci.TokenBroker.KnownValues, ci.Redact).
func (r *Redactor) Values() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.forms))
	for f := range r.forms {
		out = append(out, f)
	}
	sortLongestFirst(out)
	return out
}

// snapshot returns the current matcher; nil means nothing is known.
func (r *Redactor) snapshot() *matcher {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.m
}

// Redact replaces every known form in s and reports how many it replaced.
func (r *Redactor) Redact(s string) (string, int) {
	m := r.snapshot()
	if m == nil {
		return s, 0
	}
	out, _, n := m.replace(nil, []byte(s), true)
	if n == 0 {
		return s, 0
	}
	return string(out), n
}

// String redacts s; it matches the scanner.Redactor and similar func(string) hooks.
func (r *Redactor) String(s string) string {
	out, _ := r.Redact(s)
	return out
}

// --- Streams: retained output and exports are redacted before any byte is written ---

// streamChunk is how much input a stream writer gathers before redacting and writing.
const streamChunk = 64 << 10

// Writer returns a writer that redacts everything written to it before passing it on to
// w; Close flushes the held-back tail. Values added after the call are not applied to it.
func (r *Redactor) Writer(w io.Writer) io.WriteCloser {
	return &redactWriter{w: w, m: r.snapshot()}
}

// redactWriter holds back fewer bytes than the longest form so a value split across two
// Write calls is still found.
type redactWriter struct {
	w   io.Writer
	m   *matcher
	buf []byte
	out []byte
	err error
}

// Write buffers p and redacts and forwards every byte that can no longer start a match.
func (x *redactWriter) Write(p []byte) (int, error) {
	if x.err != nil {
		return 0, x.err
	}
	if x.m == nil {
		return x.w.Write(p)
	}
	x.buf = append(x.buf, p...)
	if len(x.buf) >= streamChunk+x.m.max {
		x.flush(false)
	}
	if x.err != nil {
		return 0, x.err
	}
	return len(p), nil
}

// flush writes the redacted safe prefix (or everything when final) and keeps the rest.
func (x *redactWriter) flush(final bool) {
	var consumed int
	x.out, consumed, _ = x.m.replace(x.out[:0], x.buf, final)
	if _, err := x.w.Write(x.out); err != nil {
		x.err = err
		return
	}
	x.buf = x.buf[:copy(x.buf, x.buf[consumed:])]
}

// Close flushes the tail; it does not close the underlying writer.
func (x *redactWriter) Close() error {
	if x.err == nil && x.m != nil && len(x.buf) > 0 {
		x.flush(true)
	}
	return x.err
}

// --- Event payloads: the eventlog.Redactor hook applied before persist ---

// ErrUnredactable means a payload still contained a known value after redaction, for
// example a value that spans JSON syntax; the event is refused instead of stored.
var ErrUnredactable = errors.New("credentials: payload holds a known value that cannot be redacted")

// EventPayload redacts every string, object key and number of a canonical JSON payload.
// A payload without any known form is returned byte for byte. It has the signature of
// eventlog.Redactor.
func (r *Redactor) EventPayload(raw json.RawMessage) (json.RawMessage, int, error) {
	m := r.snapshot()
	if m == nil {
		return raw, 0, nil
	}
	if _, _, n := m.replace(nil, raw, true); n == 0 {
		return raw, 0, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var value any
	if err := dec.Decode(&value); err != nil {
		return nil, 0, fmt.Errorf("credentials: decode payload: %w", err)
	}
	count := 0
	value, err := r.walk(value, &count)
	if err != nil {
		return nil, 0, err
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return nil, 0, fmt.Errorf("credentials: encode payload: %w", err)
	}
	result := bytes.TrimSpace(out.Bytes())
	if _, _, left := m.replace(nil, result, true); left > 0 {
		return nil, 0, ErrUnredactable
	}
	return result, count, nil
}

// walk redacts strings, keys and numbers in a decoded JSON value, counting replacements.
func (r *Redactor) walk(value any, count *int) (any, error) {
	switch v := value.(type) {
	case string:
		out, n := r.Redact(v)
		*count += n
		return out, nil
	case json.Number:
		// A numeric secret (a PIN) becomes the redaction string.
		if out, n := r.Redact(v.String()); n > 0 {
			*count += n
			return out, nil
		}
		return v, nil
	case []any:
		for i, child := range v {
			redacted, err := r.walk(child, count)
			if err != nil {
				return nil, err
			}
			v[i] = redacted
		}
		return v, nil
	case map[string]any:
		result := make(map[string]any, len(v))
		for key, child := range v {
			safe, n := r.Redact(key)
			*count += n
			if _, dup := result[safe]; dup {
				return nil, ErrUnredactable
			}
			redacted, err := r.walk(child, count)
			if err != nil {
				return nil, err
			}
			result[safe] = redacted
		}
		return result, nil
	}
	return value, nil
}

// --- Matching: longest-first scan indexed by first byte ---

// matcher finds known forms; candidates per first byte are sorted longest first.
type matcher struct {
	first [256][][]byte
	max   int
}

// newMatcher indexes forms by their first byte.
func newMatcher(forms map[string]bool) *matcher {
	list := make([]string, 0, len(forms))
	for f := range forms {
		if f != "" {
			list = append(list, f)
		}
	}
	sortLongestFirst(list)
	m := &matcher{}
	for _, f := range list {
		m.first[f[0]] = append(m.first[f[0]], []byte(f))
		m.max = max(m.max, len(f))
	}
	return m
}

// sortLongestFirst orders forms by length, longest first, then lexically for determinism.
func sortLongestFirst(list []string) {
	slices.SortFunc(list, func(a, b string) int {
		if len(a) != len(b) {
			return len(b) - len(a)
		}
		return strings.Compare(a, b)
	})
}

// replace appends src to dst with every known form replaced. Unless final, it stops
// before the last max-1 bytes, where a match could still be incomplete, and reports how
// many bytes of src it consumed so the caller can keep the rest for the next call.
// ponytail: O(len(src)) with a per-byte candidate check; fine for tens of known values.
func (m *matcher) replace(dst, src []byte, final bool) ([]byte, int, int) {
	limit := len(src)
	if !final {
		limit = max(0, len(src)-(m.max-1))
	}
	count, i := 0, 0
	for i < limit {
		matched := false
		for _, form := range m.first[src[i]] {
			if bytes.HasPrefix(src[i:], form) {
				dst = append(dst, Redacted...)
				i += len(form)
				count++
				matched = true
				break
			}
		}
		if !matched {
			dst = append(dst, src[i])
			i++
		}
	}
	return dst, i, count
}
