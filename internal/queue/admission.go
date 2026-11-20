package queue

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

// --- Body admission: enforce byte, string, number, and object rules (?3) ---

type Detail string

// Error returns the contract detail code for the malformed body.
func (d Detail) Error() string { return string(d) }

// Code returns the problem code shared by all body admission failures.
func (d Detail) Code() string { return "malformed_body" }

const (
	InvalidUTF8      Detail = "invalid_utf8"
	InvalidUnicode   Detail = "invalid_unicode"
	DuplicateMember  Detail = "duplicate_member"
	NumberNotInteger Detail = "number_not_integer"
	NumberOutOfRange Detail = "number_out_of_range"
	MalformedJSON    Detail = "malformed_json"
	BodyMustBeObject Detail = "body_must_be_object"
	BodyNotAllowed   Detail = "body_not_allowed"
)

// Admit validates raw JSON bytes and returns an object with its original member names.
func Admit(route string, raw []byte) (map[string]any, error) {
	if route == "/v1/grants/{grant_id}/revoke" {
		if len(raw) > 0 {
			return nil, BodyNotAllowed
		}
		return nil, nil
	}
	if len(raw) == 0 {
		return nil, BodyMustBeObject
	}
	if !utf8.Valid(raw) || bytes.HasPrefix(raw, []byte{0xef, 0xbb, 0xbf}) {
		return nil, InvalidUTF8
	}
	if !json.Valid(raw) {
		return nil, MalformedJSON
	}
	if !validEscapes(raw) {
		return nil, InvalidUnicode
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	val, err := value(dec)
	if err != nil {
		return nil, err
	}
	obj, ok := val.(map[string]any)
	if !ok {
		return nil, BodyMustBeObject
	}
	return obj, nil
}

// validEscapes rejects unpaired surrogates before JSON decoding can replace them.
// Syntax was checked already, so a backslash always starts a string escape.
func validEscapes(raw []byte) bool {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if raw[i] != 'u' {
			continue
		}
		n, _ := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		i += 4
		if n < 0xd800 || n > 0xdfff {
			continue
		}
		if n > 0xdbff || i+6 >= len(raw) || string(raw[i+1:i+3]) != "\\u" {
			return false
		}
		low, _ := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
		if low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}

// value decodes one JSON value while retaining numeric lexemes and rejecting duplicate members.
func value(d *json.Decoder) (any, error) {
	tok, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Number:
		s := string(t)
		if s == "-0" || strings.ContainsAny(s, ".eE") {
			return nil, NumberNotInteger
		}
		n, err := t.Int64()
		const max = 1<<53 - 1
		if err != nil || n < -max || n > max {
			return nil, NumberOutOfRange
		}
		return n, nil
	case json.Delim:
		if t == '{' {
			m := map[string]any{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return nil, err
				}
				k := key.(string)
				if _, ok := m[k]; ok {
					return nil, DuplicateMember
				}
				v, err := value(d)
				if err != nil {
					return nil, err
				}
				m[k] = v
			}
			_, err := d.Token()
			return m, err
		}
		var a []any
		for d.More() {
			v, err := value(d)
			if err != nil {
				return nil, err
			}
			a = append(a, v)
		}
		_, err = d.Token()
		return a, err
	default:
		return t, nil
	}
}

// --- Payload identity: canonicalize the admitted value and hash it (?2) ---

// PayloadDigest admits the body before computing its canonical request identity.
func PayloadDigest(method, route string, params map[string]string, raw []byte) (string, error) {
	body, err := Admit(route, raw)
	if err != nil {
		return "", err
	}
	return digestBody(method, route, params, body)
}

// digestBody hashes the method, route template, decoded path parameters, and admitted body (?2).
func digestBody(method, route string, params map[string]string, body map[string]any) (string, error) {
	path := map[string]any{}
	for k, v := range params {
		path[k] = v
	}
	identity := map[string]any{"method": method, "route": route, "path_params": path, "body": nil}
	if body != nil {
		identity["body"] = body
	}
	var b strings.Builder
	if err := canonical(&b, identity); err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(b.String()))), nil
}

// quote writes an RFC 8785 string without Unicode normalization or HTML escaping.
func quote(b *strings.Builder, s string) {
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"', '\\':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '\b':
			b.WriteString("\\b")
		case '\f':
			b.WriteString("\\f")
		case '\n':
			b.WriteString("\\n")
		case '\r':
			b.WriteString("\\r")
		case '\t':
			b.WriteString("\\t")
		default:
			if r < 32 {
				fmt.Fprintf(b, "\\u%04x", r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
}

// canonical serializes the admitted JSON domain according to RFC 8785.
func canonical(b *strings.Builder, v any) error {
	switch x := v.(type) {
	case nil:
		b.WriteString("null")
	case bool:
		b.WriteString(strconv.FormatBool(x))
	case int64:
		b.WriteString(strconv.FormatInt(x, 10))
	case string:
		if !utf8.ValidString(x) {
			return InvalidUnicode
		}
		quote(b, x)
	case []any:
		b.WriteByte('[')
		for i, y := range x {
			if i > 0 {
				b.WriteByte(',')
			}
			if err := canonical(b, y); err != nil {
				return err
			}
		}
		b.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			if !utf8.ValidString(k) {
				return InvalidUnicode
			}
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			return slices.Compare(utf16.Encode([]rune(keys[i])), utf16.Encode([]rune(keys[j]))) < 0
		})
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			quote(b, k)
			b.WriteByte(':')
			if err := canonical(b, x[k]); err != nil {
				return err
			}
		}
		b.WriteByte('}')
	default:
		return errors.New("unsupported identity value")
	}
	return nil
}
