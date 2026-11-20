package gitops

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// sha256Hex prepares expected fixture hashes independently of the digest implementation.
func sha256Hex(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// TestWorktreeDigest proves AC6 for clean, changed, staged, ignored, and deleted files.
func TestWorktreeDigest(t *testing.T) {
	f := newFixture(t)
	s := New(allow, nil)
	thread := f.thread(t, s, "digest")
	head := git(t, thread.Path(), "rev-parse", "HEAD")
	// The expected JCS is written explicitly, including null and the unescaped U+2028.
	expect := func(entries string) string {
		t.Helper()
		want := sha256Hex("{\"entries\":[" + entries + "],\"revision\":\"" + head + "\"}")
		got, err := s.WorktreeDigest(t.Context(), thread)
		if err != nil || got != want {
			t.Fatalf("digest = %s, %v; want %s for %s", got, err, want, entries)
		}
		return got
	}
	expect("")
	put(t, thread.Path(), "discard.ignored", "ignored")
	expect("")
	if err := os.Remove(filepath.Join(thread.Path(), "CONVENTIONS.md")); err != nil {
		t.Fatal(err)
	}
	path := "caf\u00e9\u2028.txt"
	put(t, thread.Path(), path, "new\n")
	put(t, thread.Path(), sharedFile, "first uncommitted edit")
	entries := func(content string) string {
		return "{\"mode\":\"100644\",\"path\":\"CONVENTIONS.md\",\"sha256\":null}," +
			"{\"mode\":\"100644\",\"path\":\"" + path + "\",\"sha256\":\"" + sha256Hex("new\n") + "\"}," +
			"{\"mode\":\"100644\",\"path\":\"same.txt\",\"sha256\":\"" + sha256Hex(content) + "\"}"
	}
	first := expect(entries("first uncommitted edit"))
	if err := s.StageAll(t.Context(), thread); err != nil {
		t.Fatal(err)
	}
	expect(entries("first uncommitted edit"))
	put(t, thread.Path(), sharedFile, "other uncommitted edit")
	if second := expect(entries("other uncommitted edit")); second == first ||
		git(t, thread.Path(), "rev-parse", "HEAD") != head {
		t.Fatal("uncommitted edit must change digest at the same HEAD")
	}
}

// TestCanonicalJCSVectors proves AC6 using the RFC 8785 string and UTF-16 ordering examples.
func TestCanonicalJCSVectors(t *testing.T) {
	var out strings.Builder
	writeJCSString(&out, "\u20ac$\u000f\nA'B\"\\\\\"/")
	if want := "\"\u20ac$\\u000f\\nA'B\\\"\\\\\\\\\\\"/\""; out.String() != want {
		t.Fatalf("RFC 8785 string = %q, want %q", out.String(), want)
	}
	paths := []string{"\ufb33", "\U0001f600", "\u20ac", "\u00f6", "\u0080", "1", "\r", "\u2029", "\u2028"}
	var entries []digestEntry
	for _, path := range paths {
		entries = append(entries, digestEntry{path: path, mode: "100644"})
	}
	var want []string
	for _, encoded := range []string{"\\r", "1", "\u0080", "\u00f6", "\u2028", "\u2029", "\u20ac", "\U0001f600", "\ufb33"} {
		want = append(want, "{\"mode\":\"100644\",\"path\":\""+encoded+"\",\"sha256\":null}")
	}
	if actual := canonicalDigest("abc", entries); actual != "{\"entries\":["+strings.Join(want, ",")+"],\"revision\":\"abc\"}" {
		t.Fatalf("RFC 8785 order or escaping: %q", actual)
	}
}

// TestMaskedTrackedWorktreeDigest proves AC6 when Git's index hides an on-disk edit.
func TestMaskedTrackedWorktreeDigest(t *testing.T) {
	f := newFixture(t)
	s := New(allow, nil)
	thread := f.thread(t, s, "masked")
	head := git(t, thread.Path(), "rev-parse", "HEAD")
	git(t, thread.Path(), "update-index", "--assume-unchanged", "--", sharedFile)
	clean, err := s.WorktreeDigest(t.Context(), thread)
	if want := sha256Hex("{\"entries\":[],\"revision\":\"" + head + "\"}"); err != nil || clean != want {
		t.Fatalf("clean masked path digest = %s, %v, want %s", clean, err, want)
	}
	put(t, thread.Path(), sharedFile, "hidden edit")
	if got := git(t, thread.Path(), "status", "--porcelain=v1"); got != "" {
		t.Fatalf("fixture edit was visible to ordinary git status: %s", got)
	}
	changed, err := s.WorktreeDigest(t.Context(), thread)
	entry := "{\"mode\":\"100644\",\"path\":\"same.txt\",\"sha256\":\"" + sha256Hex("hidden edit") + "\"}"
	if want := sha256Hex("{\"entries\":[" + entry + "],\"revision\":\"" + head + "\"}"); err != nil || changed != want {
		t.Fatalf("changed masked path digest = %s, %v, want %s", changed, err, want)
	}
}
