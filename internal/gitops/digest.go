package gitops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"unicode/utf16"
	"unicode/utf8"
)

// digestEntry records Git's six-digit file mode and a hash of the actual
// worktree bytes. An empty hash represents a deletion.
type digestEntry struct {
	path string
	mode string
	hash string
}

// WorktreeDigest implements policy rule-file §5.2. Git supplies changed
// paths, including index changes, using its attribute and ignore rules.
// Existing files are hashed from the worktree, and deleted files retain
// their recorded Git mode. Symlinks are hashed by their link target.
func (s *Service) WorktreeDigest(ctx context.Context, t Thread) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.verifyThread(ctx, t); err != nil {
		return "", err
	}
	_, digest, err := s.worktreeState(ctx, t)
	return digest, err
}

// worktreeState binds the digest to the HEAD read in the same snapshot.
// The caller owns the service lock and has verified the worktree.
func (s *Service) worktreeState(ctx context.Context, t Thread) (string, string, error) {
	out, err := t.run(ctx, s, Status, t.Branch(), "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return "", "", err
	}
	revision := line(out)
	paths, err := s.changedPaths(ctx, t)
	if err != nil {
		return "", "", err
	}
	index, err := t.run(ctx, s, Status, t.Branch(), "ls-files", "-v", "--stage", "-z")
	if err != nil {
		return "", "", err
	}
	tracked, masked, err := splitIndex(index)
	if err != nil {
		return "", "", err
	}
	entries, err := s.digestEntries(ctx, t, revision, paths, tracked, masked)
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256([]byte(canonicalDigest(revision, entries)))
	return revision, hex.EncodeToString(sum[:]), nil
}

// changedPaths reads both columns of porcelain status and deduplicates paths
// reported in the index and as untracked. Renames are represented as two paths.
func (s *Service) changedPaths(ctx context.Context, t Thread) (map[string]struct{}, error) {
	out, err := t.run(ctx, s, Status, t.Branch(), "status", "--porcelain=v1", "-z",
		"--untracked-files=all", "--no-renames", "--ignore-submodules=none")
	if err != nil {
		return nil, err
	}
	paths := make(map[string]struct{})
	for _, entry := range records(out) {
		if len(entry) < 4 || entry[2] != ' ' || !utf8.ValidString(entry[3:]) || !filepath.IsLocal(filepath.FromSlash(entry[3:])) {
			return nil, errors.New("git status returned a path that cannot be represented safely in JCS")
		}
		paths[entry[3:]] = struct{}{}
	}
	return paths, nil
}

// splitIndex separates ordinary mode records from entries whose worktree
// changes Git status can hide via assume-unchanged or skip-worktree.
func splitIndex(out []byte) (tracked, masked []string, err error) {
	for _, record := range records(out) {
		if len(record) < 3 || record[1] != ' ' {
			return nil, nil, errors.New("unexpected git index record")
		}
		tracked = append(tracked, record[2:])
		if strings.ContainsRune("hSs", rune(record[0])) {
			masked = append(masked, record)
		}
	}
	return tracked, masked, nil
}

// --- Entries: resolve modes and hash worktree contents ---

// digestEntries enriches changed paths with index and HEAD modes and then hashes
// their contents. An os.Root limits reads to the verified worktree.
func (s *Service) digestEntries(ctx context.Context, t Thread, revision string, paths map[string]struct{}, index, masked []string) ([]digestEntry, error) {
	if len(paths) == 0 && len(masked) == 0 {
		return nil, nil
	}
	out, err := t.run(ctx, s, Status, t.Branch(), "config", "--type=bool", "--default=true", "--get", "core.filemode")
	if err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(t.path)
	if err != nil {
		return nil, fmt.Errorf("open worktree for digest: %w", err)
	}
	defer func() { _ = root.Close() }() // Closing the read-only root cannot change the digest.

	modes, err := s.recordedModes(ctx, t, revision, index)
	if err != nil {
		return nil, err
	}
	fileMode := line(out) == "true"
	entries := make([]digestEntry, 0, len(paths))
	for path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		entry, err := digestFile(root, path, modes[path], fileMode)
		if err != nil {
			return nil, fmt.Errorf("digest %q: %w", path, err)
		}
		entries = append(entries, entry)
	}
	extra, err := s.maskedEntries(ctx, t, root, fileMode, paths, masked)
	return append(entries, extra...), err
}

// recordedModes maps tracked paths to their Git modes, preferring the index
// (or our side of an unmerged index entry) over HEAD.
func (s *Service) recordedModes(ctx context.Context, t Thread, revision string, index []string) (map[string]string, error) {
	head, err := t.run(ctx, s, Status, t.Branch(), "ls-tree", "-r", "-z", "--full-tree", revision)
	if err != nil {
		return nil, err
	}
	modes := make(map[string]string)
	for _, record := range records(head) {
		metadata, path, _ := strings.Cut(record, "\t")
		mode, _, _ := strings.Cut(metadata, " ")
		modes[path] = mode
	}
	for _, record := range index {
		metadata, path, _ := strings.Cut(record, "\t")
		mode, _, _ := strings.Cut(metadata, " ")
		if modes[path] == "" || strings.HasSuffix(metadata, " 0") || strings.HasSuffix(metadata, " 2") {
			modes[path] = mode
		}
	}
	return modes, nil
}

// maskedEntries compares paths hidden by Git index flags to their recorded
// blobs. An absent sparse path is expected while an absent assumed-unchanged
// path is an unreported deletion.
func (s *Service) maskedEntries(ctx context.Context, t Thread, root *os.Root, fileMode bool, seen map[string]struct{}, masked []string) ([]digestEntry, error) {
	var entries []digestEntry
	for _, record := range masked {
		metadata, path, _ := strings.Cut(record, "\t")
		fields := strings.Fields(metadata)
		if len(fields) != 4 {
			return nil, errors.New("unexpected git index metadata")
		}
		if _, ok := seen[path]; ok || fields[3] != "0" {
			continue
		}
		entry, err := digestFile(root, path, fields[1], fileMode)
		if err != nil {
			return nil, fmt.Errorf("digest hidden path %q: %w", path, err)
		}
		if entry.hash == "" && fields[0] != "h" {
			continue
		}
		if entry.mode == fields[1] && entry.hash != "" {
			args := []string{"cat-file", "--filters", "--path=" + path, fields[2]}
			if entry.mode == "120000" {
				args = []string{"cat-file", "blob", fields[2]}
			}
			blob, err := t.run(ctx, s, Status, t.Branch(), args...)
			if err != nil {
				return nil, err
			}
			hash := sha256.Sum256(blob)
			if entry.hash == hex.EncodeToString(hash[:]) {
				continue
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// digestFile hashes a regular file or link and records deletions explicitly.
// Git's staged executable bit is authoritative where core.filemode is false.
func digestFile(root *os.Root, path, mode string, fileMode bool) (digestEntry, error) {
	name := filepath.FromSlash(path)
	entry := digestEntry{path: path, mode: mode}
	if !utf8.ValidString(path) || !filepath.IsLocal(name) {
		return entry, errors.New("path cannot be represented safely in JCS")
	}
	info, err := root.Lstat(name)
	if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
		if mode == "" {
			return entry, errors.New("untracked file disappeared during digest")
		}
		return entry, nil
	}
	if err != nil {
		return entry, err
	}
	switch {
	case mode == "160000":
		return entry, errors.New("changed Git submodule cannot be represented as a file digest")
	case info.IsDir() && mode != "":
		return entry, nil
	case info.Mode()&os.ModeSymlink != 0:
		target, err := root.Readlink(name)
		if err != nil {
			return entry, err
		}
		hash := sha256.Sum256([]byte(target))
		entry.mode, entry.hash = "120000", hex.EncodeToString(hash[:])
	case info.Mode().IsRegular():
		if mode != "120000" || fileMode {
			entry.mode = "100644"
			if (fileMode && info.Mode()&0100 != 0) || (!fileMode && mode == "100755") {
				entry.mode = "100755"
			}
		}
		entry.hash, err = hashFile(root, name)
		if err != nil {
			return entry, err
		}
	default:
		return entry, errors.New("unsupported worktree file type")
	}
	return entry, nil
}

// hashFile reads the file as bytes without applying Git clean filters.
func hashFile(root *os.Root, name string) (string, error) {
	file, err := root.Open(name)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }() // The read result already reports file I/O errors.
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// --- Canonical encoding: RFC 8785 for the fixed worktree digest shape ---

// canonicalDigest encodes the fixed keys in JCS order and compares paths as
// UTF-16 code units, like JCS property names.
func canonicalDigest(revision string, entries []digestEntry) string {
	slices.SortFunc(entries, func(left, right digestEntry) int {
		return slices.Compare(utf16.Encode([]rune(left.path)), utf16.Encode([]rune(right.path)))
	})
	var out strings.Builder
	out.WriteString("{\"entries\":[")
	for i, entry := range entries {
		if i != 0 {
			out.WriteByte(',')
		}
		out.WriteString("{\"mode\":")
		writeJCSString(&out, entry.mode)
		out.WriteString(",\"path\":")
		writeJCSString(&out, entry.path)
		out.WriteString(",\"sha256\":")
		if entry.hash == "" {
			out.WriteString("null")
		} else {
			writeJCSString(&out, entry.hash)
		}
		out.WriteByte('}')
	}
	out.WriteString("],\"revision\":")
	writeJCSString(&out, revision)
	out.WriteByte('}')
	return out.String()
}

// writeJCSString applies RFC 8785 string escaping without escaping HTML
// characters, non-ASCII code points, or U+2028 and U+2029.
func writeJCSString(out *strings.Builder, value string) {
	const hexDigits = "0123456789abcdef"
	out.WriteByte('"')
	for _, r := range value {
		switch r {
		case '\\', '"':
			out.WriteByte('\\')
			out.WriteRune(r)
		case '\b':
			out.WriteString("\\b")
		case '\t':
			out.WriteString("\\t")
		case '\n':
			out.WriteString("\\n")
		case '\f':
			out.WriteString("\\f")
		case '\r':
			out.WriteString("\\r")
		default:
			if r < 0x20 {
				out.WriteString("\\u00")
				out.WriteByte(hexDigits[r>>4])
				out.WriteByte(hexDigits[r&15])
			} else {
				out.WriteRune(r)
			}
		}
	}
	out.WriteByte('"')
}
