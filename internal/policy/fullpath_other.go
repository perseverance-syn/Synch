//go:build !windows

package policy

// osFullPath has no Win32 normalizer to consult off Windows. It returns p
// unchanged, so only the static rules apply (tests force windowsPaths there).
func osFullPath(p string) (string, error) { return p, nil }
