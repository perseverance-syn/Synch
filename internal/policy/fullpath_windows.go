//go:build windows

package policy

import "syscall"

// osFullPath returns what the Win32 normalizer (GetFullPathNameW) makes of p.
func osFullPath(p string) (string, error) { return syscall.FullPath(p) }
