//go:build !windows

package credentials

// windowsKeychain is unavailable off Windows.
func windowsKeychain() Keychain { return nil }
