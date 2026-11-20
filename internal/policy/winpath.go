package policy

import (
	"os"
	"runtime"
	"strings"
)

// --- Windows path spellings: extended-length paths and the Win32 normalizer (G4, K4, V3) ---

// windowsPaths enables Windows namespace handling; on by default on Windows.
var windowsPaths = runtime.GOOS == "windows"

// plainWindowsPath rewrites a Windows extended-length path to its ordinary
// spelling so both compare equal (G4): \\?\C:\x becomes C:\x and
// \\?\UNC\server\share\x becomes \\server\share\x. An extended path bypasses
// the Win32 normalizer, so it can name objects its ordinary spelling cannot
// reach (a literal trailing dot or space, "..", a DOS device name, "/" or ":"
// inside a name). The rewrite is therefore accepted only if every component
// passes win32Stable and the OS normalizer (GetFullPathNameW, through
// osFullPath) returns the ordinary spelling exactly unchanged (V3). A
// legacy device spelling also needs proof that both names reach the same
// ordinary object. Anything Win32 changes or redirects is rejected
// and the request is blocked before matching. Every other \\?\, \\.\ or \??\
// namespace (volume GUIDs, devices, GLOBALROOT, a bare \\?\C:, //?/ with
// forward slashes) is rejected too. Paths without such a prefix, and all
// paths off Windows, pass through unchanged. Mapped drives and UNC shares
// are not equated.
func plainWindowsPath(p string) (string, bool) { return plainWindows(p, false) }

// plainGlob is plainWindowsPath for a rule glob: the wildcards * and ? are
// allowed inside components and the OS normalizer is not consulted (it does
// not interpret wildcards). Legacy device spellings remain ambiguous
// in a rule glob without a concrete file identity.
func plainGlob(g string) (string, bool) { return plainWindows(g, true) }

// plainWindows does the work of plainWindowsPath and plainGlob.
func plainWindows(p string, glob bool) (string, bool) {
	if !windowsPaths {
		return p, true
	}
	b := strings.ReplaceAll(p, "/", `\`)
	if !strings.HasPrefix(b, `\\?\`) && !strings.HasPrefix(b, `\\.\`) && !strings.HasPrefix(b, `\??\`) {
		return p, true
	}
	if b != p || !strings.HasPrefix(b, `\\?\`) { // "/" is not a separator in the extended namespace
		return "", false
	}
	var plain, rest string
	switch {
	case len(b) > 8 && strings.EqualFold(b[:8], `\\?\UNC\`):
		plain, rest = `\\`+b[8:], b[8:]
		if !strings.Contains(strings.TrimSuffix(rest, `\`), `\`) { // needs server and share
			return "", false
		}
	case len(b) >= 7 && isLetter(b[4]) && b[5] == ':' && b[6] == '\\':
		plain, rest = b[4:], b[7:]
	default:
		return "", false
	}
	parts := strings.Split(rest, `\`)
	deviceLike := false
	for i, c := range parts {
		if c == "" && i == len(parts)-1 { // a trailing separator (or a bare drive root) is the same directory
			continue
		}
		if !win32Stable(c, glob) {
			return "", false
		}
		deviceLike = deviceLike || isDosDevice(c)
	}
	if glob {
		if deviceLike {
			return "", false
		}
		return plain, true
	}
	// Win32 round-trip equality is necessary even if both files exist.
	if full, err := osFullPath(plain); err != nil || full != plain {
		return "", false
	}
	if deviceLike && (runtime.GOOS != "windows" || !sameOrdinaryFile(plain, p)) {
		return "", false
	}
	return plain, true
}

// sameOrdinaryFile distinguishes legacy device aliases from ordinary
// filesystem entries on Windows versions that permit the same spelling.
func sameOrdinaryFile(plain, extended string) bool {
	a, err := os.Lstat(plain)
	if err != nil || (!a.Mode().IsRegular() && !a.IsDir()) {
		return false
	}
	b, err := os.Lstat(extended)
	return err == nil && os.SameFile(a, b)
}

// win32Stable reports whether the Win32 normalizer leaves the name c alone,
// judged statically: not empty, ".", or ".."; no trailing dot or space; none
// of < > : " | (and, outside globs, ? *) and no control character. Device
// aliases are checked separately because the OS rules vary by version.
func win32Stable(c string, glob bool) bool {
	switch {
	case c == "" || c == "." || c == "..":
		return false
	case strings.HasSuffix(c, ".") || strings.HasSuffix(c, " "):
		return false
	case strings.ContainsAny(c, `<>:"|`) || !glob && strings.ContainsAny(c, `?*`):
		return false
	}
	for _, r := range c {
		if r < 0x20 {
			return false
		}
	}
	return true
}

// isDosDevice identifies the DOS spellings that may need a file identity
// check even after normalization. CONIN$ and CONOUT$ are exact console
// names. CON, PRN, AUX, NUL, and COM/LPT with 1–9 or a superscript 1–3
// also reserve the basename before an extension on legacy Windows.
func isDosDevice(c string) bool {
	if strings.EqualFold(c, "CONIN$") || strings.EqualFold(c, "CONOUT$") {
		return true
	}
	base := c
	if i := strings.IndexAny(base, ".:"); i >= 0 {
		base = base[:i]
	}
	base = strings.ToUpper(strings.TrimRight(base, " "))
	switch base {
	case "CON", "PRN", "AUX", "NUL":
		return true
	}
	if strings.HasPrefix(base, "COM") || strings.HasPrefix(base, "LPT") {
		switch base[3:] {
		case "1", "2", "3", "4", "5", "6", "7", "8", "9", "¹", "²", "³":
			return true
		}
	}
	return false
}

// isLetter reports whether c is an ASCII drive letter.
func isLetter(c byte) bool { return c|0x20 >= 'a' && c|0x20 <= 'z' }
