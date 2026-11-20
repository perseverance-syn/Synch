package credentials

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"time"
)

// --- Scopes and the store: user and thread secrets kept apart (spec line 74) ---

// Service is the keychain service (macOS, Linux) and target prefix (Windows) of every Synch secret.
const Service = "synch"

// MinSecretLen is the shortest secret Synch stores or brokers: a shorter value cannot be
// redacted without destroying ordinary text, so it is refused rather than leaked.
const MinSecretLen = 6

var (
	// ErrNotFound means the keychain has no secret under the scope and name.
	ErrNotFound = errors.New("credentials: secret not found")
	// ErrDenied means the caller may not obtain the secret (wrong scope or coordinator).
	ErrDenied = errors.New("credentials: secret use denied")
	// ErrInvalid means a scope, name or value failed validation.
	ErrInvalid = errors.New("credentials: invalid secret reference or value")
	// ErrFallbackUnavailable means no OS keychain was found and the age-encrypted
	// fallback is not implemented yet (card 401 C5, pending owner approval of filippo.io/age).
	ErrFallbackUnavailable = errors.New("credentials: no OS keychain and the age-encrypted fallback is not available yet")
)

// Scope names whose secret it is: the user (Thread empty) or exactly one thread.
type Scope struct{ Thread string }

// UserScope is the operator's own scope; infrastructure credentials live here.
var UserScope = Scope{}

// ThreadScope is the scope of one thread's own secrets.
func ThreadScope(id string) Scope { return Scope{Thread: id} }

// threadID and secretName bound what can reach a keychain account name. A thread ID has
// no "/", so "user/x" and "thread/<id>/x" can never name the same account.
var (
	threadID   = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	secretName = regexp.MustCompile(`^[A-Za-z0-9._:/@-]{1,200}$`)
)

// account maps a scope and name to the keychain account, validating both.
func account(scope Scope, name string) (string, error) {
	if !secretName.MatchString(name) {
		return "", fmt.Errorf("%w: secret name", ErrInvalid)
	}
	if scope.Thread == "" {
		return "user/" + name, nil
	}
	if !threadID.MatchString(scope.Thread) {
		return "", fmt.Errorf("%w: thread scope", ErrInvalid)
	}
	return "thread/" + scope.Thread + "/" + name, nil
}

// Keychain is one secret backend keyed by account under Service. Errors must never
// contain the secret value.
type Keychain interface {
	Get(account string) (string, error)
	Set(account, secret string) error
	Delete(account string) error
	// Kind names the backend for diagnostics ("wincred", "macos-security", "secret-service", "fallback").
	Kind() string
}

// Store keeps scoped secrets in one keychain.
type Store struct{ kc Keychain }

// NewStore wraps a keychain; tests pass a fake, production passes Select's result.
func NewStore(kc Keychain) *Store { return &Store{kc: kc} }

// Kind names the backing keychain.
func (s *Store) Kind() string { return s.kc.Kind() }

// Put stores a secret under scope and name, replacing any previous value.
func (s *Store) Put(scope Scope, name, value string) error {
	acct, err := account(scope, name)
	if err != nil {
		return err
	}
	if len(value) < MinSecretLen || strings.ContainsRune(value, 0) {
		return fmt.Errorf("%w: secret value must be at least %d bytes without NUL", ErrInvalid, MinSecretLen)
	}
	return s.kc.Set(acct, value)
}

// get reads a secret; only the Broker calls it, after choosing the scope from the caller.
func (s *Store) get(scope Scope, name string) (string, error) {
	acct, err := account(scope, name)
	if err != nil {
		return "", err
	}
	return s.kc.Get(acct)
}

// Delete removes a secret; a missing secret is not an error.
func (s *Store) Delete(scope Scope, name string) error {
	acct, err := account(scope, name)
	if err != nil {
		return err
	}
	if err := s.kc.Delete(acct); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

// --- Backend selection: OS keychain first, fallback when none exists (card 401 C5) ---

// Runner runs a keychain CLI with stdin and returns its stdout and exit code. err is
// set only when the command could not run at all.
type Runner func(ctx context.Context, stdin []byte, name string, args ...string) (stdout []byte, exit int, err error)

// ExecRunner runs the command for real. Secrets travel only on stdin and stdout, never argv.
func ExecRunner(ctx context.Context, stdin []byte, name string, args ...string) ([]byte, int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = bytes.NewReader(stdin)
	out, err := cmd.Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return out, exit.ExitCode(), nil
	}
	if err != nil {
		return nil, -1, err
	}
	return out, 0, nil
}

// Select returns the OS keychain for goos when it is usable, else the fallback store.
// lookPath decides whether a keychain CLI is installed (exec.LookPath in production).
func Select(goos string, run Runner, lookPath func(string) (string, error)) Keychain {
	switch goos {
	case "windows":
		if kc := windowsKeychain(); kc != nil {
			return kc
		}
	case "darwin":
		if _, err := lookPath("security"); err == nil {
			return cliKeychain{run: run, kind: "macos-security"}
		}
	case "linux", "freebsd", "openbsd", "netbsd":
		if _, err := lookPath("secret-tool"); err == nil {
			return cliKeychain{run: run, kind: "secret-service"}
		}
	}
	return Fallback()
}

// OpenOS selects the keychain of this machine with the real command runner.
func OpenOS() *Store { return NewStore(Select(runtime.GOOS, ExecRunner, exec.LookPath)) }

// Fallback returns the fallback store used when no OS keychain is available. Until the
// owner approves filippo.io/age it is a stub that fails every operation; no encryption
// format is hand-rolled here.
func Fallback() Keychain { return fallbackStub{} }

// fallbackStub is the clearly failing placeholder for the age-encrypted file store.
type fallbackStub struct{}

// Get always fails: there is nowhere safe to read from.
func (fallbackStub) Get(string) (string, error) { return "", ErrFallbackUnavailable }

// Set always fails rather than writing a secret unencrypted.
func (fallbackStub) Set(string, string) error { return ErrFallbackUnavailable }

// Delete always fails: nothing can have been stored.
func (fallbackStub) Delete(string) error { return ErrFallbackUnavailable }

// Kind names the fallback.
func (fallbackStub) Kind() string { return "fallback" }

// --- CLI keychains: macOS security and Linux Secret Service through a Runner ---

// cliTimeout bounds one keychain command so a locked keychain prompt cannot hang the daemon.
const cliTimeout = 30 * time.Second

// cliKeychain drives `security` (macOS) or `secret-tool` (Secret Service).
type cliKeychain struct {
	run  Runner
	kind string
}

// Kind names the backend.
func (c cliKeychain) Kind() string { return c.kind }

// call runs one command with a bounded context.
func (c cliKeychain) call(stdin []byte, name string, args ...string) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), cliTimeout)
	defer cancel()
	return c.run(ctx, stdin, name, args...)
}

// Get prints the secret on stdout; an absent item is ErrNotFound.
func (c cliKeychain) Get(acct string) (string, error) {
	var out []byte
	var exit int
	var err error
	if c.kind == "macos-security" {
		out, exit, err = c.call(nil, "security", "find-generic-password", "-s", Service, "-a", acct, "-w")
		out = bytes.TrimSuffix(out, []byte("\n"))
	} else {
		out, exit, err = c.call(nil, "secret-tool", "lookup", "service", Service, "account", acct)
	}
	if err != nil {
		return "", fmt.Errorf("credentials: run %s: %w", c.kind, err)
	}
	// security exits 44 for a missing item; secret-tool exits 1 (or 0) with no output.
	if exit == 44 || (exit <= 1 && len(out) == 0) {
		return "", ErrNotFound
	}
	if exit != 0 {
		return "", fmt.Errorf("credentials: %s lookup exited %d", c.kind, exit)
	}
	return string(out), nil
}

// Set writes the secret through stdin only, so it never appears in a process listing.
func (c cliKeychain) Set(acct, secret string) error {
	var exit int
	var err error
	if c.kind == "macos-security" {
		// security -i reads one command per line; quoting keeps the secret one argument.
		line := "add-generic-password -U -s " + quote(Service) + " -a " + quote(acct) + " -w " + quote(secret) + "\n"
		_, exit, err = c.call([]byte(line), "security", "-i")
	} else {
		_, exit, err = c.call([]byte(secret), "secret-tool", "store", "--label=Synch "+acct, "service", Service, "account", acct)
	}
	if err != nil {
		return fmt.Errorf("credentials: run %s: %w", c.kind, err)
	}
	if exit != 0 {
		return fmt.Errorf("credentials: %s store exited %d", c.kind, exit)
	}
	return nil
}

// Delete removes the item; an absent item is ErrNotFound.
func (c cliKeychain) Delete(acct string) error {
	var exit int
	var err error
	if c.kind == "macos-security" {
		_, exit, err = c.call(nil, "security", "delete-generic-password", "-s", Service, "-a", acct)
	} else {
		_, exit, err = c.call(nil, "secret-tool", "clear", "service", Service, "account", acct)
	}
	if err != nil {
		return fmt.Errorf("credentials: run %s: %w", c.kind, err)
	}
	if exit == 44 {
		return ErrNotFound
	}
	if exit != 0 {
		return fmt.Errorf("credentials: %s delete exited %d", c.kind, exit)
	}
	return nil
}

// quote double-quotes one argument for the `security -i` command parser.
// ponytail: assumes security's interactive tokenizer honours \" and \\ escapes; untested on a real Mac.
func quote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(s) + `"`
}
