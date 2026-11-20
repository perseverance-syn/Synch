package credentials

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/url"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"synch/internal/ci"
	"synch/internal/remote"
)

// --- Fixtures: an in-memory keychain ---

// fakeKeychain is an in-memory Keychain for tests.
type fakeKeychain map[string]string

// Get returns the stored value or ErrNotFound.
func (f fakeKeychain) Get(acct string) (string, error) {
	v, ok := f[acct]
	if !ok {
		return "", ErrNotFound
	}
	return v, nil
}

// Set stores the value.
func (f fakeKeychain) Set(acct, secret string) error { f[acct] = secret; return nil }

// Delete removes the value.
func (f fakeKeychain) Delete(acct string) error {
	if _, ok := f[acct]; !ok {
		return ErrNotFound
	}
	delete(f, acct)
	return nil
}

// Kind names the fake.
func (fakeKeychain) Kind() string { return "fake" }

// newTestBroker returns a broker over a fresh fake keychain.
func newTestBroker() (*Broker, fakeKeychain) {
	kc := fakeKeychain{}
	return NewBroker(NewStore(kc), nil), kc
}

// forms lists the planted forms of a test secret: plain, base64 and URL-encoded.
func forms(secret string) []string {
	return []string{secret, base64.StdEncoding.EncodeToString([]byte(secret)), url.QueryEscape(secret)}
}

// --- C1: scope separation ---

// TestScopeSeparation proves C1: user and thread secrets are separate; a thread reads only its
// own scope, a driver only the user scope, and the scope is chosen by role, never by the caller.
func TestScopeSeparation(t *testing.T) {
	b, kc := newTestBroker()
	ctx := context.Background()
	for scope, v := range map[Scope]string{UserScope: "user-secret-1", ThreadScope("T1"): "thread-one-1", ThreadScope("T2"): "thread-two-2"} {
		if err := b.Put(scope, "api", v); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Put(UserScope, "infra-only", "infra-secret"); err != nil {
		t.Fatal(err)
	}
	if len(kc) != 4 || kc["user/api"] != "user-secret-1" || kc["thread/T1/api"] != "thread-one-1" {
		t.Fatalf("keychain accounts %v", kc)
	}
	if v, err := b.Use(ctx, Caller{Role: RoleThread, Thread: "T1"}, "api"); err != nil || v != "thread-one-1" {
		t.Fatalf("T1 got %q, %v", v, err)
	}
	if v, err := b.Use(ctx, Caller{Role: RoleThread, Thread: "T2"}, "api"); err != nil || v != "thread-two-2" {
		t.Fatalf("T2 got %q, %v", v, err)
	}
	if v, err := b.Use(ctx, Caller{Role: RoleDriver}, "api"); err != nil || v != "user-secret-1" {
		t.Fatalf("driver got %q, %v", v, err)
	}
	// A thread can name a user secret but only ever reads its own scope.
	if _, err := b.Use(ctx, Caller{Role: RoleThread, Thread: "T1"}, "infra-only"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("thread reached a user secret: %v", err)
	}
	// Path tricks cannot cross scopes: thread IDs and names are validated.
	for _, c := range []struct{ thread, name string }{{"T1/../T2", "api"}, {"T1", "../T2/api"}, {"T1", "a b"}} {
		if v, err := b.Use(ctx, Caller{Role: RoleThread, Thread: c.thread}, c.name); err == nil || v != "" {
			t.Fatalf("%+v got %q", c, v)
		}
	}
	if _, err := account(ThreadScope("T1/x"), "api"); !errors.Is(err, ErrInvalid) {
		t.Fatal("a thread ID with a slash was accepted")
	}
}

// TestCoordinatorDenied proves C2: the coordinator role (and any unknown role) never obtains
// a secret, from any scope, whatever it names.
func TestCoordinatorDenied(t *testing.T) {
	b, _ := newTestBroker()
	if err := b.Put(UserScope, "api", "user-secret-1"); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ThreadScope("C1"), "api", "coord-secret"); err != nil {
		t.Fatal(err)
	}
	for _, c := range []Caller{{Role: RoleCoordinator}, {Role: RoleCoordinator, Thread: "C1"}, {Role: ""}, {Role: RoleThread}} {
		if v, err := b.Use(context.Background(), c, "api"); !errors.Is(err, ErrDenied) || v != "" {
			t.Fatalf("%+v got %q, %v", c, v, err)
		}
	}
}

// TestAuthorizerGatesThreadUse proves C2: a thread's use passes through the policy authorizer.
func TestAuthorizerGatesThreadUse(t *testing.T) {
	b, _ := newTestBroker()
	if err := b.Put(ThreadScope("T1"), "api", "thread-one-1"); err != nil {
		t.Fatal(err)
	}
	b.SetAuthorizer(func(_ context.Context, c Caller, name string) error {
		if c.Thread == "T1" && name == "api" {
			return errors.New("policy says no")
		}
		return nil
	})
	if _, err := b.Use(context.Background(), Caller{Role: RoleThread, Thread: "T1"}, "api"); !errors.Is(err, ErrDenied) {
		t.Fatalf("authorizer ignored: %v", err)
	}
}

// --- C2: broker at use and the driver adapters ---

// TestBrokerRegistersAtUse proves C2/C3: a value read from the keychain is known to the
// redactor before it is handed out, and a value too short to redact is refused.
func TestBrokerRegistersAtUse(t *testing.T) {
	kc := fakeKeychain{"user/ci/https://ci.example": "tok-0123456789", "user/short": "abc"}
	b := NewBroker(NewStore(kc), nil)
	if out, _ := b.Redactor().Redact("tok-0123456789"); out != "tok-0123456789" {
		t.Fatal("value known before use")
	}
	tok, err := b.CI().Token(context.Background(), "https://ci.example")
	if err != nil || tok != "tok-0123456789" {
		t.Fatalf("token %q, %v", tok, err)
	}
	for _, f := range forms(tok) {
		if out, n := b.Redactor().Redact("x " + f + " y"); n != 1 || strings.Contains(out, f) {
			t.Fatalf("form %q not redacted: %q", f, out)
		}
	}
	if got := ci.Redact("Authorization: "+tok, b.CI().KnownValues()); got != "Authorization: "+Redacted {
		t.Fatalf("ci.Redact with KnownValues: %q", got)
	}
	if _, err := b.Use(context.Background(), Caller{Role: RoleDriver}, "short"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("short secret handed out: %v", err)
	}
	if err := b.Put(UserScope, "x", "abc"); !errors.Is(err, ErrInvalid) {
		t.Fatal("short secret stored")
	}
}

// TestVMAdapter proves C2: vmdriver.Broker decodes the endpoint credential and registers the
// password, token and basic-auth pair.
func TestVMAdapter(t *testing.T) {
	kc := fakeKeychain{"user/vm/https://pve.example": `{"username":"root@pam","password":"pve-pass-77","token":"pve-token-88"}`}
	b := NewBroker(NewStore(kc), nil)
	c, err := b.VM().Credentials(context.Background(), "https://pve.example")
	if err != nil || c.Username != "root@pam" || c.Password != "pve-pass-77" || c.Token != "pve-token-88" {
		t.Fatalf("%+v, %v", c, err)
	}
	basic := base64.StdEncoding.EncodeToString([]byte("root@pam:pve-pass-77"))
	in := "Authorization: Basic " + basic + " pw=pve-pass-77 tok=pve-token-88"
	if out, _ := b.Redactor().Redact(in); strings.Contains(out, "pve-") || strings.Contains(out, basic) {
		t.Fatalf("VM credential leaked: %q", out)
	}
	kc["user/vm/bad"] = "not-json-at-all"
	if _, err := b.VM().Credentials(context.Background(), "bad"); !errors.Is(err, ErrInvalid) || strings.Contains(err.Error(), "not-json") {
		t.Fatalf("bad credential: %v", err)
	}
}

// TestSSHAdapter proves C2/C4: remote.Broker authenticates with the user-scoped infrastructure
// key, while the forwarded agent holds only the thread's own key.
func TestSSHAdapter(t *testing.T) {
	infra, infraPEM := sshKey(t)
	fwd, fwdPEM := sshKey(t)
	kc := fakeKeychain{"user/ssh/r1": infraPEM, "thread/T1/ssh-forward/r1": fwdPEM}
	b := NewBroker(NewStore(kc), nil)
	br := b.SSH()
	target := remote.Target{Runner: "r1"}
	a, err := br.Agent(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if keys, _ := a.List(); len(keys) != 1 || !bytes.Equal(keys[0].Blob, infra.Marshal()) {
		t.Fatal("auth agent does not hold the infrastructure key")
	}
	f, err := br.ForwardingAgent(context.Background(), target, "T1")
	if err != nil {
		t.Fatal(err)
	}
	if keys, _ := f.List(); len(keys) != 1 || !bytes.Equal(keys[0].Blob, fwd.Marshal()) {
		t.Fatal("forwarded agent does not hold only the thread key")
	}
	if _, err := br.ForwardingAgent(context.Background(), target, "T2"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("another thread got a forwarding key: %v", err)
	}
	body := strings.Split(infraPEM, "\n")[1]
	if out, _ := b.Redactor().Redact("log: " + body); strings.Contains(out, body) {
		t.Fatal("a PEM body line was not redacted")
	}
}

// sshKey makes a fresh ed25519 key and its OpenSSH PEM.
func sshKey(t *testing.T) (ssh.PublicKey, string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	sp, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return sp, string(pem.EncodeToMemory(block))
}

// --- C3: redaction of strings, streams (retained output, exports) and event payloads ---

// TestRedactLongestFirst proves C3: a known value containing another is removed whole.
func TestRedactLongestFirst(t *testing.T) {
	r := NewRedactor()
	if err := r.Add("abcdef", "abcdef-ghijkl"); err != nil {
		t.Fatal(err)
	}
	if out, n := r.Redact("x abcdef-ghijkl y abcdef"); out != "x "+Redacted+" y "+Redacted || n != 2 {
		t.Fatalf("%q %d", out, n)
	}
	if err := r.Add("abc"); !errors.Is(err, ErrInvalid) {
		t.Fatal("short value accepted")
	}
}

// TestRedactWriterExport proves C3 for streams (retained output and exports): plain, base64 and
// URL-encoded secrets are removed even when split across writes and chunk boundaries.
func TestRedactWriterExport(t *testing.T) {
	r := NewRedactor()
	secret := "s3cr3t/value+with=chars"
	if err := r.Add(secret); err != nil {
		t.Fatal(err)
	}
	var input strings.Builder
	for i := 0; i < 3; i++ {
		input.WriteString(strings.Repeat("n", streamChunk-7))
		for _, f := range forms(secret) {
			input.WriteString(f + "\n")
		}
	}
	for _, step := range []int{1, 13, 4096, 1 << 20} {
		var out bytes.Buffer
		w := r.Writer(&out)
		data := []byte(input.String())
		for i := 0; i < len(data); i += step {
			if _, err := w.Write(data[i:min(i+step, len(data))]); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		for _, f := range forms(secret) {
			if bytes.Contains(out.Bytes(), []byte(f)) {
				t.Fatalf("step %d leaked %q", step, f)
			}
		}
		if got := bytes.Count(out.Bytes(), []byte(Redacted)); got != 9 {
			t.Fatalf("step %d: %d redactions", step, got)
		}
	}
}

// TestEventPayload proves C3 for the journal: strings, keys, numbers and nested values are
// redacted, and a payload with no known value is returned byte for byte.
func TestEventPayload(t *testing.T) {
	r := NewRedactor()
	secret := `tok"en\value`
	if err := r.Add(secret, "424242424242"); err != nil {
		t.Fatal(err)
	}
	clean := json.RawMessage(`{"b":1.50,"a":"fine"}`)
	if out, n, err := r.EventPayload(clean); err != nil || n != 0 || string(out) != string(clean) {
		t.Fatalf("clean payload changed: %s %d %v", out, n, err)
	}
	raw, _ := json.Marshal(map[string]any{"chunk": "x " + secret, secret: "k", "n": []any{base64.StdEncoding.EncodeToString([]byte(secret))}})
	raw = append(raw[:len(raw)-1], []byte(`,"pin":424242424242}`)...)
	out, n, err := r.EventPayload(raw)
	if err != nil || n != 4 {
		t.Fatalf("%s %d %v", out, n, err)
	}
	for _, f := range append(forms(secret), "424242424242", `tok\"en`) {
		if bytes.Contains(out, []byte(f)) {
			t.Fatalf("payload kept %q: %s", f, out)
		}
	}
	if !json.Valid(out) {
		t.Fatalf("invalid JSON: %s", out)
	}
}

// --- C1/C5: backend selection, CLI keychains and the fallback stub ---

// TestSelectAndFallback proves C1/C5: the OS keychain is selected when present, the fallback
// otherwise, and the fallback fails every operation clearly.
func TestSelectAndFallback(t *testing.T) {
	found := func(string) (string, error) { return "/usr/bin/x", nil }
	missing := func(string) (string, error) { return "", errors.New("absent") }
	if k := Select("darwin", nil, found).Kind(); k != "macos-security" {
		t.Fatal(k)
	}
	if k := Select("linux", nil, found).Kind(); k != "secret-service" {
		t.Fatal(k)
	}
	if k := Select("linux", nil, missing).Kind(); k != "fallback" {
		t.Fatal(k)
	}
	if k := Select("plan9", nil, found).Kind(); k != "fallback" {
		t.Fatal(k)
	}
	b := NewBroker(nil, nil)
	if err := b.Put(UserScope, "api", "value-123"); !errors.Is(err, ErrFallbackUnavailable) {
		t.Fatalf("fallback stored: %v", err)
	}
	if _, err := b.Use(context.Background(), Caller{Role: RoleDriver}, "api"); !errors.Is(err, ErrFallbackUnavailable) {
		t.Fatalf("fallback read: %v", err)
	}
}

// TestCLIKeychainKeepsSecretOffArgv proves C1: macOS security and Linux secret-tool get the
// secret on stdin only, read it from stdout, and map "missing" to ErrNotFound.
func TestCLIKeychainKeepsSecretOffArgv(t *testing.T) {
	const secret = `pa"ss\word-123`
	for _, kind := range []string{"macos-security", "secret-service"} {
		stored := false
		var argvs []string
		run := func(_ context.Context, stdin []byte, name string, args ...string) ([]byte, int, error) {
			argv := name + " " + strings.Join(args, " ")
			argvs = append(argvs, argv)
			switch {
			case strings.Contains(argv, "find-generic-password") || strings.Contains(argv, " lookup "):
				if !stored && kind == "macos-security" {
					return nil, 44, nil
				}
				if !stored {
					return nil, 1, nil
				}
				if kind == "macos-security" {
					return []byte(secret + "\n"), 0, nil
				}
				return []byte(secret), 0, nil
			case name == "security" && args[0] == "-i":
				if !bytes.Contains(stdin, []byte(quote(secret))) {
					t.Fatalf("security stdin %q", stdin)
				}
				stored = true
			case strings.Contains(argv, " store "):
				if string(stdin) != secret {
					t.Fatalf("secret-tool stdin %q", stdin)
				}
				stored = true
			}
			return nil, 0, nil
		}
		s := NewStore(cliKeychain{run: run, kind: kind})
		if _, err := s.get(UserScope, "api"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s missing: %v", kind, err)
		}
		if err := s.Put(UserScope, "api", secret); err != nil {
			t.Fatal(err)
		}
		if v, err := s.get(UserScope, "api"); err != nil || v != secret {
			t.Fatalf("%s get %q %v", kind, v, err)
		}
		if err := s.Delete(UserScope, "api"); err != nil {
			t.Fatal(err)
		}
		for _, a := range argvs {
			if strings.Contains(a, "word-123") {
				t.Fatalf("%s secret on argv: %s", kind, a)
			}
		}
	}
}
