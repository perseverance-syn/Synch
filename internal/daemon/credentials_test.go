package daemon

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"synch/internal/credentials"
	"synch/internal/engine"
)

// memKeychain is an in-memory credentials.Keychain for daemon tests.
type memKeychain struct {
	mu sync.Mutex
	m  map[string]string
}

// Get returns the stored value or credentials.ErrNotFound.
func (k *memKeychain) Get(acct string) (string, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if v, ok := k.m[acct]; ok {
		return v, nil
	}
	return "", credentials.ErrNotFound
}

// Set stores the value.
func (k *memKeychain) Set(acct, v string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.m[acct] = v
	return nil
}

// Delete removes the value.
func (k *memKeychain) Delete(acct string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.m, acct)
	return nil
}

// Kind names the fake.
func (*memKeychain) Kind() string { return "memory" }

// lockedBuffer collects log output from concurrent goroutines.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends under the lock.
func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// Bytes copies the collected output.
func (b *lockedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.buf.Bytes())
}

// TestSecretsStayOutOfEveryPersistedByte proves card 401 C2, C3 and C6 through a real daemon run:
// a coordinator is refused a secret; a worker gets its own secret at use; the planted secret,
// in plain, base64 and URL-encoded form, is redacted from the engine prompt, the journal, the
// retained output and its excerpt (the 602 finding), is refused in engine Env, and appears in
// no persisted byte under the data directory or worktree and in no log line.
func TestSecretsStayOutOfEveryPersistedByte(t *testing.T) {
	logs := &lockedBuffer{}
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prev) })

	secret := "planted-S3CRET/thread+value"
	planted := []string{secret, base64.StdEncoding.EncodeToString([]byte(secret)), url.QueryEscape(secret)}
	kc := &memKeychain{m: map[string]string{}}
	tree := gateWorktree(t)
	cfg, starts := handoffConfig(t, tree)
	cfg.Credentials = credentials.NewBroker(credentials.NewStore(kc), nil)
	writeGateFile(t, cfg.DataDir, "policy.json", `{"policy_version":3,"source":"operator","rules":[
		{"id":"run","effect":"allow","actions":["assign.dispatch","host.exec","secret.use"]}]}`)
	d, base := serveGate(t, cfg)
	parent := gateThread(t, base, "secrets")
	child := childOf(t, d, parent)
	kc.m["thread/"+child+"/api"] = secret
	kc.m["thread/"+parent+"/api"] = secret

	// C2: the coordinator never obtains a secret, even one stored under its own thread.
	if v, err := d.UseSecret(t.Context(), parent, "api"); !errors.Is(err, credentials.ErrDenied) || v != "" {
		t.Fatalf("coordinator got %q, %v", v, err)
	}
	// Broker at use: the worker receives its own secret, which is now known to the redactor.
	if v, err := d.UseSecret(t.Context(), child, "api"); err != nil || v != secret {
		t.Fatalf("worker got %q, %v", v, err)
	}
	// C2: a known secret in engine Env refuses the launch.
	if _, err := d.redactRequest(engine.StartRequest{Env: map[string]string{"HARMLESS": "x" + secret}}); !errors.Is(err, ErrSecretInEnv) {
		t.Fatalf("env with secret: %v", err)
	}

	// C3 prompts: the instructions the engine receives carry no planted form.
	req := engine.StartRequest{ThreadID: child, Workdir: tree, Instructions: []engine.Instruction{{Text: "brief " + strings.Join(planted, " ")}}}
	if err := d.StartAssignment(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	session := <-starts
	sent := ""
	for _, in := range session.req.Instructions {
		sent += in.Text
	}
	for _, f := range planted {
		if strings.Contains(sent, f) {
			t.Fatalf("prompt carried %q", f)
		}
	}
	if !strings.Contains(sent, credentials.Redacted) {
		t.Fatal("prompt was not redacted")
	}

	// C3 journal: engine output carrying every form is persisted redacted.
	session.output(t, "leak "+strings.Join(planted, " "))
	outputs := eventsOf(t, d, child, "engine.output")
	if len(outputs) == 0 || !strings.Contains(outputs[len(outputs)-1]["chunk"].(string), credentials.Redacted) {
		t.Fatalf("engine.output %v", outputs)
	}

	// C3 retained output (602 finding): neither the excerpt nor the retained file holds a form.
	for _, input := range []string{"FAIL " + strings.Join(planted, " ") + "\n",
		strings.Repeat("noise\n", 5000) + "FAIL " + strings.Join(planted, " ") + "\n" + strings.Repeat("noise\n", 5000)} {
		ex, err := d.CapOutput(strings.NewReader(input), "fail")
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range planted {
			if strings.Contains(ex.Text, f) {
				t.Fatalf("excerpt carried %q", f)
			}
		}
	}

	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	// Grep every persisted byte of the run: data directory (event store, WAL, outputs,
	// supervisor logs) and the worktree (handoff files in git metadata), plus every log line.
	scanned := 0
	for _, root := range []string{cfg.DataDir, tree} {
		err := filepath.WalkDir(root, func(path string, e fs.DirEntry, err error) error {
			if err != nil || e.IsDir() {
				return err
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			scanned++
			for _, f := range planted {
				if bytes.Contains(raw, []byte(f)) {
					t.Errorf("%s holds planted form %q", path, f)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range planted {
		if bytes.Contains(logs.Bytes(), []byte(f)) {
			t.Errorf("logs hold planted form %q", f)
		}
	}
	if scanned < 5 {
		t.Fatalf("scanned only %d files", scanned)
	}
}
