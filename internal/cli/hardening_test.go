package cli

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// --- Transport hardening: redirects and unclassified answers (R1, R3, P1) ---

// TestCLIRefusesRedirects proves R1: a redirecting answer is reported as an
// error and never followed, so a redirect target outside the loopback policy
// cannot be contacted.
func TestCLIRefusesRedirects(t *testing.T) {
	var backendHits int
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backendHits++
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(backend.Close)

	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, backend.URL+"/v1/health", http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	f := newCLIFixture(t)
	f.url = redirector.URL
	code, out, errOut := f.run(f.authenticated("--json", "health")...)
	if code == 0 {
		t.Fatalf("redirected health = %d, %s, %s", code, out, errOut)
	}
	if !strings.Contains(errOut, "redirect") {
		t.Fatalf("redirect error = %q, want it to mention the refusal", errOut)
	}
	if backendHits != 0 {
		t.Fatalf("redirect target was contacted %d times", backendHits)
	}
}

// TestFollowPlaintextErrorIsTypedAndFatal proves R3: a plaintext 503 from the
// stream endpoint returns a typed error and exits non-zero instead of
// panicking on an unclassifiable answer.
func TestFollowPlaintextErrorIsTypedAndFatal(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("upstream unavailable"))
	}))
	t.Cleanup(stub.Close)

	var stdout, stderr strings.Builder
	code := Main([]string{"--server", stub.URL, "--token-file", stubTokenFile(t), "--json",
		"events", "follow", "all"}, &stdout, &stderr, emptyEnv)
	if code == 0 {
		t.Fatalf("follow over a plaintext 503 = %d", code)
	}
	if !strings.Contains(stderr.String(), "503") {
		t.Fatalf("stderr = %q, want the unclassified status", stderr.String())
	}
}

// --- Follow semantics: unknown types, --until and resync rebuilds (R4, R5, P2) ---

// TestFollowUnknownEventTypeAdvancesCursor proves R5: a frame of unknown type
// advances the cursor without being printed, so the follow is complete when
// the --until cursor arrives through an unknown-type frame.
func TestFollowUnknownEventTypeAdvancesCursor(t *testing.T) {
	hello := "event: hello\ndata: {\"log_epoch\":\"E1\",\"head\":10}\n\n"
	stub := newSSEStub(t,
		[]string{hello,
			"id: 1\nevent: request.accepted\ndata: {\"seq\":1}\n\n",
			"id: 2\nevent: mystery-type\ndata: {\"seq\":2}\n\n"})
	var stdout, stderr strings.Builder
	code := Main([]string{"--server", stub.server.URL, "--token-file", stubTokenFile(t), "--json",
		"events", "follow", "all", "--until", "2"}, &stdout, &stderr, emptyEnv)
	if code != exitOK {
		t.Fatalf("follow = %d, %s, %s", code, stdout.String(), stderr.String())
	}
	if got := strings.TrimSpace(stdout.String()); got != "{\"seq\":1}" {
		t.Fatalf("delivered %q, want only the known-type frame", got)
	}
}

// TestFollowUntilReachedThroughResyncPage proves R4: reaching --until through
// a resync page (the cursor arrives from pageEvents after an epoch change)
// exits 0, exactly like a live delivery.
func TestFollowUntilReachedThroughResyncPage(t *testing.T) {
	f := newCLIFixture(t)
	f.createThread("resync-until", "resync until thread")
	f.sendCommand(f.lastThreadID(), "pause")
	oldHead := int(f.head())

	// The follower starts with the old lineage already delivered and then
	// reconnects into a changed lineage whose resync rebuild passes the
	// --until cursor (one more command than the old lineage held). The
	// writer is an unlimited collector; --until is the only termination.
	writer := newStopAfterWriter()
	done := make(chan int, 1)
	go func() {
		done <- Main(f.authenticated("--json", "events", "follow", "all", "--until",
			strconv.FormatInt(int64(oldHead+1), 10)), writer, &strings.Builder{}, f.env)
	}()
	writer.awaitLines(t, oldHead)

	f.restartWithNewStoreOnTempPort()
	f.createThread("resync-until-new", "resync new thread")
	f.sendCommand(f.lastThreadID(), "pause")
	f.sendCommand(f.lastThreadID(), "resume")
	f.serveOnOriginalPort()

	select {
	case code := <-done:
		if code != exitOK {
			t.Fatalf("follow exit = %d, want 0 when --until is reached through a resync page", code)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("follow did not exit after the resync")
	}
	assertRebuiltCursors(t, writer.collected(), oldHead, oldHead+1)
}

// --- Receipts, setup and readable output (R2, R6, R7, R8, R9) ---

// TestStoredRejectionPrintsReceiptMetadata proves R2: a stored rejection
// prints the receipt id and the replay flag on the first attempt and on the
// replay, keeps the problem code, and exits non-zero on both.
func TestStoredRejectionPrintsReceiptMetadata(t *testing.T) {
	f := newCLIFixture(t)
	args := f.authenticated("--json", "thread", "cmd", "01AAAAAAAAAAAAAAAAAAAAAAAA", "pause", "--request-id", "rej-1")

	code, out, errOut := f.run(args...)
	if code != exitProblem || !strings.Contains(errOut, "not_found") {
		t.Fatalf("first stored rejection = %d, %q, %q", code, out, errOut)
	}
	if !strings.Contains(errOut, "receipt ") || !strings.Contains(errOut, "replayed false") {
		t.Fatalf("first stored rejection lacks receipt metadata: %q", errOut)
	}
	receiptID := receiptIDFromDiagnostics(t, errOut)

	code, out, errOut = f.run(args...)
	if code != exitProblem || !strings.Contains(errOut, "not_found") {
		t.Fatalf("replayed stored rejection = %d, %q, %q", code, out, errOut)
	}
	if !strings.Contains(errOut, "receipt "+receiptID) || !strings.Contains(errOut, "replayed true") {
		t.Fatalf("replayed stored rejection lacks metadata: %q (want receipt %s)", errOut, receiptID)
	}
}

// receiptIDFromDiagnostics extracts the receipt id from a diagnostics line.
func receiptIDFromDiagnostics(t *testing.T, diagnostics string) string {
	t.Helper()
	for _, line := range strings.Split(diagnostics, "\n") {
		if rest, ok := strings.CutPrefix(line, "receipt "); ok {
			return strings.Fields(rest)[0]
		}
	}
	t.Fatalf("no receipt id in %q", diagnostics)
	return ""
}

// TestSetupKeepsRecordedTokenFileAcrossEnvironmentRerun proves R6: a setup
// run whose credential comes from SYNCH_TOKEN preserves the previously saved
// token_file path, and health still works once the variable is removed.
func TestSetupKeepsRecordedTokenFileAcrossEnvironmentRerun(t *testing.T) {
	f := newCLIFixture(t)
	configPath := filepath.Join(f.dir, "config.json")
	if code, out, errOut := f.run("--config", configPath, "--server", f.url, "--token-file", f.tokenFile, "setup"); code != 0 {
		t.Fatalf("first setup = %d, %s, %s", code, out, errOut)
	}
	f.envMap = map[string]string{"SYNCH_TOKEN": cliToken}
	if code, out, errOut := f.run("--config", configPath, "--server", f.url, "setup"); code != 0 {
		t.Fatalf("environment setup = %d, %s, %s", code, out, errOut)
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"token_file":`+mustJSONString(f.tokenFile)) {
		t.Fatalf("environment rerun erased the recorded token_file: %s", raw)
	}
	f.envMap = map[string]string{}
	if code, out, errOut := f.run("--config", configPath, "health"); code != 0 {
		t.Fatalf("health from the preserved config = %d, %s, %s", code, out, errOut)
	}
}

// TestSetupReplacesConfigWithOwnerOnlyPermissions proves R7: replacing the
// config goes through a fresh owner-only file, so a replaced POSIX file has
// 0600 permissions and a read-only Windows target does not block the
// replacement.
func TestSetupReplacesConfigWithOwnerOnlyPermissions(t *testing.T) {
	f := newCLIFixture(t)
	configPath := filepath.Join(f.dir, "config.json")
	if code, out, errOut := f.run("--config", configPath, "--server", f.url, "--token-file", f.tokenFile, "setup"); code != 0 {
		t.Fatalf("first setup = %d, %s, %s", code, out, errOut)
	}
	// Make the existing file hostile to plain writes: read-only on Windows,
	// a non-owner mode on POSIX.
	if err := os.Chmod(configPath, 0444); err != nil {
		t.Fatal(err)
	}
	if code, out, errOut := f.run("--config", configPath, "--server", f.url, "--token-file", f.tokenFile, "setup"); code != 0 {
		t.Fatalf("setup over a hostile file = %d, %s, %s", code, out, errOut)
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), f.url) {
		t.Fatalf("replaced config = %s", raw)
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(configPath)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0600 {
			t.Fatalf("replaced config mode = %v, want 0600", got)
		}
	}
}

// TestSetupDoesNotChmodOutsideWindows proves P5: where the chmod-and-retry
// fallback is not allowed, a replacement blocked by a read-only target is
// returned as an error and the file mode is left untouched. It needs a host
// where a read-only target blocks rename (Windows), so it skips elsewhere.
func TestSetupDoesNotChmodOutsideWindows(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("a read-only target only blocks the replacement on Windows")
	}
	original := configGOOS
	configGOOS = "linux"
	t.Cleanup(func() { configGOOS = original })

	f := newCLIFixture(t)
	configPath := filepath.Join(f.dir, "config.json")
	if code, out, errOut := f.run("--config", configPath, "--server", f.url, "--token-file", f.tokenFile, "setup"); code != 0 {
		t.Fatalf("first setup = %d, %s, %s", code, out, errOut)
	}
	if err := os.Chmod(configPath, 0444); err != nil {
		t.Fatal(err)
	}
	code, _, errOut := f.run("--config", configPath, "--server", f.url, "--token-file", f.tokenFile, "setup")
	if code == 0 || !strings.Contains(errOut, "replace config file") {
		t.Fatalf("setup over a read-only file = %d, %q, want the replace error", code, errOut)
	}
	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0200 != 0 {
		t.Fatalf("config mode = %v: the fallback widened it", info.Mode().Perm())
	}
}

// TestSetupJSONWritesSingleDocument proves R8: setup --json writes exactly
// one JSON document holding the setup and health results.
func TestSetupJSONWritesSingleDocument(t *testing.T) {
	f := newCLIFixture(t)
	configPath := filepath.Join(f.dir, "config.json")
	code, out, errOut := f.run("--json", "--config", configPath, "--server", f.url, "--token-file", f.tokenFile, "setup")
	if code != 0 {
		t.Fatalf("setup = %d, %s, %s", code, out, errOut)
	}
	trimmed := strings.TrimSpace(out)
	if strings.Contains(trimmed, "}\n{") || strings.Contains(trimmed, "}\r\n{") {
		t.Fatalf("setup --json wrote more than one document:\n%s", trimmed)
	}
	var document struct {
		ConfigPath string         `json:"config_path"`
		Server     string         `json:"server"`
		Health     map[string]any `json:"health"`
	}
	if err := json.Unmarshal([]byte(trimmed), &document); err != nil {
		t.Fatalf("single-document parse = %v for %s", err, trimmed)
	}
	if document.ConfigPath != configPath || document.Server != f.url || document.Health["status"] != "ok" {
		t.Fatalf("setup document = %+v", document)
	}
}

// TestThreadsListRowsAndPageCursors proves R9: threads list renders readable
// rows by default, and events page prints the selected stream's cursor (seq
// for a thread stream, global_seq for all).
func TestThreadsListRowsAndPageCursors(t *testing.T) {
	f := newCLIFixture(t)
	f.createThread("rows-a", "first thread")
	second := f.createThread("rows-b", "second thread")

	code, out, errOut := f.run(f.authenticated("threads", "list")...)
	if code != 0 {
		t.Fatalf("threads list = %d, %s, %s", code, out, errOut)
	}
	for _, column := range []string{"ID\tPROJECT\tROLE\tSTATE\tDEPTH", second, "draft"} {
		if !strings.Contains(out, column) {
			t.Fatalf("threads list lacks %q:\n%s", column, out)
		}
	}

	// The thread stream's cursor is seq; the all stream's cursor is
	// global_seq, which differs once more than one thread has events.
	code, out, errOut = f.run(f.authenticated("events", "page", "thread:"+second)...)
	if code != 0 {
		t.Fatalf("thread page = %d, %s, %s", code, out, errOut)
	}
	if !strings.Contains(out, "1\trequest.accepted\t") {
		t.Fatalf("thread page lacks the seq cursor:\n%s", out)
	}
	code, out, errOut = f.run(f.authenticated("events", "page", "all")...)
	if code != 0 {
		t.Fatalf("all page = %d, %s, %s", code, out, errOut)
	}
	allHead := f.head()
	if !strings.Contains(out, strconv.FormatInt(allHead, 10)+"\t") {
		t.Fatalf("all page lacks the global_seq cursor %d:\n%s", allHead, out)
	}
}

// TestFollowRefusedRedirectIsFatal proves P1: a redirect answered to the
// stream (or to the resync page after cursor_ahead) ends the follow promptly
// with the transport exit code and the redirect message, never a retry loop,
// and the redirect target is never contacted.
func TestFollowRefusedRedirectIsFatal(t *testing.T) {
	var backendHits atomic.Int32
	backend := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		backendHits.Add(1)
	}))
	t.Cleanup(backend.Close)

	for _, status := range []int{301, 302, 307, 308} {
		for _, viaResync := range []bool{false, true} {
			name := strconv.Itoa(status) + map[bool]string{false: "/stream", true: "/resync"}[viaResync]
			t.Run(name, func(t *testing.T) {
				stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if viaResync && strings.HasSuffix(r.URL.Path, "/events") {
						writeCursorAhead(w)
						return
					}
					http.Redirect(w, r, backend.URL+"/v1/health", status)
				}))
				t.Cleanup(stub.Close)

				var stdout, stderr strings.Builder
				done := make(chan int, 1)
				go func() {
					done <- Main([]string{"--server", stub.URL, "--token-file", stubTokenFile(t), "--json",
						"events", "follow", "all"}, &stdout, &stderr, emptyEnv)
				}()
				select {
				case code := <-done:
					if code != exitUnreachable || !strings.Contains(stderr.String(), "redirect") {
						t.Fatalf("follow = %d, %q, want exit %d with the redirect message", code, stderr.String(), exitUnreachable)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("follow kept retrying a refused redirect")
				}
				if hits := backendHits.Load(); hits != 0 {
					t.Fatalf("redirect target was contacted %d times", hits)
				}
			})
		}
	}
}

// writeCursorAhead answers the stream with the 409 cursor_ahead problem.
func writeCursorAhead(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(http.StatusConflict)
	_, _ = w.Write([]byte(`{"type":"about:blank","title":"cursor ahead","status":409,"code":"cursor_ahead"}`))
}

// TestFollowResyncPageSkipsUnknownTypes proves P2: a resync rebuild does not
// print an unknown event type, still advances the cursor past it, and still
// honours --until when the cursor arrives through that page.
func TestFollowResyncPageSkipsUnknownTypes(t *testing.T) {
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/events") {
			writeCursorAhead(w)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"log_epoch":"E2","head":3,"events":[` +
			`{"seq":1,"global_seq":1,"type":"request.accepted"},` +
			`{"seq":2,"global_seq":2,"type":"mystery-type"},` +
			`{"seq":3,"global_seq":3,"type":"request.accepted"}]}`))
	}))
	t.Cleanup(stub.Close)

	var stdout, stderr strings.Builder
	code := Main([]string{"--server", stub.URL, "--token-file", stubTokenFile(t), "--json",
		"events", "follow", "all", "--until", "2"}, &stdout, &stderr, emptyEnv)
	if code != exitOK {
		t.Fatalf("follow = %d, %s, %s", code, stdout.String(), stderr.String())
	}
	got := strings.TrimSpace(stdout.String())
	if strings.Contains(got, "mystery-type") || strings.Count(got, "\n") != 0 || !strings.Contains(got, `"global_seq":1`) {
		t.Fatalf("delivered %q, want only the cursor 1 event (unknown type skipped, --until 2 honoured)", got)
	}
}

// --- Catalogue tie: the CLI's known event types follow the contract (P6) ---

// TestCatalogueEventTypesMatchContract proves P6: the CLI's known event types
// equal the types in contracts/events/catalogue.schema.json, so a type added
// to the catalogue cannot be silently dropped.
func TestCatalogueEventTypesMatchContract(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "contracts", "events", "catalogue.schema.json"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Defs map[string]struct {
			Properties struct {
				Type struct {
					Const string `json:"const"`
				} `json:"type"`
			} `json:"properties"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	inCatalogue := map[string]bool{}
	for _, def := range schema.Defs {
		if name := def.Properties.Type.Const; name != "" {
			inCatalogue[name] = true
		}
	}
	if len(inCatalogue) == 0 {
		t.Fatal("no event types found in the catalogue schema")
	}
	for name := range inCatalogue {
		if !catalogueEventTypes[name] {
			t.Errorf("catalogue type %q is missing from catalogueEventTypes", name)
		}
	}
	for name := range catalogueEventTypes {
		if !inCatalogue[name] {
			t.Errorf("catalogueEventTypes lists %q, which is not in the catalogue", name)
		}
	}
}
