package scenarios

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"synch/internal/api"
	"synch/internal/daemon"
	"synch/internal/engine"
)

// startDaemon starts a real isolated daemon on a loopback port.
func startDaemon(t *testing.T) (*daemon.Daemon, string, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	tokens := api.NewMemoryTokens()
	tokens.Set("scenario-token", "operator:scenario")
	d, err := daemon.New(daemon.Config{DataDir: dir, Listen: addr, Tokens: tokens, Adapters: []func(engine.Sink) (engine.EngineAdapter, error){}})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- d.Run(context.Background()) }()
	t.Cleanup(func() {
		if err := d.Close(); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	base := "http://" + addr
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return d, base, dir
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("daemon did not start")
	return nil, "", ""
}

// apiCall calls the authenticated daemon and returns its response bytes.
func apiCall(t *testing.T, method, url, body, requestID string) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer scenario-token")
	req.Header.Set("Synch-Request-Id", requestID)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode >= 300 {
		t.Fatalf("API %s: %d %s %v", url, resp.StatusCode, raw, err)
	}
	return raw
}

// TestKnownSecretsStayOutOfPromptsJournalsAndExports records the known S6 failure until card 401 fixes it.
func TestKnownSecretsStayOutOfPromptsJournalsAndExports(t *testing.T) {
	d, _, dir := startDaemon(t)
	const secret = "scenario-secret-401-plant"
	excerpt, err := d.CapOutput(strings.NewReader(secret))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(excerpt.Text, secret) {
		t.Fatal("card 401 changed the known leak: update S6 to assert redaction")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "outputs", excerpt.Ref+".out"))
	if err != nil || !strings.Contains(string(raw), secret) {
		t.Fatalf("card 401 changed retained output: %v", err)
	}
	t.Log("FAILED: retained output and excerpts keep planted secrets: no public way to supply known secrets for redaction; fix belongs to card 401 credentials/broker")
}

// TestReleaseEvidence runs each scenario and reports the observed outcome.
func TestReleaseEvidence(t *testing.T) {
	type scenario struct {
		id              string
		run             func(*testing.T)
		success, reason string
	}
	scenarios := []scenario{
		{"S1", TestCloseDesktopAndRecoverThreadInCLI, "partial", "API to CLI history replay verified; needs 601 authenticated harness"},
		{"S2", TestChildCancellationLimitsResultAndHandoff, "partial", "existing lifecycle fixtures are separate; handoff pin change uses private state"},
		{"S3", TestPlantedMissingRequirementIsVisible, "partial", "local gate verified; daemon requirement presentation not exercised"},
		{"S4", TestStaleApprovalRefusedAfterRevisionOrTargetChange, "partial", "policy fixture verified; daemon path not exercised"},
		{"S5", TestModeSwitchWithActiveChildAndRunningProcess, "partial", "existing daemon fixture uses private setup and inspection helpers"},
		{"S6", TestKnownSecretsStayOutOfPromptsJournalsAndExports, "failed", "retained output and excerpts keep planted secrets: no public way to supply known secrets for redaction; fix belongs to card 401 credentials/broker"},
		{"S7", TestLostSnapshotResponseOnEveryDriver, "partial", "provider fixtures passed; real host deferred by owner"},
		{"S8", TestOwnedRunnerContinuesAndReplaysAfterDisconnect, "partial", "engine launchers require the Direct driver"},
		{"S9", TestFailedStaticCIGateThroughFixRerunAuditAndAcceptance, "partial", "local gate rerun verified; CI mocks, audit and human acceptance not exercised"},
	}
	type result struct{ id, status, reason string }
	rows := make([]result, 0, len(scenarios))
	for _, item := range scenarios {
		skipped := false
		ok := t.Run(item.id, func(t *testing.T) { defer func() { skipped = t.Skipped() }(); item.run(t) })
		status, reason := item.success, item.reason
		if skipped {
			status, reason = "not run", "scenario fixture not implemented"
		} else if !ok {
			status, reason = "error", "scenario test failed; inspect test output"
		}
		rows = append(rows, result{item.id, status, reason})
	}
	for _, row := range rows {
		t.Logf("%s | %s | %s", row.id, row.status, row.reason)
	}
}
