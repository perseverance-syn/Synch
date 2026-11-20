package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"synch/internal/engine"
	"synch/internal/eventlog"
	"synch/internal/execdriver"
)

// --- Supervised launcher and version probe (D-01, D-17) ---

// Transport carries raw JSONL and exposes the supervised process lifecycle.
// Close releases both pipes and unblocks concurrent reads and writes.
type Transport interface {
	io.ReadWriteCloser
	Progress()
	Cancel(context.Context) error
	Wait(context.Context) (execdriver.ExitInfo, error)
}

// Launcher supplies version evidence and the App Server transport.
type Launcher interface {
	Version(context.Context, engine.StartRequest) (string, error)
	Launch(context.Context, engine.StartRequest) (Transport, error)
}

// DirectLauncher uses a single installed executable for the version probe and server.
type DirectLauncher struct {
	driver execdriver.ExecutionDriver
	path   string
}

// NewDirectLauncher resolves the operator's binary once and requires Direct Host access.
func NewDirectLauncher(driver execdriver.ExecutionDriver, binary string) (*DirectLauncher, error) {
	if driver == nil || driver.Kind() != execdriver.KindDirect || driver.Describe().Label != "Host access" {
		return nil, errors.New("the Codex launcher requires the Direct Host access driver")
	}
	if binary == "" {
		binary = "codex"
	}
	path, err := exec.LookPath(binary)
	if err != nil {
		return nil, &engine.HarnessError{Operation: "locate Codex", Cause: err}
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, &engine.HarnessError{Operation: "resolve Codex executable", Cause: err}
	}
	return &DirectLauncher{driver: driver, path: path}, nil
}

// Version obtains the CLI version through a bounded, supervised probe.
func (l *DirectLauncher) Version(ctx context.Context, req engine.StartRequest) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	t, err := l.spawn(ctx, req, "--version")
	if err != nil {
		return "", err
	}
	defer func() { _ = t.Close() }()
	type result struct {
		data []byte
		err  error
	}
	read := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(io.LimitReader(t, 1025))
		read <- result{data, err}
	}()
	select {
	case <-ctx.Done():
		return "", errors.Join(ctx.Err(), cancelProcess(t))
	case r := <-read:
		if r.err != nil || len(r.data) > 1024 {
			return "", errors.Join(errors.New("invalid version probe output"), r.err, cancelProcess(t))
		}
		info, err := t.Wait(ctx)
		if err != nil || info.ExecutionOutcome != "exited" || info.ExitCode != 0 || !info.DescendantsCleaned {
			return "", errors.Join(errors.New("failed Codex version probe"), err, cancelProcess(t))
		}
		return strings.TrimSpace(string(r.data)), nil
	}
}

// Launch starts App Server on stdio and configures the daemon-provided MCP endpoint.
func (l *DirectLauncher) Launch(ctx context.Context, req engine.StartRequest) (Transport, error) {
	args := []string{"app-server"}
	if req.MCPEndpoint != "" {
		endpoint, err := url.Parse(req.MCPEndpoint)
		if err != nil || endpoint.Hostname() == "" || (endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
			endpoint.User != nil || endpoint.Fragment != "" {
			return nil, errors.New("invalid Codex MCP endpoint")
		}
		value, err := json.Marshal(req.MCPEndpoint)
		if err != nil {
			return nil, fmt.Errorf("encode Codex MCP endpoint: %w", err)
		}
		args = append(args, "-c", "mcp_servers.synch.url="+string(value), "-c", "mcp_servers.synch.required=true")
	}
	return l.spawn(ctx, req, args...)
}

// spawn attaches raw streams at launch so the engine sees the very first protocol byte.
func (l *DirectLauncher) spawn(ctx context.Context, req engine.StartRequest, args ...string) (Transport, error) {
	env, err := processEnv(req.Env)
	if err != nil {
		return nil, err
	}
	proc, err := l.driver.Spawn(ctx, execdriver.ProcSpec{
		ID: eventlog.NewID(), ThreadID: req.ThreadID, Path: l.path, Args: args,
		Dir: req.Workdir, Env: env, ProtocolIO: true,
	})
	if err != nil {
		return nil, fmt.Errorf("spawn Codex: %w", err)
	}
	in, out := proc.ProtocolStreams()
	t := &processTransport{Proc: proc, WriteCloser: in, ReadCloser: out}
	if in == nil || out == nil {
		return nil, errors.Join(errors.New("missing protocol streams from Direct process"), t.Close(), cancelProcess(t))
	}
	return t, nil
}

// --- Environment and pipe ownership (engine-adapter sections 3 and 4) ---

// processEnv inherits only host runtime settings needed to find the operator's installation and login.
func processEnv(brokered map[string]string) ([]string, error) {
	if err := engine.ValidateEnv(brokered); err != nil {
		return nil, err
	}
	names := []string{"PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "HOME", "USERPROFILE",
		"LOCALAPPDATA", "APPDATA", "PROGRAMDATA", "HOMEDRIVE", "HOMEPATH", "CODEX_HOME",
		"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_RUNTIME_DIR",
		"TMPDIR", "TMP", "TEMP", "LANG", "LC_ALL", "LC_CTYPE", "TERM"}
	env := make([]string, 0, len(names)+len(brokered))
	for _, name := range names {
		if value, ok := os.LookupEnv(name); ok {
			env = append(env, name+"="+value)
		}
	}
	for key, value := range brokered {
		if runtime.GOOS == "windows" {
			key = strings.ToUpper(key)
		}
		env = append(env, key+"="+value)
	}
	return env, nil
}

// cancelProcess gives the supervisor time to reap a failed launch independently of request cancellation.
func cancelProcess(t Transport) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return t.Cancel(ctx)
}

type processTransport struct {
	execdriver.Proc
	io.WriteCloser
	io.ReadCloser
}

// Close releases caller-owned handles. The supervisor still owns process termination.
func (t *processTransport) Close() error {
	var inErr, outErr error
	if t.WriteCloser != nil {
		inErr = t.WriteCloser.Close()
	}
	if t.ReadCloser != nil {
		outErr = t.ReadCloser.Close()
	}
	return errors.Join(inErr, outErr)
}
