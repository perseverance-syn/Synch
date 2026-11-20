package stdio

import (
	"context"
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

// Transport is a supervised duplex pipe. Close must unblock reads and writes.
// Wait returns after the supervisor has persisted the process exit.
type Transport interface {
	io.ReadWriteCloser
	Progress()
	Cancel(context.Context) error
	Wait(context.Context) (execdriver.ExitInfo, error)
}

// Launcher spawns a harness with the chosen arguments and raw protocol streams.
type Launcher interface {
	Launch(context.Context, engine.StartRequest, ...string) (Transport, error)
}

// DirectLauncher resolves the operator's binary and starts it on the Direct driver.
type DirectLauncher struct {
	driver execdriver.ExecutionDriver
	path   string
}

// NewDirectLauncher binds one installed executable for probes and sessions.
func NewDirectLauncher(driver execdriver.ExecutionDriver, binary string) (*DirectLauncher, error) {
	if driver == nil || driver.Kind() != execdriver.KindDirect || driver.Describe().Label != "Host access" {
		return nil, errors.New("engine launcher requires Direct Host access")
	}
	path, err := exec.LookPath(binary)
	if err != nil {
		return nil, &engine.HarnessError{Operation: "locate harness executable", Cause: err}
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, &engine.HarnessError{Operation: "resolve harness executable", Cause: err}
	}
	return &DirectLauncher{driver: driver, path: path}, nil
}

// Launch attaches the raw streams at spawn so protocol frames are lossless.
func (l *DirectLauncher) Launch(ctx context.Context, req engine.StartRequest, args ...string) (Transport, error) {
	env, err := processEnv(req.Env)
	if err != nil {
		return nil, err
	}
	proc, err := l.driver.Spawn(ctx, execdriver.ProcSpec{
		ID: eventlog.NewID(), ThreadID: req.ThreadID, Path: l.path,
		Args: args, Dir: req.Workdir, Env: env, ProtocolIO: true,
	})
	if err != nil {
		return nil, fmt.Errorf("spawn harness: %w", err)
	}
	in, out := proc.ProtocolStreams()
	t := &processTransport{Proc: proc, in: in, out: out}
	if in == nil || out == nil {
		return nil, errors.Join(errors.New("missing protocol streams from Direct process"), t.Close(), cancelProcess(t))
	}
	return t, nil
}

// processEnv supplies host runtime and login-location settings and reuses the engine credential rule.
func processEnv(brokered map[string]string) ([]string, error) {
	if err := engine.ValidateEnv(brokered); err != nil {
		return nil, err
	}
	names := []string{"PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "HOME", "USERPROFILE",
		"LOCALAPPDATA", "APPDATA", "PROGRAMDATA", "HOMEDRIVE", "HOMEPATH", "CLAUDE_CONFIG_DIR",
		"XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_CACHE_HOME", "XDG_RUNTIME_DIR",
		"TMPDIR", "TMP", "TEMP", "LANG", "LC_ALL", "LC_CTYPE", "TERM"}
	// A non-nil slice tells the driver to use exactly this environment.
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

// validateEndpoint checks a daemon-supplied HTTP MCP URL before it enters a harness config.
func validateEndpoint(raw string) error {
	if raw == "" {
		return nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.User != nil || parsed.Fragment != "" {
		return errors.New("invalid daemon MCP endpoint")
	}
	return nil
}

// cancelProcess gives supervised cleanup its own deadline independent of callers.
func cancelProcess(t Transport) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return t.Cancel(ctx)
}

type processTransport struct {
	execdriver.Proc
	in  io.WriteCloser
	out io.ReadCloser
}

// Read receives stdout bytes from the supervised process.
func (t *processTransport) Read(p []byte) (int, error) { return t.out.Read(p) }

// Write sends stdin bytes to the supervised process.
func (t *processTransport) Write(p []byte) (int, error) { return t.in.Write(p) }

// Close releases both caller-owned handles while the supervisor owns termination.
func (t *processTransport) Close() error {
	var inErr, outErr error
	if t.in != nil {
		inErr = t.in.Close()
	}
	if t.out != nil {
		outErr = t.out.Close()
	}
	return errors.Join(inErr, outErr)
}
