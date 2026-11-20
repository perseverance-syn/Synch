package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// DefaultServerURL is the local daemon address from openapi.yaml.
const DefaultServerURL = "http://127.0.0.1:7420"

// config holds the resolved connection settings for one invocation.
type config struct {
	server    string
	token     string
	tokenFile string
}

// configFile is the on-disk shape written by setup. It never holds the token.
type configFile struct {
	Server    string `json:"server"`
	TokenFile string `json:"token_file"`
}

// resolveConfig applies the precedence rules from the package comment: flags,
// then environment, then the config file, then defaults. The token is read
// from the winning token file or SYNCH_TOKEN and is never returned anywhere
// except inside config.token.
func resolveConfig(g *globals, env func(string) string) (config, error) {
	cfg := config{}
	file, err := readConfigFile(g.configPath)
	if err != nil {
		return cfg, err
	}
	var fileServer, fileTokenFile string
	if file != nil {
		fileServer, fileTokenFile = file.Server, file.TokenFile
	}
	cfg.server = firstNonEmpty(g.server, env("SYNCH_SERVER"), fileServer, DefaultServerURL)
	var tokenFile string
	switch {
	case g.tokenFile != "":
		tokenFile = g.tokenFile
	case env("SYNCH_TOKEN") != "":
		cfg.token = env("SYNCH_TOKEN")
	default:
		tokenFile = fileTokenFile
	}
	if cfg.token == "" && tokenFile != "" {
		raw, err := os.ReadFile(tokenFile)
		if err != nil {
			return cfg, fmt.Errorf("read token file: %w", err)
		}
		cfg.token = strings.TrimSpace(string(raw))
	}
	if cfg.token == "" {
		return cfg, errors.New("no caller token: set SYNCH_TOKEN or pass --token-file")
	}
	cfg.tokenFile = tokenFile
	if err := checkServerURL(cfg.server, g.allowRemote); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// firstNonEmpty returns the first value that is not empty.
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// defaultConfigPath returns the OS user config location used by setup.
func defaultConfigPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config directory: %w", err)
	}
	return filepath.Join(dir, "synch", "config.json"), nil
}

// readConfigFile loads a config file, returning nil when the path is empty or
// the file does not exist. Unknown members are ignored for forward compatibility.
func readConfigFile(path string) (*configFile, error) {
	if path == "" {
		var err error
		if path, err = defaultConfigPath(); err != nil {
			return nil, nil
		}
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var file configFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("parse config file %s: %w", path, err)
	}
	return &file, nil
}

// writeConfigFile stores the server URL and token file path without the token.
func writeConfigFile(path string, file configFile) error {
	if path == "" {
		var err error
		if path, err = defaultConfigPath(); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("prepare config directory: %w", err)
	}
	raw, err := json.Marshal(file)
	if err != nil {
		return err
	}
	// Replace through a temporary file instead of os.WriteFile: a replaced
	// file keeps its existing mode and (on Windows) its explicit ACEs, while
	// the fresh temporary file is created owner-only (0600). On Windows the
	// standard library cannot edit ACLs; the replacement escapes explicit
	// ACEs of the replaced file, but ACEs inherited from the directory are
	// inherited by the replacement as well.
	temp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("write config file: %w", err)
	}
	defer func() { _ = os.Remove(temp.Name()) }()
	if _, err := temp.Write(raw); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write config file: %w", err)
	}
	if err := temp.Chmod(0600); err != nil {
		_ = temp.Close()
		return fmt.Errorf("write config file: %w", err)
	}
	if err := temp.Close(); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		// On Windows a read-only target blocks the replacement; clearing the
		// mode bits removes the read-only attribute (the standard library
		// cannot edit ACLs), and the replacement is attempted once more. Other
		// systems return the error: a chmod there would widen the file mode.
		if configGOOS == "windows" && os.Chmod(path, 0666) == nil {
			if renameErr := os.Rename(temp.Name(), path); renameErr == nil {
				return nil
			}
		}
		return fmt.Errorf("replace config file: %w", err)
	}
	return nil
}

// configGOOS is the system the chmod-and-retry fallback is allowed on; tests
// replace it to prove other systems never widen the file mode.
var configGOOS = runtime.GOOS

// checkServerURL refuses servers a caller may not address directly. Remote
// access goes through SSH tunnels (dec. 16), so only loopback spellings are
// accepted unless the operator passes --allow-remote.
func checkServerURL(raw string, allowRemote bool) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Hostname() == "" {
		return fmt.Errorf("invalid server URL %q", raw)
	}
	if allowRemote {
		return nil
	}
	switch strings.ToLower(parsed.Hostname()) {
	case "127.0.0.1", "::1", "localhost":
		return nil
	default:
		return fmt.Errorf("server %q is not loopback; remote access goes through an SSH tunnel (pass --allow-remote to override)", raw)
	}
}
