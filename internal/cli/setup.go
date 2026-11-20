package cli

import (
	"encoding/json"
	"fmt"
)

// runSetup writes the operator's config file (server URL and token file path,
// never the token) and then checks the daemon with getHealth. Re-running with
// the same inputs rewrites the same bytes, so it is idempotent. Its --config,
// --server and --token-file options are global options stripped by
// parseGlobals before dispatch.
func runSetup(g *globals, env func(string) string, out *printer, args []string) int {
	if err := requireArgs(out, args, 0, "setup"); err != nil {
		return localError(out, err)
	}
	// The recorded token file path survives this run even when the credential
	// for it came from the environment: a token value is never written to the
	// config, and the previously saved path stays usable.
	existing, err := readConfigFile(g.configPath)
	if err != nil {
		return localError(out, err)
	}
	var recordedTokenFile string
	if existing != nil {
		recordedTokenFile = existing.TokenFile
	}
	cfg, err := resolveConfig(g, env)
	if err != nil {
		return localError(out, err)
	}
	file := configFile{Server: cfg.server, TokenFile: firstNonEmpty(g.tokenFile, recordedTokenFile)}
	if err := writeConfigFile(g.configPath, file); err != nil {
		return localError(out, err)
	}
	path := g.configPath
	if path == "" {
		if path, err = defaultConfigPath(); err != nil {
			return localError(out, err)
		}
	}
	// The health check proves the recorded settings reach the daemon.
	c := newClient(cfg)
	ctx, cancel := callContext()
	defer cancel()
	resp, err := c.get(ctx, "/v1/health")
	if err != nil {
		return unreachable(out, err)
	}
	if !resp.ok() {
		return serverError(out, resp)
	}
	if g.json {
		// Exactly one JSON document holding the setup and health results.
		var health map[string]any
		_ = json.Unmarshal(resp.body, &health)
		if err := out.rawBody(&response{body: mustJSON(map[string]any{
			"config_path": path, "server": cfg.server, "health": health,
		})}); err != nil {
			return localError(out, err)
		}
		return exitOK
	}
	if _, err := fmt.Fprintf(out.stdout, "config written to %s (server %s)\n", path, cfg.server); err != nil {
		return localError(out, err)
	}
	return printResult(out, resp)
}

// mustJSON encodes a small literal that cannot fail.
func mustJSON(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		return []byte("{}")
	}
	return raw
}
