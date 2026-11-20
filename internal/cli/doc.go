// Package cli implements the operator CLI as a client of the local Synch HTTP
// API (dec. 22: both clients call the same authenticated API). Main(args,
// stdout, stderr, env) runs one invocation and returns the process exit code.
//
// Server and token resolution: --server, then SYNCH_SERVER, then the config
// file written by setup, then the default http://127.0.0.1:7420. The token
// comes from --token-file, then SYNCH_TOKEN, then the token file recorded in
// the config file. The token is only sent as a bearer credential and is never
// printed. Non-loopback servers are refused unless --allow-remote (dec. 16:
// remote access goes through SSH tunnels).
//
// Every mutating call sends a Synch-Request-Id (generated, or --request-id to
// retry an earlier one per receipts.md) and prints the receipt id and whether
// the server replayed a stored response. problem+json errors are printed with
// their code. events follow is an SSE client implementing the streamEvents
// resume, gap, overflow and resync rules.
//
// Exit codes: 0 success; 1 local usage, configuration or I/O error; 2 the
// server answered problem+json (including stored rejections and 409
// request_id_conflict); 3 the server could not be reached.
package cli
