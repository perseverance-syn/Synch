// Package claudecode implements engine-adapter sections 1–4 for Claude Code
// stream JSON. It owns protocol translation and forwards permissions to Synch's
// policy decider. The supervisor owns process lifetime, and the operator owns login.
package claudecode
