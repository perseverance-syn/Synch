package ci

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// --- Redaction, log capping and retention by reference (ci-driver.md §6, §7) ---

// MaxRetainedLog is the hard limit on retained log bytes.
const MaxRetainedLog = 32 << 20

// Redacted replaces every redacted secret.
const Redacted = "[REDACTED]"

// Redact removes every known secret value from s. Longer secrets are replaced
// first so a secret that contains another is removed whole.
func Redact(s string, secrets []string) string {
	vals := make([]string, 0, len(secrets))
	for _, v := range secrets {
		if v != "" {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		return s
	}
	sort.Slice(vals, func(i, j int) bool { return len(vals[i]) > len(vals[j]) })
	args := make([]string, 0, 2*len(vals))
	for _, v := range vals {
		args = append(args, v, Redacted)
	}
	return strings.NewReplacer(args...).Replace(s)
}

// LogStore retains full logs; the daemon supplies the implementation.
type LogStore interface {
	// Put stores data and returns a reference and its sha256 digest.
	Put(ctx context.Context, name string, data []byte) (ref, digest string, err error)
}

// LogResult is a capped excerpt plus the reference to the full redacted log.
type LogResult struct {
	Excerpt           string
	Truncated         bool // the excerpt is shorter than the retained log
	Ref, Digest       string
	RetainedTruncated bool // the retained log was cut at MaxRetainedLog
}

// FetchLog fetches one job log, redacts it with the known values of the
// broker, retains the full redacted log in store and returns the last
// capBytes bytes. Redaction happens before retention and before capping so no
// stored or returned byte can hold a known secret.
func FetchLog(ctx context.Context, p Provider, broker TokenBroker, store LogStore, run RunRef, jobID string, capBytes int) (LogResult, error) {
	secrets := broker.KnownValues()
	raw, err := p.Logs(ctx, run, jobID)
	if err != nil {
		return LogResult{}, fmt.Errorf("ci: fetch log of job %s: %s", jobID, Redact(err.Error(), secrets))
	}
	res := LogResult{}
	if len(raw) > MaxRetainedLog {
		res.RetainedTruncated = true
		longest := 0
		for _, s := range secrets {
			longest = max(longest, len(s))
		}
		// Cut before redacting and drop one byte less than the longest secret so a
		// secret straddling the cut cannot survive as a partial prefix.
		raw = raw[:max(0, MaxRetainedLog-max(0, longest-1))]
	}
	full := Redact(string(raw), secrets)
	res.Ref, res.Digest, err = store.Put(ctx, "ci-log-"+run.ID+"-"+jobID, []byte(full))
	if err != nil {
		return LogResult{}, fmt.Errorf("ci: retain log of job %s: %s", jobID, Redact(err.Error(), secrets))
	}
	res.Excerpt = full
	if len(full) > capBytes {
		res.Excerpt, res.Truncated = full[len(full)-capBytes:], true
	}
	return res, nil
}
