package jenkins

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"synch/internal/ci"
)

// Logs returns a bounded, redacted log for a stage or the build console.
func (p *Provider) Logs(ctx context.Context, run ci.RunRef, jobID string) ([]byte, error) {
	raw, err := p.rawLog(ctx, run, jobID)
	if err != nil {
		return nil, err
	}
	secrets := p.secrets()
	if len(raw) > ci.MaxRetainedLog {
		longest := 1
		for _, secret := range secrets {
			longest = max(longest, len(secret))
		}
		// Drop a possible partial secret at the read boundary before redacting.
		raw = raw[:max(0, len(raw)-longest+1)]
	}
	redacted := ci.Redact(string(raw), secrets)
	return []byte(redacted[:min(len(redacted), ci.MaxRetainedLog+1)]), nil
}

// FetchLog runs shared retention on raw evidence with Jenkins session secrets
// included, preserving the retention-cut flag even when redaction shortens the log.
func (p *Provider) FetchLog(ctx context.Context, store ci.LogStore, run ci.RunRef, jobID string, capBytes int) (ci.LogResult, error) {
	if capBytes < 0 {
		return ci.LogResult{}, fmt.Errorf("%w: negative Jenkins log excerpt cap", ci.ErrInvalid)
	}
	raw, err := p.rawLog(ctx, run, jobID)
	if err != nil {
		return ci.LogResult{}, err
	}
	evidence := logEvidence{Provider: p, retentionBroker: p.config.Broker, raw: raw, secrets: p.secrets()}
	bounded := &boundedStore{LogStore: store}
	result, err := ci.FetchLog(ctx, evidence, evidence, bounded, run, jobID, min(capBytes, ci.MaxRetainedLog))
	result.RetainedTruncated = result.RetainedTruncated || bounded.cut
	if capBytes >= ci.MaxRetainedLog && bounded.cut {
		result.Truncated = false
	}
	return result, err
}

type retentionBroker = ci.TokenBroker

type logEvidence struct {
	*Provider
	retentionBroker
	raw     []byte
	secrets []string
}

// Logs supplies the already fetched evidence so shared retention cannot re-read a mutable log.
func (l logEvidence) Logs(context.Context, ci.RunRef, string) ([]byte, error) { return l.raw, nil }

// KnownValues returns the broker values and CSRF session values used by Jenkins.
func (l logEvidence) KnownValues() []string { return l.secrets }

type boundedStore struct {
	ci.LogStore
	cut bool
}

// Put caps the already redacted payload in case redaction expanded the log.
func (s *boundedStore) Put(ctx context.Context, name string, data []byte) (string, string, error) {
	s.cut = len(data) > ci.MaxRetainedLog
	return s.LogStore.Put(ctx, name, data[max(0, len(data)-ci.MaxRetainedLog):])
}

// rawLog reads a stage's Pipeline log or the explicitly requested build console.
func (p *Provider) rawLog(ctx context.Context, run ci.RunRef, jobID string) ([]byte, error) {
	if !numberID.MatchString(jobID) && jobID != "console" {
		return nil, fmt.Errorf("%w: invalid Jenkins stage ID", ci.ErrInvalid)
	}
	job, id, pending, err := p.resolve(ctx, run)
	if err != nil {
		return nil, err
	}
	if pending != nil {
		return nil, fmt.Errorf("%w: Jenkins build has no executable yet", ci.ErrTransient)
	}
	path := job + "/" + id
	if jobID == "console" {
		return p.consoleLog(ctx, path+"/consoleText")
	}
	resp, err := p.request(ctx, http.MethodGet, path+"/execution/node/"+jobID+"/wfapi/log", nil)
	if err != nil {
		return nil, err
	}
	var log struct {
		Text    *string
		HasMore bool
	}
	// A JSON byte may occupy six wire bytes when escaped; bound the envelope separately.
	if err := readJSON(resp, &log, 6*(ci.MaxRetainedLog+1)+4096); err != nil {
		return nil, err
	}
	if log.Text == nil || (log.HasMore && len(*log.Text) <= ci.MaxRetainedLog) {
		return nil, fmt.Errorf("%w: incomplete Jenkins stage log", ci.ErrTransient)
	}
	return []byte((*log.Text)[:min(len(*log.Text), ci.MaxRetainedLog+1)]), nil
}

// consoleLog reads consoleText, borrowing one extra byte to signal the retention cut.
func (p *Provider) consoleLog(ctx context.Context, path string) ([]byte, error) {
	resp, err := p.request(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, responseError(resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, ci.MaxRetainedLog+1))
	if err != nil {
		return nil, fmt.Errorf("%w: incomplete Jenkins console log", ci.ErrTransient)
	}
	return raw, nil
}
