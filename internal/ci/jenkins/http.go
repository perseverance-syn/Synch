package jenkins

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"synch/internal/ci"
)

// Config configures one Jenkins controller and its credential broker.
type Config struct {
	BaseURL      string
	Endpoint     string // API token key; the username is stored at Endpoint + "/user".
	Broker       ci.TokenBroker
	Client       *http.Client
	RequireCrumb bool // Acquire a fresh crumb and session before each mutation.
}

// Provider implements ci.Provider for a Jenkins Pipeline job.
type Provider struct {
	config Config
	client *http.Client
	mu     sync.Mutex
	// Jenkins may echo a previous session's secrets in a build log.
	issued map[string]struct{}
}

var _ ci.Provider = (*Provider)(nil)

// New configures a client that never forwards credentials across redirects.
func New(c Config) *Provider {
	client := http.Client{Timeout: 30 * time.Second}
	if c.Client != nil {
		client.Transport = c.Client.Transport
		client.Timeout = c.Client.Timeout
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Provider{config: c, client: &client, issued: make(map[string]struct{})}
}

// Name returns the Jenkins driver name used by the dispatcher.
func (p *Provider) Name() string { return "jenkins" }

// --- Credentials and request boundaries (ci-driver.md section 6) ---

// secrets snapshots known values and credentials issued during this provider's lifetime.
func (p *Provider) secrets() []string {
	secrets := slices.Clone(p.config.Broker.KnownValues())
	p.mu.Lock()
	defer p.mu.Unlock()
	for secret := range p.issued {
		secrets = append(secrets, secret)
	}
	return secrets
}

// remember adds issued values before any response can be exposed.
func (p *Provider) remember(values ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, value := range values {
		if value != "" {
			p.issued[value] = struct{}{}
		}
	}
}

// credentials reads Basic authentication credentials from the broker at use.
func (p *Provider) credentials(ctx context.Context) (string, string, error) {
	user, err := p.config.Broker.Token(ctx, p.config.Endpoint+"/user")
	if err != nil || user == "" || strings.Contains(user, ":") {
		return "", "", fmt.Errorf("%w: Jenkins username unavailable", ci.ErrRejected)
	}
	p.remember(user)
	token, err := p.config.Broker.Token(ctx, p.config.Endpoint)
	if err != nil || token == "" {
		return "", "", fmt.Errorf("%w: Jenkins API token unavailable", ci.ErrRejected)
	}
	p.remember(token, base64.StdEncoding.EncodeToString([]byte(user+":"+token)))
	return user, token, nil
}

// request applies auth and optional session-bound CSRF credentials to one request.
func (p *Provider) request(ctx context.Context, method, path string, form url.Values) (*http.Response, error) {
	user, token, err := p.credentials(ctx)
	if err != nil {
		return nil, err
	}
	req, err := p.newRequest(ctx, method, path, form, user, token)
	if err != nil {
		return nil, err
	}
	if method == http.MethodPost && p.config.RequireCrumb {
		if err := p.applyCrumb(ctx, req, user, token); err != nil {
			return nil, err
		}
	}
	return p.execute(req)
}

// newRequest builds an authenticated request from a configured controller and a local path.
func (p *Provider) newRequest(ctx context.Context, method, path string, form url.Values, user, token string) (*http.Request, error) {
	base, err := url.Parse(p.config.BaseURL)
	if err != nil || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" ||
		(base.Scheme != "http" && base.Scheme != "https") {
		return nil, fmt.Errorf("%w: invalid Jenkins controller URL", ci.ErrInvalid)
	}
	var body io.Reader
	if form != nil {
		body = strings.NewReader(form.Encode())
	}
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base.String(), "/")+path, body)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid Jenkins request URL", ci.ErrInvalid)
	}
	req.SetBasicAuth(user, token)
	if form != nil {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	return req, nil
}

// applyCrumb installs a fresh issuer header and its matching session cookie.
func (p *Provider) applyCrumb(ctx context.Context, mutation *http.Request, user, token string) error {
	req, err := p.newRequest(ctx, http.MethodGet, "/crumbIssuer/api/json", nil, user, token)
	if err != nil {
		return err
	}
	resp, err := p.execute(req)
	if err != nil {
		return fmt.Errorf("%w: Jenkins crumb issuer unavailable", ci.ErrRejected)
	}
	defer func() { _ = resp.Body.Close() }()
	var crumb struct {
		Field string `json:"crumbRequestField"`
		Value string `json:"crumb"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&crumb) != nil ||
		(!strings.EqualFold(crumb.Field, "Jenkins-Crumb") && crumb.Field != ".crumb") ||
		crumb.Value == "" || strings.ContainsAny(crumb.Value, "\r\n") {
		return fmt.Errorf("%w: invalid Jenkins crumb issuer response", ci.ErrRejected)
	}
	p.remember(crumb.Value)
	mutation.Header.Set(crumb.Field, crumb.Value)
	for _, cookie := range resp.Cookies() {
		if strings.HasPrefix(cookie.Name, "JSESSIONID") {
			p.remember(cookie.Value)
			mutation.AddCookie(cookie)
		}
	}
	return nil
}

// --- Send and decode bounded responses ---

// execute sends once and keeps transport diagnostics free of remote values.
func (p *Provider) execute(req *http.Request) (*http.Response, error) {
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: Jenkins HTTP transport failed", ci.ErrTransient)
	}
	return resp, nil
}

// responseError classifies status without including untrusted response text.
func responseError(status int) error {
	kind := ci.ErrTransient
	if status/100 == 4 && status != http.StatusRequestTimeout {
		kind = ci.ErrRejected
	}
	return httpFailure{code: status, kind: kind}
}

// readJSON reads one bounded response and omits server-controlled decode diagnostics.
func readJSON(resp *http.Response, dst any, limit int64) error {
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil || int64(len(data)) > limit || json.Unmarshal(data, dst) != nil {
		return fmt.Errorf("%w: unreadable Jenkins JSON response", ci.ErrTransient)
	}
	return nil
}

// getJSON reads one Jenkins REST resource into dst.
func (p *Provider) getJSON(ctx context.Context, path string, dst any) error {
	resp, err := p.request(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	return readJSON(resp, dst, 4<<20)
}
