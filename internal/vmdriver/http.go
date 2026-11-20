package vmdriver

import (
	"context"
	"net/http"
	"net/url"
	"strings"
)

// Credentials are passed only to the provider transport. The credential broker
// associates each registered endpoint with its own authorized credential.
type Credentials struct{ Username, Password, Token string }

type Broker interface {
	Credentials(context.Context, string) (Credentials, error)
}

// BrokerFunc adapts a broker lookup without exposing the underlying secret store.
type BrokerFunc func(context.Context, string) (Credentials, error)

// Credentials calls the injected lookup, which may return a secret-bearing error.
func (f BrokerFunc) Credentials(ctx context.Context, endpoint string) (Credentials, error) {
	return f(ctx, endpoint)
}

// EndpointURL validates and copies an administrator-configured HTTPS origin.
func EndpointURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, ErrInvalid
	}
	return u, nil
}

// HTTPClient copies provider transport settings and prevents credential forwarding
// or accidental mutation replay across redirects.
func HTTPClient(client *http.Client) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	return &http.Client{Transport: client.Transport, Timeout: client.Timeout,
		CheckRedirect: refuseRedirect}
}

// refuseRedirect leaves an unexpected redirect for the adapter to classify.
func refuseRedirect(_ *http.Request, _ []*http.Request) error {
	return http.ErrUseLastResponse
}

// CarriesOwner matches the exact owner line for thread, never a substring.
func CarriesOwner(description, thread string) bool {
	return thread != "" && CarriesToken(description, OwnerLine(thread))
}

// CarriesToken matches a complete description line, never a substring.
func CarriesToken(description, token string) bool {
	for line := range strings.SplitSeq(description, "\n") {
		if strings.TrimSuffix(line, "\r") == token {
			return true
		}
	}
	return false
}
