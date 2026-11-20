package proxmox

import (
	"context"
	"encoding/json"

	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"synch/internal/vmdriver"
)

// call uses the broker token against the registered origin. It never propagates
// provider bodies, transport errors or broker errors into diagnostics.
func (d *Driver) call(ctx context.Context, endpoint, method, path string, form url.Values, out any) error {
	origin := d.endpoints[endpoint]
	if origin == nil {
		return vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	creds, err := d.broker.Credentials(ctx, endpoint)
	if err != nil || creds.Token == "" {
		return vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	req, err := http.NewRequestWithContext(ctx, method, origin.Scheme+"://"+origin.Host+"/api2/json"+path,
		strings.NewReader(form.Encode()))
	if err != nil {
		return vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	req.Header.Set("Authorization", "PVEAPIToken="+creds.Token)
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}
	// Only GET requests should remain eligible for net/http transport retries.
	if method != http.MethodGet {
		req.GetBody = nil
	}
	res, err := d.client.Do(req)
	if err != nil {
		return vmdriver.ErrUncertain
	}
	defer func() { _ = res.Body.Close() }()
	switch res.StatusCode {
	case http.StatusOK:
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusMethodNotAllowed:
		if method == http.MethodGet {
			return vmdriver.ErrInvalid
		}
		return vmdriver.ErrRejected
	default:
		return vmdriver.ErrUncertain
	}

	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(io.LimitReader(res.Body, 16<<20)).Decode(&envelope); err != nil || len(envelope.Data) == 0 {
		return vmdriver.ErrUncertain
	}
	if strings.Contains(string(envelope.Data), creds.Token) {
		return vmdriver.ErrInvalid
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		// The cluster/nextid API specifies a string even though it is a VMID.
		target, ok := out.(*int)
		if !ok {
			return vmdriver.ErrUncertain
		}
		var str string
		if json.Unmarshal(envelope.Data, &str) == nil {
			*target, err = strconv.Atoi(str)
		}
		if err != nil {
			return vmdriver.ErrUncertain
		}
	}
	return nil
}
