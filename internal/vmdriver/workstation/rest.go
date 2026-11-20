package workstation

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"

	"synch/internal/vmdriver"
)

// annotate writes the ownership marker into the VM's .vmx `annotation` via
// PUT /api/vms/{id}/configparams {name, value}.
func (d *Driver) annotate(ctx context.Context, endpoint, id, marker string) error {
	body := map[string]string{"name": "annotation", "value": marker}
	return d.call(ctx, endpoint, http.MethodPut, "/vms/"+url.PathEscape(id)+"/configparams", body, nil)
}

// annotation reads the `annotation` param via GET /api/vms/{id}/params/annotation.
func (d *Driver) annotation(ctx context.Context, endpoint, id string) (string, error) {
	var p struct {
		Value string `json:"value"`
	}
	err := d.call(ctx, endpoint, http.MethodGet, "/vms/"+url.PathEscape(id)+"/params/annotation", nil, &p)
	return p.Value, err
}

// call sends one vmrest request with HTTP basic auth from the broker. It never
// puts provider bodies, transport errors or broker errors into diagnostics, and
// refuses any response that echoes the password.
func (d *Driver) call(ctx context.Context, endpoint, method, path string, body, out any) error {
	origin := d.rest[endpoint]
	if origin == nil {
		return vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	creds, err := d.broker.Credentials(ctx, endpoint)
	if err != nil || creds.Username == "" || creds.Password == "" {
		return vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	var payload io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return vmdriver.PreDispatch(vmdriver.ErrInvalid)
		}
		payload = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, origin.Scheme+"://"+origin.Host+"/api"+path, payload)
	if err != nil {
		return vmdriver.PreDispatch(vmdriver.ErrInvalid)
	}
	req.SetBasicAuth(creds.Username, creds.Password)
	req.Header.Set("Accept", "application/vnd.vmware.vmw.rest-v1+json")
	if body != nil {
		req.Header.Set("Content-Type", "application/vnd.vmware.vmw.rest-v1+json")
	}
	// Only GET requests stay eligible for net/http transport retries.
	if method != http.MethodGet {
		req.GetBody = nil
	}
	res, err := d.client.Do(req)
	if err != nil {
		return vmdriver.ErrUncertain
	}
	defer func() { _ = res.Body.Close() }()
	switch res.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusNoContent:
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound, http.StatusConflict:
		if method == http.MethodGet {
			return vmdriver.ErrInvalid
		}
		return vmdriver.ErrRejected
	default:
		return vmdriver.ErrUncertain
	}
	raw, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if err != nil || strings.Contains(string(raw), creds.Password) {
		return vmdriver.ErrInvalid
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return vmdriver.ErrUncertain
	}
	return nil
}
