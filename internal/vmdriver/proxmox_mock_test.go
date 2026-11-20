package vmdriver_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	vm "synch/internal/vmdriver"
	"synch/internal/vmdriver/proxmox"
)

const (
	pveBase  = "/api2/json/nodes/n1/qemu/100"
	pveClone = "/api2/json/nodes/n1/qemu/101"
	pveTask  = "UPID:n1:00001234:0000ABCD:12345678:qmstart:101:agent@pve!synch:"
)

// proxmoxFixture serves API2 JSON inventory, QEMU and node task routes.
func proxmoxFixture(t *testing.T, j *testJournal) *fixture {
	t.Helper()
	f := &fixture{base: "100", target: "101"}
	var cloned, checkpoint atomic.Pointer[string]
	var blocked atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "PVEAPIToken=agent@pve!synch=lab-test-token" {
			t.Error("missing Proxmox API token header")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		path := r.URL.Path
		switch {
		case r.Method == http.MethodGet && path == "/api2/json/cluster/resources":
			if r.URL.Query().Get("type") != "vm" {
				t.Error("missing cluster inventory type filter")
			}
			list := []map[string]any{
				{"vmid": 100, "node": "n1", "name": "base", "status": "stopped", "type": "qemu"},
				{"vmid": 102, "node": "n1", "name": "container", "status": "running", "type": "lxc"},
			}
			if cloned.Load() != nil {
				list = append(list, map[string]any{"vmid": 101, "node": "n1", "name": "clone", "status": "stopped", "type": "qemu"})
			}
			writeJSON(t, w, list)
		case r.Method == http.MethodGet && path == "/api2/json/cluster/nextid":
			writeJSON(t, w, "101")
		case r.Method == http.MethodGet && path == "/api2/json/nodes/n1/tasks/"+pveTask+"/status":
			if f.running.Load() {
				writeJSON(t, w, map[string]string{"status": "running"})
			} else {
				writeJSON(t, w, map[string]string{"status": "stopped", "exitstatus": "OK"})
			}
		case r.Method == http.MethodGet && path == pveClone+"/config":
			writeJSON(t, w, map[string]string{"description": loaded(cloned.Load())})
		case r.Method == http.MethodGet && path == pveBase+"/config":
			writeJSON(t, w, map[string]string{"name": "base"})
		case r.Method == http.MethodGet && strings.HasSuffix(path, "/snapshot"):
			list := []any{map[string]any{"name": "current"}}
			if path == pveClone+"/snapshot" && checkpoint.Load() != nil {
				list = append(list, map[string]any{"name": "checkpoint", "description": loaded(checkpoint.Load()), "snaptime": 1700000000})
			}
			writeJSON(t, w, list)
		case r.Method == http.MethodGet && path == pveClone+"/snapshot/checkpoint/config":
			when := 1700000000
			if f.invalidSnapshot.Load() {
				when++
			}
			writeJSON(t, w, map[string]any{"description": loaded(checkpoint.Load()), "snaptime": when})
		case r.Method == http.MethodPost && path == pveBase+"/clone":
			marker := r.FormValue("description")
			checkPersisted(t, j, marker, vm.CreateOp, "clone")
			if r.FormValue("newid") != "101" || r.FormValue("name") != "clone" ||
				r.FormValue("full") != "1" || !strings.Contains(marker, "synch-owner:"+thread) {
				t.Error("invalid QEMU clone form")
			}
			kept := f.stored(marker)
			cloned.Store(&kept)
			if !f.mutate(w) {
				writeJSON(t, w, pveTask)
			}
		case r.Method == http.MethodPost && path == pveClone+"/snapshot":
			marker := r.FormValue("description")
			checkPersisted(t, j, marker, vm.SnapshotOp, f.target)
			if r.FormValue("snapname") != "checkpoint" {
				t.Error("invalid QEMU snapshot form")
			}
			checkpoint.Store(&marker)
			if !f.mutate(w) {
				writeJSON(t, w, pveTask)
			}
		case r.Method == http.MethodPost && (path == pveBase+"/status/start" || path == pveBase+"/status/stop" ||
			path == pveClone+"/status/start" || path == pveClone+"/status/stop" || path == pveClone+"/snapshot/checkpoint/rollback"):
			if f.reject.Load() {
				http.Error(w, "denied lab-test-token", http.StatusForbidden)
				return
			}
			if path == pveBase+"/status/start" && f.enter != nil && blocked.CompareAndSwap(false, true) {
				close(f.enter)
				<-f.release
			}
			if !f.mutate(w) {
				writeJSON(t, w, pveTask)
			}
		case r.Method == http.MethodDelete && path == pveClone:
			cloned.Store(nil)
			if !f.mutate(w) {
				writeJSON(t, w, pveTask)
			}
		default:
			t.Errorf("unexpected Proxmox API call: %s %s", r.Method, path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	driver, err := proxmox.New(map[string]string{"lab": server.URL},
		vm.BrokerFunc(pveCredential), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	f.driver = driver
	return f
}

// pveCredential returns the mock's endpoint-scoped API token.
func pveCredential(_ context.Context, _ string) (vm.Credentials, error) {
	return vm.Credentials{Token: "agent@pve!synch=lab-test-token"}, nil
}

// --- Shared mock response and persistence assertions ---

// loaded returns an optional mock provider description.
func loaded(text *string) string {
	if text == nil {
		return ""
	}
	return *text
}

// writeJSON wraps a value in Proxmox's REST response envelope.
func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"data": value}); err != nil {
		t.Error(err)
	}
}

// checkPersisted makes the mock assert that mutation attribution predates dispatch.
func checkPersisted(t *testing.T, j *testJournal, marker string, op vm.Operation, resource string) {
	t.Helper()
	token, _, _ := strings.Cut(marker, "\n")
	id := strings.TrimPrefix(token, "synch-")
	entry, err := j.Load(t.Context(), id)
	req := entry.Request
	if err != nil || !strings.HasPrefix(token, "synch-") || len(token) != 32 || req.Operation != op ||
		req.Resource != resource || req.Endpoint != "lab" || len(req.ParamsDigest) != 71 ||
		!strings.HasPrefix(req.ParamsDigest, "sha256:") || len(entry.History) != 0 || entry.Task.ID != "" {
		t.Errorf("mutation lacked a persisted request: %+v, %v", req, err)
	}
}
