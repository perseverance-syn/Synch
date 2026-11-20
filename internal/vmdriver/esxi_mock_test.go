package vmdriver_test

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	vm "synch/internal/vmdriver"
	"synch/internal/vmdriver/esxi"
)

type soapCall struct {
	XMLName     xml.Name
	Target      string   `xml:"_this"`
	Obj         string   `xml:"specSet>objectSet>obj"`
	Paths       []string `xml:"specSet>propSet>pathSet"`
	Name        string   `xml:"name"`
	Annotation  string   `xml:"spec>config>annotation"`
	Description string   `xml:"description"`
	User        string   `xml:"userName"`
	Password    string   `xml:"password"`
	Folder      string   `xml:"folder"`
	Host        string   `xml:"spec>location>host"`
	Pool        string   `xml:"spec>location>pool"`
}

type soapState struct {
	cloned, checkpoint atomic.Pointer[string]
	blocked            atomic.Bool
}

// esxiFixture serves the vSphere SOAP ServiceInstance, login, properties and tasks.
func esxiFixture(t *testing.T, j *testJournal) *fixture {
	t.Helper()
	f := &fixture{base: "vm-100", target: "vm-101"}
	s := &soapState{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Body struct {
				Call soapCall `xml:",any"`
			} `xml:"Body"`
		}
		if r.URL.Path != "/sdk" || r.Method != http.MethodPost || r.Header.Get("SOAPAction") != `"urn:vim25/8.0.0.0"` ||
			xml.NewDecoder(r.Body).Decode(&request) != nil {
			t.Error("invalid vSphere SOAP request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		call := request.Body.Call
		method := call.XMLName.Local
		if method != "RetrieveServiceContent" && method != "Login" {
			if cookie, err := r.Cookie("vmware_soap_session"); err != nil || cookie.Value != "lab-session" {
				t.Error("missing vSphere session cookie")
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}
		var answer string
		switch method {
		case "RetrieveServiceContent":
			answer = "<returnval>" + mor("rootFolder", "Folder", "group-root") + mor("sessionManager", "SessionManager", "session-1") +
				mor("viewManager", "ViewManager", "view-manager") + mor("propertyCollector", "PropertyCollector", "collector") + "</returnval>"
		case "Login":
			if call.Target != "session-1" || call.User != "lab-user" || call.Password != "test<&password" {
				t.Error("invalid login parameters")
			}
			http.SetCookie(w, &http.Cookie{Name: "vmware_soap_session", Value: "lab-session", Path: "/", HttpOnly: true, Secure: true})
		case "Logout", "DestroyView":
		case "CreateContainerView":
			answer = mor("returnval", "ContainerView", "view-vms")
		case "RetrievePropertiesEx":
			answer = "<returnval>" + s.properties(t, f, call) + "</returnval>"
		case "ContinueRetrievePropertiesEx":
			answer = "<returnval>" + objectProperties("VirtualMachine", f.target, property("name", "clone")+
				property("runtime.powerState", "poweredOff")+property("config.annotation", escapeXML(loaded(s.cloned.Load())))+
				property("snapshot", s.tree(f))) + "</returnval>"
		case "CloneVM_Task":
			checkPersisted(t, j, call.Annotation, vm.CreateOp, "clone")
			if call.Target != f.base || call.Name != "clone" || call.Folder != "group-root" ||
				call.Host != "host-1" || call.Pool != "resgroup-1" || !strings.Contains(call.Annotation, "synch-owner:"+thread) {
				t.Error("invalid vSphere clone parameters")
			}
			kept := f.stored(call.Annotation)
			s.cloned.Store(&kept)
			if f.mutate(w) {
				return
			}
			answer = mor("returnval", "Task", "task-1")
		case "CreateSnapshot_Task":
			checkPersisted(t, j, call.Description, vm.SnapshotOp, f.target)
			if call.Target != f.target || call.Name != "checkpoint" {
				t.Error("invalid snapshot parameters")
			}
			s.checkpoint.Store(&call.Description)
			if f.mutate(w) {
				return
			}
			answer = mor("returnval", "Task", "task-2")
		case "PowerOnVM_Task", "PowerOffVM_Task", "Destroy_Task", "RevertToSnapshot_Task":
			if f.reject.Load() {
				writeSOAPFault(t, w)
				return
			}
			switch method {
			case "PowerOnVM_Task":
				if call.Target == f.base && f.enter != nil && s.blocked.CompareAndSwap(false, true) {
					close(f.enter)
					<-f.release
				}
			case "Destroy_Task":
				s.cloned.Store(nil)
			case "RevertToSnapshot_Task":
				if call.Target != "snapshot-1" {
					t.Error("restore did not use verified snapshot object")
				}
			}
			if f.mutate(w) {
				return
			}
			answer = mor("returnval", "Task", "task-2")
		default:
			t.Errorf("unexpected SOAP method: %s", method)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/xml")
		_, err := fmt.Fprintf(w, `<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><%sResponse xmlns="urn:vim25">%s</%sResponse></soapenv:Body></soapenv:Envelope>`,
			method, answer, method)
		if err != nil {
			t.Error(err)
		}
	}))
	t.Cleanup(server.Close)
	driver, err := esxi.New(map[string]esxi.Endpoint{"lab": {URL: server.URL, Folder: "group-root", Host: "host-1", Pool: "resgroup-1"}},
		vm.BrokerFunc(esxiCredential), server.Client())
	if err != nil {
		t.Fatal(err)
	}
	f.driver = driver
	return f
}

// writeSOAPFault returns a provider fault with intentionally sensitive diagnostic text.
func writeSOAPFault(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	w.WriteHeader(http.StatusInternalServerError)
	_, err := fmt.Fprint(w, `<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><soapenv:Fault><faultcode>ServerFaultCode</faultcode><faultstring>denied test&lt;&amp;password</faultstring><detail><NoPermissionFault xmlns="urn:vim25"/></detail></soapenv:Fault></soapenv:Body></soapenv:Envelope>`)
	if err != nil {
		t.Error(err)
	}
}

// esxiCredential supplies the mock vSphere login, including XML-sensitive bytes.
func esxiCredential(_ context.Context, _ string) (vm.Credentials, error) {
	return vm.Credentials{Username: "lab-user", Password: "test<&password"}, nil
}

// --- vSphere PropertyCollector objects and snapshot data ---

// properties returns typed objects for the requested view or managed reference.
func (s *soapState) properties(t *testing.T, f *fixture, call soapCall) string {
	t.Helper()
	if call.Target != "collector" {
		t.Error("wrong property collector reference")
	}
	switch call.Obj {
	case "view-vms":
		values := objectProperties("VirtualMachine", f.base, property("name", "base")+property("runtime.powerState", "poweredOff"))
		if s.cloned.Load() != nil {
			values += "<token>next-vm</token>"
		}
		return values
	case "task-1", "task-2":
		state := "success"
		if f.running.Load() {
			state = "running"
		}
		info := "<state>" + state + "</state>"
		if call.Obj == "task-1" {
			info += mor("result", "VirtualMachine", f.target)
		}
		return objectProperties("Task", call.Obj, property("info", info))
	case "snapshot-1":
		if !slices.Contains(call.Paths, "config.name") || !slices.Contains(call.Paths, "vm") {
			t.Error("missing independent snapshot verification properties")
		}
		owner := f.target
		if f.invalidSnapshot.Load() {
			owner = f.base
		}
		return objectProperties("VirtualMachineSnapshot", call.Obj,
			"<propSet><name>vm</name>"+mor("val", "VirtualMachine", owner)+"</propSet>"+property("config.name", "clone"))
	case f.target:
		return objectProperties("VirtualMachine", call.Obj, property("snapshot", s.tree(f)))
	default:
		t.Errorf("unexpected vSphere managed object: %s", call.Obj)
		return ""
	}
}

// tree renders the snapshot list whose object is checked separately during restore.
func (s *soapState) tree(f *fixture) string {
	if s.checkpoint.Load() == nil {
		return ""
	}
	return "<rootSnapshotList>" + mor("snapshot", "VirtualMachineSnapshot", "snapshot-1") +
		mor("vm", "VirtualMachine", f.target) + "<name>checkpoint</name><description>" + escapeXML(loaded(s.checkpoint.Load())) +
		"</description><createTime>2026-11-18T00:00:00Z</createTime><state>poweredOff</state><quiesced>false</quiesced></rootSnapshotList>"
}

// mor renders a ManagedObjectReference.
func mor(tag, kind, id string) string {
	return "<" + tag + " type=\"" + kind + "\">" + id + "</" + tag + ">"
}

// property renders a DynamicProperty value.
func property(name, value string) string {
	return "<propSet><name>" + name + "</name><val>" + value + "</val></propSet>"
}

// objectProperties wraps the properties for the returned object.
func objectProperties(kind, id, fields string) string {
	return "<objects>" + mor("obj", kind, id) + fields + "</objects>"
}

// escapeXML safely reflects an annotation or token in a SOAP response.
func escapeXML(value string) string {
	var out strings.Builder
	_ = xml.EscapeText(&out, []byte(value))
	return out.String()
}
