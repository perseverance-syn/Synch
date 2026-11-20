package esxi

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"strings"
	"time"

	"synch/internal/vmdriver"
)

type ref struct {
	Type string `xml:"type,attr"`
	ID   string `xml:",chardata"`
}

// valid checks a provider reference against its expected object type.
func (r ref) valid(kind string) bool { return r.Type == kind && managedID.MatchString(r.ID) }

type serviceContent struct {
	Root      ref `xml:"rootFolder"`
	Manager   ref `xml:"sessionManager"`
	View      ref `xml:"viewManager"`
	Collector ref `xml:"propertyCollector"`
}

type session struct {
	driver  *Driver
	url     string
	content serviceContent
	cookie  *http.Cookie
}

// open discovers service references and logs in without exposing broker or SOAP errors.
func (d *Driver) open(ctx context.Context, endpoint string) (*session, error) {
	cfg := d.endpoints[endpoint]
	if cfg.origin == nil {
		return nil, vmdriver.ErrInvalid
	}
	creds, err := d.broker.Credentials(ctx, endpoint)
	if err != nil || creds.Username == "" || creds.Password == "" {
		return nil, vmdriver.ErrInvalid
	}
	s := &session{driver: d, url: cfg.origin.Scheme + "://" + cfg.origin.Host + "/sdk"}
	var content struct {
		Value serviceContent `xml:"returnval"`
	}
	if _, err := s.call(ctx, "RetrieveServiceContent", object("_this", ref{"ServiceInstance", "ServiceInstance"}), &content); err != nil ||
		!content.Value.Manager.valid("SessionManager") || !content.Value.Collector.valid("PropertyCollector") {
		return nil, vmdriver.ErrInvalid
	}
	s.content = content.Value
	auth := object("_this", s.content.Manager) + tag("userName", creds.Username) + tag("password", creds.Password)
	cookies, err := s.call(ctx, "Login", auth, nil)
	if err != nil {
		return nil, vmdriver.ErrInvalid
	}
	for _, cookie := range cookies {
		if cookie.Name == "vmware_soap_session" && cookie.Value != "" {
			s.cookie = cookie
			return s, nil
		}
	}
	return nil, vmdriver.ErrInvalid
}

// close logs out even if the caller canceled the operation.
func (s *session) close(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	_, _ = s.call(ctx, "Logout", object("_this", s.content.Manager), nil)
}

// escaped encodes user-supplied text for XML.
func escaped(value string) string {
	var output strings.Builder
	_ = xml.EscapeText(&output, []byte(value))
	return output.String()
}

// tag writes a simple SOAP field.
func tag(name, value string) string {
	return "<" + name + ">" + escaped(value) + "</" + name + ">"
}

// object writes a typed vSphere managed object reference.
func object(name string, value ref) string {
	return "<" + name + " type=\"" + escaped(value.Type) + "\">" + escaped(value.ID) + "</" + name + ">"
}

// --- SOAP invocation and fault classification (vm-driver §3) ---

// call invokes one SOAP method and retains only safe error classifications.
func (s *session) call(ctx context.Context, method, args string, out any) ([]*http.Cookie, error) {
	body := `<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soapenv:Body><` +
		method + ` xmlns="urn:vim25">` + args + "</" + method + "></soapenv:Body></soapenv:Envelope>"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(body))
	if err != nil {
		return nil, vmdriver.ErrInvalid
	}
	req.Header.Set("Content-Type", "text/xml; charset=utf-8")
	req.Header.Set("SOAPAction", `"urn:vim25/8.0.0.0"`)
	req.GetBody = nil
	if s.cookie != nil {
		req.AddCookie(s.cookie)
	}

	response, err := s.driver.client.Do(req)
	if err != nil {
		return nil, vmdriver.ErrUncertain
	}
	defer func() { _ = response.Body.Close() }()
	var envelope struct {
		Payload struct {
			XML string `xml:",innerxml"`
		} `xml:"Body"`
	}
	if xml.NewDecoder(io.LimitReader(response.Body, 16<<20)).Decode(&envelope) != nil {
		return nil, vmdriver.ErrUncertain
	}
	data := []byte(envelope.Payload.XML)
	var header struct {
		XMLName xml.Name
		Detail  struct {
			Fault struct{ XMLName xml.Name } `xml:",any"`
		} `xml:"detail"`
	}
	if xml.Unmarshal(data, &header) != nil {
		return nil, vmdriver.ErrUncertain
	}
	if header.XMLName.Local == "Fault" {
		return nil, faultOutcome(header.Detail.Fault.XMLName.Local)
	}
	if response.StatusCode != http.StatusOK || header.XMLName.Local != method+"Response" {
		return nil, vmdriver.ErrUncertain
	}
	if out != nil && xml.Unmarshal(data, out) != nil {
		return nil, vmdriver.ErrUncertain
	}
	return response.Cookies(), nil
}

// faultOutcome distinguishes known pre-execution faults from ambiguous runtime faults.
func faultOutcome(kind string) error {
	switch kind {
	case "NoPermissionFault", "NotAuthenticatedFault", "InvalidArgumentFault", "ManagedObjectNotFoundFault",
		"NotSupportedFault", "InvalidStateFault", "TaskInProgressFault", "DuplicateNameFault":
		return vmdriver.ErrRejected
	case "FileFaultFault", "VmConfigFaultFault", "InsufficientResourcesFaultFault":
		return vmdriver.ErrProvider
	default:
		return vmdriver.ErrUncertain
	}
}

// task submits a mutation and returns its managed Task reference.
func (s *session) task(ctx context.Context, obj ref, method, args string) (vmdriver.TaskRef, error) {
	var result struct {
		Task ref `xml:"returnval"`
	}
	if _, err := s.call(ctx, method, object("_this", obj)+args, &result); err != nil {
		return vmdriver.TaskRef{}, err
	}
	if !result.Task.valid("Task") || result.Task.ID == s.cookie.Value {
		return vmdriver.TaskRef{}, vmdriver.ErrUncertain
	}
	return vmdriver.TaskRef{ID: result.Task.ID}, nil
}
