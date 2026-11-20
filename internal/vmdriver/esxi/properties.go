package esxi

import (
	"context"
	"strings"
	"time"

	"synch/internal/vmdriver"
)

type properties struct {
	Ref    ref `xml:"obj"`
	Fields []struct {
		Name  string `xml:"name"`
		Value struct {
			Text string `xml:",chardata"`
			Raw  string `xml:",innerxml"`
			Type string `xml:"type,attr"`
		} `xml:"val"`
	} `xml:"propSet"`
}

// reference reads a typed managed object reference from a dynamic property.
func (p properties) reference(name string) ref {
	for _, field := range p.Fields {
		if field.Name == name {
			return ref{Type: field.Value.Type, ID: field.Value.Text}
		}
	}
	return ref{}
}

// text retrieves a scalar property.
func (p properties) text(name string) string {
	for _, field := range p.Fields {
		if field.Name == name {
			return field.Value.Text
		}
	}
	return ""
}

// value wraps a complex property for XML decoding.
func (p properties) value(name string) []byte {
	for _, field := range p.Fields {
		if field.Name == name {
			return []byte("<val>" + field.Value.Raw + "</val>")
		}
	}
	return nil
}

// get reads properties of a single exact managed object.
func (s *session) get(ctx context.Context, obj ref, paths ...string) (properties, error) {
	results, err := s.retrieve(ctx, obj.Type, object("obj", obj), paths)
	if err != nil {
		return properties{}, err
	}
	if len(results) != 1 || results[0].Ref != obj {
		return properties{}, vmdriver.ErrInvalid
	}
	return results[0], nil
}

// virtualMachines traverses the session inventory view before destroying it.
func (s *session) virtualMachines(ctx context.Context, paths ...string) ([]properties, error) {
	if !s.content.View.valid("ViewManager") || !s.content.Root.valid("Folder") {
		return nil, vmdriver.ErrUnavailable
	}
	var result struct {
		View ref `xml:"returnval"`
	}
	args := object("_this", s.content.View) + object("container", s.content.Root) +
		tag("type", "VirtualMachine") + tag("recursive", "true")
	if _, err := s.call(ctx, "CreateContainerView", args, &result); err != nil {
		return nil, err
	}
	if !result.View.valid("ContainerView") {
		return nil, vmdriver.ErrInvalid
	}
	defer s.destroyView(ctx, result.View)
	traversal := object("obj", result.View) + `<skip>true</skip><selectSet xsi:type="TraversalSpec"><type>ContainerView</type><path>view</path><skip>false</skip></selectSet>`
	return s.retrieve(ctx, "VirtualMachine", traversal, paths)
}

// destroyView frees the inventory view even when the caller canceled.
func (s *session) destroyView(ctx context.Context, view ref) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	_, _ = s.call(ctx, "DestroyView", object("_this", view), nil)
}

type page struct {
	Result struct {
		Objects []properties `xml:"objects"`
		Token   string       `xml:"token"`
	} `xml:"returnval"`
}

// retrieve reads property pages and rejects repeated continuation tokens.
func (s *session) retrieve(ctx context.Context, kind, selection string, paths []string) ([]properties, error) {
	var spec strings.Builder
	spec.WriteString("<specSet><propSet>" + tag("type", kind) + "<all>false</all>")
	for _, path := range paths {
		spec.WriteString(tag("pathSet", path))
	}
	spec.WriteString("</propSet><objectSet>" + selection + "</objectSet></specSet><options/>")
	target := object("_this", s.content.Collector)
	var current page
	if _, err := s.call(ctx, "RetrievePropertiesEx", target+spec.String(), &current); err != nil {
		return nil, err
	}
	results := current.Result.Objects
	seen := make(map[string]bool)
	for token := current.Result.Token; token != ""; token = current.Result.Token {
		if seen[token] {
			return nil, vmdriver.ErrUncertain
		}
		seen[token] = true
		current = page{}
		if _, err := s.call(ctx, "ContinueRetrievePropertiesEx", target+tag("token", token), &current); err != nil {
			return nil, err
		}
		results = append(results, current.Result.Objects...)
	}
	return results, nil
}
