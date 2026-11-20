package children

import (
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"synch/internal/eventlog"
)

// schemaDoc is the parsed catalogue schema used to resolve local $ref links.
type schemaDoc map[string]any

// loadCatalogue reads the repository's catalogue schema and checks its version.
func loadCatalogue(t *testing.T) schemaDoc {
	t.Helper()
	raw, err := os.ReadFile("../../contracts/events/catalogue.schema.json")
	must(t, err)
	var doc schemaDoc
	must(t, json.Unmarshal(raw, &doc))
	if !strings.HasSuffix(doc["$id"].(string), "/4.1.0") {
		t.Fatalf("catalogue id = %v, want 4.1.0", doc["$id"])
	}
	return doc
}

// payloadSchema returns the payload schema of one event definition after
// checking its type constant and type_version.
func (d schemaDoc) payloadSchema(t *testing.T, def, eventType string) map[string]any {
	t.Helper()
	e := d["$defs"].(map[string]any)[def].(map[string]any)["properties"].(map[string]any)
	if e["type"].(map[string]any)["const"] != eventType || e["type_version"].(map[string]any)["const"] != float64(1) {
		t.Fatalf("%s is not %s v1", def, eventType)
	}
	return e["payload"].(map[string]any)
}

// check validates v against the JSON-schema subset the catalogue payloads use:
// $ref, type, const, enum, pattern, minLength, minItems, uniqueItems, items,
// required, properties, additionalProperties and anyOf.
func (d schemaDoc) check(path string, s map[string]any, v any) []string {
	var errs []string
	fail := func(f string, a ...any) { errs = append(errs, path+": "+fmt.Sprintf(f, a...)) }
	if ref, ok := s["$ref"].(string); ok {
		return d.check(path, d["$defs"].(map[string]any)[strings.TrimPrefix(ref, "#/$defs/")].(map[string]any), v)
	}
	if c, ok := s["const"]; ok && c != v {
		fail("want const %v", c)
	}
	if en, ok := s["enum"].([]any); ok && !slices.Contains(en, v) {
		fail("%v not in %v", v, en)
	}
	switch val := v.(type) {
	case string:
		if s["type"] != nil && s["type"] != "string" {
			fail("type")
		}
		if n, ok := s["minLength"].(float64); ok && float64(len(val)) < n {
			fail("shorter than %v", n)
		}
		if p, ok := s["pattern"].(string); ok && !regexp.MustCompile(p).MatchString(val) {
			fail("%q does not match %s", val, p)
		}
	case []any:
		if s["type"] != "array" && s["type"] != nil {
			fail("type")
		}
		if n, ok := s["minItems"].(float64); ok && float64(len(val)) < n {
			fail("fewer than %v items", n)
		}
		seen := map[string]bool{}
		for i, item := range val {
			if s["uniqueItems"] == true {
				k, _ := json.Marshal(item)
				if seen[string(k)] {
					fail("duplicate item %s", k)
				}
				seen[string(k)] = true
			}
			if is, ok := s["items"].(map[string]any); ok {
				errs = append(errs, d.check(fmt.Sprintf("%s[%d]", path, i), is, item)...)
			}
		}
	case map[string]any:
		if s["type"] != "object" && s["type"] != nil {
			fail("type")
		}
		props, _ := s["properties"].(map[string]any)
		for _, r := range anyStrings(s["required"]) {
			if _, ok := val[r]; !ok {
				fail("missing required %q", r)
			}
		}
		for k, item := range val {
			if ps, ok := props[k].(map[string]any); ok {
				errs = append(errs, d.check(path+"."+k, ps, item)...)
			} else if s["additionalProperties"] == false {
				fail("unknown field %q", k)
			}
		}
		if alts, ok := s["anyOf"].([]any); ok {
			okAny := false
			for _, alt := range alts {
				okAny = okAny || len(d.check(path, alt.(map[string]any), v)) == 0
			}
			if !okAny {
				fail("no anyOf branch holds")
			}
		}
	default:
		if t, _ := s["type"].(string); t == "array" || t == "object" || t == "string" {
			fail("got %T, want %s", v, t)
		}
	}
	return errs
}

// anyStrings converts a decoded JSON string array.
func anyStrings(v any) []string {
	var out []string
	items, _ := v.([]any)
	for _, i := range items {
		out = append(out, i.(string))
	}
	return out
}

// assertPayload validates marshaled record bytes against the catalogue payload schema.
func assertPayload(t *testing.T, d schemaDoc, def, eventType string, payload []byte, err error) {
	t.Helper()
	must(t, err)
	var v any
	must(t, json.Unmarshal(payload, &v))
	if errs := d.check(eventType, d.payloadSchema(t, def, eventType), v); len(errs) > 0 {
		t.Fatalf("%s payload %s drifts from catalogue 4.1.0: %v", eventType, payload, errs)
	}
}

// TestRecordsMatchCatalogue proves C4/L2-L6 the three records marshal into payloads that satisfy catalogue 4.1.0, including empty optional lists.
func TestRecordsMatchCatalogue(t *testing.T) {
	d := loadCatalogue(t)
	a := assignment(eventlog.NewID())
	a.Dependencies = []string{eventlog.NewID()}
	a.AllowedCapabilities = []string{"workspace.read"}
	p, err := a.Payload()
	assertPayload(t, d, "e_child_assignment_recorded", "child.assignment_recorded", p, err)
	a.AllowedCapabilities, a.Dependencies, a.InputRevision, a.Target = nil, nil, "", "dir"
	p, err = a.Payload()
	assertPayload(t, d, "e_child_assignment_recorded", "child.assignment_recorded", p, err)

	full := resultFor(a, "failed")
	full.Checks = map[string][]Check{"passed": {{Check: "build"}}, "failed": {{"test", "red"}}, "error": {{"vet", "x"}},
		"partial": {{"race", "x"}}, "not_run": {{"lint", "x"}}}
	full.Limitations, full.OpenRequirements = []string{"l"}, []string{"o"}
	p, err = full.Payload()
	assertPayload(t, d, "e_child_result_recorded", "child.result_recorded", p, err)
	p, err = resultFor(a, "cancelled").Payload() // Nil groups and lists must become arrays.
	assertPayload(t, d, "e_child_result_recorded", "child.result_recorded", p, err)

	for _, s := range []string{"off", "level1", "level2", "level3"} {
		p, err = AuditSetting{ThreadID: eventlog.NewID(), Setting: s, Reason: "why"}.Payload()
		assertPayload(t, d, "e_audit_setting_recorded", "audit.setting_recorded", p, err)
	}
}

// TestCatalogueCheckerRejectsDrift proves the schema check fails on renamed fields, bad enums and missing groups.
func TestCatalogueCheckerRejectsDrift(t *testing.T) {
	d := loadCatalogue(t)
	s := d.payloadSchema(t, "e_child_result_recorded", "child.result_recorded")
	const ids = `"child_thread_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV","assignment_id":"01ARZ3NDEKTSV4RRFFQ69G5FAV"`
	const lists = `"limitations":[],"open_requirements":[]`
	for name, raw := range map[string]string{
		"renamed field": `{` + ids + `,"result":"completed"}`,
		"bad enum":      `{` + ids + `,"outcome":"done","revision":"r","checks":{"passed":[],"failed":[],"error":[],"partial":[],"not_run":[]},` + lists + `}`,
		"missing group": `{` + ids + `,"outcome":"failed","revision":"r","checks":{"passed":[],"failed":[]},` + lists + `}`,
		"no reason":     `{` + ids + `,"outcome":"failed","revision":"r","checks":{"passed":[],"failed":[{"check":"t"}],"error":[],"partial":[],"not_run":[]},` + lists + `}`,
	} {
		var v any
		must(t, json.Unmarshal([]byte(raw), &v))
		if len(d.check("x", s, v)) == 0 {
			t.Errorf("%s: accepted", name)
		}
	}
}
