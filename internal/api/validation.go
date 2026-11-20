package api

import (
	"regexp"
	"slices"
	"time"
)

var (
	requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)
	ulidPattern      = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	shaPattern       = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	revisionPattern  = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)
	actionPattern    = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$`)
)

// checker validates an admitted value against its command schema.
type checker func(any) bool

// schema names every allowed member and the subset that must be present.
type schema struct {
	required []string
	fields   map[string]checker
}

// valid checks required members, types, and the closed object shape.
func (s schema) valid(body map[string]any) bool {
	for _, key := range s.required {
		if _, ok := body[key]; !ok {
			return false
		}
	}
	for k, value := range body {
		check, ok := s.fields[k]
		if !ok || !check(value) {
			return false
		}
	}
	return true
}

// matches checks a string against an OpenAPI pattern.
func matches(pattern *regexp.Regexp) checker {
	return func(value any) bool {
		s, ok := value.(string)
		return ok && pattern.MatchString(s)
	}
}

// oneOf checks a string against the supplied enum values.
func oneOf(values ...string) checker {
	return func(value any) bool {
		s, ok := value.(string)
		return ok && slices.Contains(values, s)
	}
}

// anyString checks a string field without a minimum length.
func anyString(value any) bool { _, ok := value.(string); return ok }

// nonempty checks a string with minLength 1.
func nonempty(value any) bool { s, ok := value.(string); return ok && s != "" }

// boolean checks a JSON boolean field.
func boolean(value any) bool { _, ok := value.(bool); return ok }

// stringsMatching validates array elements and the schema's optional size and uniqueness constraints.
func stringsMatching(check checker, min int, unique bool) checker {
	return func(value any) bool {
		values, ok := value.([]any)
		if !ok || len(values) < min {
			return false
		}
		seen := make(map[any]bool)
		for _, value := range values {
			if !check(value) || (unique && seen[value]) {
				return false
			}
			if unique {
				seen[value] = true
			}
		}
		return true
	}
}

// dateTime requires an RFC 3339 timestamp with a clock and offset.
func dateTime(value any) bool {
	s, ok := value.(string)
	if !ok {
		return false
	}
	_, err := time.Parse(time.RFC3339, s)
	return err == nil
}

// --- Commands: mirror OpenAPI's closed bodies after the receipt lookup ---

var harness = oneOf("codex", "claude-code", "kimi-code")

var pin = schema{[]string{"harness", "model"}, map[string]checker{"harness": harness, "model": nonempty}}

// harnessPin checks the nested optional createThread pin.
func harnessPin(value any) bool {
	v, ok := value.(map[string]any)
	return ok && pin.valid(v)
}

var createThreadBody = schema{[]string{"project_id", "brief"}, map[string]checker{
	"project_id": nonempty, "brief": nonempty, "criteria": stringsMatching(nonempty, 0, false),
	"review_plan_first": boolean, "audit": oneOf("default", "off", "level1", "level2", "level3"), "pin": harnessPin,
}}

var threadCommands = map[string]schema{
	"pause":          {nil, map[string]checker{"command": anyString}},
	"resume":         {nil, map[string]checker{"command": anyString}},
	"cancel":         {nil, map[string]checker{"command": anyString}},
	"approve_plan":   {[]string{"plan_digest"}, map[string]checker{"command": anyString, "plan_digest": matches(shaPattern)}},
	"select_harness": {[]string{"harness", "model"}, map[string]checker{"command": anyString, "harness": harness, "model": nonempty}},
	"decide":         {[]string{"decision_id", "choice"}, map[string]checker{"command": anyString, "decision_id": nonempty, "choice": nonempty, "note": anyString}},
	"accept":         {[]string{"revision"}, map[string]checker{"command": anyString, "revision": matches(revisionPattern), "note": anyString}},
	"reject":         {[]string{"revision"}, map[string]checker{"command": anyString, "revision": matches(revisionPattern), "note": anyString}},
	"change_mode":    {[]string{"target", "to"}, map[string]checker{"command": anyString, "target": nonempty, "to": oneOf("sandbox", "provider", "full_control")}},
	"resolve_external_request": {[]string{"external_request_id", "resolution", "note"}, map[string]checker{
		"command": anyString, "external_request_id": matches(ulidPattern), "resolution": oneOf("succeeded", "failed", "not_executed"), "note": nonempty,
	}},
}

var createGrantBody = schema{[]string{"host", "identity"}, map[string]checker{"host": nonempty, "identity": nonempty}}

var createApprovalBody = schema{[]string{"worktree_digest", "target", "actions", "expires_at"}, map[string]checker{
	"revision": matches(revisionPattern), "plan_digest": matches(shaPattern), "worktree_digest": matches(shaPattern),
	"target": nonempty, "actions": stringsMatching(matches(actionPattern), 1, true), "expires_at": dateTime,
	"rollback": nonempty, "irreversible_effects": stringsMatching(nonempty, 0, false),
}}

// validApproval enforces the revision/plan choice and the plan-bound action requirements.
func validApproval(body map[string]any) bool {
	if !createApprovalBody.valid(body) {
		return false
	}
	_, revision := body["revision"]
	_, plan := body["plan_digest"]
	if revision == plan {
		return false
	}
	for _, action := range body["actions"].([]any) {
		switch action {
		case "net.firewall_change", "net.network_change", "deploy.production":
			return plan && body["rollback"] != nil && body["irreversible_effects"] != nil
		}
	}
	return true
}
