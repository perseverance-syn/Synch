package roles

import (
	"fmt"
	"regexp"
	"strings"
)

// Choice is the specialist the coordinator picked and the reason to record in the
// assignment (child.assignment_recorded role and role_reason, decision 06).
type Choice struct {
	Role   string
	Reason string
}

// rule maps one required-outcome pattern to a role. Rules run in order: the first
// match wins, so a security outcome is never downgraded to Builder.
type rule struct {
	role    string
	outcome string
	re      *regexp.Regexp
}

// rules are the deterministic selection rules (decision 06, until routing evidence
// can refine them). Security covers defensive triage and fix verification; Ops covers
// infrastructure, CI and deployment; everything else is implementation for a Builder.
var rules = []rule{
	{"security", "a defensive security outcome", regexp.MustCompile(`(?i)\b(vulnerabilit\w*|cve-\d+|security (finding|triage|scan|review)|threat model\w*|trivy|gitleaks|secret leak\w*|leaked secret\w*|hardening|exploit\w*)`)},
	{"ops", "an infrastructure, CI or deployment outcome", regexp.MustCompile(`(?i)\b(deploy\w*|infrastructure|ci pipeline|ci/cd|github actions|gitlab ci|jenkins|firewall|vms?|virtual machine\w*|runbook|rollback|terraform|provision\w*|monitoring)\b`)},
}

// Choose picks Builder, Ops or Security from the brief and the acceptance criteria
// and returns the reason. The same input always gives the same choice. It only names a
// role: authority still comes from the daemon's policy for that role.
func Choose(brief string, criteria []string) Choice {
	text := brief + "\n" + strings.Join(criteria, "\n")
	for _, r := range rules {
		if m := r.re.FindString(text); m != "" {
			return Choice{Role: r.role, Reason: fmt.Sprintf("The brief or criteria require %s (matched %q), so a %s child is assigned (dec. 06).", r.outcome, strings.ToLower(m), roleLabel(r.role))}
		}
	}
	return Choice{Role: "builder", Reason: "The required outcome is an implementation change with evidence and no security or operations outcome, so a Builder child carries out the brief (dec. 05, 06)."}
}

// roleLabel is the display name of a role for recorded reasons.
func roleLabel(role string) string {
	if role == "" {
		return role
	}
	return strings.ToUpper(role[:1]) + role[1:]
}
