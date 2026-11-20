package scanner

import (
	"encoding/json"
	"sort"
	"strings"

	"synch/internal/findings"
)

const redacted = "[REDACTED]"

// gitleaksEntry retains finding metadata without source lines, commit messages,
// author information or other fields that could repeat undiscovered secrets.
type gitleaksEntry struct {
	RuleID, File, Match, Secret string
	StartLine, EndLine          int
	Commit                      string
}

// gitleaksReport accepts the pinned tool's unversioned array and retains a safe
// projection. Match and Secret are always replaced even if the tool redacts them.
func (d *Driver) gitleaksReport(output []byte) ([]byte, []findings.Finding, error) {
	var entries []gitleaksEntry
	if json.Unmarshal(output, &entries) != nil || entries == nil {
		return nil, nil, ErrReport
	}
	removeMatches := gitleaksMatches(entries)
	var normalized []findings.Finding
	for i := range entries {
		entry := &entries[i]
		if err := d.redactGitleaks(entry, removeMatches); err != nil {
			return nil, nil, err
		}
		if entry.RuleID == "" || entry.File == "" || entry.StartLine <= 0 || entry.EndLine < entry.StartLine {
			return nil, nil, ErrReport
		}
		normalized = append(normalized, findings.Finding{
			Rule:      findings.Rule{ID: entry.RuleID},
			Severity:  findings.High,
			Component: findings.Component{Type: "secret", Name: entry.RuleID},
			Location:  findings.Location{Path: entry.File, StartLine: entry.StartLine, EndLine: entry.EndLine, Commit: entry.Commit},
			Evidence:  findings.Evidence{Summary: entry.RuleID, SnippetRedacted: redacted},
		})
	}
	report, err := json.Marshal(entries)
	if err != nil {
		return nil, nil, ErrReport
	}
	return report, normalized, nil
}

// gitleaksMatches also removes a match repeated in another finding's metadata.
func gitleaksMatches(entries []gitleaksEntry) *strings.Replacer {
	secrets := make(map[string]bool)
	for _, entry := range entries {
		for _, value := range []string{entry.Match, entry.Secret} {
			if value != "" && value != redacted {
				secrets[value] = true
			}
		}
	}
	ordered := make([]string, 0, len(secrets))
	for value := range secrets {
		ordered = append(ordered, value)
	}
	// Longer matches win at the same position so a prefix match cannot leave a suffix behind.
	sort.Slice(ordered, func(i, j int) bool { return len(ordered[i]) > len(ordered[j]) })
	pairs := make([]string, 0, 2*len(ordered))
	for _, value := range ordered {
		pairs = append(pairs, value, redacted)
	}
	return strings.NewReplacer(pairs...)
}

// redactGitleaks removes raw matches and secrets from every retained string field.
func (d *Driver) redactGitleaks(entry *gitleaksEntry, matches *strings.Replacer) error {
	for _, field := range []*string{&entry.RuleID, &entry.File, &entry.Commit} {
		safe, err := d.redactText(matches.Replace(*field))
		if err != nil {
			return err
		}
		*field = safe
	}
	entry.Match, entry.Secret = redacted, redacted
	return nil
}
