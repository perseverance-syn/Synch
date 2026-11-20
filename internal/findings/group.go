package findings

import (
	"errors"
	"slices"
	"time"
)

var ErrInvalidTriage = errors.New("invalid finding triage")

type Triage struct {
	Status string     `json:"status"`
	Reason string     `json:"reason,omitempty"`
	By     string     `json:"by,omitempty"`
	At     *time.Time `json:"at,omitempty"`
}

// SetTriage applies the actor and reason requirements before changing a finding.
func (f *Finding) SetTriage(triage Triage) error {
	switch triage.Status {
	case "false_positive", "accepted_risk":
		if triage.Reason == "" || triage.By == "" {
			return ErrInvalidTriage
		}
	case "open", "confirmed", "fixed":
	default:
		return ErrInvalidTriage
	}
	f.Triage = &triage
	return nil
}

// Group combines repeated normalized findings by fingerprint, preserving the
// earliest representative in input order and each distinct provenance entry.
func Group(input []Finding) []Finding {
	groups := make([]Finding, 0, len(input))
	indices := make(map[string]int, len(input))
	for _, finding := range input {
		index, exists := indices[finding.Fingerprint]
		if !exists {
			index = len(groups)
			indices[finding.Fingerprint] = index
			representative := finding
			representative.Provenance = nil
			groups = append(groups, representative)
		}
		group := &groups[index]
		for _, source := range finding.Provenance {
			if !slices.Contains(group.Provenance, source) {
				group.Provenance = append(group.Provenance, source)
			}
		}
	}
	return groups
}
