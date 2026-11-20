package api

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"synch/internal/eventlog"
	"synch/internal/findings"
	"time"
)

// findingGroup is one finding after grouping. Scanner findings group by fingerprint; review
// findings (audit, security_review) carry none and group by tool, rule and location.
type findingGroup struct {
	Key         string                `json:"key"`
	Fingerprint string                `json:"fingerprint,omitempty"`
	Source      string                `json:"source"`
	Tool        string                `json:"tool"`
	Rule        string                `json:"rule,omitempty"`
	Location    string                `json:"location,omitempty"`
	Summary     string                `json:"summary,omitempty"`
	Severity    string                `json:"severity"`
	Status      string                `json:"status"`
	Threads     []string              `json:"threads"`
	FindingIDs  []string              `json:"finding_ids"`
	Provenance  []findings.Provenance `json:"provenance"`
	Occurrences int                   `json:"occurrences"`
	FirstSeen   time.Time             `json:"first_seen"`
	LastSeen    time.Time             `json:"last_seen"`
}

// listFindings serves GET /v1/findings?thread=&severity=&status=.
// Status is the latest recorded triage status of the group, "open" when none was recorded.
func (s *Server) listFindings(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	id, severity, status := q.Get("thread"), q.Get("severity"), q.Get("status")
	if (id != "" && !ulidPattern.MatchString(id)) || (severity != "" && findings.Severity(severity).Rank() < 0) ||
		(status != "" && !slices.Contains([]string{"open", "confirmed", "fixed", "false_positive", "accepted_risk"}, status)) {
		writeProblem(w, newProblem(400, "validation_failed", "invalid thread, severity or status filter"))
		return
	}
	var groups []*findingGroup
	index := map[string]*findingGroup{}
	err := s.scanGlobal([]string{"finding.recorded"}, func(e eventlog.Event) {
		var p struct {
			Source  string          `json:"source"`
			Finding json.RawMessage `json:"finding"`
		}
		if json.Unmarshal(e.Payload, &p) != nil {
			return
		}
		g := findingGroup{Source: p.Source, Status: "open", FirstSeen: e.RecordedAt, LastSeen: e.RecordedAt}
		if p.Source == "scanner" {
			var f findings.Finding
			if json.Unmarshal(p.Finding, &f) != nil {
				return
			}
			g.Key, g.Fingerprint, g.Tool, g.Rule, g.Summary = f.Fingerprint, f.Fingerprint, f.Tool.Name, f.Rule.ID, f.Evidence.Summary
			g.Location, g.Severity, g.Provenance, g.FindingIDs = f.Location.Path, string(f.Severity), f.Provenance, []string{f.ID}
			if f.Triage != nil {
				g.Status = f.Triage.Status
			}
		} else {
			var f struct {
				ID           string `json:"id"`
				Summary      string `json:"summary"`
				Severity     string `json:"severity"`
				CriterionID  string `json:"criterion_id"`
				EvidenceRefs []struct {
					URI    string `json:"uri"`
					Digest string `json:"digest"`
				} `json:"evidence_refs"`
			}
			if json.Unmarshal(p.Finding, &f) != nil {
				return
			}
			var uris []string
			for _, ref := range f.EvidenceRefs {
				uris = append(uris, ref.URI)
				g.Provenance = append(g.Provenance, findings.Provenance{ScanID: e.ID, Tool: p.Source,
					Report: findings.ReportRef{URI: ref.URI, Digest: ref.Digest}})
			}
			g.Tool, g.Rule, g.Location, g.Summary, g.Severity, g.FindingIDs = p.Source, f.CriterionID, strings.Join(uris, ","), f.Summary, f.Severity, []string{f.ID}
			g.Key = strings.Join([]string{p.Source, f.CriterionID, g.Location}, "\x00")
		}
		g.Threads, g.Occurrences = []string{e.ThreadID}, 1
		cur := index[g.Key]
		if cur == nil {
			index[g.Key] = &g
			groups = append(groups, &g)
			return
		}
		cur.Occurrences++
		cur.LastSeen, cur.Status = g.LastSeen, g.Status
		if findings.Severity(g.Severity).Rank() > findings.Severity(cur.Severity).Rank() {
			cur.Severity = g.Severity
		}
		if !slices.Contains(cur.Threads, e.ThreadID) {
			cur.Threads = append(cur.Threads, e.ThreadID)
		}
		cur.FindingIDs = append(cur.FindingIDs, g.FindingIDs...)
		for _, pv := range g.Provenance {
			if !slices.Contains(cur.Provenance, pv) {
				cur.Provenance = append(cur.Provenance, pv)
			}
		}
	})
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	list := []*findingGroup{}
	for _, g := range groups {
		if g.Provenance == nil {
			g.Provenance = []findings.Provenance{}
		}
		if (id == "" || slices.Contains(g.Threads, id)) && (severity == "" || g.Severity == severity) && (status == "" || g.Status == status) {
			list = append(list, g)
		}
	}
	writeJSON(w, map[string]any{"findings": list})
}
