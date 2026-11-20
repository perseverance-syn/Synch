package api

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
)

// OutputMatch is one capped line of a retained output that matched a search.
type OutputMatch struct {
	Line int    `json:"line"`
	Text string `json:"text"`
}

// Output search errors. A reference the thread may not read is reported as not found.
var (
	ErrOutputNotFound = errors.New("output reference not found")
	ErrBadPattern     = errors.New("invalid search pattern")
)

// SearchOutputFunc searches a retained output on behalf of a thread. It must refuse a reference
// that belongs to neither the thread nor one of its children with ErrOutputNotFound.
type SearchOutputFunc func(ctx context.Context, thread, ref, pattern string, limit int) ([]OutputMatch, error)

var outputRefPattern = regexp.MustCompile(`^out_[0-9a-f]{32}$`)

const searchLimitDefault, searchLimitMax = 50, 200

// searchOutput serves GET /v1/outputs/{ref}/search?thread=&pattern=&limit=. It returns capped
// excerpts only, never the whole retained output.
func (s *Server) searchOutput(w http.ResponseWriter, r *http.Request, ref string) {
	if !outputRefPattern.MatchString(ref) {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	q := r.URL.Query()
	id, pattern := q.Get("thread"), q.Get("pattern")
	limit := searchLimitDefault
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > searchLimitMax {
			writeProblem(w, newProblem(400, "validation_failed", "limit must be between 1 and 200"))
			return
		}
		limit = n
	}
	if !ulidPattern.MatchString(id) || pattern == "" || len(pattern) > 1024 {
		writeProblem(w, newProblem(400, "validation_failed", "thread and pattern are required"))
		return
	}
	if s.config.SearchOutput == nil || s.config.Threads == nil {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	if _, err := s.config.Threads.Status(r.Context(), id); err != nil {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	matches, err := s.config.SearchOutput(r.Context(), id, ref, pattern, limit)
	switch {
	case errors.Is(err, ErrOutputNotFound):
		writeProblem(w, newProblem(404, "not_found", ""))
	case errors.Is(err, ErrBadPattern):
		writeProblem(w, newProblem(400, "validation_failed", "invalid search pattern"))
	case err != nil:
		writeProblem(w, newProblem(500, "internal", ""))
	default:
		if matches == nil {
			matches = []OutputMatch{}
		}
		writeJSON(w, map[string]any{"ref": ref, "matches": matches, "truncated": len(matches) == limit})
	}
}
