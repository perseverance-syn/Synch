package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"synch/internal/eventlog"
	"synch/internal/thread"
	"time"
)

const pageDefault = 200
const pageMax = 1000
const frameWriteTimeout = 5 * time.Second

var decimal = regexp.MustCompile(`^[0-9]+$`)

// logPosition is common to the hello frame, page response and cursor errors.
type logPosition struct {
	LogEpoch string `json:"log_epoch"`
	Head     int64  `json:"head"`
}

// parseCursor accepts only nonnegative decimal cursors and preserves an explicit empty value as invalid.
func parseCursor(values []string, fallback int64) (int64, error) {
	if len(values) == 0 {
		return fallback, nil
	}
	if len(values) != 1 || !decimal.MatchString(values[0]) {
		return 0, errors.New("invalid cursor")
	}
	return strconv.ParseInt(values[0], 10, 64)
}

// serveEvents validates cursors and reports resync information before committing a response.
func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request, stream string, live bool) {
	threadID, isThread := strings.CutPrefix(stream, "thread:")
	if stream != "all" && stream != "system" && (!isThread || !ulidPattern.MatchString(threadID)) {
		writeProblem(w, newProblem(404, "not_found", ""))
		return
	}
	query := r.URL.Query()
	afterValues := query["after"]
	if id := r.Header.Values("Last-Event-ID"); live && len(id) != 0 {
		afterValues = id
	}
	after, err := parseCursor(afterValues, 0)
	afterOutOfRange := errors.Is(err, strconv.ErrRange)
	if err != nil && !afterOutOfRange {
		writeProblem(w, newProblem(400, "validation_failed", "invalid after cursor"))
		return
	}
	limit := int64(pageDefault)
	if !live {
		limit, err = parseCursor(query["limit"], pageDefault)
		if err != nil || limit < 1 || limit > pageMax {
			writeProblem(w, newProblem(400, "validation_failed", "limit must be between 1 and 1000"))
			return
		}
	}
	head, err := s.streamHead(stream)
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	epoch, err := s.config.Store.LogEpoch()
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	position := logPosition{epoch, head}
	if afterOutOfRange || after > head {
		p := newProblem(409, "cursor_ahead", "")
		p.Head, p.LogEpoch = &head, epoch
		writeProblem(w, p)
		return
	}
	if live {
		if head == 0 && isThread {
			if s.config.Threads == nil {
				writeProblem(w, newProblem(404, "not_found", ""))
				return
			}
			_, err := s.config.Threads.Status(r.Context(), threadID)
			if errors.Is(err, thread.ErrMissing) {
				writeProblem(w, newProblem(404, "not_found", ""))
				return
			}
			if err != nil {
				writeProblem(w, newProblem(500, "internal", ""))
				return
			}
		}
		s.streamEvents(w, r, stream, after, position)
		return
	}
	events, err := s.readEvents(stream, after, int(min(limit, head-after)))
	if err != nil {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	if events == nil {
		events = []eventlog.Event{}
	}
	writeJSON(w, struct {
		logPosition
		Events []eventlog.Event `json:"events"`
	}{position, events})
}

// streamHead uses the global head as a bound when finding a per-stream head by contiguous cursor.
func (s *Server) streamHead(stream string) (int64, error) {
	high, err := s.config.Store.Head()
	if err != nil || stream == "all" {
		return high, err
	}
	var low int64
	// A single-row read at a cursor detects whether that cursor exists without scanning history.
	for low < high {
		middle := low + (high-low-1)/2 + 1
		events, err := s.readEvents(stream, middle-1, 1)
		if err != nil {
			return 0, err
		}
		if len(events) == 0 {
			high = middle - 1
		} else {
			low = middle
		}
	}
	return low, nil
}

// readEvents reads the stream's persisted ordered prefix after the supplied cursor.
func (s *Server) readEvents(stream string, after int64, limit int) ([]eventlog.Event, error) {
	if stream == "all" {
		return s.config.Store.ReadGlobal(after, limit)
	}
	return s.config.Store.ReadStream(stream, after, limit)
}

// cursorOf follows OpenAPI's use of global sequence only for the all stream.
func cursorOf(stream string, event eventlog.Event) int64 {
	if stream == "all" {
		return event.GlobalSeq
	}
	return event.Seq
}

// --- SSE: carry one persisted cursor through replay and live reads ---

// streamEvents sends hello, replays a snapshot prefix, then polls forward from the same cursor.
func (s *Server) streamEvents(w http.ResponseWriter, r *http.Request, stream string, after int64, position logPosition) {
	if _, ok := w.(http.Flusher); !ok {
		writeProblem(w, newProblem(500, "internal", ""))
		return
	}
	controller := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	hello, _ := json.Marshal(position)
	if err := sendFrame(w, controller, fmt.Sprintf("event: hello\ndata: %s\n\n", hello)); err != nil {
		return
	}
	// Keep initial request reads bounded, then release the read deadline for the live stream.
	if err := controller.SetReadDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return
	}

	heartbeat := time.NewTicker(s.config.Heartbeat)
	defer heartbeat.Stop()
	poll := time.NewTicker(s.config.PollInterval)
	defer poll.Stop()
	ctx := r.Context()
	for ctx.Err() == nil {
		replay := after < position.Head
		limit := s.config.LiveBuffer + 1
		if replay {
			limit = int(min(pageDefault, position.Head-after))
		}
		events, err := s.readEvents(stream, after, limit)
		if err != nil {
			return
		}
		if !replay && len(events) > s.config.LiveBuffer {
			_ = sendFrame(w, controller, "event: overflow\ndata: {}\n\n")
			return
		}
		for _, event := range events {
			if ctx.Err() != nil || !sendHeartbeatIfDue(w, controller, heartbeat.C) {
				return
			}
			cursor := cursorOf(stream, event)
			if cursor != after+1 {
				return
			}
			data, err := json.Marshal(event)
			if err != nil {
				return
			}
			if err := sendFrame(w, controller, fmt.Sprintf("id: %d\nevent: %s\ndata: %s\n\n", cursor, event.Type, data)); err != nil {
				return
			}
			after = cursor
		}
		if len(events) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
		case <-heartbeat.C:
			if err := sendFrame(w, controller, ": heartbeat\n\n"); err != nil {
				return
			}
		}
	}
}

// sendHeartbeatIfDue keeps heartbeats flowing even when there is continuous persisted traffic.
func sendHeartbeatIfDue(w http.ResponseWriter, c *http.ResponseController, due <-chan time.Time) bool {
	select {
	case <-due:
		return sendFrame(w, c, ": heartbeat\n\n") == nil
	default:
		return true
	}
}

// sendFrame bounds a stalled network write and flushes one complete SSE frame.
func sendFrame(w http.ResponseWriter, c *http.ResponseController, frame string) error {
	err := c.SetWriteDeadline(time.Now().Add(frameWriteTimeout))
	if err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	if err == nil {
		// The deadline bounds this frame only. An idle stream may wait longer for its heartbeat.
		defer func() { _ = c.SetWriteDeadline(time.Time{}) }()
	}
	if _, err := io.WriteString(w, frame); err != nil {
		return err
	}
	return c.Flush()
}
