package stdio

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"time"
)

// Calls correlates outbound requests and applies replies before processing later frames.
type Calls struct {
	mu      sync.Mutex
	next    uint64
	pending map[string]func(json.RawMessage, error) error
}

// Request registers a completion before writing on the wire, so immediate replies are safe.
func (c *Calls) Request(ctx context.Context, s *Session, frame func(string) Fields, complete func(json.RawMessage, error) error) error {
	c.mu.Lock()
	c.next++
	id := strconv.FormatUint(c.next, 10)
	if c.pending == nil {
		c.pending = make(map[string]func(json.RawMessage, error) error)
	}
	c.pending[id] = complete
	c.mu.Unlock()
	if err := s.Write(ctx, frame(id)); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return err
	}
	return nil
}

// Call waits for one bounded control exchange. An ambiguous timeout aborts the connection.
func (c *Calls) Call(ctx context.Context, s *Session, frame func(string) Fields, apply func(json.RawMessage) error) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	err := c.Request(ctx, s, frame, func(raw json.RawMessage, replyErr error) error {
		if replyErr == nil && apply != nil {
			replyErr = apply(raw)
		}
		result <- replyErr
		return nil
	})
	if err != nil {
		return err
	}
	select {
	case err := <-result:
		return err
	case <-s.Context().Done():
		// A final reply may be followed immediately by EOF.
		select {
		case err := <-result:
			return err
		default:
			return s.closedError()
		}
	case <-ctx.Done():
		s.Fail("await protocol response", ctx.Err())
		return ctx.Err()
	}
}

// Reply invokes the matching completion once and ignores stale or reordered unrelated IDs.
func (c *Calls) Reply(id string, raw json.RawMessage, err error) error {
	if id == "" {
		return errors.New("missing protocol response ID")
	}
	c.mu.Lock()
	complete := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	if complete == nil {
		return nil
	}
	return complete(raw, err)
}
