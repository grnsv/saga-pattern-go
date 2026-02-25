package idempotency

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/events"
)

// Deduplicator is a thread-safe in-memory deduplicator for event IDs with TTL-based expiry.
type Deduplicator struct {
	mu   sync.Mutex
	seen map[string]time.Time
	ttl  time.Duration
}

// NewDeduplicator creates a new Deduplicator. Processed event IDs are remembered for ttl duration.
func NewDeduplicator(ttl time.Duration) *Deduplicator {
	return &Deduplicator{
		seen: make(map[string]time.Time),
		ttl:  ttl,
	}
}

// CheckAndMark atomically checks if id was already processed and marks it if not.
// Returns true if id is a duplicate (already seen within TTL); false if new (and now marked).
// On handler failure, call Unmark to allow the event to be reprocessed on retry.
func (c *Deduplicator) CheckAndMark(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	if exp, ok := c.seen[id]; ok && now.Before(exp) {
		return true
	}
	c.evict(now)
	c.seen[id] = now.Add(c.ttl)
	return false
}

// Unmark removes id from the seen set so it can be reprocessed on retry.
func (c *Deduplicator) Unmark(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.seen, id)
}

// evict removes all expired entries. Must be called with c.mu held.
func (c *Deduplicator) evict(now time.Time) {
	for id, exp := range c.seen {
		if now.After(exp) {
			delete(c.seen, id)
		}
	}
}

// Wrap returns an EventHandler that skips duplicate events.
// If the event ID was already processed within TTL, the handler is not called.
// On handler error, the ID is unmarked to allow reprocessing on retry.
func (c *Deduplicator) Wrap(next func(context.Context, *events.Event) error) func(context.Context, *events.Event) error {
	return func(ctx context.Context, event *events.Event) error {
		if c.CheckAndMark(event.ID) {
			slog.DebugContext(ctx, "duplicate event skipped",
				"eventId", event.ID, "correlationId", event.CorrelationID, "type", event.Type)
			return nil
		}
		if err := next(ctx, event); err != nil {
			c.Unmark(event.ID)
			return err
		}
		return nil
	}
}
