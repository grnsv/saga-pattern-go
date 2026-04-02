package idempotency

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// envelope is a minimal struct for extracting the message ID from the Kafka message value.
type envelope struct {
	ID            string `json:"id"`
	CorrelationID string `json:"correlationId"`
	Type          string `json:"type"`
}

// Deduplicator is a thread-safe in-memory deduplicator for message IDs with TTL-based expiry.
type Deduplicator struct {
	mu   sync.Mutex
	seen map[string]time.Time
	ttl  time.Duration
}

// NewDeduplicator creates a new Deduplicator. Processed message IDs are remembered for ttl duration.
func NewDeduplicator(ttl time.Duration) *Deduplicator {
	return &Deduplicator{
		seen: make(map[string]time.Time),
		ttl:  ttl,
	}
}

// CheckAndMark atomically checks if id was already processed and marks it if not.
// Returns true if id is a duplicate (already seen within TTL); false if new (and now marked).
// On handler failure, call Unmark to allow the message to be reprocessed on retry.
func (d *Deduplicator) CheckAndMark(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if exp, ok := d.seen[id]; ok && now.Before(exp) {
		return true
	}
	d.evict(now)
	d.seen[id] = now.Add(d.ttl)
	return false
}

// Unmark removes id from the seen set so it can be reprocessed on retry.
func (d *Deduplicator) Unmark(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.seen, id)
}

// evict removes all expired entries. Must be called with d.mu held.
func (d *Deduplicator) evict(now time.Time) {
	for id, exp := range d.seen {
		if now.After(exp) {
			delete(d.seen, id)
		}
	}
}

// Wrap returns a MessageHandler that skips duplicate messages.
// The message ID is extracted from the JSON envelope in the Kafka message value.
// If the message ID was already processed within TTL, the handler is not called.
// On handler error, the ID is unmarked to allow reprocessing on retry.
func (d *Deduplicator) Wrap(next func(context.Context, *kafkago.Message) error) func(context.Context, *kafkago.Message) error {
	return func(ctx context.Context, msg *kafkago.Message) error {
		var env envelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			return next(ctx, msg)
		}
		if env.ID == "" {
			return next(ctx, msg)
		}
		if d.CheckAndMark(env.ID) {
			slog.DebugContext(ctx, "duplicate message skipped",
				"messageId", env.ID, "correlationId", env.CorrelationID, "type", env.Type)
			return nil
		}
		if err := next(ctx, msg); err != nil {
			d.Unmark(env.ID)
			return err
		}
		return nil
	}
}
