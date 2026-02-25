package idempotency

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/events"
)

func TestDeduplicator_CheckAndMark_Duplicate(t *testing.T) {
	c := NewDeduplicator(time.Minute)

	assert.False(t, c.CheckAndMark("evt-dup"), "first call should return false")
	assert.True(t, c.CheckAndMark("evt-dup"), "second call should return true (duplicate)")
}

func TestDeduplicator_CheckAndMark_AfterExpiry(t *testing.T) {
	c := NewDeduplicator(100 * time.Millisecond)

	assert.False(t, c.CheckAndMark("evt-exp"))
	assert.True(t, c.CheckAndMark("evt-exp"))

	time.Sleep(200 * time.Millisecond)

	assert.False(t, c.CheckAndMark("evt-exp"), "expired event should be accepted again")
}

func TestDeduplicator_Unmark_AllowsReprocessing(t *testing.T) {
	c := NewDeduplicator(time.Minute)

	assert.False(t, c.CheckAndMark("evt-retry"), "first call should return false")
	assert.True(t, c.CheckAndMark("evt-retry"), "should be marked")

	c.Unmark("evt-retry")

	assert.False(t, c.CheckAndMark("evt-retry"), "after Unmark should be accepted again")
}

func TestDeduplicator_ConcurrentAccess(t *testing.T) {
	c := NewDeduplicator(time.Minute)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("evt-%d", n)
			c.CheckAndMark(id)
			c.CheckAndMark(id)
		}(i)
	}
	wg.Wait()
}

func TestDeduplicator_Wrap_SkipsDuplicate(t *testing.T) {
	c := NewDeduplicator(time.Minute)
	calls := 0
	handler := func(_ context.Context, _ *events.Event) error {
		calls++
		return nil
	}
	wrapped := c.Wrap(handler)
	evt := &events.Event{ID: "wrap-dup", CorrelationID: "corr-1", Type: events.OrderCreated}

	require.NoError(t, wrapped(context.Background(), evt))
	require.NoError(t, wrapped(context.Background(), evt))

	assert.Equal(t, 1, calls, "handler should be called only once for duplicate event")
}

func TestDeduplicator_Wrap_UnmarksOnError(t *testing.T) {
	c := NewDeduplicator(time.Minute)
	handlerErr := errors.New("handler error")
	calls := 0
	handler := func(_ context.Context, _ *events.Event) error {
		calls++
		if calls == 1 {
			return handlerErr
		}
		return nil
	}
	wrapped := c.Wrap(handler)
	evt := &events.Event{ID: "wrap-retry", CorrelationID: "corr-2", Type: events.InventoryFailed}

	require.ErrorIs(t, wrapped(context.Background(), evt), handlerErr)
	require.NoError(t, wrapped(context.Background(), evt), "retry after error should succeed")
	assert.Equal(t, 2, calls)
}

func TestDeduplicator_Wrap_KeepsMarkOnSuccess(t *testing.T) {
	c := NewDeduplicator(time.Minute)
	calls := 0
	handler := func(_ context.Context, _ *events.Event) error {
		calls++
		return nil
	}
	wrapped := c.Wrap(handler)
	evt := &events.Event{ID: "wrap-ok", CorrelationID: "corr-3", Type: events.OrderCreated}

	assert.NoError(t, wrapped(context.Background(), evt))
	assert.NoError(t, wrapped(context.Background(), evt)) // duplicate — skipped
	assert.Equal(t, 1, calls, "handler should not be called again after success")
}
