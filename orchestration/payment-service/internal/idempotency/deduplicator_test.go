package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeduplicator_CheckAndMark_Duplicate(t *testing.T) {
	d := NewDeduplicator(time.Minute)

	assert.False(t, d.CheckAndMark("msg-dup"), "first call should return false")
	assert.True(t, d.CheckAndMark("msg-dup"), "second call should return true (duplicate)")
}

func TestDeduplicator_CheckAndMark_AfterExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d := NewDeduplicator(time.Second)

		assert.False(t, d.CheckAndMark("msg-exp"))
		assert.True(t, d.CheckAndMark("msg-exp"))

		time.Sleep(time.Second + time.Millisecond)

		assert.False(t, d.CheckAndMark("msg-exp"), "expired message should be accepted again")
	})
}

func TestDeduplicator_Unmark_AllowsReprocessing(t *testing.T) {
	d := NewDeduplicator(time.Minute)

	assert.False(t, d.CheckAndMark("msg-retry"), "first call should return false")
	assert.True(t, d.CheckAndMark("msg-retry"), "should be marked")

	d.Unmark("msg-retry")

	assert.False(t, d.CheckAndMark("msg-retry"), "after Unmark should be accepted again")
}

func TestDeduplicator_ConcurrentAccess(t *testing.T) {
	d := NewDeduplicator(time.Minute)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("msg-%d", n)
			d.CheckAndMark(id)
			d.CheckAndMark(id)
		}(i)
	}
	wg.Wait()
}

func makeMessage(id string) kafkago.Message {
	env := envelope{ID: id, CorrelationID: "corr-1", Type: "TestCommand"}
	b, _ := json.Marshal(env)
	return kafkago.Message{Key: []byte("corr-1"), Value: b}
}

func TestDeduplicator_Wrap_SkipsDuplicate(t *testing.T) {
	d := NewDeduplicator(time.Minute)
	calls := 0
	handler := func(_ context.Context, _ *kafkago.Message) error {
		calls++
		return nil
	}
	wrapped := d.Wrap(handler)
	msg := makeMessage("wrap-dup")

	require.NoError(t, wrapped(context.Background(), &msg))
	require.NoError(t, wrapped(context.Background(), &msg))

	assert.Equal(t, 1, calls, "handler should be called only once for duplicate message")
}

func TestDeduplicator_Wrap_UnmarksOnError(t *testing.T) {
	d := NewDeduplicator(time.Minute)
	handlerErr := errors.New("handler error")
	calls := 0
	handler := func(_ context.Context, _ *kafkago.Message) error {
		calls++
		if calls == 1 {
			return handlerErr
		}
		return nil
	}
	wrapped := d.Wrap(handler)
	msg := makeMessage("wrap-retry")

	require.ErrorIs(t, wrapped(context.Background(), &msg), handlerErr)
	require.NoError(t, wrapped(context.Background(), &msg), "retry after error should succeed")
	assert.Equal(t, 2, calls)
}

func TestDeduplicator_Wrap_KeepsMarkOnSuccess(t *testing.T) {
	d := NewDeduplicator(time.Minute)
	calls := 0
	handler := func(_ context.Context, _ *kafkago.Message) error {
		calls++
		return nil
	}
	wrapped := d.Wrap(handler)
	msg := makeMessage("wrap-ok")

	assert.NoError(t, wrapped(context.Background(), &msg))
	assert.NoError(t, wrapped(context.Background(), &msg))
	assert.Equal(t, 1, calls, "handler should not be called again after success")
}

func TestDeduplicator_Wrap_InvalidJSON(t *testing.T) {
	d := NewDeduplicator(time.Minute)
	calls := 0
	handler := func(_ context.Context, _ *kafkago.Message) error {
		calls++
		return nil
	}
	wrapped := d.Wrap(handler)
	msg := kafkago.Message{Value: []byte("not-json")}

	require.NoError(t, wrapped(context.Background(), &msg))
	assert.Equal(t, 1, calls, "handler should be called for unparseable messages")
}

func TestDeduplicator_Wrap_EmptyID(t *testing.T) {
	d := NewDeduplicator(time.Minute)
	calls := 0
	handler := func(_ context.Context, _ *kafkago.Message) error {
		calls++
		return nil
	}
	wrapped := d.Wrap(handler)

	env := envelope{ID: "", CorrelationID: "corr-1", Type: "TestCommand"}
	b, _ := json.Marshal(env)
	msg := kafkago.Message{Value: b}

	require.NoError(t, wrapped(context.Background(), &msg))
	require.NoError(t, wrapped(context.Background(), &msg))
	assert.Equal(t, 2, calls, "messages with empty ID should not be deduplicated")
}
