package kafka

import (
	"context"
	"errors"
	"testing"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errHandler = errors.New("handler error")

func newTestConsumer(h MessageHandler) *Consumer {
	c := &Consumer{
		topic:   "test-topic",
		handler: h,
	}
	c.retryBudget.Store(retryBudgetCapacity)
	return c
}

func TestProcessWithRetry_SuccessOnFirstCall(t *testing.T) {
	calls := 0
	c := newTestConsumer(func(_ context.Context, _ *kafkago.Message) error {
		calls++
		return nil
	})

	retries, err := c.processWithRetry(context.Background(), &kafkago.Message{})
	require.NoError(t, err)
	assert.Equal(t, 0, retries)
	assert.Equal(t, 1, calls)
}

func TestProcessWithRetry_SuccessOnSecondCall(t *testing.T) {
	calls := 0
	c := newTestConsumer(func(_ context.Context, _ *kafkago.Message) error {
		calls++
		if calls < 2 {
			return errHandler
		}
		return nil
	})

	retries, err := c.processWithRetry(context.Background(), &kafkago.Message{})
	require.NoError(t, err)
	assert.Equal(t, 1, retries)
	assert.Equal(t, 2, calls)
}

func TestProcessWithRetry_ExhaustsAllRetries(t *testing.T) {
	calls := 0
	c := newTestConsumer(func(_ context.Context, _ *kafkago.Message) error {
		calls++
		return errHandler
	})

	retries, err := c.processWithRetry(context.Background(), &kafkago.Message{})
	require.ErrorIs(t, err, errHandler)
	assert.Equal(t, maxRetries, retries)
	assert.Equal(t, maxRetries+1, calls) // initial attempt + maxRetries
}

func TestProcessWithRetry_BudgetExhaustedSkipsRetries(t *testing.T) {
	calls := 0
	c := newTestConsumer(func(_ context.Context, _ *kafkago.Message) error {
		calls++
		return errHandler
	})
	c.retryBudget.Store(0)

	retries, err := c.processWithRetry(context.Background(), &kafkago.Message{})
	require.ErrorIs(t, err, errHandler)
	assert.Equal(t, 0, retries)
	assert.Equal(t, 1, calls) // only initial attempt, no retries
}

func TestProcessWithRetry_ContextCancelDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before retry backoff starts

	c := newTestConsumer(func(_ context.Context, _ *kafkago.Message) error {
		return errHandler
	})

	_, err := c.processWithRetry(ctx, &kafkago.Message{})
	require.ErrorIs(t, err, context.Canceled)
}

func TestProcessWithRetry_BudgetRestoredOnSuccess(t *testing.T) {
	calls := 0
	c := newTestConsumer(func(_ context.Context, _ *kafkago.Message) error {
		calls++
		if calls < 2 {
			return errHandler
		}
		return nil
	})
	initialBudget := c.retryBudget.Load()

	_, err := c.processWithRetry(context.Background(), &kafkago.Message{})
	require.NoError(t, err)
	// budget restored: consumed 1 for retry, restored 1 on success
	assert.Equal(t, initialBudget, c.retryBudget.Load())
}
