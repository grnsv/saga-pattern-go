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

type mockDLQ struct {
	sends []dlqSend
	err   error
}

type dlqSend struct {
	topic   string
	retries int
	err     error
}

func (m *mockDLQ) Send(_ context.Context, originalTopic string, _ *kafkago.Message, handlerErr error, retries int) error {
	m.sends = append(m.sends, dlqSend{topic: originalTopic, retries: retries, err: handlerErr})
	return m.err
}

func (m *mockDLQ) Close() error { return nil }

func newTestConsumer(h MessageHandler) *Consumer {
	c := &Consumer{
		topic:    "test-topic",
		handler:  h,
		dlq:      &mockDLQ{},
		commitFn: func(_ context.Context, _ *kafkago.Message) {},
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

func TestProcessMessage_SendsToDLQOnRetryExhaustion(t *testing.T) {
	dlq := &mockDLQ{}
	c := &Consumer{
		topic:    "test-topic",
		handler:  func(_ context.Context, _ *kafkago.Message) error { return errHandler },
		dlq:      dlq,
		commitFn: func(_ context.Context, _ *kafkago.Message) {},
	}
	c.retryBudget.Store(retryBudgetCapacity)

	c.processMessage(context.Background(), &kafkago.Message{Key: []byte("corr-id")})

	require.Len(t, dlq.sends, 1)
	assert.Equal(t, "test-topic", dlq.sends[0].topic)
	assert.Equal(t, errHandler, dlq.sends[0].err)
	assert.Equal(t, maxRetries, dlq.sends[0].retries)
}

func TestProcessMessage_DoesNotCommitWhenDLQFails(t *testing.T) {
	committed := false
	dlq := &mockDLQ{err: errors.New("dlq unavailable")}
	c := &Consumer{
		topic:   "test-topic",
		handler: func(_ context.Context, _ *kafkago.Message) error { return errHandler },
		dlq:     dlq,
		commitFn: func(_ context.Context, _ *kafkago.Message) {
			committed = true
		},
	}
	c.retryBudget.Store(retryBudgetCapacity)

	c.processMessage(context.Background(), &kafkago.Message{})

	assert.False(t, committed)
}

func TestProcessMessage_CommitsOnSuccess(t *testing.T) {
	committed := false
	dlq := &mockDLQ{}
	c := &Consumer{
		topic:   "test-topic",
		handler: func(_ context.Context, _ *kafkago.Message) error { return nil },
		dlq:     dlq,
		commitFn: func(_ context.Context, _ *kafkago.Message) {
			committed = true
		},
	}
	c.retryBudget.Store(retryBudgetCapacity)

	c.processMessage(context.Background(), &kafkago.Message{})

	assert.True(t, committed)
	assert.Empty(t, dlq.sends)
}
