package kafka

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const (
	maxRetries          = 3
	baseDelay           = 100 * time.Millisecond
	retryBudgetCapacity = 100
)

// MessageHandler processes a raw Kafka message.
type MessageHandler func(ctx context.Context, msg *kafkago.Message) error

// dlqPublisher sends failed messages to a dead-letter queue topic.
type dlqPublisher interface {
	Send(ctx context.Context, originalTopic string, msg *kafkago.Message, handlerErr error, retries int) error
	Close() error
}

// Consumer reads messages from a Kafka topic and delegates processing to a MessageHandler.
type Consumer struct {
	reader      *kafkago.Reader
	dlq         dlqPublisher
	topic       string
	handler     MessageHandler
	retryBudget atomic.Int64
	commitFn    func(ctx context.Context, msg *kafkago.Message)
}

// NewConsumer creates a new Kafka consumer.
func NewConsumer(brokers []string, topic, groupID string, handler MessageHandler) *Consumer {
	c := &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		dlq:     NewDLQPublisher(brokers),
		topic:   topic,
		handler: handler,
	}
	c.retryBudget.Store(retryBudgetCapacity)
	c.commitFn = func(ctx context.Context, msg *kafkago.Message) {
		if err := c.reader.CommitMessages(ctx, *msg); err != nil {
			slog.ErrorContext(ctx, "failed to commit message", "topic", c.topic, "error", err)
		}
	}
	return c
}

// Start begins consuming messages. It blocks until the context is cancelled.
func (c *Consumer) Start(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.ErrorContext(ctx, "failed to fetch message", "topic", c.topic, "error", err)
			continue
		}

		c.processMessage(ctx, &msg)
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *kafkago.Message) {
	retries, err := c.processWithRetry(ctx, msg)
	if err != nil {
		slog.ErrorContext(ctx, "message handler failed after retries",
			"topic", c.topic,
			"correlationId", string(msg.Key),
			"retries", retries,
			"error", err,
		)
		if dlqErr := c.dlq.Send(ctx, c.topic, msg, err, retries); dlqErr != nil {
			slog.ErrorContext(ctx, "DLQ unavailable, offset not committed - will retry on restart",
				"topic", c.topic,
				"correlationId", string(msg.Key),
				"error", dlqErr,
			)
			return
		}
	}

	c.commitFn(ctx, msg)
}

func (c *Consumer) processWithRetry(ctx context.Context, msg *kafkago.Message) (int, error) {
	err := c.handler(ctx, msg)
	if err == nil {
		c.addBudget(1)
		return 0, nil
	}

	for retry := 1; retry <= maxRetries; retry++ {
		if !c.consumeBudget() {
			slog.WarnContext(ctx, "retry budget exhausted, skipping retries",
				"topic", c.topic,
				"correlationId", string(msg.Key),
				"error", err,
			)
			return retry - 1, err
		}

		backoff := baseDelay * (1 << (retry - 1))
		jitter := time.Duration(rand.Int64N(int64(backoff) / 2))
		slog.WarnContext(ctx, "retrying message handler",
			"topic", c.topic,
			"correlationId", string(msg.Key),
			"retry", retry,
			"delay", backoff+jitter,
			"error", err,
		)

		t := time.NewTimer(backoff + jitter)
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return retry, ctx.Err()
		}

		err = c.handler(ctx, msg)
		if err == nil {
			c.addBudget(1)
			return retry, nil
		}
	}

	return maxRetries, err
}

func (c *Consumer) consumeBudget() bool {
	for {
		v := c.retryBudget.Load()
		if v <= 0 {
			return false
		}
		if c.retryBudget.CompareAndSwap(v, v-1) {
			return true
		}
	}
}

func (c *Consumer) addBudget(n int64) {
	for {
		v := c.retryBudget.Load()
		if v >= retryBudgetCapacity {
			return
		}
		if c.retryBudget.CompareAndSwap(v, min(v+n, retryBudgetCapacity)) {
			return
		}
	}
}

// Close closes the underlying Kafka reader and DLQ publisher.
func (c *Consumer) Close() error {
	return errors.Join(c.reader.Close(), c.dlq.Close())
}
