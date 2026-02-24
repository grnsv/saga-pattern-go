package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/events"
)

const (
	maxRetries          = 3
	baseDelay           = 100 * time.Millisecond
	retryBudgetCapacity = 100
)

// EventHandler processes a deserialized event.
type EventHandler func(ctx context.Context, event *events.Event) error

// Consumer reads messages from a Kafka topic and routes them by event type.
type Consumer struct {
	reader      *kafkago.Reader
	dlqWriter   *kafkago.Writer
	topic       string
	handlers    map[events.EventType]EventHandler
	tracer      trace.Tracer
	retryBudget atomic.Int64
}

// NewConsumer creates a new Kafka consumer with event-type routing.
func NewConsumer(brokers []string, topic, groupID string, handlers map[events.EventType]EventHandler) *Consumer {
	c := &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		dlqWriter: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Balancer:     &kafkago.LeastBytes{},
			RequiredAcks: kafkago.RequireAll,
		},
		topic:    topic,
		handlers: handlers,
		tracer:   otel.Tracer("kafka.consumer"),
	}
	c.retryBudget.Store(retryBudgetCapacity)
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
			slog.Error("failed to fetch message", "topic", c.topic, "error", err)
			continue
		}

		if err := c.processMessage(ctx, &msg); err != nil {
			return err
		}
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg *kafkago.Message) error {
	var event events.Event
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		slog.Error("failed to unmarshal event", "topic", c.topic, "error", err)
		c.commit(ctx, msg)
		return nil
	}

	// Extract propagated trace context from Kafka headers.
	carrier := HeadersCarrier{Headers: &msg.Headers}
	msgCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

	handler, ok := c.handlers[event.Type]
	if !ok {
		slog.Warn("unknown event type", "topic", c.topic, "type", event.Type)
		c.commit(ctx, msg)
		return nil
	}

	msgCtx, span := c.tracer.Start(msgCtx, c.topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(c.topic),
			attribute.String("messaging.event_type", string(event.Type)),
			attribute.String("messaging.correlation_id", event.CorrelationID),
		),
	)
	defer span.End()

	retries, err := c.processWithRetry(msgCtx, &event, handler)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		slog.Error("event handler failed after retries",
			"topic", c.topic,
			"type", event.Type,
			"correlationId", event.CorrelationID,
			"retries", retries,
			"error", err,
		)
		if dlqErr := c.sendToDLQ(msgCtx, msg, err, retries); dlqErr != nil {
			slog.Error("DLQ unavailable, offset not committed - will retry on restart",
				"topic", c.topic,
				"correlationId", string(msg.Key),
				"error", dlqErr,
			)
			return nil
		}
	} else {
		span.SetStatus(codes.Ok, "")
		slog.Debug("event handled",
			"topic", c.topic,
			"type", event.Type,
			"correlationId", event.CorrelationID,
		)
	}

	c.commit(ctx, msg)
	return nil
}

func (c *Consumer) processWithRetry(ctx context.Context, event *events.Event, handler EventHandler) (int, error) {
	err := handler(ctx, event)
	if err == nil {
		c.addBudget(1)
		return 0, nil
	}

	for retry := 1; retry <= maxRetries; retry++ {
		if !c.consumeBudget() {
			slog.Warn("retry budget exhausted, skipping retries",
				"topic", c.topic,
				"type", event.Type,
				"correlationId", event.CorrelationID,
				"error", err,
			)
			return retry - 1, err
		}

		delay := baseDelay * (1 << (retry - 1))
		jitter := time.Duration(rand.Int64N(max(int64(delay)/2, 1)))
		slog.Warn("retrying event handler",
			"topic", c.topic,
			"type", event.Type,
			"correlationId", event.CorrelationID,
			"retry", retry,
			"delay", delay+jitter,
			"error", err,
		)
		trace.SpanFromContext(ctx).AddEvent("retry",
			trace.WithAttributes(
				attribute.Int("retry", retry),
				attribute.String("error", err.Error()),
			),
		)
		t := time.NewTimer(delay + jitter)
		select {
		case <-t.C:
		case <-ctx.Done():
			if !t.Stop() {
				<-t.C
			}
			return retry, ctx.Err()
		}

		err = handler(ctx, event)
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

func (c *Consumer) sendToDLQ(ctx context.Context, msg *kafkago.Message, handlerErr error, retries int) error {
	dlqTopic := c.topic + ".dlq"

	headers := make([]kafkago.Header, len(msg.Headers), len(msg.Headers)+4)
	copy(headers, msg.Headers)
	headers = append(headers,
		kafkago.Header{Key: "original-topic", Value: []byte(c.topic)},
		kafkago.Header{Key: "error", Value: []byte(handlerErr.Error())},
		kafkago.Header{Key: "failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		kafkago.Header{Key: "retry-count", Value: []byte(strconv.Itoa(retries))},
	)

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := c.dlqWriter.WriteMessages(ctx, kafkago.Message{
		Topic:   dlqTopic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}); err != nil {
		slog.Error("failed to write to DLQ", "dlq", dlqTopic, "correlationId", string(msg.Key), "error", err)
		return err
	}
	slog.Warn("message sent to DLQ",
		"dlq", dlqTopic,
		"originalTopic", c.topic,
		"correlationId", string(msg.Key),
		"error", handlerErr.Error(),
	)
	return nil
}

func (c *Consumer) commit(ctx context.Context, msg *kafkago.Message) {
	if err := c.reader.CommitMessages(ctx, *msg); err != nil {
		slog.Error("failed to commit message", "topic", c.topic, "error", err)
	}
}

// Close closes the underlying Kafka reader and DLQ writer.
func (c *Consumer) Close() error {
	return errors.Join(c.reader.Close(), c.dlqWriter.Close())
}
