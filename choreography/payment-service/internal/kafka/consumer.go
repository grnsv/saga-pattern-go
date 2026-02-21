package kafka

import (
	"context"
	"encoding/json"
	"log/slog"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/events"
)

// EventHandler processes a deserialized event.
type EventHandler func(ctx context.Context, event *events.Event) error

// Consumer reads messages from a Kafka topic and routes them by event type.
type Consumer struct {
	reader   *kafkago.Reader
	topic    string
	handlers map[events.EventType]EventHandler
	tracer   trace.Tracer
}

// NewConsumer creates a new Kafka consumer with event-type routing.
func NewConsumer(brokers []string, topic, groupID string, handlers map[events.EventType]EventHandler) *Consumer {
	return &Consumer{
		reader: kafkago.NewReader(kafkago.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
		topic:    topic,
		handlers: handlers,
		tracer:   otel.Tracer("kafka.consumer"),
	}
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

		var event events.Event
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("failed to unmarshal event", "topic", c.topic, "error", err)
			c.commit(ctx, &msg)
			continue
		}

		// Extract propagated trace context from Kafka headers.
		carrier := HeadersCarrier{Headers: &msg.Headers}
		msgCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

		handler, ok := c.handlers[event.Type]
		if !ok {
			slog.Warn("unknown event type", "topic", c.topic, "type", event.Type)
			c.commit(ctx, &msg)
			continue
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

		if err := handler(msgCtx, &event); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			slog.Error("failed to handle event",
				"topic", c.topic,
				"type", event.Type,
				"correlationId", event.CorrelationID,
				"error", err,
			)
			// TODO: retry + DLQ
		} else {
			slog.Debug("event handled",
				"topic", c.topic,
				"type", event.Type,
				"correlationId", event.CorrelationID,
			)
		}

		span.End()
		c.commit(ctx, &msg)
	}
}

func (c *Consumer) commit(ctx context.Context, msg *kafkago.Message) {
	if err := c.reader.CommitMessages(ctx, *msg); err != nil {
		slog.Error("failed to commit message", "topic", c.topic, "error", err)
	}
}

// Close closes the underlying Kafka reader.
func (c *Consumer) Close() error {
	return c.reader.Close()
}
