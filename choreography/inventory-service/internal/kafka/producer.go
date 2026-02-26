package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/events"
)

// Producer wraps a kafka.Writer for publishing events.
type Producer struct {
	writer *kafkago.Writer
	tracer trace.Tracer
}

// NewProducer creates a new Kafka producer.
func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Balancer:     &kafkago.LeastBytes{},
			RequiredAcks: kafkago.RequireAll,
		},
		tracer: otel.Tracer("kafka.producer"),
	}
}

// Publish sends an event to the specified Kafka topic.
func (p *Producer) Publish(ctx context.Context, topic, key string, event *events.Event) error {
	ctx, span := p.tracer.Start(ctx, topic+" send",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(topic),
			attribute.String("messaging.event_type", string(event.Type)),
			attribute.String("messaging.correlation_id", event.CorrelationID),
		),
	)
	defer span.End()

	var headers []kafkago.Header
	otel.GetTextMapPropagator().Inject(ctx, HeadersCarrier{Headers: &headers})

	data, err := json.Marshal(event)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("marshal event: %w", err)
	}

	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   data,
		Headers: headers,
	}); err != nil {
		span.RecordError(err)
		return fmt.Errorf("publish to %s: %w", topic, err)
	}

	return nil
}

// Close closes the underlying Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
