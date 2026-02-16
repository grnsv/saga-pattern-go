package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/events"
)

// EventPublisher is the interface for publishing events to Kafka.
type EventPublisher interface {
	Publish(ctx context.Context, topic, key string, event *events.Event) error
}

// Producer wraps a kafka.Writer for publishing events.
type Producer struct {
	writer *kafkago.Writer
}

// NewProducer creates a new Kafka producer.
func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Balancer:     &kafkago.LeastBytes{},
			RequiredAcks: kafkago.RequireAll,
		},
	}
}

// Publish sends an event to the specified Kafka topic.
func (p *Producer) Publish(ctx context.Context, topic, key string, event *events.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
	}); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}

	return nil
}

// Close closes the underlying Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
