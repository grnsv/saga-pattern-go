package kafka

import (
	"context"
	"fmt"

	kafkago "github.com/segmentio/kafka-go"
)

// Producer wraps a kafka.Writer for publishing messages.
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

// Publish sends a raw payload to the specified Kafka topic.
func (p *Producer) Publish(ctx context.Context, topic, key string, payload []byte) error {
	if err := p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: payload,
	}); err != nil {
		return fmt.Errorf("publish to %s: %w", topic, err)
	}
	return nil
}

// Close closes the underlying Kafka writer.
func (p *Producer) Close() error {
	return p.writer.Close()
}
