package kafka

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// KafkaDLQPublisher writes failed messages to a Kafka DLQ topic.
type KafkaDLQPublisher struct {
	writer *kafkago.Writer
}

// NewDLQPublisher creates a publisher that sends failed messages to <topic>.dlq.
func NewDLQPublisher(brokers []string) *KafkaDLQPublisher {
	return &KafkaDLQPublisher{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(brokers...),
			Balancer:     &kafkago.LeastBytes{},
			RequiredAcks: kafkago.RequireAll,
		},
	}
}

// Send publishes msg to <originalTopic>.dlq with diagnostic headers.
func (p *KafkaDLQPublisher) Send(ctx context.Context, originalTopic string, msg *kafkago.Message, handlerErr error, retries int) error {
	dlqTopic := originalTopic + ".dlq"

	headers := make([]kafkago.Header, len(msg.Headers), len(msg.Headers)+4)
	copy(headers, msg.Headers)
	headers = append(headers,
		kafkago.Header{Key: "original-topic", Value: []byte(originalTopic)},
		kafkago.Header{Key: "error", Value: []byte(handlerErr.Error())},
		kafkago.Header{Key: "failed-at", Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
		kafkago.Header{Key: "retry-count", Value: []byte(strconv.Itoa(retries))},
	)

	dlqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	if err := p.writer.WriteMessages(dlqCtx, kafkago.Message{
		Topic:   dlqTopic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	}); err != nil {
		slog.ErrorContext(ctx, "failed to write to DLQ",
			"dlq", dlqTopic,
			"correlationId", string(msg.Key),
			"error", err,
		)
		return err
	}

	slog.WarnContext(ctx, "message sent to DLQ",
		"dlq", dlqTopic,
		"originalTopic", originalTopic,
		"correlationId", string(msg.Key),
		"error", handlerErr.Error(),
	)
	return nil
}

// Close closes the underlying Kafka writer.
func (p *KafkaDLQPublisher) Close() error {
	return p.writer.Close()
}
