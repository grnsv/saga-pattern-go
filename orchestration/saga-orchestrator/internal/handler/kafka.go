package handler

import (
	"context"
	"encoding/json"
	"log/slog"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/messages"
)

// SagaOrchestrator is the narrow interface that the Kafka handler requires
// from the saga.Orchestrator.
type SagaOrchestrator interface {
	StartSaga(ctx context.Context, correlationID string, payload messages.StartSagaPayload) error
	HandleEvent(ctx context.Context, event *messages.Event) error
}

// KafkaHandler routes Kafka messages to the saga orchestrator.
type KafkaHandler struct {
	orchestrator SagaOrchestrator
}

// NewKafkaHandler creates a KafkaHandler backed by the given orchestrator.
func NewKafkaHandler(o SagaOrchestrator) *KafkaHandler {
	return &KafkaHandler{orchestrator: o}
}

// HandleCommand processes messages from the saga-commands topic.
// It currently handles the StartSaga command type.
func (h *KafkaHandler) HandleCommand(ctx context.Context, msg *kafkago.Message) error {
	var cmd messages.Command
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		slog.WarnContext(ctx, "failed to unmarshal command", "error", err)
		return nil
	}

	switch cmd.Type {
	case messages.CmdStartSaga:
		var payload messages.StartSagaPayload
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			slog.WarnContext(ctx, "failed to unmarshal StartSaga payload",
				"correlationId", cmd.CorrelationID, "error", err)
			return nil
		}
		slog.InfoContext(ctx, "received StartSaga",
			"correlationId", cmd.CorrelationID, "orderId", payload.OrderID)
		return h.orchestrator.StartSaga(ctx, cmd.CorrelationID, payload)

	default:
		slog.WarnContext(ctx, "unknown command type",
			"type", cmd.Type, "correlationId", cmd.CorrelationID)
		return nil
	}
}

// HandleEvent processes messages from payment-events and inventory-events topics.
func (h *KafkaHandler) HandleEvent(ctx context.Context, msg *kafkago.Message) error {
	var evt messages.Event
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		slog.WarnContext(ctx, "failed to unmarshal event", "error", err)
		return nil
	}

	slog.InfoContext(ctx, "received event",
		"type", evt.Type, "correlationId", evt.CorrelationID)

	return h.orchestrator.HandleEvent(ctx, &evt)
}
