package handler

import (
	"context"
	"encoding/json"
	"log/slog"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/grnsv/saga-pattern-go/orchestration/order-api/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/order-api/internal/model"
)

// SagaEventHandler updates order status based on saga outcome events.
type SagaEventHandler struct {
	store OrderStore
}

// NewSagaEventHandler creates a SagaEventHandler.
func NewSagaEventHandler(s OrderStore) *SagaEventHandler {
	return &SagaEventHandler{store: s}
}

// Handle processes a message from the saga-events topic.
// Signature matches kafka.MessageHandler (value receiver).
func (h *SagaEventHandler) Handle(ctx context.Context, msg kafkago.Message) error { //nolint:gocritic // value type required by kafka.MessageHandler contract
	var evt messages.Event
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		slog.WarnContext(ctx, "failed to unmarshal saga event", "error", err)
		return nil
	}

	switch evt.Type {
	case messages.EvtSagaCompleted:
		return h.updateOrderStatus(ctx, &evt, model.OrderConfirmed)
	case messages.EvtSagaFailed:
		return h.updateOrderStatus(ctx, &evt, model.OrderCancelled)
	default:
		slog.WarnContext(ctx, "unknown saga event type",
			"type", evt.Type, "correlationId", evt.CorrelationID)
		return nil
	}
}

func (h *SagaEventHandler) updateOrderStatus(ctx context.Context, evt *messages.Event, status model.OrderStatus) error {
	var payload messages.SagaResultPayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		slog.WarnContext(ctx, "failed to unmarshal SagaResultPayload",
			"correlationId", evt.CorrelationID, "error", err)
		return nil
	}

	order, err := h.store.Get(payload.OrderID)
	if err != nil {
		slog.WarnContext(ctx, "order not found for saga event",
			"correlationId", evt.CorrelationID, "orderId", payload.OrderID)
		return nil //nolint:nilerr // soft failure: unknown order is not retryable
	}

	order.Status = status
	if err := h.store.Update(order); err != nil {
		return err
	}

	slog.InfoContext(ctx, "order status updated",
		"orderId", payload.OrderID,
		"status", status,
		"correlationId", evt.CorrelationID,
	)
	return nil
}
