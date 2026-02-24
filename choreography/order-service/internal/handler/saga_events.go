package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/model"
)

// SagaEventHandler handles saga outcome events and updates order status accordingly.
type SagaEventHandler struct {
	store OrderStore
}

// NewSagaEventHandler creates a new SagaEventHandler.
func NewSagaEventHandler(store OrderStore) *SagaEventHandler {
	return &SagaEventHandler{store: store}
}

// HandleInventoryReserved processes InventoryReserved events, marking the order as CONFIRMED.
func (h *SagaEventHandler) HandleInventoryReserved(ctx context.Context, event *events.Event) error {
	var payload events.InventoryResultPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal InventoryReserved payload: %w", err)
	}

	slog.InfoContext(ctx, "processing InventoryReserved",
		"correlationId", event.CorrelationID,
		"orderId", payload.OrderID,
	)

	return h.updateOrderStatus(ctx, payload.OrderID, model.OrderConfirmed, event.CorrelationID)
}

// HandlePaymentFailed processes PaymentFailed events, marking the order as CANCELLED.
func (h *SagaEventHandler) HandlePaymentFailed(ctx context.Context, event *events.Event) error {
	var payload events.PaymentResultPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal PaymentFailed payload: %w", err)
	}

	slog.InfoContext(ctx, "processing PaymentFailed",
		"correlationId", event.CorrelationID,
		"orderId", payload.OrderID,
		"reason", payload.Reason,
	)

	return h.updateOrderStatus(ctx, payload.OrderID, model.OrderCancelled, event.CorrelationID)
}

// HandlePaymentRolledBack processes PaymentRolledBack events, marking the order as CANCELLED.
func (h *SagaEventHandler) HandlePaymentRolledBack(ctx context.Context, event *events.Event) error {
	var payload events.PaymentResultPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal PaymentRolledBack payload: %w", err)
	}

	slog.InfoContext(ctx, "processing PaymentRolledBack",
		"correlationId", event.CorrelationID,
		"orderId", payload.OrderID,
		"reason", payload.Reason,
	)

	return h.updateOrderStatus(ctx, payload.OrderID, model.OrderCancelled, event.CorrelationID)
}

func (h *SagaEventHandler) updateOrderStatus(ctx context.Context, orderID string, status model.OrderStatus, correlationID string) error {
	order, err := h.store.Get(orderID)
	if err != nil {
		return fmt.Errorf("get order %s: %w", orderID, err)
	}

	order.Status = status

	if err := h.store.Update(order); err != nil {
		return fmt.Errorf("update order %s: %w", orderID, err)
	}

	slog.InfoContext(ctx, "order status updated",
		"correlationId", correlationID,
		"orderId", orderID,
		"status", status,
	)

	return nil
}
