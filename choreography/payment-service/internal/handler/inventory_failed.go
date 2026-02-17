package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/model"
)

// InventoryFailedHandler handles InventoryFailed events by rolling back payment.
type InventoryFailedHandler struct {
	store    PaymentStore
	producer EventPublisher
}

// NewInventoryFailedHandler creates a new InventoryFailed event handler.
func NewInventoryFailedHandler(store PaymentStore, producer EventPublisher) *InventoryFailedHandler {
	return &InventoryFailedHandler{
		store:    store,
		producer: producer,
	}
}

// Handle processes an InventoryFailed event by compensating the payment.
func (h *InventoryFailedHandler) Handle(ctx context.Context, event *events.Event) error {
	var payload events.InventoryResultPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal InventoryFailed payload: %w", err)
	}

	slog.Info("processing InventoryFailed",
		"correlationId", event.CorrelationID,
		"orderId", payload.OrderID,
		"reason", payload.Reason,
	)

	payment, err := h.store.GetByOrderID(payload.OrderID)
	if err != nil {
		return fmt.Errorf("get payment for order %s: %w", payload.OrderID, err)
	}

	if err := h.store.UpdateStatus(payment.ID, model.PaymentRolledBack); err != nil {
		return fmt.Errorf("update payment %s status: %w", payment.ID, err)
	}

	resultPayload, err := json.Marshal(events.PaymentResultPayload{
		OrderID:   payload.OrderID,
		PaymentID: payment.ID,
		Amount:    payment.Amount,
		Reason:    "inventory reservation failed: " + payload.Reason,
	})
	if err != nil {
		return fmt.Errorf("marshal PaymentRolledBack payload: %w", err)
	}

	resultEvent := &events.Event{
		ID:            uuid.NewString(),
		CorrelationID: event.CorrelationID,
		Type:          events.PaymentRolledBack,
		Timestamp:     time.Now(),
		Payload:       resultPayload,
	}

	if err := h.producer.Publish(ctx, "payment-events", event.CorrelationID, resultEvent); err != nil {
		return fmt.Errorf("publish PaymentRolledBack: %w", err)
	}

	slog.Info("payment rolled back",
		"correlationId", event.CorrelationID,
		"paymentId", payment.ID,
		"orderId", payload.OrderID,
	)

	return nil
}
