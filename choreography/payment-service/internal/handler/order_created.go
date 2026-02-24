package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/model"
)

// PaymentStore defines the interface for payment persistence.
type PaymentStore interface {
	Create(payment *model.Payment) (string, error)
	Get(id string) (*model.Payment, error)
	GetByOrderID(orderID string) (*model.Payment, error)
	UpdateStatus(id string, status model.PaymentStatus) error
}

// EventPublisher defines the interface for publishing events to Kafka.
type EventPublisher interface {
	Publish(ctx context.Context, topic, key string, event *events.Event) error
}

// OrderCreatedHandler handles OrderCreated events by reserving payment.
type OrderCreatedHandler struct {
	store       PaymentStore
	producer    EventPublisher
	successRate float64
}

// NewOrderCreatedHandler creates a new OrderCreated event handler.
func NewOrderCreatedHandler(store PaymentStore, producer EventPublisher, successRate float64) *OrderCreatedHandler {
	return &OrderCreatedHandler{
		store:       store,
		producer:    producer,
		successRate: successRate,
	}
}

// Handle processes an OrderCreated event.
func (h *OrderCreatedHandler) Handle(ctx context.Context, event *events.Event) error {
	var payload events.OrderCreatedPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal OrderCreated payload: %w", err)
	}

	slog.InfoContext(ctx, "processing OrderCreated",
		"correlationId", event.CorrelationID,
		"orderId", payload.OrderID,
		"amount", payload.Amount,
	)

	if rand.Float64() >= h.successRate {
		return h.publishPaymentFailed(ctx, event, payload)
	}

	return h.publishPaymentReserved(ctx, event, payload)
}

func (h *OrderCreatedHandler) publishPaymentReserved(ctx context.Context, event *events.Event, payload events.OrderCreatedPayload) error {
	payment := &model.Payment{
		OrderID: payload.OrderID,
		Amount:  payload.Amount,
		Status:  model.PaymentReserved,
	}

	id, err := h.store.Create(payment)
	if err != nil {
		return fmt.Errorf("create payment: %w", err)
	}

	resultPayload, err := json.Marshal(events.PaymentResultPayload{
		OrderID:   payload.OrderID,
		PaymentID: id,
		Amount:    payload.Amount,
		Item:      payload.Item,
		Qty:       payload.Qty,
	})
	if err != nil {
		return fmt.Errorf("marshal PaymentReserved payload: %w", err)
	}

	resultEvent := &events.Event{
		ID:            uuid.NewString(),
		CorrelationID: event.CorrelationID,
		Type:          events.PaymentReserved,
		Timestamp:     time.Now(),
		Payload:       resultPayload,
	}

	if err := h.producer.Publish(ctx, "payment-events", event.CorrelationID, resultEvent); err != nil {
		return fmt.Errorf("publish PaymentReserved: %w", err)
	}

	slog.InfoContext(ctx, "payment reserved",
		"correlationId", event.CorrelationID,
		"paymentId", id,
		"orderId", payload.OrderID,
	)

	return nil
}

func (h *OrderCreatedHandler) publishPaymentFailed(ctx context.Context, event *events.Event, payload events.OrderCreatedPayload) error {
	resultPayload, err := json.Marshal(events.PaymentResultPayload{
		OrderID: payload.OrderID,
		Amount:  payload.Amount,
		Item:    payload.Item,
		Qty:     payload.Qty,
		Reason:  "payment declined",
	})
	if err != nil {
		return fmt.Errorf("marshal PaymentFailed payload: %w", err)
	}

	resultEvent := &events.Event{
		ID:            uuid.NewString(),
		CorrelationID: event.CorrelationID,
		Type:          events.PaymentFailed,
		Timestamp:     time.Now(),
		Payload:       resultPayload,
	}

	if err := h.producer.Publish(ctx, "payment-events", event.CorrelationID, resultEvent); err != nil {
		return fmt.Errorf("publish PaymentFailed: %w", err)
	}

	slog.InfoContext(ctx, "payment failed",
		"correlationId", event.CorrelationID,
		"orderId", payload.OrderID,
		"reason", "payment declined",
	)

	return nil
}
