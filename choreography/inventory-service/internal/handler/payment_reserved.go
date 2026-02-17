package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/model"
)

// InventoryStore defines the interface for inventory persistence.
type InventoryStore interface {
	Create(reservation *model.InventoryReservation) (string, error)
}

// EventPublisher defines the interface for publishing events to Kafka.
type EventPublisher interface {
	Publish(ctx context.Context, topic, key string, event *events.Event) error
}

// PaymentReservedHandler handles PaymentReserved events by reserving inventory.
type PaymentReservedHandler struct {
	store       InventoryStore
	producer    EventPublisher
	successRate float64
}

// NewPaymentReservedHandler creates a new PaymentReserved event handler.
func NewPaymentReservedHandler(store InventoryStore, producer EventPublisher, successRate float64) *PaymentReservedHandler {
	return &PaymentReservedHandler{
		store:       store,
		producer:    producer,
		successRate: successRate,
	}
}

// Handle processes a PaymentReserved event.
func (h *PaymentReservedHandler) Handle(ctx context.Context, event *events.Event) error {
	var payload events.PaymentResultPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal PaymentReserved payload: %w", err)
	}

	slog.Info("processing PaymentReserved",
		"correlationId", event.CorrelationID,
		"orderId", payload.OrderID,
	)

	if rand.Float64() >= h.successRate {
		return h.publishInventoryFailed(ctx, event, &payload)
	}

	return h.publishInventoryReserved(ctx, event, &payload)
}

func (h *PaymentReservedHandler) publishInventoryReserved(ctx context.Context, event *events.Event, payload *events.PaymentResultPayload) error {
	reservation := &model.InventoryReservation{
		OrderID: payload.OrderID,
		Item:    payload.Item,
		Qty:     payload.Qty,
		Status:  model.ReservationConfirmed,
	}

	id, err := h.store.Create(reservation)
	if err != nil {
		return fmt.Errorf("create reservation: %w", err)
	}

	resultPayload, err := json.Marshal(events.InventoryResultPayload{
		OrderID:       payload.OrderID,
		ReservationID: id,
		Item:          payload.Item,
		Qty:           payload.Qty,
	})
	if err != nil {
		return fmt.Errorf("marshal InventoryReserved payload: %w", err)
	}

	resultEvent := &events.Event{
		ID:            uuid.NewString(),
		CorrelationID: event.CorrelationID,
		Type:          events.InventoryReserved,
		Timestamp:     time.Now(),
		Payload:       resultPayload,
	}

	if err := h.producer.Publish(ctx, "inventory-events", event.CorrelationID, resultEvent); err != nil {
		return fmt.Errorf("publish InventoryReserved: %w", err)
	}

	slog.Info("inventory reserved",
		"correlationId", event.CorrelationID,
		"reservationId", id,
		"orderId", payload.OrderID,
	)

	return nil
}

func (h *PaymentReservedHandler) publishInventoryFailed(ctx context.Context, event *events.Event, payload *events.PaymentResultPayload) error {
	resultPayload, err := json.Marshal(events.InventoryResultPayload{
		OrderID: payload.OrderID,
		Item:    payload.Item,
		Qty:     payload.Qty,
		Reason:  "insufficient inventory",
	})
	if err != nil {
		return fmt.Errorf("marshal InventoryFailed payload: %w", err)
	}

	resultEvent := &events.Event{
		ID:            uuid.NewString(),
		CorrelationID: event.CorrelationID,
		Type:          events.InventoryFailed,
		Timestamp:     time.Now(),
		Payload:       resultPayload,
	}

	if err := h.producer.Publish(ctx, "inventory-events", event.CorrelationID, resultEvent); err != nil {
		return fmt.Errorf("publish InventoryFailed: %w", err)
	}

	slog.Info("inventory reservation failed",
		"correlationId", event.CorrelationID,
		"orderId", payload.OrderID,
		"reason", "insufficient inventory",
	)

	return nil
}
