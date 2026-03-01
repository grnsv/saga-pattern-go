package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/model"
)

const inventoryEventsTopic = "inventory-events"

// EventPublisher publishes serialised events to a Kafka topic.
type EventPublisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// InventoryStore defines the storage operations required by CommandHandler.
type InventoryStore interface {
	Create(reservation *model.InventoryReservation) error
	Get(correlationID string) (*model.InventoryReservation, error)
}

// CommandHandler routes incoming Kafka inventory commands to business logic.
type CommandHandler struct {
	store       InventoryStore
	publisher   EventPublisher
	successRate float64
}

// NewCommandHandler creates a CommandHandler.
func NewCommandHandler(s InventoryStore, p EventPublisher, successRate float64) *CommandHandler {
	return &CommandHandler{store: s, publisher: p, successRate: successRate}
}

// Handle deserialises a Kafka message and dispatches it to the correct handler.
func (h *CommandHandler) Handle(ctx context.Context, msg *kafkago.Message) error {
	var cmd messages.Command
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		slog.WarnContext(ctx, "failed to unmarshal command", "error", err)
		return nil
	}

	switch cmd.Type {
	case messages.CmdReserveInventory:
		return h.reserveInventory(ctx, &cmd)
	default:
		slog.WarnContext(ctx, "unknown command type", "type", cmd.Type, "correlationId", cmd.CorrelationID)
		return nil
	}
}

func (h *CommandHandler) reserveInventory(ctx context.Context, cmd *messages.Command) error {
	var payload messages.ReserveInventoryPayload
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
		slog.WarnContext(ctx, "failed to unmarshal ReserveInventory payload",
			"correlationId", cmd.CorrelationID, "error", err)
		return nil
	}

	slog.InfoContext(ctx, "processing ReserveInventory",
		"correlationId", cmd.CorrelationID, "orderId", payload.OrderID, "item", payload.Item, "qty", payload.Qty)

	// Idempotency: if already reserved, re-publish success without re-reserving.
	if existing, err := h.store.Get(cmd.CorrelationID); err == nil {
		slog.InfoContext(ctx, "inventory already reserved, re-publishing event", "correlationId", cmd.CorrelationID)
		return h.publishEvent(ctx, cmd.CorrelationID, messages.EvtInventoryReserved, messages.InventoryResultPayload{
			OrderID:       payload.OrderID,
			ReservationID: existing.ID,
			Item:          existing.Item,
			Qty:           existing.Qty,
		})
	}

	if rand.Float64() >= h.successRate {
		slog.InfoContext(ctx, "inventory reservation failed (simulation)", "correlationId", cmd.CorrelationID)
		return h.publishEvent(ctx, cmd.CorrelationID, messages.EvtInventoryFailed, messages.InventoryResultPayload{
			OrderID: payload.OrderID,
			Item:    payload.Item,
			Qty:     payload.Qty,
			Reason:  "out of stock",
		})
	}

	reservation := &model.InventoryReservation{
		ID:            uuid.NewString(),
		CorrelationID: cmd.CorrelationID,
		OrderID:       payload.OrderID,
		Item:          payload.Item,
		Qty:           payload.Qty,
	}
	if err := h.store.Create(reservation); err != nil {
		return err
	}

	slog.InfoContext(ctx, "inventory reserved",
		"correlationId", cmd.CorrelationID, "reservationId", reservation.ID, "item", reservation.Item, "qty", reservation.Qty)
	return h.publishEvent(ctx, cmd.CorrelationID, messages.EvtInventoryReserved, messages.InventoryResultPayload{
		OrderID:       payload.OrderID,
		ReservationID: reservation.ID,
		Item:          reservation.Item,
		Qty:           reservation.Qty,
	})
}

func (h *CommandHandler) publishEvent(ctx context.Context, correlationID string, evtType messages.EventType, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	event := messages.Event{
		ID:            uuid.NewString(),
		CorrelationID: correlationID,
		Type:          evtType,
		Timestamp:     time.Now().UTC(),
		Payload:       data,
	}
	envelope, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return h.publisher.Publish(ctx, inventoryEventsTopic, correlationID, envelope)
}
