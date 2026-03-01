package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"

	"github.com/grnsv/saga-pattern-go/orchestration/payment-service/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/payment-service/internal/model"
)

const paymentEventsTopic = "payment-events"

// EventPublisher publishes serialised events to a Kafka topic.
type EventPublisher interface {
	Publish(ctx context.Context, topic, key string, payload []byte) error
}

// PaymentStore defines the storage operations required by CommandHandler.
type PaymentStore interface {
	Create(payment *model.Payment) error
	Get(correlationID string) (*model.Payment, error)
	// Delete removes a reservation. It is a no-op when the payment is not found.
	Delete(correlationID string) error
}

// CommandHandler routes incoming Kafka payment commands to business logic.
type CommandHandler struct {
	store       PaymentStore
	publisher   EventPublisher
	successRate float64
}

// NewCommandHandler creates a CommandHandler.
func NewCommandHandler(s PaymentStore, p EventPublisher, successRate float64) *CommandHandler {
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
	case messages.CmdReservePayment:
		return h.reservePayment(ctx, &cmd)
	case messages.CmdCancelPayment:
		return h.cancelPayment(ctx, &cmd)
	default:
		slog.WarnContext(ctx, "unknown command type", "type", cmd.Type, "correlationId", cmd.CorrelationID)
		return nil
	}
}

func (h *CommandHandler) reservePayment(ctx context.Context, cmd *messages.Command) error {
	var payload messages.ReservePaymentPayload
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
		slog.WarnContext(ctx, "failed to unmarshal ReservePayment payload",
			"correlationId", cmd.CorrelationID, "error", err)
		return nil
	}

	slog.InfoContext(ctx, "processing ReservePayment",
		"correlationId", cmd.CorrelationID, "orderId", payload.OrderID, "amount", payload.Amount)

	// Idempotency: if already reserved, re-publish success without re-reserving.
	if existing, err := h.store.Get(cmd.CorrelationID); err == nil {
		slog.InfoContext(ctx, "payment already reserved, re-publishing event", "correlationId", cmd.CorrelationID)
		return h.publishEvent(ctx, cmd.CorrelationID, messages.EvtPaymentReserved, messages.PaymentResultPayload{
			OrderID:   payload.OrderID,
			PaymentID: existing.ID,
			Amount:    existing.Amount,
		})
	}

	if rand.Float64() >= h.successRate {
		slog.InfoContext(ctx, "payment reservation failed (simulation)", "correlationId", cmd.CorrelationID)
		return h.publishEvent(ctx, cmd.CorrelationID, messages.EvtPaymentFailed, messages.PaymentResultPayload{
			OrderID: payload.OrderID,
			Amount:  payload.Amount,
			Reason:  "insufficient balance",
		})
	}

	payment := &model.Payment{
		ID:            uuid.NewString(),
		CorrelationID: cmd.CorrelationID,
		OrderID:       payload.OrderID,
		Amount:        payload.Amount,
	}
	if err := h.store.Create(payment); err != nil {
		return err
	}

	slog.InfoContext(ctx, "payment reserved",
		"correlationId", cmd.CorrelationID, "paymentId", payment.ID, "amount", payment.Amount)
	return h.publishEvent(ctx, cmd.CorrelationID, messages.EvtPaymentReserved, messages.PaymentResultPayload{
		OrderID:   payload.OrderID,
		PaymentID: payment.ID,
		Amount:    payment.Amount,
	})
}

func (h *CommandHandler) cancelPayment(ctx context.Context, cmd *messages.Command) error {
	var payload messages.CancelPaymentPayload
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
		slog.WarnContext(ctx, "failed to unmarshal CancelPayment payload",
			"correlationId", cmd.CorrelationID, "error", err)
		return nil
	}

	slog.InfoContext(ctx, "processing CancelPayment",
		"correlationId", cmd.CorrelationID, "orderId", payload.OrderID)

	// Delete is idempotent; ignore not-found.
	if err := h.store.Delete(cmd.CorrelationID); err != nil {
		return err
	}

	slog.InfoContext(ctx, "payment cancelled", "correlationId", cmd.CorrelationID)
	return h.publishEvent(ctx, cmd.CorrelationID, messages.EvtPaymentCancelled, messages.PaymentResultPayload{
		OrderID: payload.OrderID,
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
	return h.publisher.Publish(ctx, paymentEventsTopic, correlationID, envelope)
}
