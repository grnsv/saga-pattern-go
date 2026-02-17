package handler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/model"
)

func makeInventoryFailedEvent(orderID, reason string) *events.Event {
	payload, _ := json.Marshal(events.InventoryResultPayload{
		OrderID: orderID,
		Item:    "widget",
		Qty:     1,
		Reason:  reason,
	})
	return &events.Event{
		ID:            "evt-2",
		CorrelationID: "corr-1",
		Type:          events.InventoryFailed,
		Timestamp:     time.Now(),
		Payload:       payload,
	}
}

func TestInventoryFailedHandler_Success(t *testing.T) {
	store := newMockPaymentStore()
	store.payments["pay-1"] = &model.Payment{
		ID:      "pay-1",
		OrderID: "order-1",
		Amount:  9.99,
		Status:  model.PaymentReserved,
	}
	store.byOrder["order-1"] = store.payments["pay-1"]

	producer := newMockProducer()
	h := NewInventoryFailedHandler(store, producer)

	event := makeInventoryFailedEvent("order-1", "out of stock")
	err := h.Handle(context.Background(), event)
	require.NoError(t, err)

	assert.Equal(t, model.PaymentRolledBack, store.payments["pay-1"].Status)

	require.Len(t, producer.published, 1)
	assert.Equal(t, "payment-events", producer.published[0].topic)
	assert.Equal(t, events.PaymentRolledBack, producer.published[0].event.Type)
	assert.Equal(t, "corr-1", producer.published[0].event.CorrelationID)

	var resultPayload events.PaymentResultPayload
	err = json.Unmarshal(producer.published[0].event.Payload, &resultPayload)
	require.NoError(t, err)
	assert.Equal(t, "order-1", resultPayload.OrderID)
	assert.Equal(t, "pay-1", resultPayload.PaymentID)
	assert.Contains(t, resultPayload.Reason, "out of stock")
}

func TestInventoryFailedHandler_PaymentNotFound(t *testing.T) {
	store := newMockPaymentStore()
	producer := newMockProducer()
	h := NewInventoryFailedHandler(store, producer)

	event := makeInventoryFailedEvent("order-999", "no items available")
	err := h.Handle(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get payment for order")
}

func TestInventoryFailedHandler_UpdateStatusError(t *testing.T) {
	store := newMockPaymentStore()
	store.payments["pay-1"] = &model.Payment{
		ID:      "pay-1",
		OrderID: "order-1",
		Amount:  9.99,
		Status:  model.PaymentReserved,
	}
	store.byOrder["order-1"] = store.payments["pay-1"]
	store.updateStatusErr = errors.New("store failure")

	producer := newMockProducer()
	h := NewInventoryFailedHandler(store, producer)

	event := makeInventoryFailedEvent("order-1", "warehouse error")
	err := h.Handle(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "update payment")
}

func TestInventoryFailedHandler_PublishError(t *testing.T) {
	store := newMockPaymentStore()
	store.payments["pay-1"] = &model.Payment{
		ID:      "pay-1",
		OrderID: "order-1",
		Amount:  9.99,
		Status:  model.PaymentReserved,
	}
	store.byOrder["order-1"] = store.payments["pay-1"]

	producer := newMockProducer()
	producer.err = errors.New("kafka down")
	h := NewInventoryFailedHandler(store, producer)

	event := makeInventoryFailedEvent("order-1", "out of stock")
	err := h.Handle(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish PaymentRolledBack")
}
