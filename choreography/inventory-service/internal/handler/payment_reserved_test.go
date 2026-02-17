package handler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/events"
)

func makePaymentReservedEvent(orderID string, amount float64) *events.Event {
	payload, _ := json.Marshal(events.PaymentResultPayload{
		OrderID:   orderID,
		PaymentID: "pay-1",
		Amount:    amount,
		Item:      "widget",
		Qty:       1,
	})
	return &events.Event{
		ID:            "evt-1",
		CorrelationID: "corr-1",
		Type:          events.PaymentReserved,
		Timestamp:     time.Now(),
		Payload:       payload,
	}
}

func TestPaymentReservedHandler_Success(t *testing.T) {
	store := newMockInventoryStore()
	producer := newMockProducer()
	h := NewPaymentReservedHandler(store, producer, 1.0)

	event := makePaymentReservedEvent("order-1", 9.99)
	err := h.Handle(context.Background(), event)
	require.NoError(t, err)

	assert.Len(t, store.reservations, 1)
	reservation := store.reservations["res-test-uuid"]
	require.NotNil(t, reservation)
	assert.Equal(t, "order-1", reservation.OrderID)
	assert.Equal(t, "widget", reservation.Item)
	assert.Equal(t, 1, reservation.Qty)

	require.Len(t, producer.published, 1)
	assert.Equal(t, "inventory-events", producer.published[0].topic)
	assert.Equal(t, "corr-1", producer.published[0].key)
	assert.Equal(t, events.InventoryReserved, producer.published[0].event.Type)

	var resultPayload events.InventoryResultPayload
	err = json.Unmarshal(producer.published[0].event.Payload, &resultPayload)
	require.NoError(t, err)
	assert.Equal(t, "order-1", resultPayload.OrderID)
	assert.Equal(t, "res-test-uuid", resultPayload.ReservationID)
}

func TestPaymentReservedHandler_Failure(t *testing.T) {
	store := newMockInventoryStore()
	producer := newMockProducer()
	h := NewPaymentReservedHandler(store, producer, 0.0)

	event := makePaymentReservedEvent("order-2", 19.98)
	err := h.Handle(context.Background(), event)
	require.NoError(t, err)

	assert.Empty(t, store.reservations)

	require.Len(t, producer.published, 1)
	assert.Equal(t, "inventory-events", producer.published[0].topic)
	assert.Equal(t, events.InventoryFailed, producer.published[0].event.Type)

	var resultPayload events.InventoryResultPayload
	err = json.Unmarshal(producer.published[0].event.Payload, &resultPayload)
	require.NoError(t, err)
	assert.Equal(t, "order-2", resultPayload.OrderID)
	assert.Equal(t, "insufficient inventory", resultPayload.Reason)
}

func TestPaymentReservedHandler_StoreError(t *testing.T) {
	store := newMockInventoryStore()
	store.createErr = errors.New("store failure")
	producer := newMockProducer()
	h := NewPaymentReservedHandler(store, producer, 1.0)

	event := makePaymentReservedEvent("order-1", 9.99)
	err := h.Handle(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create reservation")
}

func TestPaymentReservedHandler_PublishError(t *testing.T) {
	store := newMockInventoryStore()
	producer := newMockProducer()
	producer.err = errors.New("kafka down")
	h := NewPaymentReservedHandler(store, producer, 1.0)

	event := makePaymentReservedEvent("order-1", 9.99)
	err := h.Handle(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish InventoryReserved")
}
