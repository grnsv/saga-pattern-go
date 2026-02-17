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
)

func makeOrderCreatedEvent(orderID string, amount float64) *events.Event {
	payload, _ := json.Marshal(events.OrderCreatedPayload{
		OrderID: orderID,
		Item:    "widget",
		Qty:     1,
		Amount:  amount,
	})
	return &events.Event{
		ID:            "evt-1",
		CorrelationID: "corr-1",
		Type:          events.OrderCreated,
		Timestamp:     time.Now(),
		Payload:       payload,
	}
}

func TestOrderCreatedHandler_Success(t *testing.T) {
	store := newMockPaymentStore()
	producer := newMockProducer()
	h := NewOrderCreatedHandler(store, producer, 1.0)

	event := makeOrderCreatedEvent("order-1", 9.99)
	err := h.Handle(context.Background(), event)
	require.NoError(t, err)

	assert.Len(t, store.payments, 1)
	payment := store.payments["pay-test-uuid"]
	require.NotNil(t, payment)
	assert.Equal(t, "order-1", payment.OrderID)
	assert.InDelta(t, 9.99, payment.Amount, 0.001)

	require.Len(t, producer.published, 1)
	assert.Equal(t, "payment-events", producer.published[0].topic)
	assert.Equal(t, "corr-1", producer.published[0].key)
	assert.Equal(t, events.PaymentReserved, producer.published[0].event.Type)
	assert.Equal(t, "corr-1", producer.published[0].event.CorrelationID)

	var resultPayload events.PaymentResultPayload
	err = json.Unmarshal(producer.published[0].event.Payload, &resultPayload)
	require.NoError(t, err)
	assert.Equal(t, "order-1", resultPayload.OrderID)
	assert.Equal(t, "pay-test-uuid", resultPayload.PaymentID)
	assert.Equal(t, "widget", resultPayload.Item)
	assert.Equal(t, 1, resultPayload.Qty)
}

func TestOrderCreatedHandler_Failure(t *testing.T) {
	store := newMockPaymentStore()
	producer := newMockProducer()
	h := NewOrderCreatedHandler(store, producer, 0.0)

	event := makeOrderCreatedEvent("order-2", 19.98)
	err := h.Handle(context.Background(), event)
	require.NoError(t, err)

	assert.Empty(t, store.payments)

	require.Len(t, producer.published, 1)
	assert.Equal(t, "payment-events", producer.published[0].topic)
	assert.Equal(t, events.PaymentFailed, producer.published[0].event.Type)

	var resultPayload events.PaymentResultPayload
	err = json.Unmarshal(producer.published[0].event.Payload, &resultPayload)
	require.NoError(t, err)
	assert.Equal(t, "order-2", resultPayload.OrderID)
	assert.Equal(t, "payment declined", resultPayload.Reason)
}

func TestOrderCreatedHandler_StoreError(t *testing.T) {
	store := newMockPaymentStore()
	store.createErr = errors.New("store failure")
	producer := newMockProducer()
	h := NewOrderCreatedHandler(store, producer, 1.0)

	event := makeOrderCreatedEvent("order-1", 9.99)
	err := h.Handle(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create payment")
}

func TestOrderCreatedHandler_PublishError(t *testing.T) {
	store := newMockPaymentStore()
	producer := newMockProducer()
	producer.err = errors.New("kafka down")
	h := NewOrderCreatedHandler(store, producer, 1.0)

	event := makeOrderCreatedEvent("order-1", 9.99)
	err := h.Handle(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "publish PaymentReserved")
}
