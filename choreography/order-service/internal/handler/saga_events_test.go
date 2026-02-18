package handler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/events"
	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/model"
)

func makeSagaEvent(t events.EventType, payload any, correlationID string) *events.Event {
	data, _ := json.Marshal(payload)
	return &events.Event{
		ID:            "evt-1",
		CorrelationID: correlationID,
		Type:          t,
		Timestamp:     time.Now(),
		Payload:       data,
	}
}

func storeWithOrder(id string) *mockOrderStore {
	s := newMockStore()
	s.orders[id] = &model.Order{
		ID:     id,
		Item:   "widget",
		Qty:    1,
		Amount: 9.99,
		Status: model.OrderPending,
	}
	return s
}

func TestSagaEventHandler_InventoryReserved(t *testing.T) {
	s := storeWithOrder("order-1")
	h := NewSagaEventHandler(s)

	event := makeSagaEvent(events.InventoryReserved, events.InventoryResultPayload{
		OrderID:       "order-1",
		ReservationID: "res-1",
		Item:          "widget",
		Qty:           1,
	}, "order-1")

	err := h.HandleInventoryReserved(context.Background(), event)
	require.NoError(t, err)
	assert.Equal(t, model.OrderConfirmed, s.orders["order-1"].Status)
}

func TestSagaEventHandler_PaymentFailed(t *testing.T) {
	s := storeWithOrder("order-2")
	h := NewSagaEventHandler(s)

	event := makeSagaEvent(events.PaymentFailed, events.PaymentResultPayload{
		OrderID: "order-2",
		Reason:  "payment declined",
	}, "order-2")

	err := h.HandlePaymentFailed(context.Background(), event)
	require.NoError(t, err)
	assert.Equal(t, model.OrderCancelled, s.orders["order-2"].Status)
}

func TestSagaEventHandler_PaymentRolledBack(t *testing.T) {
	s := storeWithOrder("order-3")
	h := NewSagaEventHandler(s)

	event := makeSagaEvent(events.PaymentRolledBack, events.PaymentResultPayload{
		OrderID: "order-3",
		Reason:  "inventory reservation failed: out of stock",
	}, "order-3")

	err := h.HandlePaymentRolledBack(context.Background(), event)
	require.NoError(t, err)
	assert.Equal(t, model.OrderCancelled, s.orders["order-3"].Status)
}

func TestSagaEventHandler_OrderNotFound(t *testing.T) {
	s := newMockStore()
	h := NewSagaEventHandler(s)

	event := makeSagaEvent(events.InventoryReserved, events.InventoryResultPayload{
		OrderID: "nonexistent",
		Item:    "widget",
		Qty:     1,
	}, "nonexistent")

	err := h.HandleInventoryReserved(context.Background(), event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "get order")
}
