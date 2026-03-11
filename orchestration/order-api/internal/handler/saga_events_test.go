package handler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/orchestration/order-api/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/order-api/internal/model"
)

func makeSagaEventMsg(evtType messages.EventType, orderID string) kafkago.Message {
	payload, _ := json.Marshal(messages.SagaResultPayload{OrderID: orderID})
	evt := messages.Event{
		ID:            "evt-1",
		CorrelationID: orderID,
		Type:          evtType,
		Timestamp:     time.Now(),
		Payload:       payload,
	}
	b, _ := json.Marshal(evt)
	return kafkago.Message{Key: []byte(orderID), Value: b}
}

func storeWithOrder(orderID string) *mockOrderStore {
	s := newMockStore()
	s.orders[orderID] = &model.Order{
		ID:     orderID,
		Item:   "widget",
		Qty:    1,
		Amount: 9.99,
		Status: model.OrderPending,
	}
	return s
}

func TestSagaEventHandler_SagaCompleted(t *testing.T) {
	s := storeWithOrder("order-1")
	h := NewSagaEventHandler(s)

	err := h.Handle(context.Background(), makeSagaEventMsg(messages.EvtSagaCompleted, "order-1"))
	require.NoError(t, err)

	assert.Equal(t, model.OrderConfirmed, s.orders["order-1"].Status)
}

func TestSagaEventHandler_SagaFailed(t *testing.T) {
	s := storeWithOrder("order-2")
	h := NewSagaEventHandler(s)

	err := h.Handle(context.Background(), makeSagaEventMsg(messages.EvtSagaFailed, "order-2"))
	require.NoError(t, err)

	assert.Equal(t, model.OrderCancelled, s.orders["order-2"].Status)
}

func TestSagaEventHandler_UnknownEventType(t *testing.T) {
	s := newMockStore()
	h := NewSagaEventHandler(s)

	evt := messages.Event{
		ID: "e1", CorrelationID: "corr-1",
		Type:    "SomeOtherEvent",
		Payload: json.RawMessage(`{}`),
	}
	b, _ := json.Marshal(evt)
	msg := kafkago.Message{Value: b}

	assert.NoError(t, h.Handle(context.Background(), msg))
}

func TestSagaEventHandler_OrderNotFound(t *testing.T) {
	// OrderID in payload does not exist in store — should log and return nil.
	s := newMockStore()
	h := NewSagaEventHandler(s)

	err := h.Handle(context.Background(), makeSagaEventMsg(messages.EvtSagaCompleted, "missing-order"))
	assert.NoError(t, err)
}

func TestSagaEventHandler_InvalidJSON(t *testing.T) {
	h := NewSagaEventHandler(newMockStore())
	msg := kafkago.Message{Value: []byte("not-json")}
	assert.NoError(t, h.Handle(context.Background(), msg))
}
