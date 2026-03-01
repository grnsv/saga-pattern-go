package handler

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/orchestration/payment-service/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/payment-service/internal/model"
)

func makeCommandMsg(t *testing.T, cmdType messages.CommandType, correlationID string, payload any) kafkago.Message {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	cmd := messages.Command{
		ID:            "cmd-id",
		CorrelationID: correlationID,
		Type:          cmdType,
		Timestamp:     time.Now(),
		Payload:       raw,
	}
	body, err := json.Marshal(cmd)
	require.NoError(t, err)
	return kafkago.Message{Key: []byte(correlationID), Value: body}
}

func unmarshalEvent(t *testing.T, data []byte) messages.Event {
	t.Helper()
	var evt messages.Event
	require.NoError(t, json.Unmarshal(data, &evt))
	return evt
}

func TestReservePayment_Success(t *testing.T) {
	store := newMockPaymentStore()
	pub := &mockPublisher{}
	h := NewCommandHandler(store, pub, 1.0) // always succeed

	msg := makeCommandMsg(t, messages.CmdReservePayment, "corr-1", messages.ReservePaymentPayload{
		OrderID: "order-1", Item: "widget", Qty: 1, Amount: 9.99,
	})
	require.NoError(t, h.Handle(context.Background(), &msg))

	require.Len(t, pub.published, 1)
	assert.Equal(t, paymentEventsTopic, pub.published[0].topic)
	assert.Equal(t, "corr-1", pub.published[0].key)

	evt := unmarshalEvent(t, pub.published[0].payload)
	assert.Equal(t, messages.EvtPaymentReserved, evt.Type)
	assert.Equal(t, "corr-1", evt.CorrelationID)

	var result messages.PaymentResultPayload
	require.NoError(t, json.Unmarshal(evt.Payload, &result))
	assert.Equal(t, "order-1", result.OrderID)
	assert.NotEmpty(t, result.PaymentID)
	assert.InDelta(t, 9.99, result.Amount, 0.001)

	// Payment should be persisted.
	_, err := store.Get("corr-1")
	assert.NoError(t, err)
}

func TestReservePayment_Failure(t *testing.T) {
	store := newMockPaymentStore()
	pub := &mockPublisher{}
	h := NewCommandHandler(store, pub, 0.0) // always fail

	msg := makeCommandMsg(t, messages.CmdReservePayment, "corr-2", messages.ReservePaymentPayload{
		OrderID: "order-2", Item: "widget", Qty: 1, Amount: 9.99,
	})
	require.NoError(t, h.Handle(context.Background(), &msg))

	require.Len(t, pub.published, 1)
	evt := unmarshalEvent(t, pub.published[0].payload)
	assert.Equal(t, messages.EvtPaymentFailed, evt.Type)

	var result messages.PaymentResultPayload
	require.NoError(t, json.Unmarshal(evt.Payload, &result))
	assert.NotEmpty(t, result.Reason)
}

func TestReservePayment_Idempotent(t *testing.T) {
	store := newMockPaymentStore()
	pub := &mockPublisher{}
	h := NewCommandHandler(store, pub, 1.0)

	// Pre-populate — payment already reserved.
	_ = store.Create(&model.Payment{ID: "pay-existing", CorrelationID: "corr-3", OrderID: "order-3", Amount: 9.99})

	msg := makeCommandMsg(t, messages.CmdReservePayment, "corr-3", messages.ReservePaymentPayload{
		OrderID: "order-3", Item: "widget", Qty: 1, Amount: 9.99,
	})
	require.NoError(t, h.Handle(context.Background(), &msg))

	// Re-published PaymentReserved without creating a new record.
	require.Len(t, pub.published, 1)
	evt := unmarshalEvent(t, pub.published[0].payload)
	assert.Equal(t, messages.EvtPaymentReserved, evt.Type)

	var result messages.PaymentResultPayload
	require.NoError(t, json.Unmarshal(evt.Payload, &result))
	assert.Equal(t, "pay-existing", result.PaymentID)
}

func TestCancelPayment(t *testing.T) {
	store := newMockPaymentStore()
	pub := &mockPublisher{}
	h := NewCommandHandler(store, pub, 1.0)

	_ = store.Create(&model.Payment{ID: "pay-1", CorrelationID: "corr-4", OrderID: "order-4", Amount: 9.99})

	msg := makeCommandMsg(t, messages.CmdCancelPayment, "corr-4", messages.CancelPaymentPayload{OrderID: "order-4"})
	require.NoError(t, h.Handle(context.Background(), &msg))

	require.Len(t, pub.published, 1)
	evt := unmarshalEvent(t, pub.published[0].payload)
	assert.Equal(t, messages.EvtPaymentCancelled, evt.Type)
	assert.Equal(t, "corr-4", evt.CorrelationID)

	// Payment should be gone.
	_, err := store.Get("corr-4")
	assert.Error(t, err)
}

func TestCancelPayment_Idempotent(t *testing.T) {
	// Cancel when payment was never reserved — still publishes PaymentCancelled.
	store := newMockPaymentStore()
	pub := &mockPublisher{}
	h := NewCommandHandler(store, pub, 1.0)

	msg := makeCommandMsg(t, messages.CmdCancelPayment, "corr-5", messages.CancelPaymentPayload{OrderID: "order-5"})
	require.NoError(t, h.Handle(context.Background(), &msg))

	require.Len(t, pub.published, 1)
	evt := unmarshalEvent(t, pub.published[0].payload)
	assert.Equal(t, messages.EvtPaymentCancelled, evt.Type)
}

func TestHandle_ReservePayment_StoreError(t *testing.T) {
	store := newMockPaymentStore()
	store.createErr = errors.New("db unavailable")
	pub := &mockPublisher{}
	h := NewCommandHandler(store, pub, 1.0)

	msg := makeCommandMsg(t, messages.CmdReservePayment, "corr-6", messages.ReservePaymentPayload{
		OrderID: "order-6", Item: "widget", Qty: 1, Amount: 9.99,
	})
	err := h.Handle(context.Background(), &msg)
	assert.Error(t, err)
}

func TestHandle_UnknownType(t *testing.T) {
	h := NewCommandHandler(newMockPaymentStore(), &mockPublisher{}, 1.0)
	msg := makeCommandMsg(t, messages.CommandType("UnknownCommand"), "corr-x", struct{}{})
	assert.NoError(t, h.Handle(context.Background(), &msg))
}

func TestHandle_MalformedMessage(t *testing.T) {
	h := NewCommandHandler(newMockPaymentStore(), &mockPublisher{}, 1.0)
	msg := kafkago.Message{Key: []byte("corr-bad"), Value: []byte("not-json")}
	assert.NoError(t, h.Handle(context.Background(), &msg))
}
