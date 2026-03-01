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

	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/model"
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

func TestReserveInventory_Success(t *testing.T) {
	store := newMockInventoryStore()
	pub := &mockPublisher{}
	h := NewCommandHandler(store, pub, 1.0) // always succeed

	msg := makeCommandMsg(t, messages.CmdReserveInventory, "corr-1", messages.ReserveInventoryPayload{
		OrderID: "order-1", Item: "widget", Qty: 2,
	})
	require.NoError(t, h.Handle(context.Background(), &msg))

	require.Len(t, pub.published, 1)
	assert.Equal(t, inventoryEventsTopic, pub.published[0].topic)
	assert.Equal(t, "corr-1", pub.published[0].key)

	evt := unmarshalEvent(t, pub.published[0].payload)
	assert.Equal(t, messages.EvtInventoryReserved, evt.Type)
	assert.Equal(t, "corr-1", evt.CorrelationID)

	var result messages.InventoryResultPayload
	require.NoError(t, json.Unmarshal(evt.Payload, &result))
	assert.Equal(t, "order-1", result.OrderID)
	assert.NotEmpty(t, result.ReservationID)
	assert.Equal(t, "widget", result.Item)
	assert.Equal(t, 2, result.Qty)

	// Reservation should be persisted.
	_, err := store.Get("corr-1")
	assert.NoError(t, err)
}

func TestReserveInventory_Failure(t *testing.T) {
	store := newMockInventoryStore()
	pub := &mockPublisher{}
	h := NewCommandHandler(store, pub, 0.0) // always fail

	msg := makeCommandMsg(t, messages.CmdReserveInventory, "corr-2", messages.ReserveInventoryPayload{
		OrderID: "order-2", Item: "widget", Qty: 1,
	})
	require.NoError(t, h.Handle(context.Background(), &msg))

	require.Len(t, pub.published, 1)
	evt := unmarshalEvent(t, pub.published[0].payload)
	assert.Equal(t, messages.EvtInventoryFailed, evt.Type)

	var result messages.InventoryResultPayload
	require.NoError(t, json.Unmarshal(evt.Payload, &result))
	assert.NotEmpty(t, result.Reason)
}

func TestReserveInventory_Idempotent(t *testing.T) {
	store := newMockInventoryStore()
	pub := &mockPublisher{}
	h := NewCommandHandler(store, pub, 1.0)

	// Pre-populate — reservation already exists.
	_ = store.Create(&model.InventoryReservation{
		ID: "res-existing", CorrelationID: "corr-3", OrderID: "order-3", Item: "widget", Qty: 1,
	})

	msg := makeCommandMsg(t, messages.CmdReserveInventory, "corr-3", messages.ReserveInventoryPayload{
		OrderID: "order-3", Item: "widget", Qty: 1,
	})
	require.NoError(t, h.Handle(context.Background(), &msg))

	require.Len(t, pub.published, 1)
	evt := unmarshalEvent(t, pub.published[0].payload)
	assert.Equal(t, messages.EvtInventoryReserved, evt.Type)

	var result messages.InventoryResultPayload
	require.NoError(t, json.Unmarshal(evt.Payload, &result))
	assert.Equal(t, "res-existing", result.ReservationID)
}

func TestHandle_ReserveInventory_StoreError(t *testing.T) {
	store := newMockInventoryStore()
	store.createErr = errors.New("db unavailable")
	pub := &mockPublisher{}
	h := NewCommandHandler(store, pub, 1.0)

	msg := makeCommandMsg(t, messages.CmdReserveInventory, "corr-4", messages.ReserveInventoryPayload{
		OrderID: "order-4", Item: "widget", Qty: 1,
	})
	err := h.Handle(context.Background(), &msg)
	assert.Error(t, err)
}

func TestHandle_UnknownType(t *testing.T) {
	h := NewCommandHandler(newMockInventoryStore(), &mockPublisher{}, 1.0)
	// Use makeCommandMsg with a non-CmdReserveInventory type to exercise the default branch
	// and satisfy the unparam linter (cmdType is not always CmdReserveInventory).
	msg := makeCommandMsg(t, messages.CommandType("UnknownCommand"), "corr-x", struct{}{})
	assert.NoError(t, h.Handle(context.Background(), &msg))
}

func TestHandle_MalformedMessage(t *testing.T) {
	h := NewCommandHandler(newMockInventoryStore(), &mockPublisher{}, 1.0)
	msg := kafkago.Message{Key: []byte("corr-bad"), Value: []byte("not-json")}
	assert.NoError(t, h.Handle(context.Background(), &msg))
}
