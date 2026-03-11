package handler

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/messages"
)

func makeCommandMsg(cmdType messages.CommandType, payload any) kafkago.Message {
	p, _ := json.Marshal(payload)
	cmd := messages.Command{
		ID:            "cmd-1",
		CorrelationID: "corr-1",
		Type:          cmdType,
		Timestamp:     time.Now(),
		Payload:       p,
	}
	b, _ := json.Marshal(cmd)
	return kafkago.Message{Key: []byte("corr-1"), Value: b}
}

func makeEventMsg(evtType messages.EventType) kafkago.Message {
	evt := messages.Event{
		ID:            "evt-1",
		CorrelationID: "corr-1",
		Type:          evtType,
		Timestamp:     time.Now(),
		Payload:       json.RawMessage(`{}`),
	}
	b, _ := json.Marshal(evt)
	return kafkago.Message{Key: []byte("corr-1"), Value: b}
}

func TestKafkaHandler_HandleCommand_StartSaga(t *testing.T) {
	orch := &mockOrchestrator{}
	h := NewKafkaHandler(orch)

	msg := makeCommandMsg(messages.CmdStartSaga, messages.StartSagaPayload{
		OrderID: "order-1", Item: "widget", Qty: 1, Amount: 9.99,
	})
	require.NoError(t, h.HandleCommand(context.Background(), msg))

	require.Len(t, orch.startCalls, 1)
	assert.Equal(t, "corr-1", orch.startCalls[0].correlationID)
	assert.Equal(t, "order-1", orch.startCalls[0].payload.OrderID)
}

func TestKafkaHandler_HandleCommand_UnknownType(t *testing.T) {
	orch := &mockOrchestrator{}
	h := NewKafkaHandler(orch)

	cmd := messages.Command{
		ID: "c1", CorrelationID: "corr-1",
		Type:    "UnknownCmd",
		Payload: json.RawMessage(`{}`),
	}
	b, _ := json.Marshal(cmd)
	msg := kafkago.Message{Value: b}

	require.NoError(t, h.HandleCommand(context.Background(), msg))
	assert.Empty(t, orch.startCalls)
}

func TestKafkaHandler_HandleCommand_InvalidJSON(t *testing.T) {
	h := NewKafkaHandler(&mockOrchestrator{})
	msg := kafkago.Message{Value: []byte("not-json")}
	assert.NoError(t, h.HandleCommand(context.Background(), msg))
}

func TestKafkaHandler_HandleEvent_PaymentReserved(t *testing.T) {
	orch := &mockOrchestrator{}
	h := NewKafkaHandler(orch)

	msg := makeEventMsg(messages.EvtPaymentReserved)
	require.NoError(t, h.HandleEvent(context.Background(), msg))

	require.Len(t, orch.eventCalls, 1)
	assert.Equal(t, messages.EvtPaymentReserved, orch.eventCalls[0].Type)
}

func TestKafkaHandler_HandleEvent_InvalidJSON(t *testing.T) {
	h := NewKafkaHandler(&mockOrchestrator{})
	msg := kafkago.Message{Value: []byte("not-json")}
	assert.NoError(t, h.HandleEvent(context.Background(), msg))
}
