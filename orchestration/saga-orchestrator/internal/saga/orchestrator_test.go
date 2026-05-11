package saga_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/messages"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/saga"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/store"
)

// mockPublisher records every Publish call for assertions.
type mockPublisher struct {
	calls []publishCall
}

type publishCall struct {
	topic   string
	key     string
	payload []byte
}

func (m *mockPublisher) Publish(_ context.Context, topic, key string, payload []byte) error {
	m.calls = append(m.calls, publishCall{topic, key, payload})
	return nil
}

func (m *mockPublisher) lastCall() publishCall {
	return m.calls[len(m.calls)-1]
}

func (m *mockPublisher) commandType(t *testing.T) messages.CommandType {
	t.Helper()
	var cmd messages.Command
	require.NoError(t, json.Unmarshal(m.lastCall().payload, &cmd))
	return cmd.Type
}

func (m *mockPublisher) eventType(t *testing.T) messages.EventType {
	t.Helper()
	var evt messages.Event
	require.NoError(t, json.Unmarshal(m.lastCall().payload, &evt))
	return evt.Type
}

func newOrchestrator(t *testing.T) (*saga.Orchestrator, *store.InMemorySagaStore, *mockPublisher) {
	t.Helper()
	s := store.NewInMemorySagaStore()
	p := &mockPublisher{}
	o := saga.NewOrchestrator(s, p, 5*time.Second)
	return o, s, p
}

func startSaga(t *testing.T, o *saga.Orchestrator, correlationID string) {
	t.Helper()
	err := o.StartSaga(context.Background(), correlationID, messages.StartSagaPayload{
		OrderID: "order-" + correlationID,
		Item:    "widget",
		Qty:     2,
		Amount:  19.98,
	})
	require.NoError(t, err)
}

func sendEvent(t *testing.T, o *saga.Orchestrator, correlationID string, evtType messages.EventType) {
	t.Helper()
	evt := &messages.Event{
		ID:            "evt-id",
		CorrelationID: correlationID,
		Type:          evtType,
		Payload:       json.RawMessage(`{}`),
	}
	require.NoError(t, o.HandleEvent(context.Background(), evt))
}

// --- StartSaga tests ---

func TestOrchestrator_StartSaga_CreatesAndPublishes(t *testing.T) {
	o, s, p := newOrchestrator(t)

	err := o.StartSaga(context.Background(), "corr-1", messages.StartSagaPayload{
		OrderID: "order-1", Item: "widget", Qty: 1, Amount: 9.99,
	})
	require.NoError(t, err)

	// saga persisted in PAYMENT_PENDING state
	saved, err := s.Get(context.Background(), "corr-1")
	require.NoError(t, err)
	assert.Equal(t, model.SagaPaymentPending, saved.State)

	// ReservePayment command published to payment-commands
	require.Len(t, p.calls, 1)
	assert.Equal(t, "payment-commands", p.calls[0].topic)
	assert.Equal(t, messages.CmdReservePayment, p.commandType(t))

	history, err := s.ListHistory(context.Background(), saved.ID)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, model.SagaStarted, history[0].FromState)
	assert.Equal(t, model.SagaPaymentPending, history[0].ToState)
	assert.Equal(t, string(messages.CmdStartSaga), history[0].Event)
}

// --- HandleEvent tests (happy path) ---

func TestOrchestrator_HandleEvent_PaymentReserved(t *testing.T) {
	o, s, p := newOrchestrator(t)
	startSaga(t, o, "corr-pay")
	p.calls = nil

	sendEvent(t, o, "corr-pay", messages.EvtPaymentReserved)

	saga, _ := s.Get(context.Background(), "corr-pay")
	assert.Equal(t, model.SagaInventoryPending, saga.State)
	require.Len(t, p.calls, 1)
	assert.Equal(t, "inventory-commands", p.calls[0].topic)
	assert.Equal(t, messages.CmdReserveInventory, p.commandType(t))

	history, err := s.ListHistory(context.Background(), saga.ID)
	require.NoError(t, err)
	require.Len(t, history, 2)
	assert.Equal(t, model.SagaPaymentPending, history[1].FromState)
	assert.Equal(t, model.SagaInventoryPending, history[1].ToState)
	assert.Equal(t, string(messages.EvtPaymentReserved), history[1].Event)
}

func TestOrchestrator_HandleEvent_InventoryReserved(t *testing.T) {
	o, s, p := newOrchestrator(t)
	startSaga(t, o, "corr-inv")
	sendEvent(t, o, "corr-inv", messages.EvtPaymentReserved)
	p.calls = nil

	sendEvent(t, o, "corr-inv", messages.EvtInventoryReserved)

	saved, _ := s.Get(context.Background(), "corr-inv")
	assert.Equal(t, model.SagaCompleted, saved.State)
	require.Len(t, p.calls, 1)
	assert.Equal(t, "saga-events", p.calls[0].topic)
	assert.Equal(t, messages.EvtSagaCompleted, p.eventType(t))
}

// --- HandleEvent tests (failure paths) ---

func TestOrchestrator_HandleEvent_PaymentFailed(t *testing.T) {
	o, s, p := newOrchestrator(t)
	startSaga(t, o, "corr-pfail")
	p.calls = nil

	sendEvent(t, o, "corr-pfail", messages.EvtPaymentFailed)

	saved, _ := s.Get(context.Background(), "corr-pfail")
	assert.Equal(t, model.SagaFailed, saved.State)
	require.Len(t, p.calls, 1)
	assert.Equal(t, "saga-events", p.calls[0].topic)
	assert.Equal(t, messages.EvtSagaFailed, p.eventType(t))
}

func TestOrchestrator_HandleEvent_InventoryFailed(t *testing.T) {
	o, s, p := newOrchestrator(t)
	startSaga(t, o, "corr-ifail")
	sendEvent(t, o, "corr-ifail", messages.EvtPaymentReserved)
	p.calls = nil

	sendEvent(t, o, "corr-ifail", messages.EvtInventoryFailed)

	saved, _ := s.Get(context.Background(), "corr-ifail")
	assert.Equal(t, model.SagaCancelPaymentPending, saved.State)
	require.Len(t, p.calls, 1)
	assert.Equal(t, "payment-commands", p.calls[0].topic)
	assert.Equal(t, messages.CmdCancelPayment, p.commandType(t))
}

func TestOrchestrator_HandleEvent_PaymentCancelled(t *testing.T) {
	o, s, p := newOrchestrator(t)
	startSaga(t, o, "corr-comp")
	sendEvent(t, o, "corr-comp", messages.EvtPaymentReserved)
	sendEvent(t, o, "corr-comp", messages.EvtInventoryFailed)
	p.calls = nil

	sendEvent(t, o, "corr-comp", messages.EvtPaymentCancelled)

	saved, _ := s.Get(context.Background(), "corr-comp")
	assert.Equal(t, model.SagaFailed, saved.State)
	require.Len(t, p.calls, 1)
	assert.Equal(t, "saga-events", p.calls[0].topic)
	assert.Equal(t, messages.EvtSagaFailed, p.eventType(t))
}

func TestOrchestrator_HandleEvent_UnknownCorrelationID(t *testing.T) {
	o, _, _ := newOrchestrator(t)

	evt := &messages.Event{
		CorrelationID: "unknown",
		Type:          messages.EvtPaymentReserved,
		Payload:       json.RawMessage(`{}`),
	}
	// Should not return an error; just log and skip.
	assert.NoError(t, o.HandleEvent(context.Background(), evt))
}

func TestOrchestrator_HandleEvent_InvalidTransition(t *testing.T) {
	o, _, _ := newOrchestrator(t)
	startSaga(t, o, "corr-invalid")
	// Already in PAYMENT_PENDING; PaymentCancelled has no transition from this state.
	evt := &messages.Event{
		CorrelationID: "corr-invalid",
		Type:          messages.EvtPaymentCancelled,
		Payload:       json.RawMessage(`{}`),
	}
	// Should log a warning and return nil — not an error.
	assert.NoError(t, o.HandleEvent(context.Background(), evt))
}
