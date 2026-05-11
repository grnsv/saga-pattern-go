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

const (
	testStepTimeout = 5 * time.Second
	testMaxRetries  = 3
	testInterval    = time.Hour // large so Run doesn't tick during tests
)

func newWorkerFixture(t *testing.T) (*saga.TimeoutWorker, *store.InMemorySagaStore, *mockPublisher) {
	t.Helper()
	s := store.NewInMemorySagaStore()
	p := &mockPublisher{}
	o := saga.NewOrchestrator(s, p, testStepTimeout)
	w := saga.NewTimeoutWorker(s, o, testInterval, testMaxRetries)
	return w, s, p
}

func timedOutSaga(correlationID string, state model.SagaState, retryCount int) *model.SagaInstance {
	now := time.Now()
	past := now.Add(-10 * time.Second)
	return &model.SagaInstance{
		ID:            "saga-" + correlationID,
		CorrelationID: correlationID,
		OrderID:       "order-" + correlationID,
		State:         state,
		Item:          "widget",
		Qty:           1,
		Amount:        9.99,
		RetryCount:    retryCount,
		StepDeadline:  &past,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

// --- Retry tests ---

func TestTimeoutWorker_Retry_PaymentPending(t *testing.T) {
	w, s, p := newWorkerFixture(t)
	ctx := context.Background()

	sg := timedOutSaga("corr-retry-pay", model.SagaPaymentPending, 0)
	require.NoError(t, s.Create(ctx, sg))

	require.NoError(t, w.CheckTimeouts(ctx))

	updated, _ := s.Get(ctx, "corr-retry-pay")
	assert.Equal(t, model.SagaPaymentPending, updated.State)
	assert.Equal(t, 1, updated.RetryCount)
	assert.NotNil(t, updated.StepDeadline)
	assert.True(t, updated.StepDeadline.After(time.Now().Add(-time.Second)))

	require.Len(t, p.calls, 1)
	assert.Equal(t, "payment-commands", p.calls[0].topic)
	assert.Equal(t, messages.CmdReservePayment, p.commandType(t))
}

func TestTimeoutWorker_Retry_InventoryPending(t *testing.T) {
	w, s, p := newWorkerFixture(t)
	ctx := context.Background()

	sg := timedOutSaga("corr-retry-inv", model.SagaInventoryPending, 1)
	require.NoError(t, s.Create(ctx, sg))

	require.NoError(t, w.CheckTimeouts(ctx))

	updated, _ := s.Get(ctx, "corr-retry-inv")
	assert.Equal(t, model.SagaInventoryPending, updated.State)
	assert.Equal(t, 2, updated.RetryCount)

	require.Len(t, p.calls, 1)
	assert.Equal(t, "inventory-commands", p.calls[0].topic)
	assert.Equal(t, messages.CmdReserveInventory, p.commandType(t))
}

func TestTimeoutWorker_Retry_CancelPaymentPending(t *testing.T) {
	w, s, p := newWorkerFixture(t)
	ctx := context.Background()

	sg := timedOutSaga("corr-retry-cancel", model.SagaCancelPaymentPending, 2)
	require.NoError(t, s.Create(ctx, sg))

	require.NoError(t, w.CheckTimeouts(ctx))

	updated, _ := s.Get(ctx, "corr-retry-cancel")
	assert.Equal(t, model.SagaCancelPaymentPending, updated.State)
	assert.Equal(t, 3, updated.RetryCount)

	require.Len(t, p.calls, 1)
	assert.Equal(t, "payment-commands", p.calls[0].topic)
	assert.Equal(t, messages.CmdCancelPayment, p.commandType(t))
}

// --- Exhaust tests ---

func TestTimeoutWorker_Exhaust_PaymentPending_Failed(t *testing.T) {
	w, s, p := newWorkerFixture(t)
	ctx := context.Background()

	sg := timedOutSaga("corr-exh-pay", model.SagaPaymentPending, testMaxRetries)
	require.NoError(t, s.Create(ctx, sg))

	require.NoError(t, w.CheckTimeouts(ctx))

	updated, _ := s.Get(ctx, "corr-exh-pay")
	assert.Equal(t, model.SagaFailed, updated.State)
	assert.Nil(t, updated.StepDeadline)

	require.Len(t, p.calls, 1)
	assert.Equal(t, "saga-events", p.calls[0].topic)
	assert.Equal(t, messages.EvtSagaFailed, p.eventType(t))

	history, err := s.ListHistory(ctx, updated.ID)
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, model.SagaPaymentPending, history[0].FromState)
	assert.Equal(t, model.SagaFailed, history[0].ToState)
	assert.Equal(t, "Timeout", history[0].Event)
}

func TestTimeoutWorker_Exhaust_InventoryPending_CancelPayment(t *testing.T) {
	w, s, p := newWorkerFixture(t)
	ctx := context.Background()

	sg := timedOutSaga("corr-exh-inv", model.SagaInventoryPending, testMaxRetries)
	require.NoError(t, s.Create(ctx, sg))

	require.NoError(t, w.CheckTimeouts(ctx))

	updated, _ := s.Get(ctx, "corr-exh-inv")
	assert.Equal(t, model.SagaCancelPaymentPending, updated.State)
	assert.NotNil(t, updated.StepDeadline)
	assert.Equal(t, 0, updated.RetryCount)

	require.Len(t, p.calls, 1)
	assert.Equal(t, "payment-commands", p.calls[0].topic)
	assert.Equal(t, messages.CmdCancelPayment, p.commandType(t))
}

func TestTimeoutWorker_Exhaust_CancelPaymentPending_Failed(t *testing.T) {
	w, s, p := newWorkerFixture(t)
	ctx := context.Background()

	sg := timedOutSaga("corr-exh-cancel", model.SagaCancelPaymentPending, testMaxRetries)
	require.NoError(t, s.Create(ctx, sg))

	require.NoError(t, w.CheckTimeouts(ctx))

	updated, _ := s.Get(ctx, "corr-exh-cancel")
	assert.Equal(t, model.SagaFailed, updated.State)
	assert.Nil(t, updated.StepDeadline)

	require.Len(t, p.calls, 1)
	assert.Equal(t, "saga-events", p.calls[0].topic)

	var evt messages.Event
	require.NoError(t, json.Unmarshal(p.calls[0].payload, &evt))
	assert.Equal(t, messages.EvtSagaFailed, evt.Type)
}

// --- Race condition test ---

// raceStore wraps InMemorySagaStore and injects a concurrent modification
// just before the first Update call, simulating an event handler that
// wins the race against the timeout worker.
type raceStore struct {
	*store.InMemorySagaStore
	interceptOnce func(ctx context.Context)
}

func (s *raceStore) Update(ctx context.Context, sg *model.SagaInstance) (bool, error) {
	if s.interceptOnce != nil {
		s.interceptOnce(ctx)
		s.interceptOnce = nil
	}
	return s.InMemorySagaStore.Update(ctx, sg)
}

func TestTimeoutWorker_Race_OptimisticLockLost(t *testing.T) {
	ctx := context.Background()
	inner := store.NewInMemorySagaStore()
	p := &mockPublisher{}

	sg := timedOutSaga("corr-race", model.SagaPaymentPending, 0)
	require.NoError(t, inner.Create(ctx, sg))

	rs := &raceStore{
		InMemorySagaStore: inner,
		interceptOnce: func(ctx context.Context) {
			// Simulate event handler updating the saga between ListTimedOut and Update.
			concurrent, _ := inner.Get(ctx, "corr-race")
			concurrent.State = model.SagaInventoryPending
			ok, err := inner.Update(ctx, concurrent)
			require.NoError(t, err)
			require.True(t, ok)
		},
	}

	o := saga.NewOrchestrator(rs, p, testStepTimeout)
	w := saga.NewTimeoutWorker(rs, o, testInterval, testMaxRetries)

	require.NoError(t, w.CheckTimeouts(ctx))

	// No commands should have been published — the worker lost the race.
	assert.Empty(t, p.calls)

	// State should remain as set by the concurrent writer.
	final, _ := inner.Get(ctx, "corr-race")
	assert.Equal(t, model.SagaInventoryPending, final.State)
}

// --- No timed-out sagas ---

func TestTimeoutWorker_NoTimedOutSagas(t *testing.T) {
	w, s, p := newWorkerFixture(t)
	ctx := context.Background()

	future := time.Now().Add(time.Hour)
	sg := &model.SagaInstance{
		ID:            "saga-no-timeout",
		CorrelationID: "corr-no-timeout",
		OrderID:       "order-no-timeout",
		State:         model.SagaPaymentPending,
		Item:          "widget",
		Qty:           1,
		Amount:        9.99,
		StepDeadline:  &future,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	require.NoError(t, s.Create(ctx, sg))

	require.NoError(t, w.CheckTimeouts(ctx))
	assert.Empty(t, p.calls)
}
