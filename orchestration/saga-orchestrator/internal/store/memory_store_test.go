package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/store"
)

func newSaga(correlationID string) *model.SagaInstance {
	now := time.Now().Truncate(time.Millisecond)
	return &model.SagaInstance{
		ID:            "saga-id-" + correlationID,
		CorrelationID: correlationID,
		OrderID:       "order-" + correlationID,
		State:         model.SagaPaymentPending,
		Item:          "widget",
		Qty:           1,
		Amount:        9.99,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}

func TestMemoryStore_Create_Get(t *testing.T) {
	s := store.NewInMemorySagaStore()
	saga := newSaga("corr-1")

	require.NoError(t, s.Create(context.Background(), saga))

	got, err := s.Get(context.Background(), "corr-1")
	require.NoError(t, err)
	assert.Equal(t, saga.ID, got.ID)
	assert.Equal(t, model.SagaPaymentPending, got.State)
}

func TestMemoryStore_Create_Duplicate(t *testing.T) {
	s := store.NewInMemorySagaStore()
	saga := newSaga("corr-dup")

	require.NoError(t, s.Create(context.Background(), saga))
	err := s.Create(context.Background(), saga)
	assert.Error(t, err)
}

func TestMemoryStore_Get_NotFound(t *testing.T) {
	s := store.NewInMemorySagaStore()
	_, err := s.Get(context.Background(), "nonexistent")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestMemoryStore_Get_ReturnsCopy(t *testing.T) {
	s := store.NewInMemorySagaStore()
	saga := newSaga("corr-copy")
	require.NoError(t, s.Create(context.Background(), saga))

	got, _ := s.Get(context.Background(), "corr-copy")
	got.State = model.SagaCompleted

	original, _ := s.Get(context.Background(), "corr-copy")
	assert.Equal(t, model.SagaPaymentPending, original.State)
}

func TestMemoryStore_Update_Success(t *testing.T) {
	s := store.NewInMemorySagaStore()
	saga := newSaga("corr-upd")
	require.NoError(t, s.Create(context.Background(), saga))

	saga.State = model.SagaInventoryPending
	ok, err := s.Update(context.Background(), saga)
	require.NoError(t, err)
	assert.True(t, ok)

	got, _ := s.Get(context.Background(), "corr-upd")
	assert.Equal(t, model.SagaInventoryPending, got.State)
}

func TestMemoryStore_Update_OptimisticLock(t *testing.T) {
	s := store.NewInMemorySagaStore()
	saga := newSaga("corr-lock")
	require.NoError(t, s.Create(context.Background(), saga))

	// Simulate first writer succeeds.
	first := *saga
	first.State = model.SagaInventoryPending
	ok, err := s.Update(context.Background(), &first)
	require.NoError(t, err)
	require.True(t, ok)

	// Second writer still holds the old UpdatedAt — must lose the race.
	second := *saga
	second.State = model.SagaCompleted
	ok, err = s.Update(context.Background(), &second)
	require.NoError(t, err)
	assert.False(t, ok, "expected optimistic lock failure")

	got, _ := s.Get(context.Background(), "corr-lock")
	assert.Equal(t, model.SagaInventoryPending, got.State, "state must not be overwritten")
}

func TestMemoryStore_ListByState(t *testing.T) {
	s := store.NewInMemorySagaStore()

	s1 := newSaga("corr-a")
	s1.State = model.SagaPaymentPending
	s2 := newSaga("corr-b")
	s2.State = model.SagaCompleted
	s3 := newSaga("corr-c")
	s3.State = model.SagaPaymentPending

	require.NoError(t, s.Create(context.Background(), s1))
	require.NoError(t, s.Create(context.Background(), s2))
	require.NoError(t, s.Create(context.Background(), s3))

	list, err := s.ListByState(context.Background(), model.SagaPaymentPending)
	require.NoError(t, err)
	assert.Len(t, list, 2)
}

func TestMemoryStore_ListTimedOut(t *testing.T) {
	s := store.NewInMemorySagaStore()

	past := time.Now().Add(-10 * time.Second)
	future := time.Now().Add(10 * time.Second)

	timedOut := newSaga("corr-timeout")
	timedOut.State = model.SagaPaymentPending
	timedOut.StepDeadline = &past

	notTimedOut := newSaga("corr-ok")
	notTimedOut.State = model.SagaPaymentPending
	notTimedOut.StepDeadline = &future

	completed := newSaga("corr-done")
	completed.State = model.SagaCompleted
	completed.StepDeadline = &past

	require.NoError(t, s.Create(context.Background(), timedOut))
	require.NoError(t, s.Create(context.Background(), notTimedOut))
	require.NoError(t, s.Create(context.Background(), completed))

	list, err := s.ListTimedOut(context.Background(), time.Now())
	require.NoError(t, err)
	require.Len(t, list, 1)
	assert.Equal(t, "corr-timeout", list[0].CorrelationID)
}
