package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/store"
)

// TestPostgresStore_* tests require a real PostgreSQL instance.
// Set TEST_DATABASE_URL to run them, e.g.:
//
//	TEST_DATABASE_URL=postgres://saga:saga@localhost:5432/saga_test?sslmode=disable go test ./internal/store/...
func openTestDB(t *testing.T) *store.PostgresSagaStore {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("set TEST_DATABASE_URL to run postgres store tests")
	}
	s, err := store.NewPostgresSagaStore(context.Background(), dbURL)
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

func TestPostgresStore_Create_Get(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	saga := newSaga("pg-corr-1")

	require.NoError(t, s.Create(ctx, saga))
	t.Cleanup(func() { cleanupSaga(t, s, "pg-corr-1") })

	got, err := s.Get(ctx, "pg-corr-1")
	require.NoError(t, err)
	assert.Equal(t, saga.ID, got.ID)
	assert.Equal(t, model.SagaPaymentPending, got.State)
	assert.InDelta(t, 9.99, got.Amount, 0.001)
}

func TestPostgresStore_Create_RejectsIntegerOverflow(t *testing.T) {
	s := &store.PostgresSagaStore{}
	saga := newSaga("pg-overflow")
	saga.Qty = 2147483648

	err := s.Create(context.Background(), saga)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "qty value")
}

func TestPostgresStore_Update_RejectsIntegerOverflow(t *testing.T) {
	s := &store.PostgresSagaStore{}
	saga := newSaga("pg-overflow-update")
	saga.RetryCount = 2147483648

	ok, err := s.Update(context.Background(), saga)
	require.Error(t, err)
	assert.False(t, ok)
	assert.Contains(t, err.Error(), "retry_count value")
}

func TestPostgresStore_Get_NotFound(t *testing.T) {
	s := openTestDB(t)
	_, err := s.Get(context.Background(), "does-not-exist")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestPostgresStore_Update_OptimisticLock(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()
	saga := newSaga("pg-corr-lock")
	require.NoError(t, s.Create(ctx, saga))
	t.Cleanup(func() { cleanupSaga(t, s, "pg-corr-lock") })

	first := *saga
	first.State = model.SagaInventoryPending
	ok, err := s.Update(ctx, &first)
	require.NoError(t, err)
	assert.True(t, ok)

	// second writer uses stale UpdatedAt — must lose.
	second := *saga
	second.State = model.SagaCompleted
	ok, err = s.Update(ctx, &second)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestPostgresStore_ListByState(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	s1 := newSaga("pg-ls-1")
	s1.State = model.SagaPaymentPending
	s2 := newSaga("pg-ls-2")
	s2.State = model.SagaCompleted

	require.NoError(t, s.Create(ctx, s1))
	require.NoError(t, s.Create(ctx, s2))
	t.Cleanup(func() {
		cleanupSaga(t, s, "pg-ls-1")
		cleanupSaga(t, s, "pg-ls-2")
	})

	list, err := s.ListByState(ctx, model.SagaPaymentPending)
	require.NoError(t, err)
	found := false
	for _, item := range list {
		if item.CorrelationID == "pg-ls-1" {
			found = true
		}
	}
	assert.True(t, found)
}

func TestPostgresStore_ListTimedOut(t *testing.T) {
	s := openTestDB(t)
	ctx := context.Background()

	past := time.Now().Add(-10 * time.Second)
	timedOut := newSaga("pg-timeout-1")
	timedOut.State = model.SagaPaymentPending
	timedOut.StepDeadline = &past

	require.NoError(t, s.Create(ctx, timedOut))
	t.Cleanup(func() { cleanupSaga(t, s, "pg-timeout-1") })

	list, err := s.ListTimedOut(ctx, time.Now())
	require.NoError(t, err)
	found := false
	for _, item := range list {
		if item.CorrelationID == "pg-timeout-1" {
			found = true
		}
	}
	assert.True(t, found)
}

// cleanupSaga deletes a saga by correlationID for test isolation.
func cleanupSaga(t *testing.T, s *store.PostgresSagaStore, correlationID string) {
	t.Helper()
	_ = s.DeleteByCorrelationID(context.Background(), correlationID)
}
