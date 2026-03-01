package store_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/orchestration/payment-service/internal/model"
	"github.com/grnsv/saga-pattern-go/orchestration/payment-service/internal/store"
)

func TestPaymentStore_Create_Get(t *testing.T) {
	s := store.NewInMemoryPaymentStore()
	p := &model.Payment{
		ID:            "pay-1",
		CorrelationID: "corr-1",
		OrderID:       "order-1",
		Amount:        9.99,
	}

	require.NoError(t, s.Create(p))

	got, err := s.Get("corr-1")
	require.NoError(t, err)
	assert.Equal(t, p.ID, got.ID)
	assert.Equal(t, p.OrderID, got.OrderID)
	assert.InDelta(t, p.Amount, got.Amount, 0.001)
}

func TestPaymentStore_Get_NotFound(t *testing.T) {
	s := store.NewInMemoryPaymentStore()
	_, err := s.Get("nonexistent")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestPaymentStore_Get_ReturnsCopy(t *testing.T) {
	s := store.NewInMemoryPaymentStore()
	p := &model.Payment{ID: "pay-1", CorrelationID: "corr-1", Amount: 9.99}
	require.NoError(t, s.Create(p))

	got, _ := s.Get("corr-1")
	got.Amount = 999
	original, _ := s.Get("corr-1")
	assert.InDelta(t, 9.99, original.Amount, 0.001)
}

func TestPaymentStore_Delete(t *testing.T) {
	s := store.NewInMemoryPaymentStore()
	p := &model.Payment{ID: "pay-2", CorrelationID: "corr-2", OrderID: "order-2", Amount: 19.98}
	require.NoError(t, s.Create(p))

	require.NoError(t, s.Delete("corr-2"))

	_, err := s.Get("corr-2")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestPaymentStore_Delete_Idempotent(t *testing.T) {
	s := store.NewInMemoryPaymentStore()
	// Delete of non-existent key is a no-op.
	assert.NoError(t, s.Delete("nonexistent"))
}
