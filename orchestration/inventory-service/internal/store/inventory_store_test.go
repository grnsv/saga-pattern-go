package store_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/model"
	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/store"
)

func TestInventoryStore_Create_Get(t *testing.T) {
	s := store.NewInMemoryInventoryStore()
	r := &model.InventoryReservation{
		ID:            "res-1",
		CorrelationID: "corr-1",
		OrderID:       "order-1",
		Item:          "widget",
		Qty:           2,
	}

	require.NoError(t, s.Create(r))

	got, err := s.Get("corr-1")
	require.NoError(t, err)
	assert.Equal(t, r.ID, got.ID)
	assert.Equal(t, r.Item, got.Item)
	assert.Equal(t, r.Qty, got.Qty)
}

func TestInventoryStore_Get_NotFound(t *testing.T) {
	s := store.NewInMemoryInventoryStore()
	_, err := s.Get("nonexistent")
	assert.ErrorIs(t, err, store.ErrNotFound)
}

func TestInventoryStore_Get_ReturnsCopy(t *testing.T) {
	s := store.NewInMemoryInventoryStore()
	r := &model.InventoryReservation{ID: "res-1", CorrelationID: "corr-1", Item: "widget", Qty: 1}
	require.NoError(t, s.Create(r))

	got, _ := s.Get("corr-1")
	got.Qty = 999
	original, _ := s.Get("corr-1")
	assert.Equal(t, 1, original.Qty)
}
