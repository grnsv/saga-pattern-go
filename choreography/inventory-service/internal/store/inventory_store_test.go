package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/model"
)

func TestInventoryStore_Create_and_Get(t *testing.T) {
	s := NewInMemoryInventoryStore()
	reservation := &model.InventoryReservation{
		OrderID: "order-1",
		Item:    "widget",
		Qty:     2,
		Status:  model.ReservationConfirmed,
	}

	id, err := s.Create(reservation)
	require.NoError(t, err)
	assert.NotEmpty(t, id)
	assert.Equal(t, id, reservation.ID)

	got, err := s.Get(id)
	require.NoError(t, err)
	assert.Equal(t, reservation, got)
}

func TestInventoryStore_Get_NotFound(t *testing.T) {
	s := NewInMemoryInventoryStore()

	_, err := s.Get("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
