package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/model"
)

func TestOrderStore_Create_and_Get(t *testing.T) {
	s := NewInMemoryOrderStore()
	order := &model.Order{
		Item:   "widget",
		Qty:    2,
		Amount: 19.98,
		Status: model.OrderPending,
	}

	id, err := s.Create(order)
	require.NoError(t, err)
	assert.NotEmpty(t, id)
	assert.Equal(t, id, order.ID)

	got, err := s.Get(id)
	require.NoError(t, err)
	assert.Equal(t, order, got)
}

func TestOrderStore_Get_NotFound(t *testing.T) {
	s := NewInMemoryOrderStore()

	_, err := s.Get("nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestOrderStore_Update(t *testing.T) {
	s := NewInMemoryOrderStore()
	order := &model.Order{
		Item:   "widget",
		Qty:    1,
		Amount: 9.99,
		Status: model.OrderPending,
	}
	id, err := s.Create(order)
	require.NoError(t, err)

	updated := &model.Order{
		ID:     id,
		Item:   "widget",
		Qty:    1,
		Amount: 9.99,
		Status: model.OrderConfirmed,
	}
	err = s.Update(updated)
	require.NoError(t, err)

	got, err := s.Get(id)
	require.NoError(t, err)
	assert.Equal(t, model.OrderConfirmed, got.Status)
}

func TestOrderStore_Update_NotFound(t *testing.T) {
	s := NewInMemoryOrderStore()

	err := s.Update(&model.Order{ID: "nonexistent"})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
