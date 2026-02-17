package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/model"
)

func TestPaymentStore_Create_and_Get(t *testing.T) {
	s := NewInMemoryPaymentStore()
	payment := &model.Payment{
		OrderID: "order-1",
		Amount:  19.98,
		Status:  model.PaymentReserved,
	}

	id, err := s.Create(payment)
	require.NoError(t, err)
	assert.NotEmpty(t, id)
	assert.Equal(t, id, payment.ID)

	got, err := s.Get(id)
	require.NoError(t, err)
	assert.Equal(t, payment, got)
}

func TestPaymentStore_Get_NotFound(t *testing.T) {
	s := NewInMemoryPaymentStore()

	_, err := s.Get("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestPaymentStore_GetByOrderID(t *testing.T) {
	s := NewInMemoryPaymentStore()
	payment := &model.Payment{
		OrderID: "order-1",
		Amount:  9.99,
		Status:  model.PaymentReserved,
	}
	_, err := s.Create(payment)
	require.NoError(t, err)

	got, err := s.GetByOrderID("order-1")
	require.NoError(t, err)
	assert.Equal(t, payment, got)
}

func TestPaymentStore_GetByOrderID_NotFound(t *testing.T) {
	s := NewInMemoryPaymentStore()

	_, err := s.GetByOrderID("nonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestPaymentStore_UpdateStatus(t *testing.T) {
	s := NewInMemoryPaymentStore()
	payment := &model.Payment{
		OrderID: "order-1",
		Amount:  9.99,
		Status:  model.PaymentReserved,
	}
	id, err := s.Create(payment)
	require.NoError(t, err)

	err = s.UpdateStatus(id, model.PaymentRolledBack)
	require.NoError(t, err)

	got, err := s.Get(id)
	require.NoError(t, err)
	assert.Equal(t, model.PaymentRolledBack, got.Status)
}

func TestPaymentStore_UpdateStatus_NotFound(t *testing.T) {
	s := NewInMemoryPaymentStore()

	err := s.UpdateStatus("nonexistent", model.PaymentRolledBack)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
