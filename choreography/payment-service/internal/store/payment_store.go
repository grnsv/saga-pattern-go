package store

import (
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/grnsv/saga-pattern-go/choreography/payment-service/internal/model"
)

// InMemoryPaymentStore is a thread-safe in-memory payment store.
type InMemoryPaymentStore struct {
	mu        sync.RWMutex
	payments  map[string]*model.Payment
	byOrderID map[string]string // orderID -> paymentID
}

// NewInMemoryPaymentStore creates a new in-memory payment store.
func NewInMemoryPaymentStore() *InMemoryPaymentStore {
	return &InMemoryPaymentStore{
		payments:  make(map[string]*model.Payment),
		byOrderID: make(map[string]string),
	}
}

func (s *InMemoryPaymentStore) Create(payment *model.Payment) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate payment id: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	payment.ID = id.String()
	s.payments[payment.ID] = payment
	s.byOrderID[payment.OrderID] = payment.ID
	return payment.ID, nil
}

func (s *InMemoryPaymentStore) Get(id string) (*model.Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	payment, ok := s.payments[id]
	if !ok {
		return nil, fmt.Errorf("payment %s not found", id)
	}
	return payment, nil
}

func (s *InMemoryPaymentStore) GetByOrderID(orderID string) (*model.Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	paymentID, ok := s.byOrderID[orderID]
	if !ok {
		return nil, fmt.Errorf("payment for order %s not found", orderID)
	}
	payment, ok := s.payments[paymentID]
	if !ok {
		return nil, fmt.Errorf("payment %s not found", paymentID)
	}
	return payment, nil
}

func (s *InMemoryPaymentStore) UpdateStatus(id string, status model.PaymentStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	payment, ok := s.payments[id]
	if !ok {
		return fmt.Errorf("payment %s not found", id)
	}
	payment.Status = status
	return nil
}
