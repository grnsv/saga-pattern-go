package store

import (
	"errors"
	"sync"

	"github.com/grnsv/saga-pattern-go/orchestration/payment-service/internal/model"
)

// ErrNotFound is returned when a payment is not found.
var ErrNotFound = errors.New("payment not found")

// InMemoryPaymentStore is a thread-safe in-memory payment store.
type InMemoryPaymentStore struct {
	mu       sync.RWMutex
	payments map[string]*model.Payment
}

// NewInMemoryPaymentStore creates a new InMemoryPaymentStore.
func NewInMemoryPaymentStore() *InMemoryPaymentStore {
	return &InMemoryPaymentStore{
		payments: make(map[string]*model.Payment),
	}
}

// Create stores a payment reservation keyed by CorrelationID.
func (s *InMemoryPaymentStore) Create(payment *model.Payment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *payment
	s.payments[payment.CorrelationID] = &cp
	return nil
}

// Get retrieves a payment by correlationID.
func (s *InMemoryPaymentStore) Get(correlationID string) (*model.Payment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.payments[correlationID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *p
	return &cp, nil
}

// Delete removes a payment reservation. It is a no-op if the payment does not exist.
func (s *InMemoryPaymentStore) Delete(correlationID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.payments, correlationID)
	return nil
}
