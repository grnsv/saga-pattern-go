package store

import (
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/grnsv/saga-pattern-go/choreography/order-service/internal/model"
)

type inMemoryOrderStore struct {
	mu     sync.RWMutex
	orders map[string]*model.Order
}

// NewInMemoryOrderStore creates a new in-memory order store.
func NewInMemoryOrderStore() *inMemoryOrderStore {
	return &inMemoryOrderStore{
		orders: make(map[string]*model.Order),
	}
}

func (s *inMemoryOrderStore) Create(order *model.Order) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate order id: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	order.ID = id.String()
	s.orders[order.ID] = order
	return order.ID, nil
}

func (s *inMemoryOrderStore) Get(id string) (*model.Order, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	order, ok := s.orders[id]
	if !ok {
		return nil, fmt.Errorf("order %s not found", id)
	}
	return order, nil
}

func (s *inMemoryOrderStore) Update(order *model.Order) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.orders[order.ID]; !ok {
		return fmt.Errorf("order %s not found", order.ID)
	}
	s.orders[order.ID] = order
	return nil
}
