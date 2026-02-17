package store

import (
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/grnsv/saga-pattern-go/choreography/inventory-service/internal/model"
)

// InMemoryInventoryStore is a thread-safe in-memory inventory store.
type InMemoryInventoryStore struct {
	mu           sync.RWMutex
	reservations map[string]*model.InventoryReservation
}

// NewInMemoryInventoryStore creates a new in-memory inventory store.
func NewInMemoryInventoryStore() *InMemoryInventoryStore {
	return &InMemoryInventoryStore{
		reservations: make(map[string]*model.InventoryReservation),
	}
}

func (s *InMemoryInventoryStore) Create(reservation *model.InventoryReservation) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate reservation id: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	reservation.ID = id.String()
	s.reservations[reservation.ID] = reservation
	return reservation.ID, nil
}

func (s *InMemoryInventoryStore) Get(id string) (*model.InventoryReservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	reservation, ok := s.reservations[id]
	if !ok {
		return nil, fmt.Errorf("reservation %s not found", id)
	}
	return reservation, nil
}
