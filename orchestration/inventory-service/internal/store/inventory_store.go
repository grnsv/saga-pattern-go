package store

import (
	"errors"
	"sync"

	"github.com/grnsv/saga-pattern-go/orchestration/inventory-service/internal/model"
)

// ErrNotFound is returned when a reservation is not found.
var ErrNotFound = errors.New("reservation not found")

// InMemoryInventoryStore is a thread-safe in-memory inventory reservation store.
type InMemoryInventoryStore struct {
	mu           sync.RWMutex
	reservations map[string]*model.InventoryReservation
}

// NewInMemoryInventoryStore creates a new InMemoryInventoryStore.
func NewInMemoryInventoryStore() *InMemoryInventoryStore {
	return &InMemoryInventoryStore{
		reservations: make(map[string]*model.InventoryReservation),
	}
}

// Create stores a reservation keyed by CorrelationID.
func (s *InMemoryInventoryStore) Create(reservation *model.InventoryReservation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *reservation
	s.reservations[reservation.CorrelationID] = &cp
	return nil
}

// Get retrieves a reservation by correlationID.
func (s *InMemoryInventoryStore) Get(correlationID string) (*model.InventoryReservation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.reservations[correlationID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *r
	return &cp, nil
}
