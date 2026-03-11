package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
)

// InMemorySagaStore is a thread-safe in-memory SagaStore implementation.
// It is intended for unit tests only; production code uses PostgresSagaStore.
type InMemorySagaStore struct {
	mu    sync.RWMutex
	sagas map[string]*model.SagaInstance // keyed by correlationID
}

// NewInMemorySagaStore creates a new in-memory saga store.
func NewInMemorySagaStore() *InMemorySagaStore {
	return &InMemorySagaStore{sagas: make(map[string]*model.SagaInstance)}
}

func (s *InMemorySagaStore) Create(ctx context.Context, saga *model.SagaInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.sagas[saga.CorrelationID]; exists {
		return fmt.Errorf("saga with correlationId %s already exists", saga.CorrelationID)
	}
	cp := *saga
	s.sagas[saga.CorrelationID] = &cp
	return nil
}

func (s *InMemorySagaStore) Get(ctx context.Context, correlationID string) (*model.SagaInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	saga, ok := s.sagas[correlationID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *saga
	return &cp, nil
}

// Update applies optimistic locking: it checks that the stored saga's UpdatedAt
// matches saga.UpdatedAt before writing. Returns (false, nil) on a lost race.
func (s *InMemorySagaStore) Update(ctx context.Context, saga *model.SagaInstance) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.sagas[saga.CorrelationID]
	if !ok {
		return false, fmt.Errorf("saga %s not found", saga.CorrelationID)
	}
	if !existing.UpdatedAt.Equal(saga.UpdatedAt) {
		return false, nil
	}
	cp := *saga
	cp.UpdatedAt = time.Now()
	s.sagas[saga.CorrelationID] = &cp
	return true, nil
}

func (s *InMemorySagaStore) ListByState(ctx context.Context, state model.SagaState) ([]*model.SagaInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*model.SagaInstance
	for _, saga := range s.sagas {
		if saga.State == state {
			cp := *saga
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (s *InMemorySagaStore) ListTimedOut(ctx context.Context, now time.Time) ([]*model.SagaInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	intermediate := map[model.SagaState]bool{
		model.SagaPaymentPending:       true,
		model.SagaInventoryPending:     true,
		model.SagaCancelPaymentPending: true,
	}
	var result []*model.SagaInstance
	for _, saga := range s.sagas {
		if intermediate[saga.State] && saga.StepDeadline != nil && saga.StepDeadline.Before(now) {
			cp := *saga
			result = append(result, &cp)
		}
	}
	return result, nil
}
