package store

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
)

// InMemorySagaStore is a thread-safe in-memory SagaStore implementation.
// It is intended for unit tests only; production code uses PostgresSagaStore.
type InMemorySagaStore struct {
	mu            sync.RWMutex
	sagas         map[string]*model.SagaInstance // keyed by correlationID
	history       map[string][]*model.SagaHistoryEntry
	nextHistoryID int64
}

// NewInMemorySagaStore creates a new in-memory saga store.
func NewInMemorySagaStore() *InMemorySagaStore {
	return &InMemorySagaStore{
		sagas:   make(map[string]*model.SagaInstance),
		history: make(map[string][]*model.SagaHistoryEntry),
	}
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

func (s *InMemorySagaStore) GetByID(ctx context.Context, id string) (*model.SagaInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, saga := range s.sagas {
		if saga.ID == id {
			cp := *saga
			return &cp, nil
		}
	}
	return nil, ErrNotFound
}

func (s *InMemorySagaStore) List(ctx context.Context, state *model.SagaState) ([]*model.SagaInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*model.SagaInstance, 0)
	for _, saga := range s.sagas {
		if state != nil && saga.State != *state {
			continue
		}
		cp := *saga
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})
	return result, nil
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
	result := make([]*model.SagaInstance, 0)
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
	result := make([]*model.SagaInstance, 0)
	for _, saga := range s.sagas {
		if intermediate[saga.State] && saga.StepDeadline != nil && saga.StepDeadline.Before(now) {
			cp := *saga
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (s *InMemorySagaStore) RecordHistory(ctx context.Context, entry *model.SagaHistoryEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.sagaIDExistsLocked(entry.SagaID) {
		return ErrNotFound
	}
	s.recordHistoryLocked(entry)
	return nil
}

func (s *InMemorySagaStore) sagaIDExistsLocked(sagaID string) bool {
	for _, saga := range s.sagas {
		if saga.ID == sagaID {
			return true
		}
	}
	return false
}

func (s *InMemorySagaStore) recordHistoryLocked(entry *model.SagaHistoryEntry) {
	s.nextHistoryID++
	cp := *entry
	cp.ID = s.nextHistoryID
	if cp.CreatedAt.IsZero() {
		cp.CreatedAt = time.Now()
	}
	s.history[cp.SagaID] = append(s.history[cp.SagaID], &cp)
}

func (s *InMemorySagaStore) ListHistory(ctx context.Context, sagaID string) ([]*model.SagaHistoryEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.sagaIDExistsLocked(sagaID) {
		return nil, ErrNotFound
	}
	entries := s.history[sagaID]
	result := make([]*model.SagaHistoryEntry, 0, len(entries))
	for _, entry := range entries {
		cp := *entry
		result = append(result, &cp)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}
