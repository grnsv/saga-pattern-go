package store

import (
	"context"
	"time"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/apperrors"
	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
)

// ErrNotFound is returned when a saga cannot be found by the requested key.
var ErrNotFound = apperrors.ErrNotFound

// SagaStore defines persistence operations for saga instances.
type SagaStore interface {
	// Create persists a new saga instance.
	Create(ctx context.Context, saga *model.SagaInstance) error

	// Get retrieves a saga by its correlation ID.
	Get(ctx context.Context, correlationID string) (*model.SagaInstance, error)

	// GetByID retrieves a saga by its stable saga ID.
	GetByID(ctx context.Context, id string) (*model.SagaInstance, error)

	// List returns sagas, optionally filtered by state when state is non-nil.
	List(ctx context.Context, state *model.SagaState) ([]*model.SagaInstance, error)

	// Update persists state changes to an existing saga using optimistic locking.
	// It checks that updated_at in the store matches saga.UpdatedAt before writing.
	// Returns (false, nil) when the saga was concurrently modified (lost race);
	// the caller should skip further processing in that case.
	Update(ctx context.Context, saga *model.SagaInstance) (bool, error)

	// ListByState returns all sagas in the given state.
	ListByState(ctx context.Context, state model.SagaState) ([]*model.SagaInstance, error)

	// ListTimedOut returns sagas whose step_deadline has passed and are still
	// in an intermediate state. Used by the timeout worker.
	ListTimedOut(ctx context.Context, now time.Time) ([]*model.SagaInstance, error)

	// RecordHistory persists one saga transition history entry.
	RecordHistory(ctx context.Context, entry *model.SagaHistoryEntry) error

	// ListHistory returns transition history for a saga by saga ID.
	ListHistory(ctx context.Context, sagaID string) ([]*model.SagaHistoryEntry, error)
}
