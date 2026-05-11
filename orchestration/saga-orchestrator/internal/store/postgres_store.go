package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
	storedb "github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/store/db"
)

// PostgresSagaStore is the production SagaStore backed by PostgreSQL via pgxpool.
type PostgresSagaStore struct {
	pool    *pgxpool.Pool
	queries *storedb.Queries
}

// NewPostgresSagaStore connects to PostgreSQL and returns a ready store.
func NewPostgresSagaStore(ctx context.Context, databaseURL string) (*PostgresSagaStore, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &PostgresSagaStore{
		pool:    pool,
		queries: storedb.New(pool),
	}, nil
}

// Close releases all connections in the pool.
func (s *PostgresSagaStore) Close() {
	s.pool.Close()
}

// DeleteByCorrelationID removes a saga by correlationID. Intended for test cleanup only.
func (s *PostgresSagaStore) DeleteByCorrelationID(ctx context.Context, correlationID string) error {
	return s.queries.DeleteSagaByCorrelationID(ctx, correlationID)
}

func (s *PostgresSagaStore) Create(ctx context.Context, saga *model.SagaInstance) error {
	qty, err := int32Param("qty", saga.Qty)
	if err != nil {
		return err
	}
	retryCount, err := int32Param("retry_count", saga.RetryCount)
	if err != nil {
		return err
	}
	return s.queries.CreateSaga(ctx, storedb.CreateSagaParams{
		ID:            saga.ID,
		CorrelationID: saga.CorrelationID,
		OrderID:       saga.OrderID,
		State:         string(saga.State),
		Item:          saga.Item,
		Qty:           qty,
		Amount:        saga.Amount,
		RetryCount:    retryCount,
		StepDeadline:  saga.StepDeadline,
		CreatedAt:     saga.CreatedAt,
		UpdatedAt:     saga.UpdatedAt,
	})
}

func (s *PostgresSagaStore) Get(ctx context.Context, correlationID string) (*model.SagaInstance, error) {
	row, err := s.queries.GetSagaByCorrelationID(ctx, correlationID)
	if err != nil {
		return nil, mapNotFound(err)
	}
	return sagaFromCorrelationIDRow(&row), nil
}

func (s *PostgresSagaStore) GetByID(ctx context.Context, id string) (*model.SagaInstance, error) {
	row, err := s.queries.GetSagaByID(ctx, id)
	if err != nil {
		return nil, mapNotFound(err)
	}
	return sagaFromIDRow(&row), nil
}

// Update persists state changes using optimistic locking on updated_at.
// Returns (false, nil) when the row was modified by a concurrent writer.
func (s *PostgresSagaStore) Update(ctx context.Context, saga *model.SagaInstance) (bool, error) {
	retryCount, err := int32Param("retry_count", saga.RetryCount)
	if err != nil {
		return false, err
	}
	rowsAffected, err := s.queries.UpdateSaga(ctx, storedb.UpdateSagaParams{
		State:        string(saga.State),
		RetryCount:   retryCount,
		StepDeadline: saga.StepDeadline,
		ID:           saga.ID,
		UpdatedAt:    saga.UpdatedAt,
	})
	if err != nil {
		return false, err
	}
	return rowsAffected > 0, nil
}

func (s *PostgresSagaStore) ListByState(ctx context.Context, state model.SagaState) ([]*model.SagaInstance, error) {
	return s.List(ctx, &state)
}

func (s *PostgresSagaStore) List(ctx context.Context, state *model.SagaState) ([]*model.SagaInstance, error) {
	if state == nil {
		rows, err := s.queries.ListSagas(ctx)
		if err != nil {
			return nil, err
		}
		result := make([]*model.SagaInstance, 0, len(rows))
		for i := range rows {
			result = append(result, sagaFromListRow(&rows[i]))
		}
		return result, nil
	}

	rows, err := s.queries.ListSagasByState(ctx, string(*state))
	if err != nil {
		return nil, err
	}
	result := make([]*model.SagaInstance, 0, len(rows))
	for i := range rows {
		result = append(result, sagaFromListByStateRow(&rows[i]))
	}
	return result, nil
}

func (s *PostgresSagaStore) ListTimedOut(ctx context.Context, now time.Time) ([]*model.SagaInstance, error) {
	rows, err := s.queries.ListTimedOutSagas(ctx, storedb.ListTimedOutSagasParams{
		Now: now,
		States: []string{
			string(model.SagaPaymentPending),
			string(model.SagaInventoryPending),
			string(model.SagaCancelPaymentPending),
		},
	})
	if err != nil {
		return nil, err
	}
	result := make([]*model.SagaInstance, 0, len(rows))
	for i := range rows {
		result = append(result, sagaFromTimedOutRow(&rows[i]))
	}
	return result, nil
}

func (s *PostgresSagaStore) RecordHistory(ctx context.Context, entry *model.SagaHistoryEntry) error {
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	row, err := s.queries.CreateHistoryEntry(ctx, storedb.CreateHistoryEntryParams{
		SagaID:    entry.SagaID,
		FromState: string(entry.FromState),
		ToState:   string(entry.ToState),
		Event:     entry.Event,
		CreatedAt: entry.CreatedAt,
	})
	if err != nil {
		return err
	}
	entry.ID = row.ID
	entry.CreatedAt = row.CreatedAt
	return nil
}

func (s *PostgresSagaStore) ListHistory(ctx context.Context, sagaID string) ([]*model.SagaHistoryEntry, error) {
	rows, err := s.queries.ListSagaHistory(ctx, sagaID)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		if _, err := s.GetByID(ctx, sagaID); err != nil {
			return nil, err
		}
	}
	result := make([]*model.SagaHistoryEntry, 0, len(rows))
	for _, row := range rows {
		result = append(result, &model.SagaHistoryEntry{
			ID:        row.ID,
			SagaID:    row.SagaID,
			FromState: model.SagaState(row.FromState),
			ToState:   model.SagaState(row.ToState),
			Event:     row.Event,
			CreatedAt: row.CreatedAt,
		})
	}
	return result, nil
}

func mapNotFound(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

func int32Param(name string, value int) (int32, error) {
	const (
		minInt32 = -2147483648
		maxInt32 = 2147483647
	)
	if value < minInt32 || value > maxInt32 {
		return 0, fmt.Errorf("%s value %d is outside PostgreSQL integer range", name, value)
	}
	return int32(value), nil
}

func sagaFromCorrelationIDRow(row *storedb.GetSagaByCorrelationIDRow) *model.SagaInstance {
	return &model.SagaInstance{
		ID:            row.ID,
		CorrelationID: row.CorrelationID,
		OrderID:       row.OrderID,
		State:         model.SagaState(row.State),
		Item:          row.Item,
		Qty:           int(row.Qty),
		Amount:        row.Amount,
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
		StepDeadline:  row.StepDeadline,
		RetryCount:    int(row.RetryCount),
	}
}

func sagaFromIDRow(row *storedb.GetSagaByIDRow) *model.SagaInstance {
	return &model.SagaInstance{
		ID:            row.ID,
		CorrelationID: row.CorrelationID,
		OrderID:       row.OrderID,
		State:         model.SagaState(row.State),
		Item:          row.Item,
		Qty:           int(row.Qty),
		Amount:        row.Amount,
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
		StepDeadline:  row.StepDeadline,
		RetryCount:    int(row.RetryCount),
	}
}

func sagaFromListRow(row *storedb.ListSagasRow) *model.SagaInstance {
	return &model.SagaInstance{
		ID:            row.ID,
		CorrelationID: row.CorrelationID,
		OrderID:       row.OrderID,
		State:         model.SagaState(row.State),
		Item:          row.Item,
		Qty:           int(row.Qty),
		Amount:        row.Amount,
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
		StepDeadline:  row.StepDeadline,
		RetryCount:    int(row.RetryCount),
	}
}

func sagaFromListByStateRow(row *storedb.ListSagasByStateRow) *model.SagaInstance {
	return &model.SagaInstance{
		ID:            row.ID,
		CorrelationID: row.CorrelationID,
		OrderID:       row.OrderID,
		State:         model.SagaState(row.State),
		Item:          row.Item,
		Qty:           int(row.Qty),
		Amount:        row.Amount,
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
		StepDeadline:  row.StepDeadline,
		RetryCount:    int(row.RetryCount),
	}
}

func sagaFromTimedOutRow(row *storedb.ListTimedOutSagasRow) *model.SagaInstance {
	return &model.SagaInstance{
		ID:            row.ID,
		CorrelationID: row.CorrelationID,
		OrderID:       row.OrderID,
		State:         model.SagaState(row.State),
		Item:          row.Item,
		Qty:           int(row.Qty),
		Amount:        row.Amount,
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
		StepDeadline:  row.StepDeadline,
		RetryCount:    int(row.RetryCount),
	}
}
