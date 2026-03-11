package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/grnsv/saga-pattern-go/orchestration/saga-orchestrator/internal/model"
)

// PostgresSagaStore is the production SagaStore backed by PostgreSQL via pgxpool.
type PostgresSagaStore struct {
	pool *pgxpool.Pool
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
	return &PostgresSagaStore{pool: pool}, nil
}

// Close releases all connections in the pool.
func (s *PostgresSagaStore) Close() {
	s.pool.Close()
}

// DeleteByCorrelationID removes a saga by correlationID. Intended for test cleanup only.
func (s *PostgresSagaStore) DeleteByCorrelationID(ctx context.Context, correlationID string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM sagas WHERE correlation_id = $1`, correlationID)
	return err
}

func (s *PostgresSagaStore) Create(ctx context.Context, saga *model.SagaInstance) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO sagas
			(id, correlation_id, order_id, state, item, qty, amount,
			 retry_count, step_deadline, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		saga.ID, saga.CorrelationID, saga.OrderID, string(saga.State),
		saga.Item, saga.Qty, saga.Amount,
		saga.RetryCount, saga.StepDeadline, saga.CreatedAt, saga.UpdatedAt,
	)
	return err
}

func (s *PostgresSagaStore) Get(ctx context.Context, correlationID string) (*model.SagaInstance, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, correlation_id, order_id, state, item, qty,
		       amount::float8, retry_count, step_deadline, created_at, updated_at
		FROM sagas
		WHERE correlation_id = $1`, correlationID)
	return scanSaga(row)
}

// Update persists state changes using optimistic locking on updated_at.
// Returns (false, nil) when the row was modified by a concurrent writer.
func (s *PostgresSagaStore) Update(ctx context.Context, saga *model.SagaInstance) (bool, error) {
	result, err := s.pool.Exec(ctx, `
		UPDATE sagas
		SET state=$2, retry_count=$3, step_deadline=$4, updated_at=NOW()
		WHERE id=$1 AND updated_at=$5`,
		saga.ID, string(saga.State), saga.RetryCount, saga.StepDeadline, saga.UpdatedAt,
	)
	if err != nil {
		return false, err
	}
	return result.RowsAffected() > 0, nil
}

func (s *PostgresSagaStore) ListByState(ctx context.Context, state model.SagaState) ([]*model.SagaInstance, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, correlation_id, order_id, state, item, qty,
		       amount::float8, retry_count, step_deadline, created_at, updated_at
		FROM sagas
		WHERE state = $1`, string(state))
	if err != nil {
		return nil, err
	}
	return collectSagas(rows)
}

func (s *PostgresSagaStore) ListTimedOut(ctx context.Context, now time.Time) ([]*model.SagaInstance, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, correlation_id, order_id, state, item, qty,
		       amount::float8, retry_count, step_deadline, created_at, updated_at
		FROM sagas
		WHERE step_deadline < $1
		  AND state = ANY($2)`,
		now,
		[]string{
			string(model.SagaPaymentPending),
			string(model.SagaInventoryPending),
			string(model.SagaCancelPaymentPending),
		})
	if err != nil {
		return nil, err
	}
	return collectSagas(rows)
}

// scanSaga reads a single row into a SagaInstance.
func scanSaga(row pgx.Row) (*model.SagaInstance, error) {
	var saga model.SagaInstance
	var state string
	var deadline pgtype.Timestamptz
	err := row.Scan(
		&saga.ID, &saga.CorrelationID, &saga.OrderID, &state,
		&saga.Item, &saga.Qty, &saga.Amount,
		&saga.RetryCount, &deadline, &saga.CreatedAt, &saga.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	saga.State = model.SagaState(state)
	if deadline.Valid {
		t := deadline.Time
		saga.StepDeadline = &t
	}
	return &saga, nil
}

// collectSagas iterates a pgx.Rows result and returns all sagas.
func collectSagas(rows pgx.Rows) ([]*model.SagaInstance, error) {
	defer rows.Close()
	var result []*model.SagaInstance
	for rows.Next() {
		var saga model.SagaInstance
		var state string
		var deadline pgtype.Timestamptz
		if err := rows.Scan(
			&saga.ID, &saga.CorrelationID, &saga.OrderID, &state,
			&saga.Item, &saga.Qty, &saga.Amount,
			&saga.RetryCount, &deadline, &saga.CreatedAt, &saga.UpdatedAt,
		); err != nil {
			return nil, err
		}
		saga.State = model.SagaState(state)
		if deadline.Valid {
			t := deadline.Time
			saga.StepDeadline = &t
		}
		result = append(result, &saga)
	}
	return result, rows.Err()
}
