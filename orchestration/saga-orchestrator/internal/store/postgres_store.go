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

func (s *PostgresSagaStore) GetByID(ctx context.Context, id string) (*model.SagaInstance, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, correlation_id, order_id, state, item, qty,
		       amount::float8, retry_count, step_deadline, created_at, updated_at
		FROM sagas
		WHERE id = $1`, id)
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
	return s.List(ctx, &state)
}

func (s *PostgresSagaStore) List(ctx context.Context, state *model.SagaState) ([]*model.SagaInstance, error) {
	if state == nil {
		rows, err := s.pool.Query(ctx, `
			SELECT id, correlation_id, order_id, state, item, qty,
			       amount::float8, retry_count, step_deadline, created_at, updated_at
			FROM sagas
			ORDER BY created_at DESC`)
		if err != nil {
			return nil, err
		}
		return collectSagas(rows)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, correlation_id, order_id, state, item, qty,
		       amount::float8, retry_count, step_deadline, created_at, updated_at
		FROM sagas
		WHERE state = $1
		ORDER BY created_at DESC`, string(*state))
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

func (s *PostgresSagaStore) RecordHistory(ctx context.Context, entry *model.SagaHistoryEntry) error {
	return insertHistory(ctx, s.pool, entry)
}

type historyInserter interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func insertHistory(ctx context.Context, q historyInserter, entry *model.SagaHistoryEntry) error {
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = time.Now()
	}
	row := q.QueryRow(ctx, `
		INSERT INTO saga_history (saga_id, from_state, to_state, event, created_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, created_at`,
		entry.SagaID, string(entry.FromState), string(entry.ToState), entry.Event, entry.CreatedAt,
	)
	return row.Scan(&entry.ID, &entry.CreatedAt)
}

func (s *PostgresSagaStore) ListHistory(ctx context.Context, sagaID string) ([]*model.SagaHistoryEntry, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, saga_id, from_state, to_state, COALESCE(event, ''), created_at
		FROM saga_history
		WHERE saga_id = $1
		ORDER BY created_at ASC, id ASC`, sagaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]*model.SagaHistoryEntry, 0)
	for rows.Next() {
		var entry model.SagaHistoryEntry
		var fromState, toState string
		if err := rows.Scan(
			&entry.ID, &entry.SagaID, &fromState, &toState, &entry.Event, &entry.CreatedAt,
		); err != nil {
			return nil, err
		}
		entry.FromState = model.SagaState(fromState)
		entry.ToState = model.SagaState(toState)
		result = append(result, &entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		if _, err := s.GetByID(ctx, sagaID); err != nil {
			return nil, err
		}
	}
	return result, nil
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
	result := make([]*model.SagaInstance, 0)
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
