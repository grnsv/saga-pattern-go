-- name: DeleteSagaByCorrelationID :exec
DELETE FROM sagas
WHERE correlation_id = $1;

-- name: CreateSaga :exec
INSERT INTO sagas (
    id,
    correlation_id,
    order_id,
    state,
    item,
    qty,
    amount,
    retry_count,
    step_deadline,
    created_at,
    updated_at
) VALUES (
    sqlc.arg(id)::uuid,
    sqlc.arg(correlation_id)::uuid,
    sqlc.arg(order_id)::uuid,
    sqlc.arg(state)::text,
    sqlc.arg(item)::text,
    sqlc.arg(qty)::integer,
    sqlc.arg(amount)::float8,
    sqlc.arg(retry_count)::integer,
    sqlc.narg(step_deadline)::timestamptz,
    sqlc.arg(created_at)::timestamptz,
    sqlc.arg(updated_at)::timestamptz
);

-- name: GetSagaByCorrelationID :one
SELECT
    id,
    correlation_id,
    order_id,
    state,
    item,
    qty,
    amount::float8 AS amount,
    COALESCE(retry_count, 0)::integer AS retry_count,
    step_deadline,
    created_at,
    updated_at
FROM sagas
WHERE correlation_id = $1;

-- name: GetSagaByID :one
SELECT
    id,
    correlation_id,
    order_id,
    state,
    item,
    qty,
    amount::float8 AS amount,
    COALESCE(retry_count, 0)::integer AS retry_count,
    step_deadline,
    created_at,
    updated_at
FROM sagas
WHERE id = $1;

-- name: UpdateSaga :execrows
UPDATE sagas
SET
    state = sqlc.arg(state)::text,
    retry_count = sqlc.arg(retry_count)::integer,
    step_deadline = sqlc.narg(step_deadline)::timestamptz,
    updated_at = NOW()
WHERE id = sqlc.arg(id)::uuid
  AND updated_at = sqlc.arg(updated_at)::timestamptz;

-- name: ListSagas :many
SELECT
    id,
    correlation_id,
    order_id,
    state,
    item,
    qty,
    amount::float8 AS amount,
    COALESCE(retry_count, 0)::integer AS retry_count,
    step_deadline,
    created_at,
    updated_at
FROM sagas
ORDER BY created_at DESC;

-- name: ListSagasByState :many
SELECT
    id,
    correlation_id,
    order_id,
    state,
    item,
    qty,
    amount::float8 AS amount,
    COALESCE(retry_count, 0)::integer AS retry_count,
    step_deadline,
    created_at,
    updated_at
FROM sagas
WHERE state = sqlc.arg(state)::text
ORDER BY created_at DESC;

-- name: ListTimedOutSagas :many
SELECT
    id,
    correlation_id,
    order_id,
    state,
    item,
    qty,
    amount::float8 AS amount,
    COALESCE(retry_count, 0)::integer AS retry_count,
    step_deadline,
    created_at,
    updated_at
FROM sagas
WHERE step_deadline < sqlc.arg(now)::timestamptz
  AND state = ANY(sqlc.arg(states)::text[]);
