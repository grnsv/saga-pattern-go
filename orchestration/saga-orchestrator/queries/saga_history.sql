-- name: CreateHistoryEntry :one
INSERT INTO saga_history (
    saga_id,
    from_state,
    to_state,
    event,
    created_at
) VALUES (
    sqlc.arg(saga_id)::uuid,
    sqlc.arg(from_state)::text,
    sqlc.arg(to_state)::text,
    sqlc.arg(event)::text,
    sqlc.arg(created_at)::timestamptz
)
RETURNING id, created_at;

-- name: ListSagaHistory :many
SELECT
    id,
    saga_id,
    from_state,
    to_state,
    COALESCE(event, '') AS event,
    created_at
FROM saga_history
WHERE saga_id = sqlc.arg(saga_id)::uuid
ORDER BY created_at ASC, id ASC;
