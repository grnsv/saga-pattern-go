CREATE TABLE saga_history (
    id         BIGSERIAL PRIMARY KEY,
    saga_id    UUID NOT NULL REFERENCES sagas(id) ON DELETE CASCADE,
    from_state TEXT NOT NULL,
    to_state   TEXT NOT NULL,
    event      TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_saga_history_saga_id ON saga_history(saga_id);
