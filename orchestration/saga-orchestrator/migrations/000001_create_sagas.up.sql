CREATE TABLE sagas (
    id              UUID PRIMARY KEY,
    correlation_id  UUID UNIQUE NOT NULL,
    order_id        UUID NOT NULL,
    state           TEXT NOT NULL,
    item            TEXT NOT NULL,
    qty             INTEGER NOT NULL,
    amount          NUMERIC(12,2) NOT NULL,
    retry_count     INTEGER DEFAULT 0,
    step_deadline   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_sagas_state ON sagas(state);
CREATE INDEX idx_sagas_deadline ON sagas(step_deadline) WHERE step_deadline IS NOT NULL;
