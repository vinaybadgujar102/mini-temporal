ALTER TYPE workflow_status ADD VALUE IF NOT EXISTS 'COMPENSATING';

CREATE TABLE compensation_operations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    workflow_id UUID NOT NULL REFERENCES workflows(id),
    task_id UUID NOT NULL REFERENCES tasks(id),

    idempotency_key TEXT NOT NULL UNIQUE,

    -- Examples: REFUND_PAYMENT, RELEASE_INVENTORY.
    compensation_type TEXT NOT NULL,

    -- Identifies the original successful operation.
    original_operation_id TEXT NOT NULL,

    -- Data required to execute compensation.
    input JSONB NOT NULL DEFAULT '{}'::jsonb,
    result JSONB,
    error JSONB,

    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN (
            'PENDING',
            'READY',
            'RUNNING',
            'COMPLETED',
            'FAILED'
        )),

    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    UNIQUE (workflow_id, task_id, compensation_type)
);


