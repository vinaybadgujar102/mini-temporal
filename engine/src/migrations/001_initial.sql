-- ============================================================
-- ENUM TYPES
-- ============================================================

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_type WHERE typname = 'workflow_status'
  ) THEN
    CREATE TYPE workflow_status AS ENUM (
      'RUNNING',
      'COMPLETED',
      'FAILED',
      'CANCELLED'
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_type WHERE typname = 'task_status'
  ) THEN
    CREATE TYPE task_status AS ENUM (
      'PENDING',
      'READY',
      'RUNNING',
      'COMPLETED',
      'FAILED',
      'CANCELLED'
    );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_type WHERE typname = 'attempt_status'
  ) THEN
    CREATE TYPE attempt_status AS ENUM (
      'RUNNING',
      'COMPLETED',
      'FAILED',
      'TIMED_OUT'
    );
  END IF;
END
$$;


-- ============================================================
-- WORKFLOWS
-- ============================================================

CREATE TABLE IF NOT EXISTS workflows (
  id UUID PRIMARY KEY,
  type TEXT NOT NULL,
  name TEXT NOT NULL,
  version INTEGER NOT NULL,
  revision BIGINT NOT NULL DEFAULT 0,
  status workflow_status NOT NULL DEFAULT 'RUNNING',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ============================================================
-- TASKS
-- ============================================================

CREATE TABLE IF NOT EXISTS tasks (
  id UUID PRIMARY KEY,
  workflow_id UUID NOT NULL
    REFERENCES workflows(id),

  name TEXT NOT NULL,
  type TEXT NOT NULL,
  operation_id TEXT NOT NULL,

  status task_status NOT NULL DEFAULT 'PENDING',
  attempt_count INTEGER NOT NULL DEFAULT 0,

  input JSONB,
  result JSONB,
  error JSONB,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (workflow_id, operation_id)
);


-- ============================================================
-- TASK ATTEMPTS
-- ============================================================

CREATE TABLE IF NOT EXISTS task_attempts (
  id UUID PRIMARY KEY,

  task_id UUID NOT NULL
    REFERENCES tasks(id),

  attempt_number INTEGER NOT NULL,
  status attempt_status NOT NULL,

  result JSONB,
  error JSONB,

  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,

  UNIQUE (task_id, attempt_number)
);


-- ============================================================
-- TASK DEPENDENCIES
-- ============================================================

CREATE TABLE IF NOT EXISTS task_dependencies (
  task_id UUID NOT NULL
    REFERENCES tasks(id),

  depends_on_task_id UUID NOT NULL
    REFERENCES tasks(id),

  PRIMARY KEY (task_id, depends_on_task_id),

  CHECK (task_id <> depends_on_task_id)
);


-- ============================================================
-- WORKFLOW EVENTS
-- ============================================================

CREATE TABLE IF NOT EXISTS workflow_events (
  id UUID PRIMARY KEY,

  workflow_id UUID
    REFERENCES workflows(id),

  sequence BIGINT NOT NULL,
  event_type TEXT NOT NULL,

  data JSONB,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (workflow_id, sequence)
);


-- ============================================================
-- OUTBOX
-- ============================================================

CREATE TABLE IF NOT EXISTS outbox (
  id UUID PRIMARY KEY,

  workflow_id UUID NOT NULL
    REFERENCES workflows(id),

  task_id UUID
    REFERENCES tasks(id),

  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ
);
