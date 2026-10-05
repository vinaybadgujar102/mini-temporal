ALTER TABLE task_attempts
ADD COLUMN worker_id TEXT,
ADD COLUMN fencing_token BIGINT NOT NULL,
ADD COLUMN lease_until TIMESTAMPTZ,
ADD COLUMN last_heartbeat_at TIMESTAMPTZ;

CREATE UNIQUE INDEX idx_task_attempt_fencing
ON task_attempts(task_id, fencing_token);

 
