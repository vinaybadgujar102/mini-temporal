ALTER TABLE compensation_operations
    ADD COLUMN lease_expires_at TIMESTAMPTZ,
    ADD COLUMN fencing_token BIGINT NOT NULL DEFAULT 0;

