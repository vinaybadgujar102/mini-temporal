
ALTER TABLE payment_operations
ADD COLUMN refunded_at TIMESTAMPTZ;

ALTER TABLE payment_operations
ADD COLUMN refund_idempotency_key TEXT UNIQUE;

