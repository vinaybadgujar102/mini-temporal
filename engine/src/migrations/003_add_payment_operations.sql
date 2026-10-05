CREATE TABLE payment_operations (
  idempotency_key TEXT PRIMARY KEY,
  transaction_id UUID NOT NULL,
  amount BIGINT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)
