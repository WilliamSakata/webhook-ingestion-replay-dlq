CREATE TABLE IF NOT EXISTS payments (
  payment_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
