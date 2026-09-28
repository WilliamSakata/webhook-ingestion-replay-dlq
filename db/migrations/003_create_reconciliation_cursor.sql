CREATE TABLE IF NOT EXISTS reconciliation_cursor (
  id TEXT PRIMARY KEY,
  cursor TEXT,
  updated_at TIMESTAMPTZ NOT NULL
);
