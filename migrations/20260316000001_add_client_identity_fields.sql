ALTER TABLE clients
  ADD COLUMN provider TEXT NOT NULL DEFAULT 'widget',
  ADD COLUMN external_id TEXT,
  ADD COLUMN name TEXT,
  ADD COLUMN username TEXT,
  ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE UNIQUE INDEX idx_clients_provider_external_id
  ON clients (provider, external_id)
  WHERE external_id IS NOT NULL;
