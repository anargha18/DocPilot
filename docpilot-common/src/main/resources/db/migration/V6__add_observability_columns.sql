-- DocPilot V6 Migration: Add observability columns (correlation_id and trace_parent)

ALTER TABLE documents ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(255);
ALTER TABLE documents ADD COLUMN IF NOT EXISTS trace_parent VARCHAR(255);
CREATE INDEX IF NOT EXISTS idx_documents_correlation_id ON documents(correlation_id);

ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(255);
ALTER TABLE outbox_events ADD COLUMN IF NOT EXISTS trace_parent VARCHAR(255);
CREATE INDEX IF NOT EXISTS idx_outbox_events_correlation_id ON outbox_events(correlation_id);

ALTER TABLE communications ADD COLUMN IF NOT EXISTS correlation_id VARCHAR(255);
CREATE INDEX IF NOT EXISTS idx_communications_correlation_id ON communications(correlation_id);
