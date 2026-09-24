-- DocPilot V2 Schema Migration
-- Processed events table for tracking idempotent consumers

CREATE TABLE processed_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id VARCHAR(255) NOT NULL,
    consumer_name VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    payload_hash VARCHAR(64),
    CONSTRAINT uq_processed_events_event_consumer UNIQUE (event_id, consumer_name)
);

CREATE INDEX idx_processed_events_consumer_event ON processed_events(consumer_name, event_id);
