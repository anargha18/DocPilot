-- DocPilot V5 Schema Migration: Add optimistic locking version columns
ALTER TABLE documents ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE communications ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;
