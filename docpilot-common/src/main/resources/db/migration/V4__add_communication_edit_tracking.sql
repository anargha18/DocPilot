-- DocPilot V4 Schema Migration: Track edit count and edited flag on communications
ALTER TABLE communications ADD COLUMN IF NOT EXISTS edit_count INT NOT NULL DEFAULT 0;
ALTER TABLE communications ADD COLUMN IF NOT EXISTS edited BOOLEAN NOT NULL DEFAULT FALSE;
