-- DocPilot V1 Schema Migration
-- Enables required extensions, domain tables, audit trail, and transactional outbox

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Documents table
CREATE TABLE documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    filename VARCHAR(255) NOT NULL,
    storage_key VARCHAR(500) NOT NULL,
    content_type VARCHAR(100) NOT NULL,
    file_size BIGINT NOT NULL,
    upload_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    status VARCHAR(50) NOT NULL DEFAULT 'UPLOADED',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_document_status CHECK (status IN (
        'UPLOADED', 'ANALYZING', 'ANALYZED', 'GENERATING',
        'PENDING_APPROVAL', 'APPROVED', 'SENT', 'FAILED'
    ))
);

CREATE INDEX idx_documents_status ON documents(status);
CREATE INDEX idx_documents_upload_timestamp ON documents(upload_timestamp DESC);

-- Document Analyses table (1:1 with documents)
CREATE TABLE document_analyses (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL UNIQUE REFERENCES documents(id) ON DELETE CASCADE,
    summary TEXT NOT NULL,
    extracted_context TEXT,
    category VARCHAR(100),
    raw_extraction_json JSONB,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_document_analyses_document_id ON document_analyses(document_id);

-- Communications table (1:N with documents for EMAIL or SOCIAL outputs)
CREATE TABLE communications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    type VARCHAR(50) NOT NULL,
    generated_content TEXT,
    edited_content TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'DRAFT',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_communication_type CHECK (type IN ('EMAIL', 'SOCIAL')),
    CONSTRAINT chk_communication_status CHECK (status IN (
        'DRAFT', 'EDITED', 'APPROVED', 'REJECTED', 'SENT', 'FAILED'
    ))
);

CREATE INDEX idx_communications_document_id ON communications(document_id);
CREATE INDEX idx_communications_document_type ON communications(document_id, type);
CREATE INDEX idx_communications_status ON communications(status);

-- Activity Logs table (audit trail for document lifecycle events)
CREATE TABLE activity_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
    action_type VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    details TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_activity_logs_document_timestamp ON activity_logs(document_id, timestamp ASC);

-- Outbox Events table for Transactional Outbox Pattern
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    published BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_outbox_events_published_created ON outbox_events(published, created_at ASC);
CREATE INDEX idx_outbox_events_aggregate ON outbox_events(aggregate_type, aggregate_id);

