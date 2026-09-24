-- DocPilot V3 Schema Migration: vector_store table for Spring AI PgVectorStore
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS vector_store (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    content TEXT,
    metadata JSON,
    embedding vector(1536)
);

CREATE INDEX IF NOT EXISTS idx_vector_store_embedding_hnsw 
    ON vector_store USING hnsw (embedding vector_cosine_ops);
