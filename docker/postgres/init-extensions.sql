-- DocPilot PostgreSQL Extension Initializer
-- Enables pgvector for RAG/semantic search and uuid-ossp for UUID keys

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Verify extensions
DO $$
BEGIN
   RAISE NOTICE 'Installed extensions: %', (SELECT string_agg(extname, ', ') FROM pg_extension);
END $$;

