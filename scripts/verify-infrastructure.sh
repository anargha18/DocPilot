#!/usr/bin/env bash
set -e

echo "===================================================="
echo "          DocPilot Infrastructure Health Check      "
echo "===================================================="

# 1. Check Docker daemon
if ! docker info >/dev/null 2>&1; then
  echo "❌ Error: Docker daemon is not running. Please start Docker first."
  exit 1
fi
echo "✅ Docker daemon is running."

# 2. Check containers
echo "--- Checking Docker Compose Services ---"
docker compose ps

# 3. Check PostgreSQL and pgvector
echo "--- Checking PostgreSQL & pgvector extension ---"
if docker compose exec -T postgres psql -U docpilot -d docpilot -c "SELECT extname, extversion FROM pg_extension WHERE extname = 'vector';" | grep -q "vector"; then
  echo "✅ PostgreSQL is running and pgvector extension is enabled!"
  docker compose exec -T postgres psql -U docpilot -d docpilot -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('vector', 'uuid-ossp', 'pg_trgm');"
else
  echo "❌ Error: pgvector extension not found or Postgres is not accessible."
  exit 1
fi

# 4. Check Redis
echo "--- Checking Redis ---"
REDIS_PING=$(docker compose exec -T redis redis-cli ping 2>/dev/null || true)
if [ "$REDIS_PING" = "PONG" ]; then
  echo "✅ Redis is responding (PONG)."
else
  echo "⚠️ Redis ping failed: $REDIS_PING"
fi

# 5. Check Kafka
echo "--- Checking Kafka (KRaft) ---"
if docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
  echo "✅ Kafka broker is healthy and responding to topic requests."
else
  echo "⚠️ Kafka broker is starting up or not ready yet."
fi

# 6. Check MinIO
echo "--- Checking MinIO ---"
if curl -sf http://localhost:9000/minio/health/live >/dev/null 2>&1; then
  echo "✅ MinIO is live at http://localhost:9000"
else
  echo "⚠️ MinIO not reachable on localhost:9000"
fi

echo "===================================================="
echo " Infrastructure checks completed. "
echo "===================================================="

