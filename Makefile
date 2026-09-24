.PHONY: help up down restart logs ps build test run-api run-worker check-db clean

help:
	@echo "DocPilot Development Commands:"
	@echo "  make up           - Start all infrastructure containers (Postgres, Kafka, Kafka UI, Redis, MinIO)"
	@echo "  make down         - Stop all infrastructure containers"
	@echo "  make restart      - Restart infrastructure containers"
	@echo "  make logs         - Tail logs of all containers"
	@echo "  make ps           - View running container status"
	@echo "  make check-db     - Verify Postgres connection and pgvector extension"
	@echo "  make build        - Build all Gradle modules"
	@echo "  make test         - Run test suite"
	@echo "  make run-api      - Run DocPilot API service locally"
	@echo "  make run-worker   - Run DocPilot Worker service locally"
	@echo "  make clean        - Clean Gradle build artifacts"

up:
	docker compose up -d

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

ps:
	docker compose ps

check-db:
	@echo "Checking Postgres and pgvector extension..."
	@docker compose exec postgres psql -U docpilot -d docpilot -c "SELECT extname, extversion FROM pg_extension WHERE extname IN ('vector', 'uuid-ossp', 'pg_trgm');"

build:
	./gradlew build

test:
	./gradlew test

run-api:
	./gradlew :docpilot-api:bootRun --args='--spring.profiles.active=local'

run-worker:
	./gradlew :docpilot-worker:bootRun --args='--spring.profiles.active=local'

clean:
	./gradlew clean

