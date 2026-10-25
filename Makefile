.PHONY: help up up-infra up-build down restart logs ps build test build-images run-api run-worker check-db k8s-apply k8s-delete clean

help:
	@echo "DocPilot Development Commands:"
	@echo "  make up           - Start full stack (DocPilot API, Worker, and all infra)"
	@echo "  make up-build     - Rebuild images and start full stack"
	@echo "  make up-infra     - Start infrastructure only (without app containers)"
	@echo "  make down         - Stop all containers"
	@echo "  make restart      - Restart containers"
	@echo "  make logs         - Tail logs of all containers"
	@echo "  make ps           - View running container status"
	@echo "  make check-db     - Verify Postgres connection and pgvector extension"
	@echo "  make build        - Build all Gradle modules"
	@echo "  make build-images - Build Docker images for DocPilot API and Worker"
	@echo "  make test         - Run test suite"
	@echo "  make run-api      - Run DocPilot API service locally"
	@echo "  make run-worker   - Run DocPilot Worker service locally"
	@echo "  make k8s-apply    - Apply Kubernetes manifests (kind/minikube/cluster)"
	@echo "  make k8s-delete   - Delete Kubernetes resources"
	@echo "  make clean        - Clean Gradle build artifacts"

up:
	docker compose up -d

up-build:
	docker compose up -d --build

up-infra:
	docker compose up -d postgres kafka kafka-ui redis minio minio-init jaeger prometheus grafana

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

build-images:
	docker build -t docpilot-api:latest -f Dockerfile.api .
	docker build -t docpilot-worker:latest -f Dockerfile.worker .

test:
	./gradlew test

run-api:
	./gradlew :docpilot-api:bootRun --args='--spring.profiles.active=local'

run-worker:
	./gradlew :docpilot-worker:bootRun --args='--spring.profiles.active=local'

k8s-apply:
	kubectl apply -k k8s/

k8s-delete:
	kubectl delete -k k8s/

clean:
	./gradlew clean

