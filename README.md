# DocPilot — AI-Assisted Organizational Communication Platform

[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/projects/jdk/21/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.3.4-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Spring AI](https://img.shields.io/badge/Spring%20AI-1.0.0-blue.svg)](https://spring.io/projects/spring-ai)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.8.0-black.svg)](https://kafka.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL%20%2B%20pgvector-16-blue.svg)](https://github.com/pgvector/pgvector)
[![Swagger](https://img.shields.io/badge/OpenAPI%203.0-Swagger%20UI-green.svg)](http://localhost:8080/swagger-ui.html)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

DocPilot is an enterprise-grade, event-driven Java 21 / Spring Boot platform that transforms organizational documents into ready-to-deliver communications (Email and Social Media). It combines **Spring AI** and **PostgreSQL + pgvector** retrieval-augmented generation (RAG) with a distributed, fault-tolerant backend engineered with **Apache Kafka**, the **Transactional Outbox Pattern**, idempotent consumers, Dead Letter Queue (DLQ) replay, **Redis** rate limiting, and full **OpenTelemetry** observability.

---

## What This Project Demonstrates

DocPilot was designed to showcase production-grade backend engineering and distributed systems principles:

- **Backend Engineering**: Layered architecture, RESTful API design with Spring MVC, stateless JWT authentication with role-based access control (`ROLE_USER`, `ROLE_ADMIN`), declarative transactions with Spring Data JPA/Hibernate, and Flyway database migrations.
- **Distributed Systems**: Asynchronous event-driven pipeline powered by Apache Kafka, KRaft cluster mode, partition keying for per-document ordering, consumer groups for horizontal scale-out, and eventual consistency.
- **Enterprise Reliability & Fault Isolation**:
  - **Transactional Outbox Pattern**: Eliminates dual-write vulnerabilities between PostgreSQL and Kafka by atomically persisting events and dispatching them via a scheduled outbox relay.
  - **Idempotency**: Message deduplication via a dedicated `processed_events` table tracking `(event_id, consumer_name)` preventing duplicate execution.
  - **Resilience & Circuit Breaking**: Resilience4j Circuit Breakers and Time Limiters isolating external service delivery (SMTP / X API) from core operations.
  - **Dead Letter Queue (DLQ) & Replay Engine**: Automatic error recovery routing exhausted retries to `*.dlq` topics with an administrative REST API for inspection and re-injection.
  - **Optimistic Locking**: Entity versioning (`@Version`) on `Document` and `Communication` guarding against lost updates in concurrent workflows.
- **Data & Storage**: PostgreSQL 16 with `pgvector` HNSW indexing for cosine similarity retrieval, MinIO S3-compatible object storage for document files, and Redis for distributed rate limiting and token caching.
- **AI & RAG Integration**: Spring AI `ChatClient` with structured prompt templates, document parsing via Apache Tika, semantic text chunking, OpenAI embeddings, and similarity-grounded content generation.
- **Cloud Native**: Multi-stage Docker builds producing lightweight Alpine JRE images running as non-root users, Kubernetes manifests with Kustomize, and health/liveness probes tied directly to Spring Boot Actuator.
- **Full-Stack Observability**: Structured JSON logging via Logstash, Prometheus metrics scraping, custom business metrics via Micrometer, Grafana dashboards, OpenTelemetry distributed tracing with Jaeger, and correlation ID propagation across HTTP headers and Kafka records.

---

## Architecture

### System Architecture Diagram (ASCII)

```text
                                       +-------------------------+
                                       |   Client / Web / CLI    |
                                       +------------+------------+
                                                    |
                                      REST + JWT    |  Correlation ID
                                                    v
                                      +-------------+------------+
                                      |    docpilot-api (:8080)  |
                                      |  (Swagger UI / Actuator) |
                                      +--+----------+---------+--+
                                         |          |         |
                  Multipart File Upload  |          |         | Distributed Rate Limiting
                         v               |          |         v
       +-------------------------+       |          |      +------------------+
       |   MinIO S3 (:9000)      |       |          |      |  Redis (:6379)   |
       |  (docpilot-documents)   |       |          |      +------------------+
       +-------------------------+       |          |
                                         |          | ACID Transaction (Outbox Pattern)
                                         |          v
                                         |   +------------------------------------+
                                         |   |  PostgreSQL 16 + pgvector (:5432)  |
                                         |   |  - documents      - communications |
                                         |   |  - outbox_events  - vector_store   |
                                         |   |  - activity_logs  - processed_evts |
                                         |   +------------------------------------+
                                         |                  ^
                      Outbox Relay Poller|                  |
                                         v                  |
                   +---------------------+------------------+---+
                   |              Apache Kafka (:9092)          |
                   |  Topics:                                   |
                   |   - document.uploaded  -->  *.dlq          |
                   |   - content.generated                      |
                   |   - content.approved   -->  *.dlq          |
                   |   - communication.sent / failed            |
                   +---------------------+----------------------+
                                         |
                       Kafka Consumer    | Event-Driven Processing
                                         v
                               +---------+------------------+
                               |  docpilot-worker (:8081)   |
                               |  - Tika Text Extraction    |
                               |  - pgvector RAG Embedding  |
                               |  - Spring AI / LLM Client  |
                               |  - Delivery Engine         |
                               +----+-------------------+---+
                                    |                   |
            Resilience4j Protected  |                   |  Resilience4j Protected
            SMTP / SendGrid         |                   |  X (Twitter) API
                                    v                   v
                        +-----------+----+         +----+-----------+
                        | Email Provider |         | Social Media   |
                        +----------------+         +----------------+

  ============================== OBSERVABILITY ==============================
     docpilot-api & docpilot-worker  -->  OTel Traces   -->  Jaeger (:16686)
     docpilot-api & docpilot-worker  -->  Prom Metrics  -->  Prometheus (:9090)  -->  Grafana (:3000)
```

### Module Structure

```text
DocPilot/
├── docpilot-common/     # Shared domain entities, DTOs, events, outbox relay, RAG embeddings, MinIO client
├── docpilot-api/        # REST controllers, OpenAPI/Swagger UI, JWT security, Redis rate limiter, outbox publisher
├── docpilot-worker/     # Kafka consumers, Spring AI analysis, RAG pipeline, SMTP & Social delivery with Resilience4j
├── k8s/                 # Production-grade Kubernetes manifests (Kind / Minikube compatible)
├── docker/              # Prometheus, Grafana, and Jaeger provisioning configurations
└── docker-compose.yml   # Full-stack multi-container composition
```

---

## Interactive API Documentation (Swagger / OpenAPI)

When running the application, live OpenAPI 3.0 documentation and the interactive Swagger UI are accessible at:

- **Swagger UI**: [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html)
- **OpenAPI JSON Specification**: [http://localhost:8080/v3/api-docs](http://localhost:8080/v3/api-docs)

### API Endpoints Summary

| Tag | HTTP Method | Path | Access | Description |
|---|---|---|---|---|
| **Authentication** | `POST` | `/api/auth/login` | Public | Authenticates credentials and issues signed JWT token |
| **Authentication** | `GET` | `/api/auth/me` | Authenticated | Retrieves current authenticated user details and roles |
| **Documents** | `POST` | `/api/documents` | Authenticated | Uploads document to MinIO and enqueues Kafka outbox event |
| **Documents** | `GET` | `/api/documents` | Authenticated | Lists all uploaded documents ordered by timestamp |
| **Documents** | `GET` | `/api/documents/{id}` | Authenticated | Retrieves document processing status and metadata |
| **Documents** | `GET` | `/api/documents/{id}/activities` | Authenticated | Chronological audit trail of document lifecycle events |
| **Communications** | `POST` | `/api/documents/{id}/communications` | Authenticated | Generates AI draft (Email/Social) grounded with RAG |
| **Communications** | `GET` | `/api/documents/{id}/communications` | Authenticated | Lists all communication drafts for a document |
| **Communications** | `GET` | `/api/communications/{id}` | Authenticated | Gets communication details and generated/edited text |
| **Communications** | `PATCH` | `/api/communications/{id}` | Authenticated | Applies single-round user edit (max 280 chars for social) |
| **Communications** | `POST` | `/api/communications/{id}/approve` | Authenticated | Approves draft and enqueues Kafka outbox event for dispatch |
| **Communications** | `POST` | `/api/communications/{id}/reject` | Authenticated | Rejects draft communication |
| **Admin DLQ** | `GET` | `/api/admin/dlq/messages` | `ROLE_ADMIN` | Inspects uncommitted dead letter queue messages |
| **Admin DLQ** | `POST` | `/api/admin/dlq/replay` | `ROLE_ADMIN` | Replays DLQ messages and resets idempotency records |
| **System** | `GET` | `/api/v1/ping` | Public | Lightweight liveness probe |
| **Actuator** | `GET` | `/actuator/health` | Public | Spring Boot Actuator health probe (Liveness & Readiness) |

---

## Quickstart & Setup Instructions

### Prerequisites

- **Java 21** (JDK)
- **Docker** and **Docker Compose**
- *(Optional)* **Kubernetes CLI (`kubectl`)** and **Kind / Minikube** for Kubernetes deployment

---

### Option 1: One-Command Full Stack (Docker Compose) — Recommended

Start all 10 services (API, Worker, PostgreSQL + pgvector, Kafka KRaft, Kafka-UI, Redis, MinIO, Prometheus, Grafana, Jaeger):

```bash
# 1. Clone repository
git clone https://github.com/docpilot/docpilot.git
cd docpilot

# 2. Build and launch all services in detached mode
docker compose up -d --build
```

Verify all containers are up and healthy:

```bash
docker compose ps
```

#### Service Port Reference

| Service | Host Port | Purpose | UI / Health URL |
|---|---|---|---|
| **docpilot-api** | `8080` | Core REST API & Swagger UI | [http://localhost:8080/swagger-ui.html](http://localhost:8080/swagger-ui.html) |
| **docpilot-worker** | `8081` | Asynchronous Kafka Worker & RAG | [http://localhost:8081/actuator/health](http://localhost:8081/actuator/health) |
| **PostgreSQL** | `5432` | PostgreSQL + pgvector database | `localhost:5432` (`docpilot`/`docpilot_secret`) |
| **Apache Kafka** | `9092` | Event Streaming Platform (KRaft) | `localhost:9092` |
| **Kafka UI** | `8085` | Topic & Consumer Group Console | [http://localhost:8085](http://localhost:8085) |
| **Redis** | `6379` | Distributed Cache & Rate Limiting | `redis-cli -p 6379 ping` |
| **MinIO API** | `9000` | S3-compatible Object Storage | [http://localhost:9000/minio/health/live](http://localhost:9000/minio/health/live) |
| **MinIO Console** | `9001` | S3 Storage Web Console | [http://localhost:9001](http://localhost:9001) (`minioadmin`/`minioadmin`) |
| **Grafana** | `3000` | Metric Visualizations & Dashboards | [http://localhost:3000](http://localhost:3000) (`admin`/`admin`) |
| **Prometheus** | `9090` | Timeseries Metrics Scraper | [http://localhost:9090](http://localhost:9090) |
| **Jaeger** | `16686` | Distributed Tracing Dashboard | [http://localhost:16686](http://localhost:16686) |

---

### Option 2: Local Development Setup (Gradle + Infra Containers)

If you wish to run the Spring Boot applications directly via Gradle with active debugger support:

```bash
# 1. Start backing infrastructure services only
make up-infra
# Or: docker compose up -d postgres kafka kafka-ui redis minio minio-init jaeger prometheus grafana

# 2. Run docpilot-api in a terminal
./gradlew :docpilot-api:bootRun

# 3. Run docpilot-worker in a second terminal
./gradlew :docpilot-worker:bootRun
```

Both applications will bind to `localhost:8080` and `localhost:8081` using the `local` Spring profile.

---

## End-to-End Pipeline Walkthrough

Once the stack is running, execute this step-by-step walkthrough to test the entire lifecycle:

### 1. Authenticate (Obtain JWT)

Pre-configured credentials:
- **Regular User**: `user` / `password`
- **Administrator**: `admin` / `password`

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "user", "password": "password"}' | jq -r '.token')

echo "Acquired JWT: $TOKEN"
```

### 2. Upload a Document

```bash
# Create sample announcement document
cat << 'EOF' > product-launch.txt
DocPilot 1.0 has officially launched across enterprise environments.
Key capabilities include multi-channel AI generation, Kafka-driven reliability,
pgvector semantic grounding, and full OpenTelemetry distributed tracing.
EOF

# Upload document
DOC_RESPONSE=$(curl -s -X POST http://localhost:8080/api/documents \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@product-launch.txt")

DOC_ID=$(echo $DOC_RESPONSE | jq -r '.id')
echo "Uploaded Document ID: $DOC_ID"
```

### 3. Verify Asynchronous AI Analysis & RAG Grounding

The document is stored in MinIO, an event `document.uploaded` is emitted to Kafka via the Transactional Outbox, and `docpilot-worker` extracts text, chunks content into pgvector, and runs AI analysis:

```bash
# Wait 2 seconds, then query document status
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/documents/$DOC_ID | jq .
```

Status transitions to **`ANALYZED`**, displaying extracted context, category, and summary.

### 4. Generate AI Communication Content (Email or Social Media)

Select target channel:

```bash
# Generate Email draft
COMM_RESPONSE=$(curl -s -X POST "http://localhost:8080/api/documents/$DOC_ID/communications" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "EMAIL",
    "tone": "EXECUTIVE_ANNOUNCEMENT"
  }')

COMM_ID=$(echo $COMM_RESPONSE | jq -r '.id')
echo "Communication Draft ID: $COMM_ID"
echo $COMM_RESPONSE | jq .
```

### 5. Review & Single-Round Edit

DocPilot enforces strict governance: exactly **one** edit round is permitted:

```bash
# Edit draft content
curl -s -X PATCH "http://localhost:8080/api/communications/$COMM_ID" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"content": "Subject: DocPilot 1.0 Enterprise Launch\n\nTeam,\nDocPilot 1.0 is live with Kafka and Spring AI support."}' | jq .
```

### 6. Approve for Asynchronous Dispatch

Only approved content can reach execution:

```bash
# Approve content
curl -s -X POST "http://localhost:8080/api/communications/$COMM_ID/approve" \
  -H "Authorization: Bearer $TOKEN" | jq .
```

The transactional outbox publishes `content.approved`, and `docpilot-worker` dispatches the message via SMTP or X API.

### 7. Audit Activity Trail

View the complete immutable audit trail of actions taken on the document:

```bash
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/documents/$DOC_ID/activities | jq .
```

---

## Testing & Quality Assurance

DocPilot maintains rigorous test coverage spanning unit tests, integration tests, and chaos recovery scenarios:

```bash
# Run all tests across the multi-module project
./gradlew test
```

### Test Coverage Breakdown

- **Service Logic Unit Tests (JUnit 5 + Mockito)**:
  - `DocumentServiceTest`: File storage delegation, entity persistence, activity logging, outbox event serialization.
  - `JwtServiceTest`: Token creation, expiration validation, claims parsing, role mapping, secret key hashing fallback.
  - `DistributedRateLimiterTest`: Throttling thresholds, window expiration, fail-open resiliency.
  - `CommunicationReviewServiceTest`: Single-round edit enforcement, draft status checks, 280-char limit for social media, outbox dispatch.
  - `ContentGenerationServiceTest`: Prompt assembly, RAG chunk retrieval, LLM call, outbox event generation.
  - `CommunicationExecutionServiceTest`: Rejection of unapproved communications.
  - `DlqManagementServiceTest`: DLQ consumer inspection, target topic auto-resolution, replay dispatch.
  - `OutboxRelayTest`: Scheduled polling, Kafka producer dispatch, published flag update.
  - `RetryHelperTest`: Exponential backoff calculation, retry exhaustion, metrics recording.
  - `EmailServiceTest`: SMTP delivery, circuit breaker fallback, failure state handling.
  - `SocialMediaServiceTest`: X API delivery, 280-char enforcement, circuit breaker fallback.
  - `DocumentTextExtractorTest`: Apache Tika text parsing across multiple file types.

- **Infrastructure Integration Tests (Testcontainers)**:
  - **PostgreSQL + pgvector**: `AbstractPostgresRepositoryTest`, `OptimisticLockingConcurrencyTest`, `DocumentRepositoryTest`, `CommunicationRepositoryTest`, `OutboxEventRepositoryTest`, `PgVectorAnalysisIntegrationTest`.
  - **Apache Kafka**: `EventDrivenBackboneIntegrationTest`, `ConsumerIdempotencyIntegrationTest`, `DeliveryChannelIntegrationTest`, `DlqInspectionAndReplayIntegrationTest`, `RecoveryAfterCrashChaosTest`.
  - **MinIO S3**: `MinioStorageServiceTestcontainersIntegrationTest` (runs `minio/minio:latest`, tests bucket creation, object uploads, downloads, and checksums).
  - **Redis**: `RedisRateLimiterIntegrationTest` (runs `redis:7-alpine`, tests real Redis key increments, window TTL expiration, and rate enforcement).

---

## Kubernetes Deployment (Kind / Minikube)

Manifests under `k8s/` provide a production deployment topology with native Kustomize support:

```bash
# 1. Create a local cluster
kind create cluster --name docpilot

# 2. Load built images into cluster
kind load docker-image docpilot-api:latest --name docpilot
kind load docker-image docpilot-worker:latest --name docpilot

# 3. Deploy full stack
kubectl apply -k k8s/

# 4. Monitor deployment rollouts
kubectl get pods -n docpilot -w
```

---

## Dead Letter Queue (DLQ) Management & Replay

When unrecoverable errors occur or retry policies exhaust, messages route to Dead Letter Queues (`document.uploaded.dlq` or `content.approved.dlq`). Administrators can inspect and replay them via the REST API:

```bash
ADMIN_TOKEN=$(curl -s -X POST http://localhost:8080/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "password"}' | jq -r '.token')

# 1. Inspect uncommitted DLQ messages
curl -s -X GET "http://localhost:8080/api/admin/dlq/messages?topic=document.uploaded.dlq&max=20" \
  -H "Authorization: Bearer $ADMIN_TOKEN" | jq .

# 2. Replay messages back into the pipeline
curl -s -X POST "http://localhost:8080/api/admin/dlq/replay" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "document.uploaded.dlq",
    "targetTopic": "document.uploaded",
    "maxRecords": 50
  }' | jq .
```

---

## Useful Makefile Commands

| Command | Action |
|---|---|
| `make up` | Start full Docker Compose stack |
| `make up-build` | Rebuild application images and start full Docker Compose stack |
| `make up-infra` | Start infrastructure services only (Postgres, Kafka, Redis, MinIO, Jaeger, Prometheus, Grafana) |
| `make down` | Stop and remove all containers and networks |
| `make ps` | Check health and status of all containers |
| `make logs` | Follow aggregate logs from all containers |
| `make test` | Run test suite across all subprojects |
| `make k8s-apply` | Apply Kubernetes manifests to current cluster context |
| `make k8s-delete` | Delete Kubernetes resources |
| `make check-db` | Inspect PostgreSQL connectivity and pgvector extension status |
