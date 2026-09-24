# DocPilot — Event-Driven AI Organizational Communication Platform

DocPilot is an event-driven Java/Spring Boot platform that analyzes uploaded organizational documents and auto-generates ready-to-send email or social media content with Spring AI, PostgreSQL + pgvector, Apache Kafka, Redis, and MinIO.

---

## Architecture Overview

DocPilot is organized as a multi-module Gradle project:

- **`docpilot-common`**: Shared JPA entities (`Document`, `BaseEntity`), enums (`DocumentStatus`), repository interfaces, common DTOs, and event models.
- **`docpilot-api`**: REST API service handling document uploads, approval workflows, activity history, Swagger documentation, and Spring Actuator health monitoring. Emits events to Kafka / transactional outbox.
- **`docpilot-worker`**: Distributed asynchronous worker consuming Kafka events to run document parsing, Spring AI / RAG analysis, and execution to external delivery providers.

---

## Infrastructure Services

All dependencies are orchestrated via Docker Compose:

| Service | Port | Image | Description |
|---|---|---|---|
| **PostgreSQL** | `5432` | `pgvector/pgvector:pg16` | Database with `vector` (pgvector) and `uuid-ossp` extensions |
| **Apache Kafka** | `9092` | `apache/kafka:3.8.0` | Event broker running in KRaft mode (no Zookeeper) |
| **Kafka UI** | `8085` | `provectuslabs/kafka-ui:latest` | Web UI for inspecting Kafka topics, messages, and consumer groups |
| **Redis** | `6379` | `redis:7-alpine` | Distributed caching and rate limiting |
| **MinIO (API)** | `9000` | `minio/minio:latest` | S3-compatible object storage for uploaded documents |
| **MinIO (Console)**| `9001` | `minio/minio:latest` | MinIO web management UI |

---

## Prerequisites

- **Java 21** (Temurin / Eclipse Adoptium recommended; Gradle Foojay resolver can auto-provision)
- **Docker & Docker Compose** (v2+)

---

## Quickstart Guide

### 1. Start Infrastructure

Start Postgres, Kafka, Redis, and MinIO in detached mode:

```bash
make up
# or: docker compose up -d
```

### 2. Verify Infrastructure & pgvector

Run the verification script or Makefile command:

```bash
make check-db
# or: ./scripts/verify-infrastructure.sh
```

This verifies all containers are running and confirms `vector` is enabled on PostgreSQL.

### 3. Build Project

```bash
make build
# or: ./gradlew build
```

### 4. Run Spring Boot API

```bash
make run-api
# or: ./gradlew :docpilot-api:bootRun --args='--spring.profiles.active=local'
```

### 5. Check Health Endpoint

Once the API is running, verify Spring Boot Actuator:

```bash
curl -i http://localhost:8080/actuator/health
```

Expected output:
```json
{
  "status": "UP",
  "components": {
    "db": {
      "status": "UP",
      "details": {
        "database": "PostgreSQL",
        "validationQuery": "isValid()"
      }
    },
    "diskSpace": {
      "status": "UP"
    },
    "ping": {
      "status": "UP"
    }
  }
}
```

You can also check the ping endpoint:
```bash
curl http://localhost:8080/api/v1/ping
```

---

## Configuration Profiles

- **`local`** (`application-local.yml`): For running the Spring Boot application on your host machine against localhost Docker ports (`localhost:5432`, `localhost:9092`, `localhost:6379`, `localhost:9000`).
- **`docker`** (`application-docker.yml`): For running containerized Spring Boot applications using Docker internal service names (`postgres:5432`, `kafka:29092`, `redis:6379`, `minio:9000`).

