# DocPilot — AI-Assisted Organizational Communication Platform

## Resume-Ready Summary

Use these directly as resume/portfolio bullets, or adapt to your format:

- Designed and built **DocPilot**, an event-driven Java/Spring Boot platform that uses **Spring AI** to analyze uploaded organizational documents and auto-generate ready-to-send email or social media content.
- Architected an **asynchronous, Kafka-driven processing pipeline** with the **Transactional Outbox Pattern**, idempotent consumers, and exponential-backoff retries with Dead Letter Queues to guarantee reliable, fault-isolated delivery across independent communication channels.
- Implemented **RAG-based content grounding** using PostgreSQL + pgvector embeddings so AI-generated communications stay accurate to the source document.
- Built full observability into the platform — structured logging, Micrometer/Prometheus metrics, Grafana dashboards, and OpenTelemetry distributed tracing with correlation IDs across async Kafka events.

---

## Project Description

**DocPilot** is an AI-assisted, event-driven platform that transforms organizational documents into ready-to-use communications. A user uploads a document, and the system uses **Spring AI** to analyze its content, extract key information, identify its context, and present two output options: **Email** or **Social Media Post**.

Based on the user's selection, DocPilot generates the corresponding content using AI. The user reviews and edits the generated content once, then approves it for delivery. Approved communications are processed asynchronously and delivered through the relevant external service.

The platform uses a distributed, event-driven architecture to provide scalable processing, reliable delivery, fault isolation, idempotent operations, and failure recovery. It also maintains a full document and activity history, so users can review previously uploaded documents and every action taken on them.

---

## Functional Requirements

### 1. Document Management
- Upload organizational documents in PDF, DOCX, or TXT format.
- Store original files in object storage.
- Persist document metadata and processing state in PostgreSQL.
- View previously uploaded documents and track their current processing status.

### 2. AI Document Analysis
- Analyze uploaded documents using Spring AI.
- Extract key facts and generate a concise summary.
- Identify the document's context/category.
- Pass extracted context and facts forward to the content-generation stage.
- Use RAG to ground generated content in the organization's uploaded source material.

### 3. Communication Selection
After analysis, the user selects an output type:

```text
📧 Email
📱 Social Media
```

The selection determines which type of content is generated next.

### 4. AI Content Generation
**Email**
- Generate a subject line and body.
- Adapt tone and content to the document's context.
- Produce professional, organization-ready communication.

**Social Media**
- Generate concise, public-facing content.
- Extract key announcements and highlights.
- Format the content for the target platform.

### 5. Content Review & Approval
- Display AI-generated content before execution.
- Allow the user one round of edits.
- Require explicit approval — only approved content can be executed.

### 6. Communication Execution
After approval, content flows through one of two independent pipelines:

```text
Email Selected → Email Service → Email Provider
Social Media Selected → Social Media Service → Social Media API
```

### 7. Activity History
DocPilot maintains a full history of actions taken on each document, for example:

```text
Document Uploaded → Document Analyzed → Email Selected →
Content Generated → Content Edited → Content Approved → Email Sent
```

Users can view their previous documents and every action performed on each one.

---

## Distributed-System & Non-Functional Requirements

**Asynchronous Processing** — Long-running operations (document processing, AI analysis, content generation, communication delivery) are handled asynchronously via Kafka.

**Scalability**
- Independently scale AI-processing and communication workers.
- Support horizontal scaling across multiple service instances.
- Distribute workload using Kafka consumer groups.

**Reliability**
- Prevent duplicate processing through idempotency.
- Retry transient failures with exponential backoff.
- Route repeatedly failed messages to a Dead Letter Queue.
- Persist processing state to support recovery after failure.
- Isolate failures so one communication channel can't affect another.

**Data Consistency**
- Use PostgreSQL transactions for business operations.
- Implement the Transactional Outbox Pattern for reliable database-to-Kafka event publishing.
- Handle concurrent updates with appropriate locking/concurrency strategies.
- Keep document and communication states consistent throughout the pipeline.

**Fault Tolerance**
- Apply timeouts and circuit breakers to external service calls.
- Isolate failures between email and social-media processing paths.
- Recover from worker/service failures without losing pending operations.

**Observability**
- Centralized structured logging.
- Application and infrastructure metrics.
- Distributed tracing with correlation IDs across async requests and Kafka events.
- Monitoring of processing latency, failures, retries, and Kafka consumer lag.

---

## Technology Stack

| Category | Technology |
|---|---|
| Language | Java 21 |
| Backend | Spring Boot 3 |
| AI Framework | Spring AI |
| LLM | OpenAI |
| AI Interface | Spring AI `ChatClient` |
| RAG | Spring AI + Embeddings |
| Vector Storage | PostgreSQL + pgvector |
| Database | PostgreSQL |
| ORM | Spring Data JPA / Hibernate |
| Messaging | Apache Kafka |
| Caching | Redis |
| Object Storage | MinIO / AWS S3 |
| Security | Spring Security + JWT |
| Email | SMTP / SendGrid |
| Social Media | X API |
| Containerization | Docker |
| Orchestration | Kubernetes |
| Metrics | Micrometer + Prometheus |
| Visualization | Grafana |
| Tracing | OpenTelemetry + Jaeger |
| API Documentation | Swagger / OpenAPI |
| Testing | JUnit 5, Mockito, Testcontainers |
| Build | Gradle |

---

## Core Concepts Covered

**Backend** — REST API design, layered architecture, authentication & authorization, database transactions, JPA/Hibernate, file processing, external API integration, state management

**Distributed Systems** — event-driven architecture, asynchronous communication, Kafka partitions, consumer groups, message ordering, consumer scaling, eventual consistency, backpressure, horizontal scaling

**Reliability** — idempotency, Transactional Outbox Pattern, retry with exponential backoff, Dead Letter Queues, circuit breakers, timeouts, fault isolation, failure recovery

**Data** — PostgreSQL, indexing, optimistic locking, ACID transactions, Redis caching, distributed rate limiting, pgvector and semantic search

**AI** — Spring AI, `ChatClient`, prompt templates, structured output, embeddings, RAG, vector similarity search, function/tool calling, document classification, information extraction, AI content generation

**Cloud Native** — Docker, Kubernetes, horizontal pod scaling, service discovery, health/readiness probes, configuration and secrets management

**Observability** — structured logging, Prometheus, Grafana, OpenTelemetry, distributed tracing, correlation IDs, Kafka consumer-lag monitoring

---

## Architecture Summary

DocPilot uses AI to understand and transform documents, while a distributed backend handles reliably processing, approving, tracking, and delivering the resulting communication. AI is a well-bounded capability within a system whose core identity is backend engineering and distributed systems design — which is exactly what the project is meant to showcase.