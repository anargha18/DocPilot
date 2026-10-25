package com.docpilot.worker.consumer;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentAnalysis;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.DocumentUploadedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.DocumentAnalysisRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.common.storage.StorageService;
import com.docpilot.common.service.DocumentEmbeddingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:postgresql://localhost:5432/docpilot",
        "spring.datasource.username=docpilot",
        "spring.datasource.password=docpilot_secret",
        "spring.datasource.driver-class-name=org.postgresql.Driver",
        "spring.jpa.hibernate.ddl-auto=none",
        "spring.flyway.enabled=true",
        "spring.flyway.validate-on-migrate=false",
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.ai.vectorstore.type=pgvector",
        "spring.ai.vectorstore.pgvector.initialize-schema=false",
        "spring.ai.vectorstore.pgvector.schema-validation=false",
        "docpilot.storage.endpoint=http://localhost:9000",
        "docpilot.ai.mock=true",
        "docpilot.dummy-consumer.enabled=false",
        "docpilot.analysis-consumer.enabled=true",
        "docpilot.outbox.relay.enabled=false"
})
@ActiveProfiles("local")
class DocumentAnalysisLivePipelineIntegrationTest {

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private DocumentRepository documentRepository;

    @Autowired
    private DocumentAnalysisRepository documentAnalysisRepository;

    @Autowired
    private ActivityLogRepository activityLogRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private StorageService storageService;

    @Autowired
    private DocumentEmbeddingService embeddingService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("DOD Full Pipeline: Upload document to MinIO, consume via Kafka, analyze with Spring AI, chunk & embed into pgvector, verify similarity search")
    void shouldExecuteFullLiveAnalysisPipeline() throws Exception {
        UUID eventId = UUID.randomUUID();
        String filename = "executive_product_launch.txt";
        String content = """
                DocPilot Product Announcement: AI Communication Engine v1.0
                We are thrilled to unveil DocPilot 1.0, enabling enterprise organizations
                to turn raw documents into highly targeted social media posts and executive emails.
                Key highlights:
                - Automated RAG semantic chunking with PostgreSQL pgvector
                - Event-driven asynchronous messaging using Apache Kafka
                - Full audit trail and transactional outbox reliability
                Deployment is scheduled for October 2026 across all client clusters.
                """;

        // 1. Upload to MinIO object storage
        String storageKey = storageService.upload(filename, content.getBytes(StandardCharsets.UTF_8), "text/plain");
        assertThat(storageKey).isNotBlank();

        // 2. Persist Document in UPLOADED status
        Document doc = Document.builder()
                .filename(filename)
                .contentType("text/plain")
                .fileSize((long) content.length())
                .storageKey(storageKey)
                .uploadTimestamp(Instant.now())
                .status(DocumentStatus.UPLOADED)
                .build();
        doc = documentRepository.save(doc);
        UUID docId = doc.getId();

        // 3. Emit DocumentUploadedEvent to Kafka topic
        DocumentUploadedEvent event = DocumentUploadedEvent.builder()
                .documentId(docId)
                .filename(filename)
                .storageKey(storageKey)
                .contentType("text/plain")
                .fileSize(doc.getFileSize())
                .uploadTimestamp(doc.getUploadTimestamp())
                .eventId(eventId)
                .forceFailure(false)
                .build();

        String payload = objectMapper.writeValueAsString(event);
        ProducerRecord<Object, Object> record = new ProducerRecord<>(
                KafkaTopics.DOCUMENT_UPLOADED,
                docId.toString(),
                payload
        );
        record.headers().add(new RecordHeader("X-Event-Id", eventId.toString().getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // 4. Await consumer processing: UPLOADED -> ANALYZING -> ANALYZED
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            Optional<DocumentAnalysis> analysisOpt = documentAnalysisRepository.findByDocumentId(docId);
            assertThat(analysisOpt).isPresent();

            DocumentAnalysis analysis = analysisOpt.get();
            assertThat(analysis.getSummary()).isNotBlank();
            assertThat(analysis.getExtractedContext()).isNotBlank();
            assertThat(analysis.getCategory()).isNotBlank();
            assertThat(analysis.getRawExtractionJson()).contains("summary");

            Document updatedDoc = documentRepository.findById(docId).orElseThrow();
            assertThat(updatedDoc.getStatus()).isEqualTo(DocumentStatus.ANALYZED);
        });

        // 5. Verify ActivityLog
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(docId);
        assertThat(logs).isNotEmpty();
        assertThat(logs).anyMatch(l -> ActionType.DOCUMENT_ANALYZED.name().equals(l.getActionType()));

        // 6. Verify OutboxEvent for document.analyzed
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).anyMatch(oe ->
                KafkaTopics.DOCUMENT_ANALYZED.equals(oe.getEventType()) &&
                        docId.toString().equals(oe.getAggregateId())
        );

        // 7. Verify RAG similarity search against pgvector
        List<org.springframework.ai.document.Document> searchResults = embeddingService.similaritySearch(
                "DocPilot Product Announcement RAG pgvector",
                3
        );
        assertThat(searchResults).isNotEmpty();
        org.springframework.ai.document.Document topMatch = searchResults.get(0);
        assertThat(topMatch.getText()).contains("DocPilot");
        assertThat(topMatch.getMetadata()).containsKey("documentId");
    }
}
