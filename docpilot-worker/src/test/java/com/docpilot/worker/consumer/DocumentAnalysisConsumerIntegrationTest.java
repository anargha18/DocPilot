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
import com.docpilot.common.repository.ProcessedEventRepository;
import com.docpilot.common.storage.StorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
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
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "docpilot.outbox.relay.enabled=false",
        "docpilot.dummy-consumer.enabled=false",
        "docpilot.analysis-consumer.enabled=true",
        "docpilot.ai.mock=true",
        "spring.ai.vectorstore.type=simple",
        "docpilot.kafka.retry.initial-interval-ms=100",
        "docpilot.kafka.retry.max-attempts=3",
        "docpilot.kafka.retry.multiplier=1.2",
        "docpilot.kafka.retry.max-interval-ms=300"
})
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = {
                KafkaTopics.DOCUMENT_UPLOADED,
                KafkaTopics.DOCUMENT_UPLOADED + KafkaTopics.DLQ_SUFFIX
        }
)
class DocumentAnalysisConsumerIntegrationTest {

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
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private StorageService storageService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("DOD: Uploading a document results in populated DocumentAnalysis, status transition UPLOADED -> ANALYZED, activity log, and outbox event")
    void shouldSuccessfullyAnalyzeUploadedDocument() throws Exception {
        UUID eventId = UUID.randomUUID();
        String filename = "security-policy-update.txt";
        String contentText = """
                DocPilot Security Policy and Compliance Guidelines
                All engineering teams must implement role-based access control and TLS 1.3 encryption.
                Audits will be conducted on a quarterly cadence starting Q4 2026.
                """;

        String storageKey = storageService.upload(filename, contentText.getBytes(StandardCharsets.UTF_8), "text/plain");

        Document doc = Document.builder()
                .filename(filename)
                .contentType("text/plain")
                .fileSize((long) contentText.length())
                .storageKey(storageKey)
                .uploadTimestamp(Instant.now())
                .status(DocumentStatus.UPLOADED)
                .build();
        doc = documentRepository.save(doc);
        UUID docId = doc.getId();

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

        // Send to Kafka
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Await DocumentAnalysis row population and status transition to ANALYZED
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Optional<DocumentAnalysis> analysisOpt = documentAnalysisRepository.findByDocumentId(docId);
            assertThat(analysisOpt).isPresent();

            DocumentAnalysis analysis = analysisOpt.get();
            assertThat(analysis.getSummary()).isNotBlank();
            assertThat(analysis.getExtractedContext()).isNotBlank();
            assertThat(analysis.getCategory()).isNotBlank();
            assertThat(analysis.getRawExtractionJson()).contains("summary");

            Document refreshedDoc = documentRepository.findById(docId).orElseThrow();
            assertThat(refreshedDoc.getStatus()).isEqualTo(DocumentStatus.ANALYZED);
        });

        // Verify ActivityLog entry
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(docId);
        assertThat(logs).isNotEmpty();
        assertThat(logs).anyMatch(l -> ActionType.DOCUMENT_ANALYZED.name().equals(l.getActionType()));

        // Verify OutboxEvent entry
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).anyMatch(oe ->
                KafkaTopics.DOCUMENT_ANALYZED.equals(oe.getEventType()) &&
                        docId.toString().equals(oe.getAggregateId()) &&
                        !oe.isPublished()
        );

        // Verify Idempotent tracking
        assertThat(processedEventRepository.existsByEventIdAndConsumerName(eventId.toString(), DocumentAnalysisConsumer.CONSUMER_NAME))
                .isTrue();
    }

    @Test
    @DisplayName("DOD: Document status transitions to FAILED with a logged reason on failure path")
    void shouldTransitionToFailedOnFailurePath() throws Exception {
        UUID eventId = UUID.randomUUID();
        String filename = "corrupted-dlq-fail.pdf";

        Document doc = Document.builder()
                .filename(filename)
                .contentType("application/pdf")
                .fileSize(256L)
                .storageKey("documents/" + UUID.randomUUID() + "/nonexistent.pdf")
                .uploadTimestamp(Instant.now())
                .status(DocumentStatus.UPLOADED)
                .build();
        doc = documentRepository.save(doc);
        UUID docId = doc.getId();

        DocumentUploadedEvent event = DocumentUploadedEvent.builder()
                .documentId(docId)
                .filename(filename)
                .storageKey(doc.getStorageKey())
                .contentType("application/pdf")
                .fileSize(doc.getFileSize())
                .uploadTimestamp(doc.getUploadTimestamp())
                .eventId(eventId)
                .forceFailure(true)
                .build();

        String payload = objectMapper.writeValueAsString(event);
        ProducerRecord<Object, Object> record = new ProducerRecord<>(
                KafkaTopics.DOCUMENT_UPLOADED,
                docId.toString(),
                payload
        );
        record.headers().add(new RecordHeader("X-Event-Id", eventId.toString().getBytes(StandardCharsets.UTF_8)));
        record.headers().add(new RecordHeader("X-Force-Failure", "true".getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Await failure transition
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Document refreshedDoc = documentRepository.findById(docId).orElseThrow();
            assertThat(refreshedDoc.getStatus()).isEqualTo(DocumentStatus.FAILED);
        });

        // Verify failure logged in ActivityLog
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(docId);
        assertThat(logs).isNotEmpty();
        assertThat(logs).anyMatch(l -> ActionType.FAILED.name().equals(l.getActionType()) && l.getDetails().contains("failed"));
    }

    @Test
    @DisplayName("DOD: Duplicate delivery does not re-process already analyzed document")
    void shouldSkipDuplicateMessageIdempotently() throws Exception {
        UUID eventId = UUID.randomUUID();
        String filename = "idempotent-test.txt";
        String contentText = "Annual shareholder report and financial performance overview for fiscal year 2026.";

        String storageKey = storageService.upload(filename, contentText.getBytes(StandardCharsets.UTF_8), "text/plain");

        Document doc = Document.builder()
                .filename(filename)
                .contentType("text/plain")
                .fileSize((long) contentText.length())
                .storageKey(storageKey)
                .uploadTimestamp(Instant.now())
                .status(DocumentStatus.UPLOADED)
                .build();
        doc = documentRepository.save(doc);
        UUID docId = doc.getId();

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

        // Send 1st
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(documentAnalysisRepository.findByDocumentId(docId)).isPresent();
        });

        long initialAnalysisCount = documentAnalysisRepository.count();

        // Send 2nd duplicate
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Allow some time for Kafka consumer to see the duplicate
        Thread.sleep(1500);

        // Ensure analysis count has NOT increased
        assertThat(documentAnalysisRepository.count()).isEqualTo(initialAnalysisCount);
    }
}
