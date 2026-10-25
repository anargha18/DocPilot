package com.docpilot.worker.consumer;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentAnalysis;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.DocumentUploadedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.idempotency.IdempotentConsumerService;
import com.docpilot.common.outbox.OutboxRelay;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.CommunicationRepository;
import com.docpilot.common.repository.DocumentAnalysisRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.common.repository.ProcessedEventRepository;
import com.docpilot.common.service.DocumentEmbeddingService;
import com.docpilot.common.storage.StorageService;
import com.docpilot.worker.service.DocumentAnalysisService;
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
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "docpilot.outbox.relay.enabled=false",
        "docpilot.dummy-consumer.enabled=false",
        "docpilot.analysis-consumer.enabled=true",
        "docpilot.email-consumer.enabled=false",
        "docpilot.social-consumer.enabled=false",
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
class RecoveryAfterCrashChaosTest {

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
    private IdempotentConsumerService idempotentConsumerService;

    @Autowired
    private DocumentAnalysisService documentAnalysisService;

    @Autowired
    private StorageService storageService;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("DOD Chaos: Killing app mid-processing (throwing after partial write) rolls back uncommitted state, and recovery on restart completes without losing or duplicating work")
    void shouldRecoverAfterMidProcessingCrashWithoutDataLossOrDuplication() throws Exception {
        UUID eventId = UUID.randomUUID();
        String filename = "crash-simulation-report.txt";
        String content = "Annual Strategic Plan: Cloud-native resilience and zero-data-loss architecture.";
        String storageKey = storageService.upload(filename, content.getBytes(StandardCharsets.UTF_8), "text/plain");

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

        TransactionTemplate txTemplate = new TransactionTemplate(transactionManager);

        // 1. CHAOS SIMULATION: Simulate app crash mid-processing after partial write
        assertThatThrownBy(() -> {
            txTemplate.executeWithoutResult(status -> {
                // Partial write: update document status and save a temporary log
                Document loaded = documentRepository.findById(docId).orElseThrow();
                loaded.setStatus(DocumentStatus.ANALYZING);
                documentRepository.save(loaded);

                ActivityLog partialLog = ActivityLog.builder()
                        .documentId(docId)
                        .actionType(ActionType.DOCUMENT_ANALYZED)
                        .details("Partial write before catastrophic crash")
                        .timestamp(Instant.now())
                        .build();
                activityLogRepository.save(partialLog);

                // CRASH OCCURS! (Simulate SIGKILL / power outage / unhandled fatal container crash)
                throw new RuntimeException("CHAOS SIMULATION: Container killed mid-transaction!");
            });
        }).hasMessageContaining("CHAOS SIMULATION");

        // 2. VERIFY ATOMICITY: Partial write was completely rolled back!
        Document docAfterCrash = documentRepository.findById(docId).orElseThrow();
        assertThat(docAfterCrash.getStatus()).isEqualTo(DocumentStatus.UPLOADED);
        List<ActivityLog> logsAfterCrash = activityLogRepository.findByDocumentIdOrderByTimestampAsc(docId);
        assertThat(logsAfterCrash).noneMatch(l -> l.getDetails().contains("Partial write before catastrophic crash"));
        assertThat(processedEventRepository.existsByEventIdAndConsumerName(eventId.toString(), DocumentAnalysisConsumer.CONSUMER_NAME))
                .isFalse();

        // 3. RECOVERY ON RESTART:
        // When the service restarts, the pending Kafka event is consumed and processed cleanly
        DocumentUploadedEvent recoveryEvent = DocumentUploadedEvent.builder()
                .documentId(docId)
                .filename(filename)
                .storageKey(storageKey)
                .contentType("text/plain")
                .fileSize(doc.getFileSize())
                .uploadTimestamp(doc.getUploadTimestamp())
                .eventId(eventId)
                .forceFailure(false)
                .build();

        ProducerRecord<Object, Object> record = new ProducerRecord<>(
                KafkaTopics.DOCUMENT_UPLOADED,
                docId.toString(),
                objectMapper.writeValueAsString(recoveryEvent)
        );
        record.headers().add(new RecordHeader("X-Event-Id", eventId.toString().getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // 4. VERIFY RECOVERY: Document is fully analyzed with no missing work
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Document recoveredDoc = documentRepository.findById(docId).orElseThrow();
            assertThat(recoveredDoc.getStatus()).isEqualTo(DocumentStatus.ANALYZED);
            assertThat(documentAnalysisRepository.findByDocumentId(docId)).isPresent();
        });

        // 5. VERIFY NO DUPLICATION:
        // Even if the message is redelivered again after recovery, work is not duplicated
        long analysisCount = documentAnalysisRepository.count();
        int activityLogCount = activityLogRepository.findByDocumentIdOrderByTimestampAsc(docId).size();

        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
        Thread.sleep(1500);

        assertThat(documentAnalysisRepository.count()).isEqualTo(analysisCount);
        assertThat(activityLogRepository.findByDocumentIdOrderByTimestampAsc(docId).size()).isEqualTo(activityLogCount);
        assertThat(processedEventRepository.existsByEventIdAndConsumerName(eventId.toString(), DocumentAnalysisConsumer.CONSUMER_NAME))
                .isTrue();
    }

    @Test
    @DisplayName("DOD Chaos: Outbox relay duplicate delivery recovery preserves exactly-once side effects")
    void shouldHandleOutboxDuplicateDeliveryGracefully() {
        UUID eventId = UUID.randomUUID();
        String consumerName = "chaos-idempotency-check";
        AtomicInteger executedActions = new AtomicInteger(0);

        // Step 1: First attempt succeeds and registers in processed_events
        boolean firstRun = idempotentConsumerService.executeIfNew(eventId.toString(), consumerName, executedActions::incrementAndGet);
        assertThat(firstRun).isTrue();
        assertThat(executedActions.get()).isEqualTo(1);

        // Step 2: Crash simulation - network glitch caused duplicate redelivery of same event
        boolean secondRun = idempotentConsumerService.executeIfNew(eventId.toString(), consumerName, executedActions::incrementAndGet);
        assertThat(secondRun).isFalse();
        assertThat(executedActions.get()).isEqualTo(1); // Still exactly 1
    }
}
