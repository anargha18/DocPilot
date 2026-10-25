package com.docpilot.worker.consumer;

import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentAnalysis;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.dto.DlqMessageDto;
import com.docpilot.common.event.DocumentUploadedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.repository.DocumentAnalysisRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.service.DlqManagementService;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
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
        "docpilot.kafka.retry.max-attempts=2",
        "docpilot.kafka.retry.multiplier=1.0",
        "docpilot.kafka.retry.max-interval-ms=200"
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
class DlqInspectionAndReplayIntegrationTest {

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private DocumentRepository documentRepository;

    @Autowired
    private DocumentAnalysisRepository documentAnalysisRepository;

    @Autowired
    private StorageService storageService;

    @Autowired
    private DlqManagementService dlqManagementService;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    @DisplayName("DOD DLQ: Retries exhaust on consumer failure -> routes to DLQ -> inspectable via DLQ service -> replayed successfully")
    void shouldRouteToDlqInspectAndReplay() throws Exception {
        UUID eventId = UUID.randomUUID();
        String filename = "retry-and-recovery-audit.txt";
        String content = "Testing failure path routing and Dead Letter Queue inspection/replay.";
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

        // 1. Publish event with forceFailure in header to test DLQ routing
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
        ProducerRecord<Object, Object> failRecord = new ProducerRecord<>(
                KafkaTopics.DOCUMENT_UPLOADED,
                docId.toString(),
                payload
        );
        failRecord.headers().add(new RecordHeader("X-Event-Id", eventId.toString().getBytes(StandardCharsets.UTF_8)));
        failRecord.headers().add(new RecordHeader("X-Force-Failure", "true".getBytes(StandardCharsets.UTF_8)));

        kafkaTemplate.send(failRecord).get(5, TimeUnit.SECONDS);

        // 2. Wait until retries are exhausted and message lands in document.uploaded.dlq
        String dlqTopic = KafkaTopics.DOCUMENT_UPLOADED + KafkaTopics.DLQ_SUFFIX;

        await().atMost(Duration.ofSeconds(12)).untilAsserted(() -> {
            List<DlqMessageDto> dlqMessages = dlqManagementService.inspectDlq(dlqTopic, 10);
            assertThat(dlqMessages).isNotEmpty();
            assertThat(dlqMessages).anyMatch(m -> docId.toString().equals(m.getKey()));
        });

        // 3. Verify DLQ Inspection DTO attributes
        List<DlqMessageDto> dlqMessages = dlqManagementService.inspectDlq(dlqTopic, 10);
        DlqMessageDto captured = dlqMessages.stream()
                .filter(m -> docId.toString().equals(m.getKey()))
                .findFirst()
                .orElseThrow();

        assertThat(captured.getTopic()).isEqualTo(dlqTopic);
        assertThat(captured.getPayload()).contains(filename);
        assertThat(captured.getHeaders()).containsKey("X-Event-Id");

        // 4. Test DLQ Replay: Replay message back to main topic using DlqManagementService
        int replayed = dlqManagementService.replayDlq(dlqTopic, KafkaTopics.DOCUMENT_UPLOADED, 10);
        assertThat(replayed).isGreaterThanOrEqualTo(1);

        // 5. Verify consumer successfully processes the replayed event from DLQ
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Document refreshedDoc = documentRepository.findById(docId).orElseThrow();
            assertThat(refreshedDoc.getStatus()).isEqualTo(DocumentStatus.ANALYZED);
            assertThat(documentAnalysisRepository.findByDocumentId(docId)).isPresent();
        });
    }
}
