package com.docpilot.worker.consumer;

import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.DocumentUploadedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.idempotency.IdempotentConsumerService;
import com.docpilot.common.outbox.OutboxRelay;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.common.repository.ProcessedEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "docpilot.outbox.relay.enabled=true",
        "docpilot.dummy-consumer.enabled=true",
        "docpilot.analysis-consumer.enabled=false",
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
class EventDrivenBackboneIntegrationTest {

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private DummyDocumentConsumer dummyDocumentConsumer;

    @Autowired
    private DummyDlqConsumer dummyDlqConsumer;

    @Autowired
    private IdempotentConsumerService idempotentConsumerService;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private OutboxRelay outboxRelay;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        dummyDlqConsumer.clear();
    }

    @Test
    @DisplayName("DOD: A dummy consumer processes messages idempotently (duplicate message has no duplicate side effect)")
    void shouldProcessMessageIdempotently() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID docId = UUID.randomUUID();

        DocumentUploadedEvent event = DocumentUploadedEvent.builder()
                .documentId(docId)
                .filename("annual-plan.pdf")
                .contentType("application/pdf")
                .fileSize(1024L)
                .storageKey("documents/" + docId + "/annual-plan.pdf")
                .uploadTimestamp(Instant.now())
                .eventId(eventId)
                .forceFailure(false)
                .build();

        String payload = objectMapper.writeValueAsString(event);
        int initialSideEffects = dummyDocumentConsumer.getSideEffectExecutionCount().get();
        int initialInvocations = dummyDocumentConsumer.getInvocationCount().get();

        ProducerRecord<Object, Object> record = new ProducerRecord<>(
                KafkaTopics.DOCUMENT_UPLOADED,
                docId.toString(),
                payload
        );
        record.headers().add(new RecordHeader("X-Event-Id", eventId.toString().getBytes(StandardCharsets.UTF_8)));

        // Send 1st message
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Await first execution
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(dummyDocumentConsumer.getSideEffectExecutionCount().get()).isEqualTo(initialSideEffects + 1);
            assertThat(dummyDocumentConsumer.getInvocationCount().get()).isGreaterThanOrEqualTo(initialInvocations + 1);
        });

        // Verify tracked in processed_events table
        assertThat(processedEventRepository.existsByEventIdAndConsumerName(eventId.toString(), DummyDocumentConsumer.CONSUMER_NAME))
                .isTrue();

        // Send 2nd duplicate message with exact same eventId
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Wait to ensure consumer receives the message
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(dummyDocumentConsumer.getInvocationCount().get()).isGreaterThanOrEqualTo(initialInvocations + 2);
        });

        // Crucial verification: Side effect executed EXACTLY once despite two deliveries
        assertThat(dummyDocumentConsumer.getSideEffectExecutionCount().get()).isEqualTo(initialSideEffects + 1);
    }

    @Test
    @DisplayName("DOD: Forcing a consumer failure routes the message to its DLQ after retries are exhausted")
    void shouldRouteToDlqWhenConsumerFailureForced() throws Exception {
        UUID eventId = UUID.randomUUID();
        UUID docId = UUID.randomUUID();

        DocumentUploadedEvent event = DocumentUploadedEvent.builder()
                .documentId(docId)
                .filename("corrupt-fail-document.pdf")
                .contentType("application/pdf")
                .fileSize(512L)
                .storageKey("documents/" + docId + "/corrupt.pdf")
                .uploadTimestamp(Instant.now())
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

        // Await DLQ routing after retry backoff
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            assertThat(dummyDlqConsumer.getDlqReceivedCount().get()).isGreaterThanOrEqualTo(1);
        });

        ConsumerRecord<String, String> dlqRecord = dummyDlqConsumer.getReceivedDlqRecords().get(0);
        assertThat(dlqRecord.topic()).isEqualTo(KafkaTopics.DOCUMENT_UPLOADED + KafkaTopics.DLQ_SUFFIX);
        assertThat(dlqRecord.value()).contains("corrupt-fail-document.pdf");
    }

    @Test
    @DisplayName("Outbox relay polls unpublished OutboxEvents, publishes to Kafka, and marks them published")
    void shouldRelayOutboxEventsToKafka() throws Exception {
        UUID docId = UUID.randomUUID();
        DocumentUploadedEvent event = DocumentUploadedEvent.builder()
                .documentId(docId)
                .filename("outbox-test.pdf")
                .contentType("application/pdf")
                .fileSize(2048L)
                .storageKey("documents/" + docId + "/outbox-test.pdf")
                .uploadTimestamp(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(event);

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateType("DOCUMENT")
                .aggregateId(docId.toString())
                .eventType(KafkaTopics.DOCUMENT_UPLOADED)
                .payload(payload)
                .published(false)
                .build();
        OutboxEvent savedOutboxEvent = outboxEventRepository.save(outboxEvent);

        assertThat(savedOutboxEvent.isPublished()).isFalse();

        // Trigger outbox relay
        int published = outboxRelay.publishPendingEvents();
        assertThat(published).isGreaterThanOrEqualTo(1);

        // Verify outbox record updated in database
        OutboxEvent refreshed = outboxEventRepository.findById(savedOutboxEvent.getId()).orElseThrow();
        assertThat(refreshed.isPublished()).isTrue();

        // Verify dummy consumer received the relayed message
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            assertThat(dummyDocumentConsumer.getProcessedEventIds()).contains(savedOutboxEvent.getId().toString());
        });
    }
}
