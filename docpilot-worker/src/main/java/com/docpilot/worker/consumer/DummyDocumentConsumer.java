package com.docpilot.worker.consumer;

import com.docpilot.common.event.DocumentUploadedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.idempotency.IdempotentConsumerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@ConditionalOnProperty(name = "docpilot.dummy-consumer.enabled", havingValue = "true", matchIfMissing = false)
@RequiredArgsConstructor
public class DummyDocumentConsumer {

    public static final String CONSUMER_NAME = "dummy-document-consumer";

    private final IdempotentConsumerService idempotentConsumerService;
    private final ObjectMapper objectMapper;

    @Getter
    private final AtomicInteger invocationCount = new AtomicInteger(0);

    @Getter
    private final AtomicInteger sideEffectExecutionCount = new AtomicInteger(0);

    @Getter
    private final List<String> processedEventIds = new CopyOnWriteArrayList<>();

    @KafkaListener(
            topics = KafkaTopics.DOCUMENT_UPLOADED,
            groupId = "${spring.kafka.consumer.group-id:docpilot-worker-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record) {
        invocationCount.incrementAndGet();
        String payload = record.value();
        String key = record.key();

        log.info("Dummy consumer received record on topic [{}] partition [{}] offset [{}]: key={}, payload={}",
                record.topic(), record.partition(), record.offset(), key, payload);

        // 1. Resolve eventId and failure simulation flag
        String eventId = extractHeader(record, "X-Event-Id");
        boolean forceFailure = "true".equalsIgnoreCase(extractHeader(record, "X-Force-Failure"));

        DocumentUploadedEvent event = null;
        if (payload != null && !payload.isBlank()) {
            try {
                event = objectMapper.readValue(payload, DocumentUploadedEvent.class);
                if (eventId == null && event.getEventId() != null) {
                    eventId = event.getEventId().toString();
                }
                if (!forceFailure && event.isForceFailure()) {
                    forceFailure = true;
                }
                if (!forceFailure && event.getFilename() != null) {
                    String lower = event.getFilename().toLowerCase();
                    if (lower.contains("fail") || lower.contains("dlq")) {
                        forceFailure = true;
                    }
                }
            } catch (Exception e) {
                log.warn("Could not parse payload as DocumentUploadedEvent: {}", e.getMessage());
            }
        }

        if (eventId == null) {
            eventId = key != null ? key : (record.topic() + "-" + record.partition() + "-" + record.offset());
        }

        final String finalEventId = eventId;
        final boolean simulateFailure = forceFailure;

        // 2. Check for simulated failure BEFORE marking processed so retry and DLQ trigger
        if (simulateFailure) {
            log.warn("Simulating consumer failure for eventId [{}] (attempt on topic [{}] offset [{}]). Triggering retry/DLQ.",
                    finalEventId, record.topic(), record.offset());
            throw new RuntimeException("Simulated consumer failure for DLQ routing test: " + finalEventId);
        }

        // 3. Process idempotently
        boolean executed = idempotentConsumerService.executeIfNew(finalEventId, CONSUMER_NAME, () -> {
            log.info("[SIDE EFFECT] Successfully processed document event [{}] with payload: {}", finalEventId, payload);
            sideEffectExecutionCount.incrementAndGet();
            processedEventIds.add(finalEventId);
        });

        if (!executed) {
            log.info("[IDEMPOTENT SKIP] Event [{}] was already processed by [{}]. No duplicate side effects executed.",
                    finalEventId, CONSUMER_NAME);
        }
    }

    private String extractHeader(ConsumerRecord<String, String> record, String headerKey) {
        Header header = record.headers().lastHeader(headerKey);
        if (header != null && header.value() != null) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }
}
