package com.docpilot.worker.consumer;

import com.docpilot.common.event.DocumentUploadedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.idempotency.IdempotentConsumerService;
import com.docpilot.worker.service.DocumentAnalysisService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class DocumentAnalysisConsumer {

    public static final String CONSUMER_NAME = "document-analysis-consumer";

    private final DocumentAnalysisService documentAnalysisService;
    private final IdempotentConsumerService idempotentConsumerService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = KafkaTopics.DOCUMENT_UPLOADED,
            groupId = "${spring.kafka.consumer.group-id:docpilot-worker-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record) {
        String payload = record.value();
        String key = record.key();

        log.info("DocumentAnalysisConsumer received record on topic [{}] partition [{}] offset [{}]: key={}",
                record.topic(), record.partition(), record.offset(), key);

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
                log.warn("Could not deserialize payload as DocumentUploadedEvent: {}", e.getMessage());
            }
        }

        if (eventId == null) {
            eventId = key != null ? key : (record.topic() + "-" + record.partition() + "-" + record.offset());
        }

        UUID documentId = null;
        String storageKey = null;
        String filename = null;
        if (event != null) {
            documentId = event.getDocumentId();
            storageKey = event.getStorageKey();
            filename = event.getFilename();
        } else if (key != null) {
            try {
                documentId = UUID.fromString(key);
            } catch (Exception ignored) {
            }
        }

        if (forceFailure) {
            if (documentId != null) {
                documentAnalysisService.markFailed(documentId, "Forced failure simulation: " + eventId);
            }
            log.warn("Simulating failure for eventId [{}]. Triggering retry/DLQ.", eventId);
            throw new RuntimeException("Simulated consumer failure for event: " + eventId);
        }

        if (idempotentConsumerService.isAlreadyProcessed(eventId, CONSUMER_NAME)) {
            log.info("[IDEMPOTENT SKIP] Event [{}] was already processed by [{}]", eventId, CONSUMER_NAME);
            return;
        }

        if (documentId == null) {
            log.error("Unable to resolve documentId from event [{}]. Skipping processing.", eventId);
            return;
        }

        try {
            documentAnalysisService.markAnalyzing(documentId);

            UUID parsedEventId;
            try {
                parsedEventId = UUID.fromString(eventId);
            } catch (Exception ex) {
                parsedEventId = UUID.randomUUID();
            }

            documentAnalysisService.analyzeAndPersist(documentId, parsedEventId, storageKey, filename);

            idempotentConsumerService.markAsProcessed(eventId, CONSUMER_NAME, null);
        } catch (Exception e) {
            log.error("Error processing document analysis for document [{}] event [{}]: {}",
                    documentId, eventId, e.getMessage(), e);
            documentAnalysisService.markFailed(documentId, e.getMessage());
            throw new RuntimeException("Document analysis processing failed: " + eventId, e);
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
