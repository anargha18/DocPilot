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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
@Component
@ConditionalOnProperty(name = "docpilot.analysis-consumer.enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class DocumentAnalysisConsumer {

    public static final String CONSUMER_NAME = "document-analysis-consumer";

    private final DocumentAnalysisService documentAnalysisService;
    private final IdempotentConsumerService idempotentConsumerService;
    private final ObjectMapper objectMapper;
    private final org.springframework.beans.factory.ObjectProvider<com.docpilot.common.observability.DocPilotTracer> tracerProvider;

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
        String headerCorrelationId = extractHeader(record, "X-Correlation-Id");
        String traceParent = extractHeader(record, "traceparent");
        boolean forceFailure = "true".equalsIgnoreCase(extractHeader(record, "X-Force-Failure"));

        DocumentUploadedEvent event = null;
        if (payload != null && !payload.isBlank()) {
            try {
                event = objectMapper.readValue(payload, DocumentUploadedEvent.class);
                if (eventId == null && event.getEventId() != null) {
                    eventId = event.getEventId().toString();
                }
                if (headerCorrelationId == null && event.getCorrelationId() != null) {
                    headerCorrelationId = event.getCorrelationId();
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

        String correlationId = headerCorrelationId != null ? headerCorrelationId : (key != null ? key : UUID.randomUUID().toString());
        com.docpilot.common.observability.CorrelationContext.setCorrelationId(correlationId);

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

        com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
        com.docpilot.common.observability.DocPilotTracer.TraceScope stageScope = null;
        if (tracer != null) {
            stageScope = tracer.startStageSpan("analysis", correlationId, documentId.toString(), traceParent);
        }

        try {
            documentAnalysisService.markAnalyzing(documentId);

            UUID parsedEventId;
            try {
                parsedEventId = UUID.fromString(eventId);
            } catch (Exception ex) {
                parsedEventId = UUID.randomUUID();
            }

            String currentTraceParent = stageScope != null ? stageScope.getTraceParent() : traceParent;

            documentAnalysisService.analyzeAndPersist(documentId, parsedEventId, storageKey, filename, correlationId, currentTraceParent);

            idempotentConsumerService.markAsProcessed(eventId, CONSUMER_NAME, null);
        } catch (Exception e) {
            if (stageScope != null) {
                stageScope.markFailed(e);
            }
            log.error("Error processing document analysis for document [{}] event [{}]: {}",
                    documentId, eventId, e.getMessage(), e);
            documentAnalysisService.markFailed(documentId, e.getMessage());
            throw new RuntimeException("Document analysis processing failed: " + eventId, e);
        } finally {
            if (stageScope != null) {
                stageScope.close();
            }
            com.docpilot.common.observability.CorrelationContext.clear();
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
