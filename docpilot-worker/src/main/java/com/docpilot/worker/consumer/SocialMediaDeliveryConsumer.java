package com.docpilot.worker.consumer;

import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.event.ContentApprovedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.idempotency.IdempotentConsumerService;
import com.docpilot.worker.service.SocialMediaService;
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
@ConditionalOnProperty(name = "docpilot.social-consumer.enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class SocialMediaDeliveryConsumer {

    public static final String CONSUMER_NAME = "social-media-delivery-consumer";

    private final SocialMediaService socialMediaService;
    private final IdempotentConsumerService idempotentConsumerService;
    private final ObjectMapper objectMapper;
    private final org.springframework.beans.factory.ObjectProvider<com.docpilot.common.observability.DocPilotTracer> tracerProvider;

    @KafkaListener(
            topics = KafkaTopics.CONTENT_APPROVED,
            groupId = "${docpilot.kafka.consumer.social-group-id:docpilot-social-delivery-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(ConsumerRecord<String, String> record) {
        String payload = record.value();
        log.info("SocialMediaDeliveryConsumer received record on topic [{}] partition [{}] offset [{}]: key={}",
                record.topic(), record.partition(), record.offset(), record.key());

        String headerEventId = extractHeader(record, "X-Event-Id");
        String headerCorrelationId = extractHeader(record, "X-Correlation-Id");
        String traceParent = extractHeader(record, "traceparent");
        boolean forceFailure = "true".equalsIgnoreCase(extractHeader(record, "X-Force-Failure"));

        ContentApprovedEvent event = parseEvent(payload);
        if (event != null && event.getType() != null && event.getType() != CommunicationType.SOCIAL) {
            log.debug("SocialMediaDeliveryConsumer: Ignoring event for non-social type [{}] (commId: {})",
                    event.getType(), event.getCommunicationId());
            return;
        }

        UUID communicationId = null;
        String eventId = headerEventId;
        String correlationId = headerCorrelationId;
        if (event != null) {
            communicationId = event.getCommunicationId();
            if (eventId == null && event.getEventId() != null) {
                eventId = event.getEventId().toString();
            }
            if (correlationId == null && event.getCorrelationId() != null) {
                correlationId = event.getCorrelationId();
            }
        }

        if (communicationId == null && record.key() != null) {
            try {
                communicationId = UUID.fromString(record.key());
            } catch (Exception ignored) {
            }
        }

        if (correlationId == null) {
            correlationId = record.key() != null ? record.key() : UUID.randomUUID().toString();
        }
        com.docpilot.common.observability.CorrelationContext.setCorrelationId(correlationId);

        if (eventId == null) {
            eventId = record.key() != null ? record.key() : (record.topic() + "-" + record.partition() + "-" + record.offset());
        }

        if (communicationId == null) {
            log.warn("SocialMediaDeliveryConsumer: Unable to resolve communicationId for event [{}]. Skipping.", eventId);
            return;
        }

        if (forceFailure) {
            log.warn("Simulated failure for social consumer event [{}]. Triggering retry/DLQ.", eventId);
            throw new RuntimeException("Simulated social media consumer failure for event: " + eventId);
        }

        if (idempotentConsumerService.isAlreadyProcessed(eventId, CONSUMER_NAME)) {
            log.info("[IDEMPOTENT SKIP] Event [{}] was already processed by [{}]", eventId, CONSUMER_NAME);
            return;
        }

        com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
        com.docpilot.common.observability.DocPilotTracer.TraceScope stageScope = null;
        if (tracer != null) {
            stageScope = tracer.startStageSpan("delivery", correlationId, communicationId.toString(), traceParent);
        }

        try {
            boolean delivered = socialMediaService.deliver(communicationId);
            if (!delivered) {
                log.error("Social media delivery failed for communication [{}] (eventId={}). Triggering retry/DLQ.",
                        communicationId, eventId);
                if (stageScope != null) {
                    stageScope.markFailed(new RuntimeException("Social media delivery failed"));
                }
                throw new RuntimeException("Social media delivery failed for communication: " + communicationId);
            }

            idempotentConsumerService.markAsProcessed(eventId, CONSUMER_NAME, null);
        } catch (Exception e) {
            if (stageScope != null) {
                stageScope.markFailed(e);
            }
            throw e;
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

    private ContentApprovedEvent parseEvent(String payload) {
        if (payload == null || payload.isBlank()) {
            return null;
        }
        try {
            return objectMapper.readValue(payload, ContentApprovedEvent.class);
        } catch (Exception e) {
            log.warn("SocialMediaDeliveryConsumer: Could not parse ContentApprovedEvent: {}", e.getMessage());
            return null;
        }
    }
}
