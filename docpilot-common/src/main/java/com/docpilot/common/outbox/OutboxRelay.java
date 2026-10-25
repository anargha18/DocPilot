package com.docpilot.common.outbox;

import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "docpilot.outbox.relay.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<Object, Object> kafkaTemplate;

    @Value("${docpilot.outbox.relay.batch-size:50}")
    private int batchSize;

    @Scheduled(
            fixedDelayString = "${docpilot.outbox.relay.fixed-delay-ms:1000}",
            initialDelayString = "${docpilot.outbox.relay.initial-delay-ms:1000}"
    )
    public void scheduledRelay() {
        publishPendingEvents();
    }

    @Transactional
    public int publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findUnpublishedEventsBatch(batchSize);
        if (pendingEvents.isEmpty()) {
            return 0;
        }

        int publishedCount = 0;
        for (OutboxEvent event : pendingEvents) {
            boolean success = publishSingleEvent(event);
            if (success) {
                event.setPublished(true);
                outboxEventRepository.save(event);
                publishedCount++;
            } else {
                log.warn("Will retry unpublished outbox event [{}] on next cycle", event.getId());
            }
        }

        if (publishedCount > 0) {
            log.info("Outbox relay published {} events to Kafka", publishedCount);
        }
        return publishedCount;
    }

    private boolean publishSingleEvent(OutboxEvent event) {
        String topic = event.getEventType();
        String key = event.getAggregateId();
        String payload = event.getPayload();
        String correlationId = event.getCorrelationId();

        try (var ignored = com.docpilot.common.observability.CorrelationContext.open(correlationId)) {
            ProducerRecord<Object, Object> record = new ProducerRecord<>(topic, key, payload);
            record.headers().add(new RecordHeader("X-Event-Id", event.getId().toString().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("X-Aggregate-Id", event.getAggregateId().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("X-Aggregate-Type", event.getAggregateType().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("X-Event-Type", event.getEventType().getBytes(StandardCharsets.UTF_8)));

            if (correlationId != null) {
                record.headers().add(new RecordHeader("X-Correlation-Id", correlationId.getBytes(StandardCharsets.UTF_8)));
            }
            if (event.getTraceParent() != null) {
                record.headers().add(new RecordHeader("traceparent", event.getTraceParent().getBytes(StandardCharsets.UTF_8)));
            }

            try {
                kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);
                log.info("Published outbox event [id={}, type={}, aggregateId={}, correlationId={}] to topic [{}]",
                        event.getId(), event.getEventType(), event.getAggregateId(), correlationId, topic);
                return true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Publishing outbox event [{}] interrupted: {}", event.getId(), e.getMessage());
                return false;
            } catch (Exception e) {
                log.error("Failed to publish outbox event [{}] to topic [{}]: {}",
                        event.getId(), topic, e.getMessage());
                return false;
            }
        }
    }
}
