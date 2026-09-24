package com.docpilot.common.repository;

import com.docpilot.common.AbstractPostgresRepositoryTest;
import com.docpilot.common.domain.OutboxEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class OutboxEventRepositoryTest extends AbstractPostgresRepositoryTest {

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    @DisplayName("Should persist outbox event with JSON payload, poll unpublished events, and update published state")
    void shouldPersistAndProcessOutboxEvents() {
        String aggregateId = UUID.randomUUID().toString();
        String payload = "{\"documentId\": \"" + aggregateId + "\", \"event\": \"DOCUMENT_UPLOADED\"}";

        OutboxEvent event = outboxEventRepository.save(OutboxEvent.builder()
                .aggregateType("DOCUMENT")
                .aggregateId(aggregateId)
                .eventType("DOCUMENT_UPLOADED")
                .payload(payload)
                .build());

        assertThat(event.getId()).isNotNull();
        assertThat(event.isPublished()).isFalse();
        assertThat(event.getPayload()).isEqualTo(payload);

        // Find unpublished events
        List<OutboxEvent> unpublished = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc();
        assertThat(unpublished).extracting(OutboxEvent::getId).contains(event.getId());

        // Find by aggregate
        List<OutboxEvent> byAggregate = outboxEventRepository.findByAggregateTypeAndAggregateId("DOCUMENT", aggregateId);
        assertThat(byAggregate).hasSize(1);
        assertThat(byAggregate.get(0).getId()).isEqualTo(event.getId());

        // Mark as published
        event.setPublished(true);
        outboxEventRepository.save(event);

        List<OutboxEvent> afterPublish = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc();
        assertThat(afterPublish).extracting(OutboxEvent::getId).doesNotContain(event.getId());
    }
}

