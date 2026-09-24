package com.docpilot.common.repository;

import com.docpilot.common.AbstractPostgresRepositoryTest;
import com.docpilot.common.domain.ProcessedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Transactional
class ProcessedEventRepositoryTest extends AbstractPostgresRepositoryTest {

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Test
    @DisplayName("Should persist and check existence of processed events")
    void shouldPersistAndCheckProcessedEvent() {
        String eventId = UUID.randomUUID().toString();
        String consumerName = "dummy-consumer";

        assertThat(processedEventRepository.existsByEventIdAndConsumerName(eventId, consumerName)).isFalse();

        ProcessedEvent record = processedEventRepository.save(ProcessedEvent.builder()
                .eventId(eventId)
                .consumerName(consumerName)
                .processedAt(Instant.now())
                .payloadHash("abc123hash")
                .build());

        assertThat(record.getId()).isNotNull();
        assertThat(processedEventRepository.existsByEventIdAndConsumerName(eventId, consumerName)).isTrue();
        assertThat(processedEventRepository.existsByEventIdAndConsumerName(eventId, "different-consumer")).isFalse();
    }

    @Test
    @DisplayName("Should enforce uniqueness on (event_id, consumer_name)")
    void shouldEnforceUniqueConstraint() {
        String eventId = UUID.randomUUID().toString();
        String consumerName = "audit-consumer";

        processedEventRepository.saveAndFlush(ProcessedEvent.builder()
                .eventId(eventId)
                .consumerName(consumerName)
                .processedAt(Instant.now())
                .build());

        assertThatThrownBy(() -> {
            processedEventRepository.saveAndFlush(ProcessedEvent.builder()
                    .eventId(eventId)
                    .consumerName(consumerName)
                    .processedAt(Instant.now())
                    .build());
        }).isInstanceOf(DataIntegrityViolationException.class);
    }
}
