package com.docpilot.common.idempotency;

import com.docpilot.common.AbstractPostgresRepositoryTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotentConsumerServiceTest extends AbstractPostgresRepositoryTest {

    @Autowired
    private IdempotentConsumerService idempotentConsumerService;

    @Test
    @DisplayName("Should execute side effect once and skip duplicate invocations")
    void shouldExecuteOnlyOnceForDuplicateEvents() {
        String eventId = UUID.randomUUID().toString();
        String consumerName = "test-worker";
        AtomicInteger counter = new AtomicInteger(0);

        boolean firstExecution = idempotentConsumerService.executeIfNew(eventId, consumerName, counter::incrementAndGet);
        assertThat(firstExecution).isTrue();
        assertThat(counter.get()).isEqualTo(1);
        assertThat(idempotentConsumerService.isAlreadyProcessed(eventId, consumerName)).isTrue();

        // Second duplicate call with same eventId and consumerName
        boolean secondExecution = idempotentConsumerService.executeIfNew(eventId, consumerName, counter::incrementAndGet);
        assertThat(secondExecution).isFalse();
        assertThat(counter.get()).isEqualTo(1); // No duplicate side effect!
    }
}
