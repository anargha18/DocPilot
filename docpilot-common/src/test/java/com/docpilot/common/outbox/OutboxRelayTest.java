package com.docpilot.common.outbox;

import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.repository.OutboxEventRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTest {

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private KafkaTemplate<Object, Object> kafkaTemplate;

    private OutboxRelay outboxRelay;

    @BeforeEach
    void setUp() {
        outboxRelay = new OutboxRelay(outboxEventRepository, kafkaTemplate);
        ReflectionTestUtils.setField(outboxRelay, "batchSize", 50);
    }

    @Test
    @DisplayName("publishPendingEvents should return 0 when no events are pending")
    void publishPendingEvents_Empty_ReturnsZero() {
        when(outboxEventRepository.findUnpublishedEventsBatch(50)).thenReturn(Collections.emptyList());

        int published = outboxRelay.publishPendingEvents();

        assertThat(published).isZero();
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    @DisplayName("publishPendingEvents should dispatch pending events to Kafka and mark them published")
    void publishPendingEvents_Success() {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = OutboxEvent.builder()
                .aggregateType("DOCUMENT")
                .aggregateId("doc-123")
                .eventType("document.uploaded")
                .payload("{\"doc\":\"123\"}")
                .correlationId("test-corr-id")
                .published(false)
                .build();
        event.setId(eventId);

        when(outboxEventRepository.findUnpublishedEventsBatch(50)).thenReturn(List.of(event));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        when(outboxEventRepository.save(any(OutboxEvent.class))).thenAnswer(i -> i.getArgument(0));

        int published = outboxRelay.publishPendingEvents();

        assertThat(published).isEqualTo(1);
        assertThat(event.isPublished()).isTrue();

        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("document.uploaded");
        assertThat(captor.getValue().key()).isEqualTo("doc-123");

        verify(outboxEventRepository).save(event);
    }

    @Test
    @DisplayName("publishPendingEvents should leave event unpublished when Kafka send fails")
    void publishPendingEvents_SendFails_LeavesUnpublished() {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = OutboxEvent.builder()
                .aggregateType("DOCUMENT")
                .aggregateId("doc-456")
                .eventType("document.uploaded")
                .payload("{\"doc\":\"456\"}")
                .published(false)
                .build();
        event.setId(eventId);

        when(outboxEventRepository.findUnpublishedEventsBatch(50)).thenReturn(List.of(event));
        CompletableFuture failedFuture = new CompletableFuture();
        failedFuture.completeExceptionally(new RuntimeException("Kafka broker unreachable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failedFuture);

        int published = outboxRelay.publishPendingEvents();

        assertThat(published).isZero();
        assertThat(event.isPublished()).isFalse();
        verify(outboxEventRepository, never()).save(event);
    }
}
