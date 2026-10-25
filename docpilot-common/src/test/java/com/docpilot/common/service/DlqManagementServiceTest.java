package com.docpilot.common.service;

import com.docpilot.common.dto.DlqMessageDto;
import com.docpilot.common.repository.ProcessedEventRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DlqManagementServiceTest {

    @Mock
    private ConsumerFactory<Object, Object> consumerFactory;

    @Mock
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Mock
    private ProcessedEventRepository processedEventRepository;

    @Mock
    private Consumer<Object, Object> consumer;

    private DlqManagementService dlqService;

    @BeforeEach
    void setUp() {
        dlqService = new DlqManagementService(consumerFactory, kafkaTemplate, processedEventRepository);
    }

    @Test
    @DisplayName("inspectDlq should reject blank topic name")
    void inspectDlq_BlankTopic_ThrowsException() {
        assertThatThrownBy(() -> dlqService.inspectDlq("", 10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("inspectDlq should return empty list if consumerFactory is null")
    void inspectDlq_NullConsumerFactory_ReturnsEmpty() {
        DlqManagementService noFactoryService = new DlqManagementService(null, kafkaTemplate, processedEventRepository);
        List<DlqMessageDto> result = noFactoryService.inspectDlq("document.uploaded.dlq", 10);
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("inspectDlq should poll records and map to DlqMessageDto")
    void inspectDlq_Success() {
        when(consumerFactory.createConsumer(eq("dlq-inspect-group"), anyString())).thenReturn(consumer);
        when(consumer.partitionsFor("document.uploaded.dlq"))
                .thenReturn(List.of(new PartitionInfo("document.uploaded.dlq", 0, null, null, null)));

        TopicPartition tp = new TopicPartition("document.uploaded.dlq", 0);
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "document.uploaded.dlq", 0, 10L, 12345L, null, 0, 0,
                "msg-key", "{\"documentId\":\"123\"}", new RecordHeaders(), null
        );

        when(consumer.poll(any(Duration.class)))
                .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(record))));

        List<DlqMessageDto> messages = dlqService.inspectDlq("document.uploaded.dlq", 5);

        assertThat(messages).hasSize(1);
        assertThat(messages.get(0).getTopic()).isEqualTo("document.uploaded.dlq");
        assertThat(messages.get(0).getKey()).isEqualTo("msg-key");
        assertThat(messages.get(0).getPayload()).isEqualTo("{\"documentId\":\"123\"}");

        verify(consumer).assign(eq(List.of(tp)));
        verify(consumer).seekToBeginning(eq(List.of(tp)));
    }

    @Test
    @DisplayName("replayDlq should auto-resolve target topic by stripping .dlq suffix")
    void replayDlq_AutoResolvesTargetTopic() {
        when(consumerFactory.createConsumer(eq("dlq-inspect-group"), anyString())).thenReturn(consumer);
        when(consumer.partitionsFor("document.uploaded.dlq"))
                .thenReturn(List.of(new PartitionInfo("document.uploaded.dlq", 0, null, null, null)));

        TopicPartition tp = new TopicPartition("document.uploaded.dlq", 0);
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "document.uploaded.dlq", 0, 5L, 12345L, null, 0, 0,
                "key-1", "test-payload", new RecordHeaders(), null
        );

        when(consumer.poll(any(Duration.class)))
                .thenReturn(new ConsumerRecords<>(Map.of(tp, List.of(record))));

        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));

        int replayed = dlqService.replayDlq("document.uploaded.dlq", null, 10);

        assertThat(replayed).isEqualTo(1);

        ArgumentCaptor<ProducerRecord> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("document.uploaded");
    }
}
