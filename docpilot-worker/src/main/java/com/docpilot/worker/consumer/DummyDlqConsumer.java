package com.docpilot.worker.consumer;

import com.docpilot.common.event.KafkaTopics;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class DummyDlqConsumer {

    @Getter
    private final AtomicInteger dlqReceivedCount = new AtomicInteger(0);

    @Getter
    private final List<ConsumerRecord<String, String>> receivedDlqRecords = new CopyOnWriteArrayList<>();

    @KafkaListener(
            topics = KafkaTopics.DOCUMENT_UPLOADED + KafkaTopics.DLQ_SUFFIX,
            groupId = "docpilot-dlq-monitor-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDlq(ConsumerRecord<String, String> record) {
        dlqReceivedCount.incrementAndGet();
        receivedDlqRecords.add(record);
        log.warn("[DLQ MONITOR] Received dead-letter record on topic [{}] key [{}] offset [{}]: payload={}",
                record.topic(), record.key(), record.offset(), record.value());
    }

    public void clear() {
        dlqReceivedCount.set(0);
        receivedDlqRecords.clear();
    }
}
