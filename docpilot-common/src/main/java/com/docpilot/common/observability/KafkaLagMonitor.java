package com.docpilot.common.observability;

import com.docpilot.common.event.KafkaTopics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "docpilot.kafka.lag-monitor.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaLagMonitor {

    private final DocPilotMetrics docPilotMetrics;
    private final Optional<KafkaAdmin> kafkaAdmin;

    private static final List<String> TRACKED_GROUPS = List.of(
            "docpilot-worker-group",
            "docpilot-email-delivery-group",
            "docpilot-social-delivery-group",
            "docpilot-api-group"
    );

    @Scheduled(
            fixedDelayString = "${docpilot.kafka.lag-monitor.interval-ms:5000}",
            initialDelayString = "${docpilot.kafka.lag-monitor.initial-delay-ms:5000}"
    )
    public void checkAndRecordLag() {
        if (kafkaAdmin.isEmpty()) {
            return;
        }

        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.get().getConfigurationProperties())) {
            for (String group : TRACKED_GROUPS) {
                try {
                    recordLagForGroup(adminClient, group);
                } catch (Exception e) {
                    log.debug("Could not record lag for group [{}]: {}", group, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.debug("KafkaLagMonitor could not connect to Kafka Admin: {}", e.getMessage());
        }
    }

    private void recordLagForGroup(AdminClient adminClient, String group) throws Exception {
        ListConsumerGroupOffsetsResult groupOffsetsResult = adminClient.listConsumerGroupOffsets(group);
        Map<TopicPartition, OffsetAndMetadata> consumedOffsets = groupOffsetsResult.partitionsToOffsetAndMetadata()
                .get(3, TimeUnit.SECONDS);

        if (consumedOffsets.isEmpty()) {
            // For known topics, report 0 lag if no active offset
            for (String topic : KafkaTopics.ALL_MAIN_TOPICS) {
                docPilotMetrics.updateConsumerLag(group, topic, 0);
            }
            return;
        }

        Map<TopicPartition, OffsetSpec> offsetSpecMap = new HashMap<>();
        for (TopicPartition tp : consumedOffsets.keySet()) {
            offsetSpecMap.put(tp, OffsetSpec.latest());
        }

        ListOffsetsResult latestOffsetsResult = adminClient.listOffsets(offsetSpecMap);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets = latestOffsetsResult.all()
                .get(3, TimeUnit.SECONDS);

        Map<String, Long> lagPerTopic = new HashMap<>();

        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : consumedOffsets.entrySet()) {
            TopicPartition tp = entry.getKey();
            OffsetAndMetadata metadata = entry.getValue();

            long currentOffset = metadata != null ? metadata.offset() : 0;
            ListOffsetsResult.ListOffsetsResultInfo latestInfo = latestOffsets.get(tp);
            long endOffset = latestInfo != null ? latestInfo.offset() : currentOffset;

            long lag = Math.max(0, endOffset - currentOffset);
            lagPerTopic.merge(tp.topic(), lag, Long::sum);
        }

        for (Map.Entry<String, Long> entry : lagPerTopic.entrySet()) {
            docPilotMetrics.updateConsumerLag(group, entry.getKey(), entry.getValue());
        }
    }
}
