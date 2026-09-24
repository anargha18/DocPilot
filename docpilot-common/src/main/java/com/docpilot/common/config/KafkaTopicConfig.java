package com.docpilot.common.config;

import com.docpilot.common.event.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.ArrayList;
import java.util.List;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@Configuration
@ConditionalOnProperty(name = "spring.kafka.bootstrap-servers")
public class KafkaTopicConfig {

    @Value("${docpilot.kafka.topic.partitions:3}")
    private int partitions;

    @Value("${docpilot.kafka.topic.replication-factor:1}")
    private short replicationFactor;

    @Bean
    public KafkaAdmin.NewTopics docPilotKafkaTopics() {
        List<NewTopic> topicList = new ArrayList<>();
        for (String topic : KafkaTopics.ALL_MAIN_TOPICS) {
            topicList.add(TopicBuilder.name(topic)
                    .partitions(partitions)
                    .replicas(replicationFactor)
                    .build());
            topicList.add(TopicBuilder.name(KafkaTopics.dlqFor(topic))
                    .partitions(partitions)
                    .replicas(replicationFactor)
                    .build());
        }
        return new KafkaAdmin.NewTopics(topicList.toArray(new NewTopic[0]));
    }
}
