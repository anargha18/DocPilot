package com.docpilot.common.config;

import com.docpilot.common.event.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Slf4j
@Configuration
@ConditionalOnClass(ConcurrentKafkaListenerContainerFactory.class)
@ConditionalOnProperty(name = "spring.kafka.bootstrap-servers")
public class KafkaConsumerConfig {

    @Value("${docpilot.kafka.retry.max-attempts:3}")
    private int maxAttempts;

    @Value("${docpilot.kafka.retry.initial-interval-ms:1000}")
    private long initialIntervalMs;

    @Value("${docpilot.kafka.retry.multiplier:2.0}")
    private double multiplier;

    @Value("${docpilot.kafka.retry.max-interval-ms:10000}")
    private long maxIntervalMs;

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaOperations<?, ?> kafkaOperations) {
        return new DeadLetterPublishingRecoverer(kafkaOperations, (record, exception) -> {
            String originalTopic = record.topic();
            String dlqTopic = KafkaTopics.dlqFor(originalTopic);
            log.warn("Retries exhausted for message [key={}, partition={}, offset={}] from topic [{}]. Routing to DLQ [{}] due to error: {}",
                    record.key(), record.partition(), record.offset(), originalTopic, dlqTopic,
                    exception != null ? exception.getMessage() : "unknown");
            return new TopicPartition(dlqTopic, -1);
        });
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(Math.max(1, maxAttempts - 1));
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.info("Kafka consumer retry attempt {} of {} for topic [{}] key [{}] due to: {}",
                    deliveryAttempt, maxAttempts, record.topic(), record.key(), ex.getMessage());
        });

        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
            ConsumerFactory<Object, Object> consumerFactory,
            DefaultErrorHandler kafkaErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        configurer.configure(factory, consumerFactory);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }
}
