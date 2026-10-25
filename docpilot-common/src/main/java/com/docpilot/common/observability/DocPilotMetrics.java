package com.docpilot.common.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class DocPilotMetrics {

    public static final String METRIC_STAGE_DURATION = "docpilot.stage.duration";
    public static final String METRIC_RETRIES_TOTAL = "docpilot.retries.total";
    public static final String METRIC_DLQ_MESSAGES_TOTAL = "docpilot.dlq.messages.total";
    public static final String METRIC_EXTERNAL_CALL_DURATION = "docpilot.external.call.duration";
    public static final String METRIC_EXTERNAL_CALL_ERRORS = "docpilot.external.call.errors.total";
    public static final String METRIC_KAFKA_CONSUMER_LAG = "docpilot.kafka.consumer.lag";

    private final MeterRegistry meterRegistry;
    private final ConcurrentMap<String, AtomicLong> lagGauges = new ConcurrentHashMap<>();

    public DocPilotMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public MeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    public void recordStageDuration(String stage, Duration duration, boolean success) {
        if (stage == null || duration == null) return;
        Timer.builder(METRIC_STAGE_DURATION)
                .description("Processing latency per pipeline stage")
                .tag("stage", stage)
                .tag("status", success ? "success" : "failure")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry)
                .record(duration);
    }

    public void recordRetry(String component, String operation) {
        Counter.builder(METRIC_RETRIES_TOTAL)
                .description("Total retry attempts across components and operations")
                .tag("component", component != null ? component : "unknown")
                .tag("operation", operation != null ? operation : "unknown")
                .register(meterRegistry)
                .increment();
    }

    public void recordDlqMessage(String topic, String dlqTopic) {
        Counter.builder(METRIC_DLQ_MESSAGES_TOTAL)
                .description("Total messages routed to dead letter queues")
                .tag("topic", topic != null ? topic : "unknown")
                .tag("dlq_topic", dlqTopic != null ? dlqTopic : "unknown")
                .register(meterRegistry)
                .increment();
    }

    public void recordExternalCall(String client, String operation, Duration duration, boolean success) {
        if (client == null || duration == null) return;
        Timer.builder(METRIC_EXTERNAL_CALL_DURATION)
                .description("Latency of external API calls (AI, storage, SMTP, social)")
                .tag("client", client)
                .tag("operation", operation != null ? operation : "unknown")
                .tag("status", success ? "success" : "failure")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(meterRegistry)
                .record(duration);
    }

    public void recordExternalCallError(String client, String operation, String errorType) {
        Counter.builder(METRIC_EXTERNAL_CALL_ERRORS)
                .description("Total external API call errors")
                .tag("client", client != null ? client : "unknown")
                .tag("operation", operation != null ? operation : "unknown")
                .tag("error_type", errorType != null ? errorType : "unknown")
                .register(meterRegistry)
                .increment();
    }

    public void updateConsumerLag(String group, String topic, long lag) {
        String key = group + "::" + topic;
        AtomicLong value = lagGauges.computeIfAbsent(key, k -> {
            AtomicLong gaugeVal = new AtomicLong(0);
            Gauge.builder(METRIC_KAFKA_CONSUMER_LAG, gaugeVal, AtomicLong::get)
                    .description("Kafka consumer lag per consumer group and topic")
                    .tag("group", group != null ? group : "unknown")
                    .tag("topic", topic != null ? topic : "unknown")
                    .register(meterRegistry);
            return gaugeVal;
        });
        value.set(Math.max(0, lag));
    }
}
