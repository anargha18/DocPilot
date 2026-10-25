package com.docpilot.common.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ObservabilityPipelineTest {

    private MeterRegistry meterRegistry;
    private DocPilotMetrics docPilotMetrics;
    private DocPilotTracer docPilotTracer;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        docPilotMetrics = new DocPilotMetrics(meterRegistry);

        Tracer tracer = io.opentelemetry.sdk.trace.SdkTracerProvider.builder().build().get("docpilot");
        StaticListableBeanFactory beanFactory = new StaticListableBeanFactory();
        beanFactory.addBean("tracer", tracer);

        docPilotTracer = new DocPilotTracer(
                beanFactory.getBeanProvider(OpenTelemetry.class),
                beanFactory.getBeanProvider(Tracer.class),
                docPilotMetrics
        );
        CorrelationContext.clear();
    }

    @Test
    @DisplayName("CorrelationContext sets and clears correlationId in MDC and restores in scope")
    void shouldManageCorrelationIdInMdc() throws Exception {
        String testCorrId = "corr-" + UUID.randomUUID();
        CorrelationContext.setCorrelationId(testCorrId);
        assertThat(CorrelationContext.getCorrelationId()).isEqualTo(testCorrId);
        assertThat(MDC.get(CorrelationContext.CORRELATION_ID_MDC_KEY)).isEqualTo(testCorrId);

        // Nested scope
        String childCorrId = "child-" + UUID.randomUUID();
        try (var scope = CorrelationContext.open(childCorrId)) {
            assertThat(CorrelationContext.getCorrelationId()).isEqualTo(childCorrId);
            assertThat(MDC.get(CorrelationContext.CORRELATION_ID_MDC_KEY)).isEqualTo(childCorrId);
        }

        // Restored
        assertThat(CorrelationContext.getCorrelationId()).isEqualTo(testCorrId);

        CorrelationContext.clear();
        assertThat(CorrelationContext.getCorrelationId()).isNull();
    }

    @Test
    @DisplayName("DocPilotTracer parses W3C traceparent and creates child spans sharing identical traceId")
    void shouldPropagateTraceIdAcrossLifecycleStages() {
        String correlationId = "corr-" + UUID.randomUUID();
        String documentId = UUID.randomUUID().toString();

        // 1. Upload stage (root of the document journey)
        String uploadTraceParent;
        String rootTraceId;
        try (var uploadScope = docPilotTracer.startStageSpan("upload", correlationId, documentId, null)) {
            uploadTraceParent = uploadScope.getTraceParent();
            rootTraceId = uploadScope.getSpan().getSpanContext().getTraceId();
            assertThat(uploadTraceParent).isNotNull().startsWith("00-" + rootTraceId);
        }

        // 2. Analysis stage (started by Kafka consumer with parent = uploadTraceParent)
        String analysisTraceParent;
        try (var analysisScope = docPilotTracer.startStageSpan("analysis", correlationId, documentId, uploadTraceParent)) {
            analysisTraceParent = analysisScope.getTraceParent();
            String analysisTraceId = analysisScope.getSpan().getSpanContext().getTraceId();
            assertThat(analysisTraceId).isEqualTo(rootTraceId);
        }

        // 3. Content Generation stage (HTTP request continues from document's stored traceParent)
        String generationTraceParent;
        try (var genScope = docPilotTracer.startStageSpan("generation", correlationId, documentId, uploadTraceParent)) {
            generationTraceParent = genScope.getTraceParent();
            String genTraceId = genScope.getSpan().getSpanContext().getTraceId();
            assertThat(genTraceId).isEqualTo(rootTraceId);
        }

        // 4. Approval stage
        String approvalTraceParent;
        try (var appScope = docPilotTracer.startStageSpan("approval", correlationId, documentId, generationTraceParent)) {
            approvalTraceParent = appScope.getTraceParent();
            String appTraceId = appScope.getSpan().getSpanContext().getTraceId();
            assertThat(appTraceId).isEqualTo(rootTraceId);
        }

        // 5. Delivery stage (consumed from content.approved with parent = approvalTraceParent)
        try (var delScope = docPilotTracer.startStageSpan("delivery", correlationId, documentId, approvalTraceParent)) {
            String delTraceId = delScope.getSpan().getSpanContext().getTraceId();
            assertThat(delTraceId).isEqualTo(rootTraceId);
        }

        // All 5 stages record metrics in Micrometer!
        Timer uploadTimer = meterRegistry.find(DocPilotMetrics.METRIC_STAGE_DURATION)
                .tag("stage", "upload").timer();
        assertThat(uploadTimer).isNotNull();
        assertThat(uploadTimer.count()).isEqualTo(1);

        Timer deliveryTimer = meterRegistry.find(DocPilotMetrics.METRIC_STAGE_DURATION)
                .tag("stage", "delivery").timer();
        assertThat(deliveryTimer).isNotNull();
        assertThat(deliveryTimer.count()).isEqualTo(1);
    }

    @Test
    @DisplayName("DocPilotMetrics tracks retries, DLQ counts, consumer lag, and external calls")
    void shouldTrackAllRequiredMetrics() {
        // Retries
        docPilotMetrics.recordRetry("kafka_consumer", "document.uploaded");
        docPilotMetrics.recordRetry("external_call", "HttpServerErrorException");

        Counter retry1 = meterRegistry.find(DocPilotMetrics.METRIC_RETRIES_TOTAL)
                .tag("component", "kafka_consumer").counter();
        assertThat(retry1).isNotNull();
        assertThat(retry1.count()).isEqualTo(1.0);

        // DLQ Messages
        docPilotMetrics.recordDlqMessage("document.uploaded", "document.uploaded.dlq");
        Counter dlqCounter = meterRegistry.find(DocPilotMetrics.METRIC_DLQ_MESSAGES_TOTAL)
                .tag("topic", "document.uploaded")
                .tag("dlq_topic", "document.uploaded.dlq").counter();
        assertThat(dlqCounter).isNotNull();
        assertThat(dlqCounter.count()).isEqualTo(1.0);

        // Consumer Lag
        docPilotMetrics.updateConsumerLag("docpilot-worker-group", "document.uploaded", 42);
        Double lag = meterRegistry.find(DocPilotMetrics.METRIC_KAFKA_CONSUMER_LAG)
                .tag("group", "docpilot-worker-group")
                .tag("topic", "document.uploaded").gauge().value();
        assertThat(lag).isEqualTo(42.0);

        // External API Calls
        docPilotMetrics.recordExternalCall("openai", "chat_completion", Duration.ofMillis(250), true);
        docPilotMetrics.recordExternalCallError("openai", "chat_completion", "TimeoutException");

        Timer extTimer = meterRegistry.find(DocPilotMetrics.METRIC_EXTERNAL_CALL_DURATION)
                .tag("client", "openai").timer();
        assertThat(extTimer).isNotNull();
        assertThat(extTimer.count()).isEqualTo(1);

        Counter extError = meterRegistry.find(DocPilotMetrics.METRIC_EXTERNAL_CALL_ERRORS)
                .tag("client", "openai").counter();
        assertThat(extError).isNotNull();
        assertThat(extError.count()).isEqualTo(1.0);
    }
}
