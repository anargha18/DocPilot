package com.docpilot.common.observability;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;

@Slf4j
@Service
public class DocPilotTracer {

    public static final String INSTRUMENTATION_NAME = "com.docpilot";

    private final Tracer tracer;
    private final DocPilotMetrics docPilotMetrics;

    public DocPilotTracer(ObjectProvider<OpenTelemetry> openTelemetryProvider,
                          ObjectProvider<Tracer> tracerProvider,
                          DocPilotMetrics docPilotMetrics) {
        this.docPilotMetrics = docPilotMetrics;

        Tracer resolvedTracer = tracerProvider.getIfAvailable();
        if (resolvedTracer == null) {
            OpenTelemetry otel = openTelemetryProvider.getIfAvailable();
            if (otel != null) {
                resolvedTracer = otel.getTracer(INSTRUMENTATION_NAME);
            } else {
                try {
                    resolvedTracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
                } catch (Exception e) {
                    try {
                        resolvedTracer = io.opentelemetry.sdk.trace.SdkTracerProvider.builder().build().get(INSTRUMENTATION_NAME);
                    } catch (Throwable t) {
                        resolvedTracer = OpenTelemetry.noop().getTracer(INSTRUMENTATION_NAME);
                    }
                }
            }
        }
        if (resolvedTracer == null) {
            try {
                resolvedTracer = io.opentelemetry.sdk.trace.SdkTracerProvider.builder().build().get(INSTRUMENTATION_NAME);
            } catch (Throwable t) {
                resolvedTracer = OpenTelemetry.noop().getTracer(INSTRUMENTATION_NAME);
            }
        }
        this.tracer = resolvedTracer;
    }

    public Tracer getTracer() {
        return tracer;
    }

    /**
     * Returns the W3C traceparent (00-traceId-spanId-flags) of the currently active span, or null.
     */
    public String getCurrentTraceParent() {
        Span currentSpan = Span.current();
        if (currentSpan != null && currentSpan.getSpanContext().isValid()) {
            SpanContext sc = currentSpan.getSpanContext();
            return String.format("00-%s-%s-%s",
                    sc.getTraceId(),
                    sc.getSpanId(),
                    sc.getTraceFlags().asHex());
        }
        return null;
    }

    /**
     * Returns the 32-character hex trace ID of the currently active span, or null.
     */
    public String getCurrentTraceId() {
        Span currentSpan = Span.current();
        if (currentSpan != null && currentSpan.getSpanContext().isValid()) {
            return currentSpan.getSpanContext().getTraceId();
        }
        return null;
    }

    /**
     * Starts a stage span that connects to parent trace context if provided,
     * updates MDC, and records stage latency when closed.
     */
    public TraceScope startStageSpan(String stage, String correlationId, String documentId, String traceParent) {
        if (correlationId != null) {
            CorrelationContext.setCorrelationId(correlationId);
        }

        SpanBuilder builder = tracer.spanBuilder("stage:" + stage);

        // If parent traceparent is provided, link as parent context
        Context parentContext = parseTraceParent(traceParent);
        if (parentContext != null) {
            builder.setParent(parentContext);
        }

        Span span = builder.startSpan();
        if (correlationId != null) {
            span.setAttribute("correlation.id", correlationId);
        }
        if (documentId != null) {
            span.setAttribute("document.id", documentId);
        }
        span.setAttribute("stage", stage);

        Scope scope = span.makeCurrent();

        // Update MDC for structured logging
        String prevTraceId = MDC.get(CorrelationContext.TRACE_ID_MDC_KEY);
        String prevSpanId = MDC.get(CorrelationContext.SPAN_ID_MDC_KEY);
        if (span.getSpanContext().isValid()) {
            MDC.put(CorrelationContext.TRACE_ID_MDC_KEY, span.getSpanContext().getTraceId());
            MDC.put(CorrelationContext.SPAN_ID_MDC_KEY, span.getSpanContext().getSpanId());
        }

        return new TraceScope(span, scope, stage, Instant.now(), docPilotMetrics, prevTraceId, prevSpanId);
    }

    /**
     * Executes an external API call within a child span and records latency/errors.
     */
    public <T> T recordExternalCall(String client, String operation, Callable<T> callable) throws Exception {
        Span span = tracer.spanBuilder("external:" + client + ":" + operation)
                .setAttribute("client", client)
                .setAttribute("operation", operation)
                .startSpan();

        Instant start = Instant.now();
        boolean success = false;
        try (Scope scope = span.makeCurrent()) {
            T result = callable.call();
            success = true;
            span.setStatus(StatusCode.OK);
            return result;
        } catch (Throwable t) {
            span.setStatus(StatusCode.ERROR, t.getMessage());
            span.recordException(t);
            docPilotMetrics.recordExternalCallError(client, operation, t.getClass().getSimpleName());
            if (t instanceof Exception e) {
                throw e;
            }
            throw new RuntimeException(t);
        } finally {
            Duration duration = Duration.between(start, Instant.now());
            docPilotMetrics.recordExternalCall(client, operation, duration, success);
            span.end();
        }
    }

    public void recordExternalCall(String client, String operation, Runnable runnable) {
        try {
            recordExternalCall(client, operation, () -> {
                runnable.run();
                return null;
            });
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Parses W3C traceparent header string (00-traceid-spanid-flags) into an OpenTelemetry Context.
     */
    public static Context parseTraceParent(String traceParent) {
        if (traceParent == null || traceParent.isBlank()) {
            return null;
        }
        String[] parts = traceParent.trim().split("-");
        if (parts.length < 4) {
            return null;
        }
        try {
            String traceId = parts[1];
            String parentSpanId = parts[2];
            String flagsHex = parts[3];
            if (traceId.length() != 32 || parentSpanId.length() != 16) {
                return null;
            }
            TraceFlags flags = TraceFlags.fromHex(flagsHex, 0);
            SpanContext sc = SpanContext.createFromRemoteParent(traceId, parentSpanId, flags, TraceState.getDefault());
            if (!sc.isValid()) {
                return null;
            }
            return Context.current().with(Span.wrap(sc));
        } catch (Exception e) {
            return null;
        }
    }

    public static class TraceScope implements AutoCloseable {
        private final Span span;
        private final Scope scope;
        private final String stage;
        private final Instant startTime;
        private final DocPilotMetrics metrics;
        private final String prevTraceId;
        private final String prevSpanId;
        private boolean success = true;

        public TraceScope(Span span, Scope scope, String stage, Instant startTime,
                          DocPilotMetrics metrics, String prevTraceId, String prevSpanId) {
            this.span = span;
            this.scope = scope;
            this.stage = stage;
            this.startTime = startTime;
            this.metrics = metrics;
            this.prevTraceId = prevTraceId;
            this.prevSpanId = prevSpanId;
        }

        public Span getSpan() {
            return span;
        }

        public void markFailed(Throwable t) {
            this.success = false;
            span.setStatus(StatusCode.ERROR, t != null ? t.getMessage() : "Failed");
            if (t != null) {
                span.recordException(t);
            }
        }

        public String getTraceParent() {
            if (span.getSpanContext().isValid()) {
                SpanContext sc = span.getSpanContext();
                return String.format("00-%s-%s-%s",
                        sc.getTraceId(),
                        sc.getSpanId(),
                        sc.getTraceFlags().asHex());
            }
            return null;
        }

        @Override
        public void close() {
            Duration duration = Duration.between(startTime, Instant.now());
            if (success) {
                span.setStatus(StatusCode.OK);
            }
            if (stage != null && metrics != null) {
                metrics.recordStageDuration(stage, duration, success);
            }
            try {
                scope.close();
            } finally {
                span.end();
                if (prevTraceId != null) {
                    MDC.put(CorrelationContext.TRACE_ID_MDC_KEY, prevTraceId);
                } else {
                    MDC.remove(CorrelationContext.TRACE_ID_MDC_KEY);
                }
                if (prevSpanId != null) {
                    MDC.put(CorrelationContext.SPAN_ID_MDC_KEY, prevSpanId);
                } else {
                    MDC.remove(CorrelationContext.SPAN_ID_MDC_KEY);
                }
            }
        }
    }
}
