package com.docpilot.common.observability;

import org.slf4j.MDC;

import java.util.UUID;

public final class CorrelationContext {

    public static final String CORRELATION_ID_HEADER = "X-Correlation-Id";
    public static final String CORRELATION_ID_MDC_KEY = "correlationId";
    public static final String TRACE_PARENT_HEADER = "traceparent";
    public static final String TRACE_ID_MDC_KEY = "traceId";
    public static final String SPAN_ID_MDC_KEY = "spanId";

    private CorrelationContext() {
        // utility class
    }

    public static String getCorrelationId() {
        return MDC.get(CORRELATION_ID_MDC_KEY);
    }

    public static void setCorrelationId(String correlationId) {
        if (correlationId != null && !correlationId.isBlank()) {
            MDC.put(CORRELATION_ID_MDC_KEY, correlationId.trim());
        }
    }

    public static String getOrGenerateCorrelationId(String candidate) {
        if (candidate != null && !candidate.isBlank()) {
            setCorrelationId(candidate.trim());
            return candidate.trim();
        }
        String current = getCorrelationId();
        if (current != null && !current.isBlank()) {
            return current;
        }
        String generated = UUID.randomUUID().toString();
        setCorrelationId(generated);
        return generated;
    }

    public static void clearCorrelationId() {
        MDC.remove(CORRELATION_ID_MDC_KEY);
    }

    public static void clear() {
        MDC.remove(CORRELATION_ID_MDC_KEY);
        MDC.remove(TRACE_ID_MDC_KEY);
        MDC.remove(SPAN_ID_MDC_KEY);
    }

    public static Scope open(String correlationId) {
        String previous = getCorrelationId();
        setCorrelationId(correlationId);
        return new Scope(previous);
    }

    public static class Scope implements AutoCloseable {
        private final String previous;

        public Scope(String previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (previous != null) {
                MDC.put(CORRELATION_ID_MDC_KEY, previous);
            } else {
                MDC.remove(CORRELATION_ID_MDC_KEY);
            }
        }
    }
}
