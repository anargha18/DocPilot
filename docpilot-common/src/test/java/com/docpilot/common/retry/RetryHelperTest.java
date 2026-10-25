package com.docpilot.common.retry;

import com.docpilot.common.observability.DocPilotMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RetryHelperTest {

    @Mock
    private ObjectProvider<DocPilotMetrics> metricsProvider;

    @Mock
    private DocPilotMetrics docPilotMetrics;

    private RetryHelper retryHelper;

    @BeforeEach
    void setUp() {
        retryHelper = new RetryHelper(3, 10, 1.5, 50, metricsProvider);
    }

    @Test
    @DisplayName("executeWithRetry should succeed on first attempt if no exception")
    void executeWithRetry_FirstAttemptSuccess() {
        String result = retryHelper.executeWithRetry(context -> "success");
        assertThat(result).isEqualTo("success");
    }

    @Test
    @DisplayName("executeWithRetry should retry on transient exception and succeed")
    void executeWithRetry_RetryAndSucceed() {
        AtomicInteger attempts = new AtomicInteger(0);

        String result = retryHelper.executeWithRetry(context -> {
            if (attempts.incrementAndGet() < 2) {
                throw new RuntimeException("Transient network hiccup");
            }
            return "recovered";
        });

        assertThat(result).isEqualTo("recovered");
        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("executeWithRetry should exhaust retries and throw exception when max attempts reached")
    void executeWithRetry_ExhaustRetries_Throws() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(() -> retryHelper.executeWithRetry(context -> {
            attempts.incrementAndGet();
            throw new IllegalStateException("Persistent failure");
        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Persistent failure");

        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("executeCallable should execute Callable and handle exceptions")
    void executeCallable_Success() throws Exception {
        String result = retryHelper.executeCallable(() -> "callable-ok");
        assertThat(result).isEqualTo("callable-ok");
    }
}
