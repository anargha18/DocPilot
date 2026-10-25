package com.docpilot.common.retry;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.Callable;

@Slf4j
@Component
@EnableRetry
public class RetryHelper {

    private final RetryTemplate retryTemplate;

    public RetryHelper(
            @Value("${docpilot.retry.max-attempts:3}") int maxAttempts,
            @Value("${docpilot.retry.initial-interval-ms:1000}") long initialIntervalMs,
            @Value("${docpilot.retry.multiplier:2.0}") double multiplier,
            @Value("${docpilot.retry.max-interval-ms:10000}") long maxIntervalMs,
            org.springframework.beans.factory.ObjectProvider<com.docpilot.common.observability.DocPilotMetrics> metricsProvider
    ) {
        this.retryTemplate = new RetryTemplate();

        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(maxAttempts);

        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(initialIntervalMs);
        backOffPolicy.setMultiplier(multiplier);
        backOffPolicy.setMaxInterval(maxIntervalMs);

        this.retryTemplate.setRetryPolicy(retryPolicy);
        this.retryTemplate.setBackOffPolicy(backOffPolicy);

        this.retryTemplate.registerListener(new RetryListener() {
            @Override
            public <T, E extends Throwable> void onError(
                    RetryContext context,
                    RetryCallback<T, E> callback,
                    Throwable throwable
            ) {
                log.warn("External call retry attempt {}/{} failed due to {}: {}. Retrying...",
                        context.getRetryCount(), maxAttempts, throwable.getClass().getSimpleName(), throwable.getMessage());
                metricsProvider.ifAvailable(m -> m.recordRetry("external_call", throwable != null ? throwable.getClass().getSimpleName() : "unknown"));
            }
        });
    }

    public <T, E extends Throwable> T executeWithRetry(RetryCallback<T, E> callback) throws E {
        return retryTemplate.execute(callback);
    }

    public <T, E extends Throwable> T executeWithRetry(
            RetryCallback<T, E> callback,
            RecoveryCallback<T> recoveryCallback
    ) throws E {
        return retryTemplate.execute(callback, recoveryCallback);
    }

    public void executeWithRetry(Runnable runnable) {
        retryTemplate.execute(context -> {
            runnable.run();
            return null;
        });
    }

    public <T> T executeCallable(Callable<T> callable) throws Exception {
        return retryTemplate.execute(context -> {
            try {
                return callable.call();
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    public RetryTemplate getRetryTemplate() {
        return retryTemplate;
    }
}
