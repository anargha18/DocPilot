package com.docpilot.common.retry;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@EnableRetry
public class RetryHelper {

    private final RetryTemplate retryTemplate;

    public RetryHelper(
            @Value("${docpilot.retry.max-attempts:3}") int maxAttempts,
            @Value("${docpilot.retry.initial-interval-ms:1000}") long initialIntervalMs,
            @Value("${docpilot.retry.multiplier:2.0}") double multiplier,
            @Value("${docpilot.retry.max-interval-ms:10000}") long maxIntervalMs
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

    public RetryTemplate getRetryTemplate() {
        return retryTemplate;
    }
}
