package com.docpilot.worker.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.timelimiter.TimeLimiter;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import io.github.resilience4j.timelimiter.TimeLimiterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class DeliveryResilienceConfig {

    public static final String EMAIL_CIRCUIT_BREAKER = "emailDelivery";
    public static final String SOCIAL_CIRCUIT_BREAKER = "socialDelivery";

    @Bean
    @ConditionalOnMissingBean(CircuitBreakerRegistry.class)
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        return CircuitBreakerRegistry.ofDefaults();
    }

    @Bean
    @ConditionalOnMissingBean(TimeLimiterRegistry.class)
    public TimeLimiterRegistry timeLimiterRegistry() {
        return TimeLimiterRegistry.ofDefaults();
    }

    @Bean
    public CircuitBreaker emailCircuitBreaker(
            CircuitBreakerRegistry registry,
            @Value("${resilience4j.circuitbreaker.instances.emailDelivery.sliding-window-size:5}") int slidingWindowSize,
            @Value("${resilience4j.circuitbreaker.instances.emailDelivery.minimum-number-of-calls:3}") int minCalls,
            @Value("${resilience4j.circuitbreaker.instances.emailDelivery.failure-rate-threshold:50.0}") float failureRate,
            @Value("${resilience4j.circuitbreaker.instances.emailDelivery.wait-duration-in-open-state:5000ms}") Duration waitInOpen
    ) {
        if (registry.find(EMAIL_CIRCUIT_BREAKER).isEmpty()) {
            CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                    .slidingWindowSize(slidingWindowSize)
                    .minimumNumberOfCalls(minCalls)
                    .failureRateThreshold(failureRate)
                    .waitDurationInOpenState(waitInOpen)
                    .permittedNumberOfCallsInHalfOpenState(2)
                    .automaticTransitionFromOpenToHalfOpenEnabled(true)
                    .build();
            return registry.circuitBreaker(EMAIL_CIRCUIT_BREAKER, config);
        }
        return registry.circuitBreaker(EMAIL_CIRCUIT_BREAKER);
    }

    @Bean
    public CircuitBreaker socialCircuitBreaker(
            CircuitBreakerRegistry registry,
            @Value("${resilience4j.circuitbreaker.instances.socialDelivery.sliding-window-size:5}") int slidingWindowSize,
            @Value("${resilience4j.circuitbreaker.instances.socialDelivery.minimum-number-of-calls:3}") int minCalls,
            @Value("${resilience4j.circuitbreaker.instances.socialDelivery.failure-rate-threshold:50.0}") float failureRate,
            @Value("${resilience4j.circuitbreaker.instances.socialDelivery.wait-duration-in-open-state:5000ms}") Duration waitInOpen
    ) {
        if (registry.find(SOCIAL_CIRCUIT_BREAKER).isEmpty()) {
            CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                    .slidingWindowSize(slidingWindowSize)
                    .minimumNumberOfCalls(minCalls)
                    .failureRateThreshold(failureRate)
                    .waitDurationInOpenState(waitInOpen)
                    .permittedNumberOfCallsInHalfOpenState(2)
                    .automaticTransitionFromOpenToHalfOpenEnabled(true)
                    .build();
            return registry.circuitBreaker(SOCIAL_CIRCUIT_BREAKER, config);
        }
        return registry.circuitBreaker(SOCIAL_CIRCUIT_BREAKER);
    }

    @Bean
    public TimeLimiter emailTimeLimiter(
            TimeLimiterRegistry registry,
            @Value("${resilience4j.timelimiter.instances.emailDelivery.timeout-duration:3s}") Duration timeout
    ) {
        if (registry.find(EMAIL_CIRCUIT_BREAKER).isEmpty()) {
            TimeLimiterConfig config = TimeLimiterConfig.custom()
                    .timeoutDuration(timeout)
                    .cancelRunningFuture(true)
                    .build();
            return registry.timeLimiter(EMAIL_CIRCUIT_BREAKER, config);
        }
        return registry.timeLimiter(EMAIL_CIRCUIT_BREAKER);
    }

    @Bean
    public TimeLimiter socialTimeLimiter(
            TimeLimiterRegistry registry,
            @Value("${resilience4j.timelimiter.instances.socialDelivery.timeout-duration:3s}") Duration timeout
    ) {
        if (registry.find(SOCIAL_CIRCUIT_BREAKER).isEmpty()) {
            TimeLimiterConfig config = TimeLimiterConfig.custom()
                    .timeoutDuration(timeout)
                    .cancelRunningFuture(true)
                    .build();
            return registry.timeLimiter(SOCIAL_CIRCUIT_BREAKER, config);
        }
        return registry.timeLimiter(SOCIAL_CIRCUIT_BREAKER);
    }

    @Bean(name = "deliveryExecutorService", destroyMethod = "shutdown")
    public ExecutorService deliveryExecutorService() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
