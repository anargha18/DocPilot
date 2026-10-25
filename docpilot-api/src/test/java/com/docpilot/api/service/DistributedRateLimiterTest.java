package com.docpilot.api.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DistributedRateLimiterTest {

    @Mock
    private ObjectProvider<StringRedisTemplate> redisTemplateProvider;

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private DistributedRateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        rateLimiter = new DistributedRateLimiter(redisTemplateProvider);
    }

    @Test
    @DisplayName("Should fail-open when Redis is unavailable")
    void isAllowed_RedisUnavailable_ReturnsTrue() {
        when(redisTemplateProvider.getIfAvailable()).thenReturn(null);

        boolean allowed = rateLimiter.isAllowed("192.168.1.1", 5, Duration.ofMinutes(1));

        assertThat(allowed).isTrue();
    }

    @Test
    @DisplayName("Should set expiration on first request and allow within limit")
    void isAllowed_FirstRequest_SetsExpiryAndAllows() {
        when(redisTemplateProvider.getIfAvailable()).thenReturn(redisTemplate);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.increment("rate_limit:client-1")).thenReturn(1L);

        boolean allowed = rateLimiter.isAllowed("client-1", 10, Duration.ofSeconds(60));

        assertThat(allowed).isTrue();
        verify(redisTemplate).expire(eq("rate_limit:client-1"), eq(Duration.ofSeconds(60)));
    }

    @Test
    @DisplayName("Should reject request when count exceeds maxRequests")
    void isAllowed_ExceedsLimit_ReturnsFalse() {
        when(redisTemplateProvider.getIfAvailable()).thenReturn(redisTemplate);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.increment("rate_limit:client-2")).thenReturn(11L);

        boolean allowed = rateLimiter.isAllowed("client-2", 10, Duration.ofSeconds(60));

        assertThat(allowed).isFalse();
    }

    @Test
    @DisplayName("reset should delete key from Redis")
    void reset_DeletesKey() {
        when(redisTemplateProvider.getIfAvailable()).thenReturn(redisTemplate);

        rateLimiter.reset("client-3");

        verify(redisTemplate).delete("rate_limit:client-3");
    }
}
