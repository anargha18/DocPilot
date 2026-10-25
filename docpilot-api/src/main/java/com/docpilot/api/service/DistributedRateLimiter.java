package com.docpilot.api.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;

/**
 * Distributed rate limiter backed by Redis.
 * Supports sliding window / fixed window throttling across multiple API instances.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DistributedRateLimiter {

    private final ObjectProvider<StringRedisTemplate> redisTemplateProvider;

    /**
     * Checks if a request is allowed under the rate limit for a given key.
     *
     * @param key identifier for the rate limited client (e.g., client IP or username)
     * @param maxRequests maximum permitted requests within the window
     * @param window duration window
     * @return true if request is permitted, false if rate limit is exceeded
     */
    public boolean isAllowed(String key, int maxRequests, Duration window) {
        StringRedisTemplate redisTemplate = redisTemplateProvider.getIfAvailable();
        if (redisTemplate == null) {
            // Fail-open if Redis is not configured or offline
            return true;
        }

        try {
            String redisKey = "rate_limit:" + key;
            Long count = redisTemplate.opsForValue().increment(redisKey);
            if (count != null && count == 1L) {
                redisTemplate.expire(redisKey, window);
            }
            if (count != null && count > maxRequests) {
                log.warn("Rate limit exceeded for [{}] - {} requests (max: {})", key, count, maxRequests);
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("Redis rate limiter encountered error for [{}], failing open: {}", key, e.getMessage());
            return true;
        }
    }

    /**
     * Resets rate limit for a given key.
     *
     * @param key identifier for the client
     */
    public void reset(String key) {
        StringRedisTemplate redisTemplate = redisTemplateProvider.getIfAvailable();
        if (redisTemplate != null) {
            try {
                redisTemplate.delete("rate_limit:" + key);
            } catch (Exception e) {
                log.warn("Failed to reset rate limit for key [{}]: {}", key, e.getMessage());
            }
        }
    }
}
