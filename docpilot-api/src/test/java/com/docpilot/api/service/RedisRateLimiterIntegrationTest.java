package com.docpilot.api.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class RedisRateLimiterIntegrationTest {

    @Container
    static final GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @DynamicPropertySource
    static void configureRedis(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", redis::getHost);
        registry.add("spring.data.redis.port", redis::getFirstMappedPort);
    }

    @Autowired
    private DistributedRateLimiter rateLimiter;

    @Test
    @DisplayName("Should enforce rate limit and block requests exceeding threshold")
    void shouldEnforceRateLimit() {
        String clientKey = "user-test-client-1";
        int maxAllowed = 3;
        Duration window = Duration.ofSeconds(10);

        // First 3 requests should be allowed
        assertThat(rateLimiter.isAllowed(clientKey, maxAllowed, window)).isTrue();
        assertThat(rateLimiter.isAllowed(clientKey, maxAllowed, window)).isTrue();
        assertThat(rateLimiter.isAllowed(clientKey, maxAllowed, window)).isTrue();

        // 4th and 5th requests should be blocked
        assertThat(rateLimiter.isAllowed(clientKey, maxAllowed, window)).isFalse();
        assertThat(rateLimiter.isAllowed(clientKey, maxAllowed, window)).isFalse();

        // After reset, requests should be permitted again
        rateLimiter.reset(clientKey);
        assertThat(rateLimiter.isAllowed(clientKey, maxAllowed, window)).isTrue();
    }

    @Test
    @DisplayName("Should handle distinct client keys independently")
    void shouldIsolateDifferentClients() {
        String clientA = "client-alpha";
        String clientB = "client-beta";
        int limit = 2;
        Duration window = Duration.ofSeconds(10);

        rateLimiter.reset(clientA);
        rateLimiter.reset(clientB);

        assertThat(rateLimiter.isAllowed(clientA, limit, window)).isTrue();
        assertThat(rateLimiter.isAllowed(clientA, limit, window)).isTrue();
        assertThat(rateLimiter.isAllowed(clientA, limit, window)).isFalse();

        // Client B is unaffected by client A hitting their limit
        assertThat(rateLimiter.isAllowed(clientB, limit, window)).isTrue();
        assertThat(rateLimiter.isAllowed(clientB, limit, window)).isTrue();
        assertThat(rateLimiter.isAllowed(clientB, limit, window)).isFalse();
    }
}
