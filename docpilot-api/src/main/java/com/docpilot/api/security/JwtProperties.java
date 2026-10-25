package com.docpilot.api.security;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "docpilot.security.jwt")
public class JwtProperties {

    /**
     * Secret key for signing HMAC-SHA256 JWTs. Must be at least 256 bits (32 bytes).
     */
    private String secret = "docpilot-secret-jwt-key-for-portfolio-authentication-must-be-at-least-256-bits-long-docpilot";

    /**
     * Token expiration duration in milliseconds (default: 24 hours).
     */
    private long expirationMs = 86_400_000L;

    /**
     * Token issuer claim.
     */
    private String issuer = "docpilot-api";
}
