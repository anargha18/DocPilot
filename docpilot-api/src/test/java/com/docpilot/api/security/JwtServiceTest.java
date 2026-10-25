package com.docpilot.api.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JwtServiceTest {

    private JwtService jwtService;
    private JwtProperties jwtProperties;

    @BeforeEach
    void setUp() {
        jwtProperties = new JwtProperties();
        jwtProperties.setSecret("docpilot-unit-test-secret-key-that-is-sufficiently-long-for-hmac-sha-256");
        jwtProperties.setExpirationMs(3600000L); // 1 hour
        jwtProperties.setIssuer("docpilot-test");

        jwtService = new JwtService(jwtProperties);
        jwtService.init();
    }

    @Test
    @DisplayName("Should generate token from UserDetails and extract claims accurately")
    void generateToken_FromUserDetails_ExtractsAccurately() {
        UserDetails userDetails = User.builder()
                .username("portfolio-reviewer")
                .password("secret")
                .authorities(new SimpleGrantedAuthority("ROLE_USER"), new SimpleGrantedAuthority("ROLE_ADMIN"))
                .build();

        String token = jwtService.generateToken(userDetails);

        assertThat(token).isNotBlank();
        assertThat(jwtService.validateToken(token)).isTrue();
        assertThat(jwtService.extractUsername(token)).isEqualTo("portfolio-reviewer");

        List<GrantedAuthority> authorities = jwtService.extractAuthorities(token);
        assertThat(authorities).extracting(GrantedAuthority::getAuthority)
                .containsExactlyInAnyOrder("ROLE_USER", "ROLE_ADMIN");
    }

    @Test
    @DisplayName("Should validate token and return false for invalid or tampered tokens")
    void validateToken_InvalidTokens() {
        assertThat(jwtService.validateToken("invalid.jwt.token")).isFalse();
        assertThat(jwtService.validateToken("")).isFalse();
        assertThat(jwtService.validateToken(null)).isFalse();

        // Generate token and tamper with signature
        String validToken = jwtService.generateToken("user", List.of("ROLE_USER"));
        String tampered = validToken.substring(0, validToken.length() - 5) + "abcde";
        assertThat(jwtService.validateToken(tampered)).isFalse();
    }

    @Test
    @DisplayName("Should support short secret by hashing with SHA-256")
    void init_WithShortSecret_SuccessfullyInitializes() {
        JwtProperties shortSecretProps = new JwtProperties();
        shortSecretProps.setSecret("short"); // under 32 bytes
        shortSecretProps.setExpirationMs(10000L);
        shortSecretProps.setIssuer("docpilot");

        JwtService shortSecretJwtService = new JwtService(shortSecretProps);
        shortSecretJwtService.init();

        String token = shortSecretJwtService.generateToken("test-user", List.of("ROLE_USER"));
        assertThat(token).isNotBlank();
        assertThat(shortSecretJwtService.validateToken(token)).isTrue();
        assertThat(shortSecretJwtService.extractUsername(token)).isEqualTo("test-user");
    }
}
