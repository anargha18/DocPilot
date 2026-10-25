package com.docpilot.api.controller;

import com.docpilot.api.dto.LoginRequest;
import com.docpilot.api.security.JwtService;
import com.docpilot.common.dto.DlqMessageDto;
import com.docpilot.common.service.DlqManagementService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class SecurityAuthenticationIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private JwtService jwtService;

    @MockBean
    private DlqManagementService dlqManagementService;

    @Test
    @DisplayName("DOD 1: Unauthenticated request to /api/documents is rejected with 401 Unauthorized")
    void unauthenticatedRequestToApiDocumentsIsRejected() throws Exception {
        mockMvc.perform(get("/api/documents"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401))
                .andExpect(jsonPath("$.error").value("Unauthorized"));
    }

    @Test
    @DisplayName("DOD 1: Unauthenticated request to /api/v1/ping is rejected with 401 Unauthorized")
    void unauthenticatedRequestToApiPingIsRejected() throws Exception {
        mockMvc.perform(get("/api/v1/ping"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401));
    }

    @Test
    @DisplayName("DOD 1: Unauthenticated request to /api/admin/dlq is rejected with 401 Unauthorized")
    void unauthenticatedRequestToAdminDlqIsRejected() throws Exception {
        mockMvc.perform(get("/api/admin/dlq/messages?topic=document.uploaded.dlq"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    @DisplayName("Actuator health endpoint is public and does not require authentication")
    void actuatorHealthIsPublic() throws Exception {
        mockMvc.perform(get("/actuator/health"))
                .andExpect(status().isOk());
    }

    @Test
    @DisplayName("Login endpoint returns valid JWT token for configured valid credentials")
    void loginReturnsValidJwtToken() throws Exception {
        LoginRequest loginRequest = LoginRequest.builder()
                .username("testuser")
                .password("password")
                .build();

        MvcResult result = mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(loginRequest)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.token", notNullValue()))
                .andExpect(jsonPath("$.tokenType").value("Bearer"))
                .andExpect(jsonPath("$.username").value("testuser"))
                .andExpect(jsonPath("$.roles").isArray())
                .andReturn();

        String responseJson = result.getResponse().getContentAsString();
        String token = objectMapper.readTree(responseJson).get("token").asText();

        assertThat(jwtService.validateToken(token)).isTrue();
        assertThat(jwtService.extractUsername(token)).isEqualTo("testuser");
    }

    @Test
    @DisplayName("Login endpoint rejects invalid credentials with 401 Unauthorized")
    void loginWithBadCredentialsFails() throws Exception {
        LoginRequest badRequest = LoginRequest.builder()
                .username("testuser")
                .password("wrongpassword")
                .build();

        mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(badRequest)))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error").value("Unauthorized"))
                .andExpect(jsonPath("$.message").value(containsString("Invalid username or password")));
    }

    @Test
    @DisplayName("DOD 2: A valid JWT allows full access to the API flow (e.g. ping and documents)")
    void validJwtAllowsAccessToProtectedEndpoints() throws Exception {
        // 1. Obtain token via login
        LoginRequest loginRequest = LoginRequest.builder()
                .username("testuser")
                .password("password")
                .build();

        MvcResult loginResult = mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(loginRequest)))
                .andExpect(status().isOk())
                .andReturn();

        String token = objectMapper.readTree(loginResult.getResponse().getContentAsString()).get("token").asText();

        // 2. Access ping endpoint with Bearer token
        mockMvc.perform(get("/api/v1/ping")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"));

        // 3. Access documents list with Bearer token
        mockMvc.perform(get("/api/documents")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token))
                .andExpect(status().isOk());

        // 4. Access /api/auth/me to verify current user context
        mockMvc.perform(get("/api/auth/me")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + token))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.username").value("testuser"));
    }

    @Test
    @DisplayName("Role separation: USER cannot access ADMIN endpoint (403 Forbidden), but ADMIN can (200 OK)")
    void roleSeparationEnforcedOnAdminEndpoints() throws Exception {
        when(dlqManagementService.inspectDlq(anyString(), anyInt())).thenReturn(Collections.emptyList());

        // Login as regular USER
        LoginRequest userLogin = LoginRequest.builder()
                .username("testuser")
                .password("password")
                .build();

        MvcResult userResult = mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(userLogin)))
                .andExpect(status().isOk())
                .andReturn();

        String userToken = objectMapper.readTree(userResult.getResponse().getContentAsString()).get("token").asText();

        // USER tries to access ADMIN endpoint -> 403 Forbidden
        mockMvc.perform(get("/api/admin/dlq/messages?topic=document.uploaded.dlq")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + userToken))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.status").value(403))
                .andExpect(jsonPath("$.error").value("Forbidden"));

        // Login as ADMIN
        LoginRequest adminLogin = LoginRequest.builder()
                .username("testadmin")
                .password("password")
                .build();

        MvcResult adminResult = mockMvc.perform(post("/api/auth/login")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(adminLogin)))
                .andExpect(status().isOk())
                .andReturn();

        String adminToken = objectMapper.readTree(adminResult.getResponse().getContentAsString()).get("token").asText();

        // ADMIN accesses ADMIN endpoint -> 200 OK
        mockMvc.perform(get("/api/admin/dlq/messages?topic=document.uploaded.dlq")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + adminToken))
                .andExpect(status().isOk());
    }

    @Test
    @DisplayName("Request with invalid/malformed JWT token is rejected with 401")
    void invalidTokenIsRejected() throws Exception {
        mockMvc.perform(get("/api/documents")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer invalid.malformed.jwt.token"))
                .andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.status").value(401));
    }
}
