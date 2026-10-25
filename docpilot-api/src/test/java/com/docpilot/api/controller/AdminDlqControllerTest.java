package com.docpilot.api.controller;

import com.docpilot.common.dto.DlqMessageDto;
import com.docpilot.common.service.DlqManagementService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@WithMockUser(username = "admin", roles = {"ADMIN"})
class AdminDlqControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private DlqManagementService dlqManagementService;

    @Test
    @DisplayName("GET /api/admin/dlq/messages returns inspected DLQ records")
    void shouldInspectDlqMessages() throws Exception {
        DlqMessageDto message = DlqMessageDto.builder()
                .topic("document.uploaded.dlq")
                .partition(0)
                .offset(42L)
                .key("test-key")
                .payload("{\"documentId\":\"12345\"}")
                .headers(Map.of("X-Event-Id", "evt-999"))
                .timestamp(System.currentTimeMillis())
                .build();

        when(dlqManagementService.inspectDlq(eq("document.uploaded.dlq"), anyInt()))
                .thenReturn(List.of(message));

        mockMvc.perform(get("/api/admin/dlq/messages")
                        .param("topic", "document.uploaded.dlq")
                        .param("max", "10"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].topic").value("document.uploaded.dlq"))
                .andExpect(jsonPath("$[0].offset").value(42))
                .andExpect(jsonPath("$[0].key").value("test-key"))
                .andExpect(jsonPath("$[0].payload").value("{\"documentId\":\"12345\"}"));
    }

    @Test
    @DisplayName("POST /api/admin/dlq/replay replays messages and returns replayedCount")
    void shouldReplayDlqMessages() throws Exception {
        when(dlqManagementService.replayDlq(eq("document.uploaded.dlq"), eq("document.uploaded"), anyInt()))
                .thenReturn(3);

        String requestJson = """
                {
                    "topic": "document.uploaded.dlq",
                    "targetTopic": "document.uploaded",
                    "maxRecords": 10
                }
                """;

        mockMvc.perform(post("/api/admin/dlq/replay")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.dlqTopic").value("document.uploaded.dlq"))
                .andExpect(jsonPath("$.targetTopic").value("document.uploaded"))
                .andExpect(jsonPath("$.replayedCount").value(3));
    }
}
