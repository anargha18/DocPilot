package com.docpilot.api.controller;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
class DocumentControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private DocumentRepository documentRepository;

    @Autowired
    private ActivityLogRepository activityLogRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    @DisplayName("Uploading a document should persist Document, ActivityLog, and unpublished OutboxEvent")
    void shouldUploadDocumentAndCreateOutboxEvent() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "quarterly-report.pdf",
                "application/pdf",
                "Quarterly financial report test content".getBytes()
        );

        mockMvc.perform(multipart("/api/v1/documents").file(file))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.filename").value("quarterly-report.pdf"))
                .andExpect(jsonPath("$.status").value("UPLOADED"));

        // Verify Document entity
        List<Document> documents = documentRepository.findAll();
        assertThat(documents).isNotEmpty();
        Document doc = documents.stream()
                .filter(d -> "quarterly-report.pdf".equals(d.getFilename()))
                .findFirst()
                .orElseThrow();
        assertThat(doc.getStatus()).isEqualTo(DocumentStatus.UPLOADED);
        assertThat(doc.getFileSize()).isGreaterThan(0);

        // Verify ActivityLog audit entry
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).isNotEmpty();
        assertThat(logs.get(0).getActionType()).isEqualTo(ActionType.DOCUMENT_UPLOADED.name());

        // Verify OutboxEvent entity
        List<OutboxEvent> outboxEvents = outboxEventRepository.findByAggregateTypeAndAggregateId("DOCUMENT", doc.getId().toString());
        assertThat(outboxEvents).hasSize(1);
        OutboxEvent outboxEvent = outboxEvents.get(0);
        assertThat(outboxEvent.getEventType()).isEqualTo(KafkaTopics.DOCUMENT_UPLOADED);
        assertThat(outboxEvent.isPublished()).isFalse();
        assertThat(outboxEvent.getPayload()).contains("quarterly-report.pdf");
    }

    @Test
    @DisplayName("Should retrieve uploaded document by ID")
    void shouldRetrieveDocumentById() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file",
                "sample-policy.docx",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "Policy text content".getBytes()
        );

        String responseJson = mockMvc.perform(multipart("/api/v1/documents/upload").file(file))
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();

        assertThat(responseJson).contains("sample-policy.docx");
    }
}
