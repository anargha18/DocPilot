package com.docpilot.api.controller;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentAnalysis;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.CommunicationRepository;
import com.docpilot.common.repository.DocumentAnalysisRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.common.service.CommunicationExecutionService;
import com.docpilot.common.service.DocumentEmbeddingService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@WithMockUser(username = "testuser", roles = {"USER"})
class CommunicationReviewIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private DocumentRepository documentRepository;

    @Autowired
    private DocumentAnalysisRepository documentAnalysisRepository;

    @Autowired
    private CommunicationRepository communicationRepository;

    @Autowired
    private ActivityLogRepository activityLogRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private DocumentEmbeddingService documentEmbeddingService;

    @Autowired
    private CommunicationExecutionService communicationExecutionService;

    private Document createAnalyzedDocumentWithDraftCommunication(CommunicationType type) {
        Document document = Document.builder()
                .filename("q3-financial-report.pdf")
                .storageKey("reports/q3-financial-report.pdf")
                .contentType("application/pdf")
                .fileSize(4096L)
                .uploadTimestamp(Instant.now())
                .status(DocumentStatus.PENDING_APPROVAL)
                .build();
        document = documentRepository.save(document);

        DocumentAnalysis analysis = DocumentAnalysis.builder()
                .document(document)
                .category("FINANCIAL")
                .extractedContext("Q3 Financial performance summary with gross revenue $50M.")
                .summary("Company achieved $50M gross revenue in Q3, up 22% YoY.")
                .build();
        documentAnalysisRepository.save(analysis);

        // Record initial activity logs
        activityLogRepository.save(ActivityLog.builder()
                .documentId(document.getId())
                .actionType(ActionType.DOCUMENT_UPLOADED)
                .details("Uploaded q3-financial-report.pdf")
                .timestamp(Instant.now().minusSeconds(30))
                .build());

        activityLogRepository.save(ActivityLog.builder()
                .documentId(document.getId())
                .actionType(ActionType.DOCUMENT_ANALYZED)
                .details("Document analyzed: category FINANCIAL")
                .timestamp(Instant.now().minusSeconds(20))
                .build());

        ActionType selectionAction = (type == CommunicationType.EMAIL) ? ActionType.EMAIL_SELECTED : ActionType.SOCIAL_SELECTED;
        activityLogRepository.save(ActivityLog.builder()
                .documentId(document.getId())
                .actionType(selectionAction)
                .details("User selected communication channel: " + type)
                .timestamp(Instant.now().minusSeconds(10))
                .build());

        Communication comm = Communication.builder()
                .document(document)
                .type(type)
                .generatedContent("Subject: Q3 Financial Results\n\nWe are delighted to report $50M in revenue.")
                .status(CommunicationStatus.DRAFT)
                .editCount(0)
                .edited(false)
                .build();
        comm = communicationRepository.save(comm);

        activityLogRepository.save(ActivityLog.builder()
                .documentId(document.getId())
                .actionType(ActionType.CONTENT_GENERATED)
                .details("Generated " + type + " communication (id: " + comm.getId() + ")")
                .timestamp(Instant.now().minusSeconds(5))
                .build());

        return document;
    }

    @Test
    @DisplayName("GET /api/communications/{id} returns draft communication details")
    void shouldGetCommunicationDraft() throws Exception {
        Document doc = createAnalyzedDocumentWithDraftCommunication(CommunicationType.EMAIL);
        Communication comm = communicationRepository.findByDocumentId(doc.getId()).get(0);

        mockMvc.perform(get("/api/communications/" + comm.getId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(comm.getId().toString()))
                .andExpect(jsonPath("$.documentId").value(doc.getId().toString()))
                .andExpect(jsonPath("$.type").value("EMAIL"))
                .andExpect(jsonPath("$.status").value("DRAFT"))
                .andExpect(jsonPath("$.generatedContent").value(containsString("Q3 Financial Results")))
                .andExpect(jsonPath("$.editCount").value(0))
                .andExpect(jsonPath("$.edited").value(false));
    }

    @Test
    @DisplayName("DOD 1: Single edit succeeds, but a second edit attempt is rejected with a clear error and not silently overwritten")
    void shouldAllowSingleEditAndRejectSecondEditAttempt() throws Exception {
        Document doc = createAnalyzedDocumentWithDraftCommunication(CommunicationType.EMAIL);
        Communication comm = communicationRepository.findByDocumentId(doc.getId()).get(0);

        String firstEditPayload = """
                {
                    "editedContent": "Subject: Q3 Financial Milestone - $50M Reached\\n\\nTeam, our Q3 revenue crossed $50M with 22% YoY growth."
                }
                """;

        // 1. First edit must succeed
        mockMvc.perform(patch("/api/communications/" + comm.getId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(firstEditPayload))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(comm.getId().toString()))
                .andExpect(jsonPath("$.status").value("EDITED"))
                .andExpect(jsonPath("$.editCount").value(1))
                .andExpect(jsonPath("$.edited").value(true))
                .andExpect(jsonPath("$.editedContent").value(containsString("Q3 Financial Milestone - $50M Reached")))
                .andExpect(jsonPath("$.generatedContent").value(containsString("Subject: Q3 Financial Results")));

        // Verify database state after first edit
        Communication afterFirstEdit = communicationRepository.findById(comm.getId()).orElseThrow();
        assertThat(afterFirstEdit.getStatus()).isEqualTo(CommunicationStatus.EDITED);
        assertThat(afterFirstEdit.getEditCount()).isEqualTo(1);
        assertThat(afterFirstEdit.isEdited()).isTrue();
        assertThat(afterFirstEdit.getEditedContent()).contains("Q3 Financial Milestone - $50M Reached");

        // Verify CONTENT_EDITED recorded in ActivityLog
        List<ActivityLog> logsAfterFirstEdit = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logsAfterFirstEdit).anyMatch(l -> ActionType.CONTENT_EDITED.name().equals(l.getActionType()));

        // 2. Second edit attempt MUST be rejected with a clear error
        String secondEditPayload = """
                {
                    "editedContent": "Subject: Malicious Overwrite\\n\\nThis should be blocked."
                }
                """;

        mockMvc.perform(patch("/api/communications/" + comm.getId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(secondEditPayload))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("Invalid State"))
                .andExpect(jsonPath("$.message").value(containsString("already been edited once")));

        // Verify second edit was NOT silently overwritten in the database
        Communication afterSecondAttempt = communicationRepository.findById(comm.getId()).orElseThrow();
        assertThat(afterSecondAttempt.getEditCount()).isEqualTo(1);
        assertThat(afterSecondAttempt.getEditedContent()).contains("Q3 Financial Milestone - $50M Reached");
        assertThat(afterSecondAttempt.getEditedContent()).doesNotContain("Malicious Overwrite");
    }

    @Test
    @DisplayName("POST /api/communications/{id}/approve transitions to APPROVED, updates Document, writes ActivityLog, and emits content.approved")
    void shouldApproveCommunicationAndEmitContentApproved() throws Exception {
        Document doc = createAnalyzedDocumentWithDraftCommunication(CommunicationType.EMAIL);
        Communication comm = communicationRepository.findByDocumentId(doc.getId()).get(0);

        // Optional edit before approval
        mockMvc.perform(patch("/api/communications/" + comm.getId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"editedContent\": \"Subject: Approved Q3 Communication\\n\\nFinal validated body.\"}"))
                .andExpect(status().isOk());

        // Call POST /approve
        mockMvc.perform(post("/api/communications/" + comm.getId() + "/approve"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(comm.getId().toString()))
                .andExpect(jsonPath("$.status").value("APPROVED"));

        // Verify Communication in database
        Communication approvedComm = communicationRepository.findById(comm.getId()).orElseThrow();
        assertThat(approvedComm.getStatus()).isEqualTo(CommunicationStatus.APPROVED);

        // Verify Document status transitioned to APPROVED
        Document updatedDoc = documentRepository.findById(doc.getId()).orElseThrow();
        assertThat(updatedDoc.getStatus()).isEqualTo(DocumentStatus.APPROVED);

        // Verify ActivityLog entry for CONTENT_APPROVED
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).anyMatch(l -> ActionType.CONTENT_APPROVED.name().equals(l.getActionType()));

        // Verify Transactional Outbox event emitted for content.approved
        List<OutboxEvent> outboxEvents = outboxEventRepository.findByAggregateTypeAndAggregateId("COMMUNICATION", comm.getId().toString());
        assertThat(outboxEvents).anyMatch(e -> KafkaTopics.CONTENT_APPROVED.equals(e.getEventType())
                && e.getPayload().contains(comm.getId().toString())
                && e.getPayload().contains("Approved Q3 Communication")
                && e.getPayload().contains("APPROVED"));

        // Verify subsequent edit is blocked after approval
        mockMvc.perform(patch("/api/communications/" + comm.getId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"editedContent\": \"Post-approval edit attempt\"}"))
                .andExpect(status().isConflict());

        // Verify subsequent approval attempt is blocked (already approved)
        mockMvc.perform(post("/api/communications/" + comm.getId() + "/approve"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.message").value(containsString("already been approved")));
    }

    @Test
    @DisplayName("POST /api/communications/{id}/reject transitions to REJECTED, resets Document to ANALYZED, and stops pipeline")
    void shouldRejectCommunicationAndResetDocumentToAnalyzed() throws Exception {
        Document doc = createAnalyzedDocumentWithDraftCommunication(CommunicationType.EMAIL);
        Communication comm = communicationRepository.findByDocumentId(doc.getId()).get(0);

        // Clear outbox events prior to test to ensure no events are emitted on reject
        outboxEventRepository.deleteAll();

        // Call POST /reject
        mockMvc.perform(post("/api/communications/" + comm.getId() + "/reject"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(comm.getId().toString()))
                .andExpect(jsonPath("$.status").value("REJECTED"));

        // Verify Communication status is REJECTED
        Communication rejectedComm = communicationRepository.findById(comm.getId()).orElseThrow();
        assertThat(rejectedComm.getStatus()).isEqualTo(CommunicationStatus.REJECTED);

        // Verify Document status is reset to ANALYZED so user can try the other communication type
        Document resetDoc = documentRepository.findById(doc.getId()).orElseThrow();
        assertThat(resetDoc.getStatus()).isEqualTo(DocumentStatus.ANALYZED);

        // Verify ActivityLog entry for CONTENT_REJECTED
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).anyMatch(l -> ActionType.CONTENT_REJECTED.name().equals(l.getActionType()));

        // Verify pipeline stopped: NO outbox event emitted for content.approved
        List<OutboxEvent> outboxEvents = outboxEventRepository.findByAggregateTypeAndAggregateId("COMMUNICATION", comm.getId().toString());
        assertThat(outboxEvents).noneMatch(e -> KafkaTopics.CONTENT_APPROVED.equals(e.getEventType()));

        // Verify that user CAN now try the other communication type (SOCIAL) since document is back in ANALYZED state!
        mockMvc.perform(post("/api/documents/" + doc.getId() + "/communications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\": \"SOCIAL\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.type").value("SOCIAL"))
                .andExpect(jsonPath("$.status").value("DRAFT"));
    }

    @Test
    @DisplayName("DOD 2: Only APPROVED communications ever reach the execution stage — sneaking a DRAFT communication is blocked")
    void shouldBlockDraftCommunicationFromReachingExecutionStage() {
        Document doc = createAnalyzedDocumentWithDraftCommunication(CommunicationType.EMAIL);
        Communication draftComm = communicationRepository.findByDocumentId(doc.getId()).get(0);

        assertThat(draftComm.getStatus()).isEqualTo(CommunicationStatus.DRAFT);

        // Attempting to execute DRAFT communication directly MUST be blocked
        assertThatThrownBy(() -> communicationExecutionService.execute(draftComm.getId()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only APPROVED communications can reach the execution stage")
                .hasMessageContaining(draftComm.getId().toString())
                .hasMessageContaining("DRAFT");

        // Attempting to execute with entity directly MUST also be blocked
        assertThatThrownBy(() -> communicationExecutionService.execute(draftComm))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only APPROVED communications can reach the execution stage");

        // Now edit and approve the communication
        draftComm.setEditedContent("Subject: Final Approval\n\nApproved message.");
        draftComm.setStatus(CommunicationStatus.APPROVED);
        Communication approvedComm = communicationRepository.save(draftComm);

        // Now executing the APPROVED communication succeeds
        boolean executed = communicationExecutionService.execute(approvedComm.getId());
        assertThat(executed).isTrue();
    }

    @Test
    @DisplayName("DOD 3: Activity history for a document shows the full edit/approve trail")
    void shouldShowFullActivityTrailForDocument() throws Exception {
        Document doc = createAnalyzedDocumentWithDraftCommunication(CommunicationType.EMAIL);
        Communication comm = communicationRepository.findByDocumentId(doc.getId()).get(0);

        // 1. Edit the communication
        mockMvc.perform(patch("/api/communications/" + comm.getId())
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"editedContent\": \"Subject: Edited Trail Test\\n\\nEdited body.\"}"))
                .andExpect(status().isOk());

        // 2. Approve the communication
        mockMvc.perform(post("/api/communications/" + comm.getId() + "/approve"))
                .andExpect(status().isOk());

        // 3. Verify ActivityLog database trail contains all sequential phases
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        List<String> actionTypes = logs.stream().map(ActivityLog::getActionType).toList();

        assertThat(actionTypes).containsSubsequence(
                ActionType.DOCUMENT_UPLOADED.name(),
                ActionType.DOCUMENT_ANALYZED.name(),
                ActionType.EMAIL_SELECTED.name(),
                ActionType.CONTENT_GENERATED.name(),
                ActionType.CONTENT_EDITED.name(),
                ActionType.CONTENT_APPROVED.name()
        );

        // 4. Verify Activity history API endpoint GET /api/documents/{id}/activities
        mockMvc.perform(get("/api/documents/" + doc.getId() + "/activities"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$.length()").value(logs.size()))
                .andExpect(jsonPath("$[?(@.actionType == 'CONTENT_EDITED')]").isNotEmpty())
                .andExpect(jsonPath("$[?(@.actionType == 'CONTENT_APPROVED')]").isNotEmpty());
    }
}
