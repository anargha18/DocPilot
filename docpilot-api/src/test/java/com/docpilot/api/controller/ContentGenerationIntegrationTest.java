package com.docpilot.api.controller;

import com.docpilot.common.ai.MockChatModel;
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
import com.docpilot.common.service.DocumentEmbeddingService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import static org.hamcrest.Matchers.containsString;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@WithMockUser(username = "testuser", roles = {"USER"})
class ContentGenerationIntegrationTest {

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

    @BeforeEach
    @AfterEach
    void resetFlags() {
        MockChatModel.FORCE_FAILURE = false;
    }

    private Document createAndAnalyzeDocument(String specificFact) {
        Document document = Document.builder()
                .filename("confidential-project-manifest.txt")
                .storageKey("manifests/project-manifest.txt")
                .contentType("text/plain")
                .fileSize(1024L)
                .uploadTimestamp(Instant.now())
                .status(DocumentStatus.ANALYZED)
                .build();
        document = documentRepository.save(document);

        // Chunk and embed the source document content into the vector store
        String documentContent = """
                DocPilot Project Manifest & Technical Specification
                Category: TECHNICAL
                Context: High-security quantum telemetry research and deployment guidelines.
                Key Fact: %s
                Operational guidelines mandate strict protocol verification prior to external delivery.
                """.formatted(specificFact);

        documentEmbeddingService.chunkAndEmbed(document.getId(), document.getFilename(), documentContent);

        DocumentAnalysis analysis = DocumentAnalysis.builder()
                .document(document)
                .category("TECHNICAL")
                .extractedContext("High-security quantum telemetry research and deployment guidelines.")
                .summary("Technical specification detailing quantum telemetry research and operational guidelines.")
                .rawExtractionJson("{\"keyFacts\": [\"" + specificFact + "\"]}")
                .build();
        documentAnalysisRepository.save(analysis);

        return document;
    }

    @Test
    @DisplayName("DOD 1: Selecting EMAIL produces draft Communication grounded with unusual specific fact")
    void shouldGenerateEmailGroundedInDocumentContentWithSpecificFact() throws Exception {
        String specificUnusualFact = "Project Chimera operates exclusively on helium-3 quantum fuel cells manufactured in Reykjavik with serial H3-9982-QX.";
        Document doc = createAndAnalyzeDocument(specificUnusualFact);

        String requestJson = """
                {
                    "type": "EMAIL"
                }
                """;

        String responseBody = mockMvc.perform(post("/api/documents/" + doc.getId() + "/communications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.documentId").value(doc.getId().toString()))
                .andExpect(jsonPath("$.type").value("EMAIL"))
                .andExpect(jsonPath("$.status").value("DRAFT"))
                .andExpect(jsonPath("$.generatedContent").isNotEmpty())
                .andReturn()
                .getResponse()
                .getContentAsString();

        // Verify that the generated email is strictly grounded with the unusual specific fact
        assertThat(responseBody).contains("Subject:");
        assertThat(responseBody).contains("H3-9982-QX");
        assertThat(responseBody).contains("helium-3 quantum fuel cells");

        // Verify database persistence of Communication row
        List<Communication> communications = communicationRepository.findByDocumentId(doc.getId());
        assertThat(communications).hasSize(1);
        Communication comm = communications.get(0);
        assertThat(comm.getStatus()).isEqualTo(CommunicationStatus.DRAFT);
        assertThat(comm.getType()).isEqualTo(CommunicationType.EMAIL);
        assertThat(comm.getGeneratedContent()).contains("H3-9982-QX");

        // Verify Document status transitioned to PENDING_APPROVAL
        Document updatedDoc = documentRepository.findById(doc.getId()).orElseThrow();
        assertThat(updatedDoc.getStatus()).isEqualTo(DocumentStatus.PENDING_APPROVAL);

        // Verify ActivityLog entries: EMAIL_SELECTED and CONTENT_GENERATED
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).isNotEmpty();
        assertThat(logs).anyMatch(l -> ActionType.EMAIL_SELECTED.name().equals(l.getActionType()));
        assertThat(logs).anyMatch(l -> ActionType.CONTENT_GENERATED.name().equals(l.getActionType()));

        // Verify Transactional Outbox event emitted for content.generated
        List<OutboxEvent> outboxEvents = outboxEventRepository.findByAggregateTypeAndAggregateId("COMMUNICATION", comm.getId().toString());
        assertThat(outboxEvents).hasSize(1);
        OutboxEvent outboxEvent = outboxEvents.get(0);
        assertThat(outboxEvent.getEventType()).isEqualTo(KafkaTopics.CONTENT_GENERATED);
        assertThat(outboxEvent.getPayload()).contains(comm.getId().toString());
        assertThat(outboxEvent.getPayload()).contains(doc.getId().toString());
        assertThat(outboxEvent.getPayload()).contains("H3-9982-QX");
    }

    @Test
    @DisplayName("DOD 1: Selecting SOCIAL produces draft Communication adhering to character constraints and grounded in unusual fact")
    void shouldGenerateSocialPostGroundedInDocumentContentWithSpecificFact() throws Exception {
        String specificUnusualFact = "Project Neptune budget allocated exactly $42,133,700 approved by Dr. Aris Thorne.";
        Document doc = createAndAnalyzeDocument(specificUnusualFact);

        String requestJson = """
                {
                    "type": "SOCIAL",
                    "targetPlatform": "X/Twitter"
                }
                """;

        String responseBody = mockMvc.perform(post("/api/documents/" + doc.getId() + "/communications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").exists())
                .andExpect(jsonPath("$.type").value("SOCIAL"))
                .andExpect(jsonPath("$.status").value("DRAFT"))
                .andReturn()
                .getResponse()
                .getContentAsString();

        // Verify ground fact and platform constraints
        List<Communication> communications = communicationRepository.findByDocumentId(doc.getId());
        assertThat(communications).hasSize(1);
        Communication comm = communications.get(0);
        assertThat(comm.getStatus()).isEqualTo(CommunicationStatus.DRAFT);
        assertThat(comm.getType()).isEqualTo(CommunicationType.SOCIAL);
        assertThat(comm.getGeneratedContent().length()).isLessThanOrEqualTo(280);
        assertThat(comm.getGeneratedContent()).contains("Aris Thorne");
        assertThat(comm.getGeneratedContent()).contains("#DocPilot");

        // Verify ActivityLog entries: SOCIAL_SELECTED and CONTENT_GENERATED
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).anyMatch(l -> ActionType.SOCIAL_SELECTED.name().equals(l.getActionType()));
        assertThat(logs).anyMatch(l -> ActionType.CONTENT_GENERATED.name().equals(l.getActionType()));
    }

    @Test
    @DisplayName("DOD 2: Generation failure when document is unanalyzed is caught and reflected in Document status, not swallowed")
    void shouldCatchAndReflectFailureWhenDocumentNotAnalyzed() throws Exception {
        Document unanalyzedDoc = Document.builder()
                .filename("raw-pending-doc.pdf")
                .storageKey("docs/raw-pending-doc.pdf")
                .contentType("application/pdf")
                .fileSize(500L)
                .uploadTimestamp(Instant.now())
                .status(DocumentStatus.UPLOADED)
                .build();
        unanalyzedDoc = documentRepository.save(unanalyzedDoc);

        String requestJson = """
                {
                    "type": "EMAIL"
                }
                """;

        // Must fail with error response (not 2xx)
        mockMvc.perform(post("/api/documents/" + unanalyzedDoc.getId() + "/communications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().is4xxClientError())
                .andExpect(jsonPath("$.error").exists())
                .andExpect(jsonPath("$.message").isNotEmpty());

        // Verify Document status is updated to FAILED
        Document failedDoc = documentRepository.findById(unanalyzedDoc.getId()).orElseThrow();
        assertThat(failedDoc.getStatus()).isEqualTo(DocumentStatus.FAILED);

        // Verify ActivityLog reflects the failure
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(unanalyzedDoc.getId());
        assertThat(logs).anyMatch(l -> ActionType.FAILED.name().equals(l.getActionType()));
    }

    @Test
    @DisplayName("DOD 2: AI model generation failure is caught, reflected in Document status, and not swallowed")
    void shouldCatchAndReflectFailureWhenAiModelFails() throws Exception {
        String specificFact = "Failure test fact payload.";
        Document doc = createAndAnalyzeDocument(specificFact);

        // Force AI failure
        MockChatModel.FORCE_FAILURE = true;

        String requestJson = """
                {
                    "type": "EMAIL"
                }
                """;

        // Must return 5xx server error, indicating failure was caught and reported
        mockMvc.perform(post("/api/documents/" + doc.getId() + "/communications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(requestJson))
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Internal Server Error"))
                .andExpect(jsonPath("$.message").value(containsString("Simulated AI generation failure")));

        // Verify Document status reflects FAILED
        Document failedDoc = documentRepository.findById(doc.getId()).orElseThrow();
        assertThat(failedDoc.getStatus()).isEqualTo(DocumentStatus.FAILED);

        // Verify ActivityLog has FAILED entry
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).anyMatch(l -> ActionType.FAILED.name().equals(l.getActionType()));
    }

    @Test
    @DisplayName("Should retrieve communications for a document via GET endpoints")
    void shouldRetrieveCommunicationsListAndSingle() throws Exception {
        String fact = "Quarterly EBITDA reached a record $18.5M in Q3 2026.";
        Document doc = createAndAnalyzeDocument(fact);

        // Generate an email
        mockMvc.perform(post("/api/v1/documents/" + doc.getId() + "/communications")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\": \"EMAIL\"}"))
                .andExpect(status().isCreated());

        // Get list of communications for document
        String listResponse = mockMvc.perform(get("/api/documents/" + doc.getId() + "/communications"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$").isArray())
                .andExpect(jsonPath("$[0].type").value("EMAIL"))
                .andReturn()
                .getResponse()
                .getContentAsString();

        assertThat(listResponse).contains("EBITDA");

        // Retrieve single communication
        List<Communication> comms = communicationRepository.findByDocumentId(doc.getId());
        assertThat(comms).isNotEmpty();
        UUID commId = comms.get(0).getId();

        mockMvc.perform(get("/api/communications/" + commId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(commId.toString()))
                .andExpect(jsonPath("$.status").value("DRAFT"));
    }
}
