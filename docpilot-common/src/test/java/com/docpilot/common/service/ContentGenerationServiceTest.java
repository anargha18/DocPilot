package com.docpilot.common.service;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentAnalysis;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.observability.DocPilotTracer;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.CommunicationRepository;
import com.docpilot.common.repository.DocumentAnalysisRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.common.retry.RetryHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContentGenerationServiceTest {

    @Mock
    private DocumentRepository documentRepository;

    @Mock
    private DocumentAnalysisRepository documentAnalysisRepository;

    @Mock
    private CommunicationRepository communicationRepository;

    @Mock
    private ActivityLogRepository activityLogRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private DocumentEmbeddingService documentEmbeddingService;

    @Mock
    private ChatClient chatClient;

    @Mock
    private ChatClient.ChatClientRequestSpec requestSpec;

    @Mock
    private ChatClient.CallResponseSpec callResponseSpec;

    @Mock
    private ObjectProvider<DocPilotTracer> tracerProvider;

    private ObjectMapper objectMapper;
    private RetryHelper retryHelper;
    private ContentGenerationService contentGenerationService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().findAndRegisterModules();
        lenient().when(tracerProvider.getIfAvailable()).thenReturn(null);

        // Simple RetryHelper without delay
        retryHelper = new RetryHelper(1, 1, 1.0, 10, new ObjectProvider<>() {
            @Override
            public com.docpilot.common.observability.DocPilotMetrics getObject(Object... args) { return null; }
            @Override
            public com.docpilot.common.observability.DocPilotMetrics getIfAvailable() { return null; }
            @Override
            public com.docpilot.common.observability.DocPilotMetrics getIfUnique() { return null; }
            @Override
            public com.docpilot.common.observability.DocPilotMetrics getObject() { return null; }
        });

        contentGenerationService = new ContentGenerationService(
                documentRepository,
                documentAnalysisRepository,
                communicationRepository,
                activityLogRepository,
                outboxEventRepository,
                documentEmbeddingService,
                chatClient,
                retryHelper,
                objectMapper,
                tracerProvider
        );
    }

    @Test
    @DisplayName("generateContent should throw if document is not found or not in ANALYZED state")
    void generateContent_InvalidState_Throws() {
        UUID docId = UUID.randomUUID();
        when(documentRepository.findById(docId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> contentGenerationService.generateContent(docId, CommunicationType.EMAIL, "Professional", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Document not found");

        Document unanalyzedDoc = Document.builder().status(DocumentStatus.UPLOADED).build();
        unanalyzedDoc.setId(docId);
        when(documentRepository.findById(docId)).thenReturn(Optional.of(unanalyzedDoc));

        assertThatThrownBy(() -> contentGenerationService.generateContent(docId, CommunicationType.EMAIL, "Professional", null))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("must be ANALYZED");
    }

    @Test
    @DisplayName("generateContent should generate Email communication, persist draft and outbox event")
    void generateContent_Email_Success() {
        UUID docId = UUID.randomUUID();
        Document document = Document.builder()
                .filename("memo.pdf")
                .status(DocumentStatus.ANALYZED)
                .correlationId("corr-123")
                .uploadTimestamp(Instant.now())
                .build();
        document.setId(docId);

        DocumentAnalysis analysis = DocumentAnalysis.builder()
                .document(document)
                .category("EXECUTIVE")
                .extractedContext("Annual strategic plan")
                .summary("Strategic targets for FY27")
                .build();

        when(documentRepository.findById(docId)).thenReturn(Optional.of(document));
        when(documentAnalysisRepository.findByDocumentId(docId)).thenReturn(Optional.of(analysis));
        when(documentEmbeddingService.similaritySearchByDocument(any(UUID.class), anyString(), anyInt()))
                .thenReturn(List.of(new org.springframework.ai.document.Document("Target is 20% growth")));

        // ChatClient fluent mocking
        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.system(anyString())).thenReturn(requestSpec);
        when(requestSpec.user(anyString())).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.content()).thenReturn("Subject: FY27 Targets\n\nDear Team, our goal is 20% growth.");

        when(documentRepository.save(any(Document.class))).thenAnswer(i -> i.getArgument(0));
        when(communicationRepository.save(any(Communication.class))).thenAnswer(i -> {
            Communication c = i.getArgument(0);
            c.setId(UUID.randomUUID());
            return c;
        });

        Communication result = contentGenerationService.generateContent(docId, CommunicationType.EMAIL, "formal", null);

        assertThat(result).isNotNull();
        assertThat(result.getType()).isEqualTo(CommunicationType.EMAIL);
        assertThat(result.getStatus()).isEqualTo(CommunicationStatus.DRAFT);
        assertThat(result.getGeneratedContent()).contains("Subject: FY27 Targets");

        // Outbox event verified
        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("content.generated");
    }

    @Test
    @DisplayName("generateContent should truncate social post exceeding 280 characters")
    void generateContent_SocialTruncatesTo280() {
        UUID docId = UUID.randomUUID();
        Document document = Document.builder()
                .status(DocumentStatus.ANALYZED)
                .uploadTimestamp(Instant.now())
                .build();
        document.setId(docId);

        DocumentAnalysis analysis = DocumentAnalysis.builder()
                .document(document)
                .category("NEWS")
                .summary("Major product launch")
                .build();

        when(documentRepository.findById(docId)).thenReturn(Optional.of(document));
        when(documentAnalysisRepository.findByDocumentId(docId)).thenReturn(Optional.of(analysis));

        when(chatClient.prompt()).thenReturn(requestSpec);
        when(requestSpec.system(anyString())).thenReturn(requestSpec);
        when(requestSpec.user(anyString())).thenReturn(requestSpec);
        when(requestSpec.call()).thenReturn(callResponseSpec);
        when(callResponseSpec.content()).thenReturn("X".repeat(300)); // over 280 chars

        when(documentRepository.save(any(Document.class))).thenAnswer(i -> i.getArgument(0));
        when(communicationRepository.save(any(Communication.class))).thenAnswer(i -> {
            Communication c = i.getArgument(0);
            c.setId(UUID.randomUUID());
            return c;
        });

        Communication result = contentGenerationService.generateContent(docId, CommunicationType.SOCIAL, null, "X/Twitter");

        assertThat(result.getGeneratedContent().length()).isLessThanOrEqualTo(280);
        assertThat(result.getGeneratedContent()).endsWith("...");
    }
}
