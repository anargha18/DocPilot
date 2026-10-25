package com.docpilot.common.service;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.observability.DocPilotTracer;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.CommunicationRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommunicationReviewServiceTest {

    @Mock
    private CommunicationRepository communicationRepository;

    @Mock
    private DocumentRepository documentRepository;

    @Mock
    private ActivityLogRepository activityLogRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private ObjectProvider<DocPilotTracer> tracerProvider;

    private ObjectMapper objectMapper;
    private CommunicationReviewService reviewService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().findAndRegisterModules();
        lenient().when(tracerProvider.getIfAvailable()).thenReturn(null);
        reviewService = new CommunicationReviewService(
                communicationRepository,
                documentRepository,
                activityLogRepository,
                outboxEventRepository,
                objectMapper,
                tracerProvider
        );
    }

    private Document createTestDocument() {
        Document doc = Document.builder()
                .filename("memo.txt")
                .status(DocumentStatus.ANALYZED)
                .uploadTimestamp(Instant.now())
                .build();
        doc.setId(UUID.randomUUID());
        return doc;
    }

    @Test
    @DisplayName("editCommunication should reject null ID or blank content")
    void editCommunication_Validation() {
        assertThatThrownBy(() -> reviewService.editCommunication(null, "content"))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> reviewService.editCommunication(UUID.randomUUID(), "   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("editCommunication should enforce exactly one edit round")
    void editCommunication_EnforcesSingleEditRound() {
        UUID commId = UUID.randomUUID();
        Communication comm = Communication.builder()
                .type(CommunicationType.EMAIL)
                .generatedContent("Original")
                .status(CommunicationStatus.EDITED)
                .editCount(1)
                .edited(true)
                .build();
        comm.setId(commId);

        when(communicationRepository.findById(commId)).thenReturn(Optional.of(comm));

        assertThatThrownBy(() -> reviewService.editCommunication(commId, "Second edit attempt"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Only exactly one edit round is permitted");

        verify(communicationRepository, never()).save(any());
    }

    @Test
    @DisplayName("editCommunication should enforce 280-character limit for social media posts")
    void editCommunication_EnforcesSocialMediaCharacterLimit() {
        UUID commId = UUID.randomUUID();
        Communication comm = Communication.builder()
                .type(CommunicationType.SOCIAL)
                .generatedContent("Original post")
                .status(CommunicationStatus.DRAFT)
                .editCount(0)
                .edited(false)
                .document(createTestDocument())
                .build();
        comm.setId(commId);

        when(communicationRepository.findById(commId)).thenReturn(Optional.of(comm));

        String longContent = "A".repeat(281);
        assertThatThrownBy(() -> reviewService.editCommunication(commId, longContent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds the maximum limit of 280 characters");
    }

    @Test
    @DisplayName("editCommunication should successfully save valid edits and audit log")
    void editCommunication_ValidEdit_Success() {
        UUID commId = UUID.randomUUID();
        Document doc = createTestDocument();
        Communication comm = Communication.builder()
                .type(CommunicationType.EMAIL)
                .generatedContent("Original email")
                .status(CommunicationStatus.DRAFT)
                .editCount(0)
                .edited(false)
                .document(doc)
                .build();
        comm.setId(commId);

        when(communicationRepository.findById(commId)).thenReturn(Optional.of(comm));
        when(communicationRepository.save(any(Communication.class))).thenAnswer(i -> i.getArgument(0));

        Communication updated = reviewService.editCommunication(commId, "Revised email content");

        assertThat(updated.getEditedContent()).isEqualTo("Revised email content");
        assertThat(updated.getEditCount()).isEqualTo(1);
        assertThat(updated.isEdited()).isTrue();
        assertThat(updated.getStatus()).isEqualTo(CommunicationStatus.EDITED);

        ArgumentCaptor<ActivityLog> logCaptor = ArgumentCaptor.forClass(ActivityLog.class);
        verify(activityLogRepository).save(logCaptor.capture());
        assertThat(logCaptor.getValue().getActionType()).isEqualTo(ActionType.CONTENT_EDITED.name());
    }

    @Test
    @DisplayName("approveCommunication should approve DRAFT or EDITED and create OutboxEvent")
    void approveCommunication_Success() {
        UUID commId = UUID.randomUUID();
        Document doc = createTestDocument();
        Communication comm = Communication.builder()
                .type(CommunicationType.EMAIL)
                .generatedContent("Generated subject and body")
                .status(CommunicationStatus.DRAFT)
                .document(doc)
                .build();
        comm.setId(commId);

        when(communicationRepository.findById(commId)).thenReturn(Optional.of(comm));
        when(communicationRepository.save(any())).thenAnswer(i -> i.getArgument(0));
        when(documentRepository.save(any())).thenAnswer(i -> i.getArgument(0));
        when(outboxEventRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        Communication approved = reviewService.approveCommunication(commId);

        assertThat(approved.getStatus()).isEqualTo(CommunicationStatus.APPROVED);

        verify(documentRepository).save(any(Document.class));

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("content.approved");

        ArgumentCaptor<ActivityLog> logCaptor = ArgumentCaptor.forClass(ActivityLog.class);
        verify(activityLogRepository).save(logCaptor.capture());
        assertThat(logCaptor.getValue().getActionType()).isEqualTo(ActionType.CONTENT_APPROVED.name());
    }

    @Test
    @DisplayName("approveCommunication should reject if already approved")
    void approveCommunication_AlreadyApproved_ThrowsException() {
        UUID commId = UUID.randomUUID();
        Communication comm = Communication.builder()
                .status(CommunicationStatus.APPROVED)
                .build();
        comm.setId(commId);

        when(communicationRepository.findById(commId)).thenReturn(Optional.of(comm));

        assertThatThrownBy(() -> reviewService.approveCommunication(commId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("has already been approved");
    }

    @Test
    @DisplayName("rejectCommunication should mark status as REJECTED")
    void rejectCommunication_Success() {
        UUID commId = UUID.randomUUID();
        Document doc = createTestDocument();
        Communication comm = Communication.builder()
                .status(CommunicationStatus.DRAFT)
                .document(doc)
                .build();
        comm.setId(commId);

        when(communicationRepository.findById(commId)).thenReturn(Optional.of(comm));
        when(communicationRepository.save(any())).thenAnswer(i -> i.getArgument(0));

        Communication rejected = reviewService.rejectCommunication(commId);

        assertThat(rejected.getStatus()).isEqualTo(CommunicationStatus.REJECTED);
        verify(activityLogRepository).save(any(ActivityLog.class));
    }
}
