package com.docpilot.api.service;

import com.docpilot.api.dto.ActivityLogResponse;
import com.docpilot.api.dto.DocumentResponse;
import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.observability.DocPilotTracer;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.common.storage.StorageService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.mock.web.MockMultipartFile;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DocumentServiceTest {

    @Mock
    private DocumentRepository documentRepository;

    @Mock
    private ActivityLogRepository activityLogRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private StorageService storageService;

    @Mock
    private ObjectProvider<DocPilotTracer> tracerProvider;

    private ObjectMapper objectMapper;
    private DocumentService documentService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().findAndRegisterModules();
        lenient().when(tracerProvider.getIfAvailable()).thenReturn(null);
        documentService = new DocumentService(
                documentRepository,
                activityLogRepository,
                outboxEventRepository,
                storageService,
                objectMapper,
                tracerProvider
        );
    }

    @Test
    @DisplayName("uploadDocument should throw IllegalArgumentException when file is null or empty")
    void uploadDocument_EmptyFile_ThrowsException() {
        assertThatThrownBy(() -> documentService.uploadDocument(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot upload an empty file");

        MockMultipartFile emptyFile = new MockMultipartFile("file", "test.txt", "text/plain", new byte[0]);
        assertThatThrownBy(() -> documentService.uploadDocument(emptyFile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot upload an empty file");

        verify(storageService, never()).upload(anyString(), any(), anyString());
        verify(documentRepository, never()).save(any());
    }

    @Test
    @DisplayName("uploadDocument should store file, persist entity, audit log and outbox event")
    void uploadDocument_ValidFile_Success() {
        byte[] bytes = "Q3 quarterly report content".getBytes();
        MockMultipartFile file = new MockMultipartFile("file", "q3_report.pdf", "application/pdf", bytes);

        String generatedKey = "documents/uuid/q3_report.pdf";
        when(storageService.upload(eq("q3_report.pdf"), eq(bytes), eq("application/pdf")))
                .thenReturn(generatedKey);

        UUID docId = UUID.randomUUID();
        when(documentRepository.save(any(Document.class))).thenAnswer(invocation -> {
            Document doc = invocation.getArgument(0);
            doc.setId(docId);
            return doc;
        });

        when(activityLogRepository.save(any(ActivityLog.class))).thenAnswer(invocation -> invocation.getArgument(0));

        when(outboxEventRepository.save(any(OutboxEvent.class))).thenAnswer(invocation -> {
            OutboxEvent outbox = invocation.getArgument(0);
            outbox.setId(UUID.randomUUID());
            return outbox;
        });

        DocumentResponse response = documentService.uploadDocument(file);

        assertThat(response).isNotNull();
        assertThat(response.getId()).isEqualTo(docId);
        assertThat(response.getFilename()).isEqualTo("q3_report.pdf");
        assertThat(response.getStatus()).isEqualTo(DocumentStatus.UPLOADED);
        assertThat(response.getFileSize()).isEqualTo(bytes.length);

        // Verify storage upload called
        verify(storageService).upload(eq("q3_report.pdf"), eq(bytes), eq("application/pdf"));

        // Verify Document entity persisted
        ArgumentCaptor<Document> docCaptor = ArgumentCaptor.forClass(Document.class);
        verify(documentRepository).save(docCaptor.capture());
        Document capturedDoc = docCaptor.getValue();
        assertThat(capturedDoc.getStorageKey()).isEqualTo(generatedKey);
        assertThat(capturedDoc.getStatus()).isEqualTo(DocumentStatus.UPLOADED);

        // Verify ActivityLog persisted
        ArgumentCaptor<ActivityLog> logCaptor = ArgumentCaptor.forClass(ActivityLog.class);
        verify(activityLogRepository).save(logCaptor.capture());
        ActivityLog capturedLog = logCaptor.getValue();
        assertThat(capturedLog.getDocumentId()).isEqualTo(docId);
        assertThat(capturedLog.getActionType()).isEqualTo(ActionType.DOCUMENT_UPLOADED.name());

        // Verify OutboxEvent persisted
        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        OutboxEvent capturedOutbox = outboxCaptor.getValue();
        assertThat(capturedOutbox.getAggregateId()).isEqualTo(docId.toString());
        assertThat(capturedOutbox.getEventType()).isEqualTo("document.uploaded");
        assertThat(capturedOutbox.isPublished()).isFalse();
    }

    @Test
    @DisplayName("getDocument should return DocumentResponse when document exists")
    void getDocument_Found_ReturnsResponse() {
        UUID docId = UUID.randomUUID();
        Document document = Document.builder()
                .filename("policy.docx")
                .storageKey("documents/policy.docx")
                .contentType("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
                .fileSize(1024L)
                .status(DocumentStatus.ANALYZED)
                .uploadTimestamp(Instant.now())
                .build();
        document.setId(docId);

        when(documentRepository.findById(docId)).thenReturn(Optional.of(document));

        DocumentResponse response = documentService.getDocument(docId);

        assertThat(response).isNotNull();
        assertThat(response.getId()).isEqualTo(docId);
        assertThat(response.getFilename()).isEqualTo("policy.docx");
        assertThat(response.getStatus()).isEqualTo(DocumentStatus.ANALYZED);
    }

    @Test
    @DisplayName("getDocument should throw IllegalArgumentException when document not found")
    void getDocument_NotFound_ThrowsException() {
        UUID docId = UUID.randomUUID();
        when(documentRepository.findById(docId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> documentService.getDocument(docId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Document not found");
    }

    @Test
    @DisplayName("listDocuments should return list of mapped document responses")
    void listDocuments_ReturnsOrderedList() {
        Document doc1 = Document.builder().filename("a.txt").fileSize(10L).status(DocumentStatus.UPLOADED).uploadTimestamp(Instant.now()).build();
        doc1.setId(UUID.randomUUID());
        Document doc2 = Document.builder().filename("b.txt").fileSize(20L).status(DocumentStatus.ANALYZED).uploadTimestamp(Instant.now()).build();
        doc2.setId(UUID.randomUUID());

        when(documentRepository.findAllByOrderByUploadTimestampDesc()).thenReturn(List.of(doc2, doc1));

        List<DocumentResponse> result = documentService.listDocuments();

        assertThat(result).hasSize(2);
        assertThat(result.get(0).getFilename()).isEqualTo("b.txt");
        assertThat(result.get(1).getFilename()).isEqualTo("a.txt");
    }

    @Test
    @DisplayName("getActivityHistory should throw when documentId is null, return activity list when present")
    void getActivityHistory_Scenarios() {
        assertThatThrownBy(() -> documentService.getActivityHistory(null))
                .isInstanceOf(IllegalArgumentException.class);

        UUID docId = UUID.randomUUID();
        ActivityLog log1 = ActivityLog.builder().documentId(docId).actionType(ActionType.DOCUMENT_UPLOADED).details("Uploaded").timestamp(Instant.now()).build();
        ActivityLog log2 = ActivityLog.builder().documentId(docId).actionType(ActionType.DOCUMENT_ANALYZED).details("Analyzed").timestamp(Instant.now().plusSeconds(5)).build();

        when(activityLogRepository.findByDocumentIdOrderByTimestampAsc(docId)).thenReturn(List.of(log1, log2));

        List<ActivityLogResponse> history = documentService.getActivityHistory(docId);

        assertThat(history).hasSize(2);
        assertThat(history.get(0).getActionType()).isEqualTo(ActionType.DOCUMENT_UPLOADED.name());
        assertThat(history.get(1).getActionType()).isEqualTo(ActionType.DOCUMENT_ANALYZED.name());
    }
}
