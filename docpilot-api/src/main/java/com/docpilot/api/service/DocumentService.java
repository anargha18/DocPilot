package com.docpilot.api.service;

import com.docpilot.api.dto.DocumentResponse;
import com.docpilot.common.storage.StorageService;
import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.DocumentUploadedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DocumentService {

    private final DocumentRepository documentRepository;
    private final ActivityLogRepository activityLogRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final StorageService storageService;
    private final ObjectMapper objectMapper;

    @Transactional
    public DocumentResponse uploadDocument(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("Cannot upload an empty file");
        }

        String originalFilename = file.getOriginalFilename() != null ? file.getOriginalFilename() : "unnamed";
        String contentType = file.getContentType() != null ? file.getContentType() : "application/octet-stream";
        byte[] content;
        try {
            content = file.getBytes();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read uploaded file contents", e);
        }

        // 1. Upload to storage (MinIO or in-memory fallback)
        String storageKey = storageService.upload(originalFilename, content, contentType);

        // 2. Persist Document entity
        Document document = Document.builder()
                .filename(originalFilename)
                .storageKey(storageKey)
                .contentType(contentType)
                .fileSize(file.getSize())
                .uploadTimestamp(Instant.now())
                .status(DocumentStatus.UPLOADED)
                .build();
        document = documentRepository.save(document);

        // 3. Persist ActivityLog audit entry
        ActivityLog logEntry = ActivityLog.builder()
                .documentId(document.getId())
                .actionType(ActionType.DOCUMENT_UPLOADED)
                .details("Document uploaded: " + originalFilename + " (" + file.getSize() + " bytes)")
                .build();
        activityLogRepository.save(logEntry);

        // 4. Construct event payload
        DocumentUploadedEvent eventPayload = DocumentUploadedEvent.builder()
                .documentId(document.getId())
                .filename(document.getFilename())
                .storageKey(document.getStorageKey())
                .contentType(document.getContentType())
                .fileSize(document.getFileSize())
                .uploadTimestamp(document.getUploadTimestamp())
                .forceFailure(originalFilename.toLowerCase().contains("fail") || originalFilename.toLowerCase().contains("dlq"))
                .build();

        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(eventPayload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize document uploaded event", e);
        }

        // 5. Persist OutboxEvent in the same database transaction
        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateType("DOCUMENT")
                .aggregateId(document.getId().toString())
                .eventType(KafkaTopics.DOCUMENT_UPLOADED)
                .payload(payloadJson)
                .published(false)
                .build();
        outboxEvent = outboxEventRepository.save(outboxEvent);

        log.info("Document [{}] uploaded and transactional outbox event [{}] created",
                document.getId(), outboxEvent.getId());

        return DocumentResponse.from(document);
    }

    @Transactional(readOnly = true)
    public DocumentResponse getDocument(UUID id) {
        Document document = documentRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Document not found: " + id));
        return DocumentResponse.from(document);
    }

    @Transactional(readOnly = true)
    public List<DocumentResponse> listDocuments() {
        return documentRepository.findAllByOrderByUploadTimestampDesc()
                .stream()
                .map(DocumentResponse::from)
                .toList();
    }
}
