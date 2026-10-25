package com.docpilot.api.dto;

import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

@Getter
@Builder
public class DocumentResponse {
    private UUID id;
    private String filename;
    private String storageKey;
    private String contentType;
    private Long fileSize;
    private Instant uploadTimestamp;
    private DocumentStatus status;
    private String correlationId;
    private Instant createdAt;
    private Instant updatedAt;

    public static DocumentResponse from(Document doc) {
        return DocumentResponse.builder()
                .id(doc.getId())
                .filename(doc.getFilename())
                .storageKey(doc.getStorageKey())
                .contentType(doc.getContentType())
                .fileSize(doc.getFileSize())
                .uploadTimestamp(doc.getUploadTimestamp())
                .status(doc.getStatus())
                .correlationId(doc.getCorrelationId())
                .createdAt(doc.getCreatedAt())
                .updatedAt(doc.getUpdatedAt())
                .build();
    }
}
