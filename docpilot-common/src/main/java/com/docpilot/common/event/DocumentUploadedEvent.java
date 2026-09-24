package com.docpilot.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class DocumentUploadedEvent {
    private UUID documentId;
    private String filename;
    private String storageKey;
    private String contentType;
    private Long fileSize;
    private Instant uploadTimestamp;
    private UUID eventId;
    private boolean forceFailure;
}
