package com.docpilot.common.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class DocumentAnalyzedEvent {
    private UUID eventId;
    private UUID documentId;
    private String summary;
    private String category;
    private String extractedContext;
    private List<String> keyFacts;
    private Instant analyzedAt;
}
