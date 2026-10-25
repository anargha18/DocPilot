package com.docpilot.api.dto;

import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommunicationResponse {

    private UUID id;
    private UUID documentId;
    private CommunicationType type;
    private String generatedContent;
    private String editedContent;
    private CommunicationStatus status;
    private int editCount;
    private boolean edited;
    private String correlationId;
    private Instant createdAt;
    private Instant updatedAt;

    public static CommunicationResponse from(Communication comm) {
        if (comm == null) {
            return null;
        }
        return CommunicationResponse.builder()
                .id(comm.getId())
                .documentId(comm.getDocument() != null ? comm.getDocument().getId() : null)
                .type(comm.getType())
                .generatedContent(comm.getGeneratedContent())
                .editedContent(comm.getEditedContent())
                .status(comm.getStatus())
                .editCount(comm.getEditCount())
                .edited(comm.isEdited())
                .correlationId(comm.getCorrelationId())
                .createdAt(comm.getCreatedAt())
                .updatedAt(comm.getUpdatedAt())
                .build();
    }
}
