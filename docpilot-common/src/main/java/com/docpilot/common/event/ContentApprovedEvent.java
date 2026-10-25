package com.docpilot.common.event;

import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
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
public class ContentApprovedEvent {
    private UUID eventId;
    private UUID documentId;
    private UUID communicationId;
    private String correlationId;
    private CommunicationType type;
    private String approvedContent;
    private CommunicationStatus status;
    private Instant approvedAt;
}
