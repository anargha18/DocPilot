package com.docpilot.api.dto;

import com.docpilot.common.domain.ActivityLog;
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
public class ActivityLogResponse {
    private UUID id;
    private UUID documentId;
    private String actionType;
    private Instant timestamp;
    private String details;

    public static ActivityLogResponse from(ActivityLog log) {
        if (log == null) {
            return null;
        }
        return ActivityLogResponse.builder()
                .id(log.getId())
                .documentId(log.getDocumentId())
                .actionType(log.getActionType())
                .timestamp(log.getTimestamp())
                .details(log.getDetails())
                .build();
    }
}
