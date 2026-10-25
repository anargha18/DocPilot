package com.docpilot.api.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReplayDlqRequest {

    @NotBlank(message = "DLQ topic is required")
    private String topic;

    private String targetTopic;

    @Builder.Default
    private int maxRecords = 50;
}
