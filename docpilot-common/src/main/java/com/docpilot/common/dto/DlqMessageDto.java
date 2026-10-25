package com.docpilot.common.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DlqMessageDto {
    private String topic;
    private int partition;
    private long offset;
    private String key;
    private String payload;
    private Map<String, String> headers;
    private long timestamp;
}
