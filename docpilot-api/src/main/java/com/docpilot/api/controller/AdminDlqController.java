package com.docpilot.api.controller;

import com.docpilot.api.dto.ReplayDlqRequest;
import com.docpilot.common.dto.DlqMessageDto;
import com.docpilot.common.service.DlqManagementService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/admin/dlq")
@RequiredArgsConstructor
@Tag(name = "Admin DLQ Management", description = "Dead Letter Queue inspection and replay operations (Requires ROLE_ADMIN)")
public class AdminDlqController {

    private final DlqManagementService dlqManagementService;

    @Operation(
            summary = "Inspect Dead Letter Queue messages",
            description = "Reads uncommitted messages from a Kafka DLQ topic without committing offsets or altering queue state.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Messages successfully retrieved from DLQ",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = DlqMessageDto.class)))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid DLQ topic specified"
            ),
            @ApiResponse(
                    responseCode = "403",
                    description = "Forbidden - Requires ROLE_ADMIN"
            )
    })
    @GetMapping("/messages")
    public ResponseEntity<List<DlqMessageDto>> inspectMessages(
            @Parameter(description = "Kafka DLQ topic name (e.g. document.uploaded.dlq, content.approved.dlq)", required = true)
            @RequestParam("topic") String topic,
            @Parameter(description = "Maximum number of records to return (default: 50)")
            @RequestParam(value = "max", defaultValue = "50") int max
    ) {
        log.info("Admin request to inspect DLQ topic [{}] (max: {})", topic, max);
        List<DlqMessageDto> messages = dlqManagementService.inspectDlq(topic, max);
        return ResponseEntity.ok(messages);
    }

    @Operation(
            summary = "Replay messages from Dead Letter Queue",
            description = "Re-publishes messages from a DLQ topic back into the main processing topic and resets idempotency records to allow safe reprocessing.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Messages replayed successfully",
                    content = @Content(mediaType = "application/json", schema = @Schema(example = "{\"status\": \"SUCCESS\", \"dlqTopic\": \"document.uploaded.dlq\", \"targetTopic\": \"document.uploaded\", \"replayedCount\": 5}"))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid request payload or topic"
            ),
            @ApiResponse(
                    responseCode = "403",
                    description = "Forbidden - Requires ROLE_ADMIN"
            )
    })
    @PostMapping("/replay")
    public ResponseEntity<Map<String, Object>> replayMessages(
            @Valid @RequestBody ReplayDlqRequest request) {
        log.info("Admin request to replay DLQ messages: topic=[{}], targetTopic=[{}], max=[{}]",
                request.getTopic(), request.getTargetTopic(), request.getMaxRecords());

        int replayedCount = dlqManagementService.replayDlq(
                request.getTopic(),
                request.getTargetTopic(),
                request.getMaxRecords()
        );

        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "dlqTopic", request.getTopic(),
                "targetTopic", request.getTargetTopic() != null ? request.getTargetTopic() : "auto-resolved",
                "replayedCount", replayedCount
        ));
    }
}
