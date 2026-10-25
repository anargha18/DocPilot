package com.docpilot.api.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "System", description = "System health, diagnostics, and liveness probes")
public class PingController {

    @Operation(
            summary = "System ping",
            description = "Lightweight diagnostic probe returning service availability and timestamp."
    )
    @ApiResponse(
            responseCode = "200",
            description = "Service is up and operational",
            content = @Content(mediaType = "application/json", schema = @Schema(example = "{\"app\": \"DocPilot API\", \"status\": \"UP\", \"timestamp\": \"2026-10-25T12:00:00Z\"}"))
    )
    @GetMapping("/ping")
    public ResponseEntity<Map<String, Object>> ping() {
        return ResponseEntity.ok(Map.of(
                "app", "DocPilot API",
                "status", "UP",
                "timestamp", Instant.now().toString()
        ));
    }
}
