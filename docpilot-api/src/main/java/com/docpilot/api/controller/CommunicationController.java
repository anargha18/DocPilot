package com.docpilot.api.controller;

import com.docpilot.api.dto.CommunicationResponse;
import com.docpilot.api.dto.CreateCommunicationRequest;
import com.docpilot.api.dto.EditCommunicationRequest;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.repository.CommunicationRepository;
import com.docpilot.common.service.CommunicationReviewService;
import com.docpilot.common.service.ContentGenerationService;
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
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
@Tag(name = "Communications", description = "AI content generation, review, single-round user edits, approval, and execution dispatch")
public class CommunicationController {

    private final ContentGenerationService contentGenerationService;
    private final CommunicationReviewService communicationReviewService;
    private final CommunicationRepository communicationRepository;

    @Operation(
            summary = "Generate AI communication content",
            description = "Triggers AI generation (Email or Social Media) using Spring AI and RAG grounded in the document context and vector similarity chunks.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "201",
                    description = "Communication draft successfully generated",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = CommunicationResponse.class))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid request payload or document not found"
            ),
            @ApiResponse(
                    responseCode = "409",
                    description = "Document is not in ANALYZED state"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized"
            )
    })
    @PostMapping({"/api/documents/{id}/communications", "/api/v1/documents/{id}/communications"})
    public ResponseEntity<CommunicationResponse> generateCommunication(
            @Parameter(description = "Document UUID", required = true)
            @PathVariable("id") UUID documentId,
            @Valid @RequestBody CreateCommunicationRequest request) {

        log.info("Received request to generate communication of type [{}] for document [{}]",
                request.getType(), documentId);

        Communication comm = contentGenerationService.generateContent(
                documentId,
                request.getType(),
                request.getTone(),
                request.getTargetPlatform()
        );

        return ResponseEntity.status(HttpStatus.CREATED).body(CommunicationResponse.from(comm));
    }

    @Operation(
            summary = "List communications for a document",
            description = "Retrieves all communication drafts and approved records associated with a specific document.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "List of communications retrieved",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = CommunicationResponse.class)))
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized"
            )
    })
    @GetMapping({"/api/documents/{id}/communications", "/api/v1/documents/{id}/communications"})
    public ResponseEntity<List<CommunicationResponse>> getCommunicationsForDocument(
            @Parameter(description = "Document UUID", required = true)
            @PathVariable("id") UUID documentId) {

        List<Communication> communications = communicationRepository.findByDocumentId(documentId);
        List<CommunicationResponse> responses = communications.stream()
                .map(CommunicationResponse::from)
                .toList();

        return ResponseEntity.ok(responses);
    }

    @Operation(
            summary = "Get communication by ID",
            description = "Retrieves communication details, generated and edited content, edit counts, and execution status.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Communication found",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = CommunicationResponse.class))
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Communication not found"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized"
            )
    })
    @GetMapping({"/api/communications/{id}", "/api/v1/communications/{id}"})
    public ResponseEntity<CommunicationResponse> getCommunication(
            @Parameter(description = "Communication UUID", required = true)
            @PathVariable("id") UUID id) {

        Communication comm = communicationRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Communication not found: " + id));

        return ResponseEntity.ok(CommunicationResponse.from(comm));
    }

    @Operation(
            summary = "Edit communication content",
            description = "Applies a user edit to draft communication content. Enforces strict business rule: only exactly one edit round is permitted.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Communication edited successfully",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = CommunicationResponse.class))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid content or exceeds character limits (280 characters for Social)"
            ),
            @ApiResponse(
                    responseCode = "409",
                    description = "Already edited once or communication not in DRAFT status"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized"
            )
    })
    @PatchMapping({"/api/communications/{id}", "/api/v1/communications/{id}"})
    public ResponseEntity<CommunicationResponse> editCommunication(
            @Parameter(description = "Communication UUID", required = true)
            @PathVariable("id") UUID id,
            @RequestBody(required = false) EditCommunicationRequest request) {

        log.info("Received request to edit communication [{}]", id);
        String content = request != null ? request.getEffectiveContent() : null;
        Communication comm = communicationReviewService.editCommunication(id, content);

        return ResponseEntity.ok(CommunicationResponse.from(comm));
    }

    @Operation(
            summary = "Approve communication",
            description = "Approves a communication for delivery. Transitions status to APPROVED and atomically enqueues a Transactional Outbox event to Kafka for asynchronous dispatch.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Communication approved and queued for delivery",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = CommunicationResponse.class))
            ),
            @ApiResponse(
                    responseCode = "409",
                    description = "Communication is already approved or not in DRAFT/EDITED status"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized"
            )
    })
    @PostMapping({"/api/communications/{id}/approve", "/api/v1/communications/{id}/approve"})
    public ResponseEntity<CommunicationResponse> approveCommunication(
            @Parameter(description = "Communication UUID", required = true)
            @PathVariable("id") UUID id) {

        log.info("Received request to approve communication [{}]", id);
        Communication comm = communicationReviewService.approveCommunication(id);

        return ResponseEntity.ok(CommunicationResponse.from(comm));
    }

    @Operation(
            summary = "Reject communication",
            description = "Rejects a communication draft, setting its status to REJECTED and preventing delivery execution.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Communication rejected successfully",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = CommunicationResponse.class))
            ),
            @ApiResponse(
                    responseCode = "409",
                    description = "Communication is not in DRAFT or EDITED status"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized"
            )
    })
    @PostMapping({"/api/communications/{id}/reject", "/api/v1/communications/{id}/reject"})
    public ResponseEntity<CommunicationResponse> rejectCommunication(
            @Parameter(description = "Communication UUID", required = true)
            @PathVariable("id") UUID id) {

        log.info("Received request to reject communication [{}]", id);
        Communication comm = communicationReviewService.rejectCommunication(id);

        return ResponseEntity.ok(CommunicationResponse.from(comm));
    }
}
