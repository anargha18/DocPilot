package com.docpilot.api.controller;

import com.docpilot.api.dto.ActivityLogResponse;
import com.docpilot.api.dto.DocumentResponse;
import com.docpilot.api.service.DocumentService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.ArraySchema;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping({"/api/v1/documents", "/api/documents"})
@RequiredArgsConstructor
@Tag(name = "Documents", description = "Document upload, metadata retrieval, status tracking, and audit activity history")
public class DocumentController {

    private final DocumentService documentService;

    @Operation(
            summary = "Upload a document",
            description = "Uploads a document (PDF, DOCX, TXT) to MinIO object storage, saves metadata in PostgreSQL, and enqueues a Transactional Outbox event to Kafka for asynchronous AI processing.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "201",
                    description = "Document successfully uploaded and queued for processing",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = DocumentResponse.class))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid or empty file"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized - Missing or invalid JWT"
            )
    })
    @PostMapping(consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<DocumentResponse> uploadDocument(
            @Parameter(description = "Document file to upload (PDF, DOCX, TXT)", required = true)
            @RequestParam("file") MultipartFile file) {
        DocumentResponse response = documentService.uploadDocument(file);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @Operation(
            summary = "Upload document alias",
            description = "Convenience endpoint alias for uploading documents (/upload).",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "201",
                    description = "Document successfully uploaded",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = DocumentResponse.class))
            ),
            @ApiResponse(
                    responseCode = "400",
                    description = "Invalid or empty file"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized"
            )
    })
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<DocumentResponse> uploadDocumentAlias(
            @Parameter(description = "Document file to upload (PDF, DOCX, TXT)", required = true)
            @RequestParam("file") MultipartFile file) {
        return uploadDocument(file);
    }

    @Operation(
            summary = "Get document by ID",
            description = "Retrieves document metadata, storage key, file size, content type, and current processing status.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Document found",
                    content = @Content(mediaType = "application/json", schema = @Schema(implementation = DocumentResponse.class))
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "Document not found"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized"
            )
    })
    @GetMapping("/{id}")
    public ResponseEntity<DocumentResponse> getDocument(
            @Parameter(description = "Document UUID", required = true)
            @PathVariable("id") UUID id) {
        return ResponseEntity.ok(documentService.getDocument(id));
    }

    @Operation(
            summary = "List all documents",
            description = "Returns all uploaded documents ordered by upload timestamp descending.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "List of documents successfully retrieved",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = DocumentResponse.class)))
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized"
            )
    })
    @GetMapping
    public ResponseEntity<List<DocumentResponse>> listDocuments() {
        return ResponseEntity.ok(documentService.listDocuments());
    }

    @Operation(
            summary = "Get document activity history",
            description = "Retrieves the complete chronological audit trail of all lifecycle events for a specific document.",
            security = @SecurityRequirement(name = "BearerAuth")
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "Chronological activity log retrieved",
                    content = @Content(mediaType = "application/json", array = @ArraySchema(schema = @Schema(implementation = ActivityLogResponse.class)))
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "Unauthorized"
            )
    })
    @GetMapping({"/{id}/activities", "/{id}/history"})
    public ResponseEntity<List<ActivityLogResponse>> getActivityHistory(
            @Parameter(description = "Document UUID", required = true)
            @PathVariable("id") UUID id) {
        return ResponseEntity.ok(documentService.getActivityHistory(id));
    }
}
