package com.docpilot.worker.service;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentAnalysis;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.DocumentAnalyzedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.model.DocumentAnalysisResult;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.DocumentAnalysisRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.common.storage.StorageService;
import com.docpilot.common.service.DocumentEmbeddingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.io.InputStream;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DocumentAnalysisService {

    private final DocumentRepository documentRepository;
    private final DocumentAnalysisRepository documentAnalysisRepository;
    private final ActivityLogRepository activityLogRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final StorageService storageService;
    private final DocumentTextExtractor textExtractor;
    private final AiAnalysisService aiAnalysisService;
    private final DocumentEmbeddingService embeddingService;
    private final ObjectMapper objectMapper;

    @Transactional
    public void markAnalyzing(UUID documentId) {
        log.info("Transitioning document [{}] to ANALYZING", documentId);
        Document document = documentRepository.findById(documentId)
                .orElseThrow(() -> new IllegalArgumentException("Document not found: " + documentId));
        document.setStatus(DocumentStatus.ANALYZING);
        documentRepository.save(document);
    }

    @Transactional
    public DocumentAnalysis analyzeAndPersist(UUID documentId, UUID eventId, String storageKey, String filename) {
        return analyzeAndPersist(documentId, eventId, storageKey, filename,
                com.docpilot.common.observability.CorrelationContext.getCorrelationId(), null);
    }

    @Transactional
    public DocumentAnalysis analyzeAndPersist(UUID documentId, UUID eventId, String storageKey, String filename,
                                             String correlationId, String traceParent) {
        if (correlationId != null) {
            com.docpilot.common.observability.CorrelationContext.setCorrelationId(correlationId);
        }
        log.info("Starting analysis workflow for document [{}] (storageKey: {}, correlationId: {})",
                documentId, storageKey, correlationId);

        Document document = documentRepository.findById(documentId)
                .orElseThrow(() -> new IllegalArgumentException("Document not found: " + documentId));

        if (correlationId != null && (document.getCorrelationId() == null || document.getCorrelationId().isBlank())) {
            document.setCorrelationId(correlationId);
        }
        if (traceParent != null && (document.getTraceParent() == null || document.getTraceParent().isBlank())) {
            document.setTraceParent(traceParent);
        }

        // 1. Download file from storage (MinIO or configured fallback)
        String key = storageKey != null ? storageKey : document.getStorageKey();
        InputStream stream = storageService.download(key);

        // 2. Extract text with Apache Tika
        String extractedText = textExtractor.extractText(stream);
        log.info("Extracted {} characters from document [{}]", extractedText.length(), documentId);

        // 3. Chunk and embed into pgvector vector store
        embeddingService.chunkAndEmbed(documentId, filename != null ? filename : document.getFilename(), extractedText);

        // 4. Spring AI ChatClient structured output
        DocumentAnalysisResult result = aiAnalysisService.analyzeDocument(extractedText);

        // 5. Persist DocumentAnalysis
        String rawJson;
        try {
            rawJson = objectMapper.writeValueAsString(result);
        } catch (Exception e) {
            rawJson = "{}";
        }

        DocumentAnalysis analysis = documentAnalysisRepository.findByDocumentId(documentId)
                .orElseGet(() -> DocumentAnalysis.builder().document(document).build());

        analysis.setSummary(result.summary());
        analysis.setExtractedContext(result.context());
        analysis.setCategory(result.category());
        analysis.setRawExtractionJson(rawJson);

        analysis = documentAnalysisRepository.save(analysis);

        // 6. Update Document status to ANALYZED
        document.setStatus(DocumentStatus.ANALYZED);
        documentRepository.save(document);

        // 7. Write ActivityLog entry
        ActivityLog logEntry = ActivityLog.builder()
                .documentId(documentId)
                .actionType(ActionType.DOCUMENT_ANALYZED)
                .details("Document analyzed: category=" + result.category() + ", summary length=" + (result.summary() != null ? result.summary().length() : 0))
                .timestamp(Instant.now())
                .build();
        activityLogRepository.save(logEntry);

        // 8. Emit document.analyzed event via outbox
        DocumentAnalyzedEvent analyzedEvent = DocumentAnalyzedEvent.builder()
                .eventId(UUID.randomUUID())
                .documentId(documentId)
                .correlationId(correlationId)
                .summary(result.summary())
                .category(result.category())
                .extractedContext(result.context())
                .keyFacts(result.keyFacts())
                .analyzedAt(Instant.now())
                .build();

        try {
            String payload = objectMapper.writeValueAsString(analyzedEvent);
            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType("DOCUMENT")
                    .aggregateId(documentId.toString())
                    .eventType(KafkaTopics.DOCUMENT_ANALYZED)
                    .payload(payload)
                    .correlationId(correlationId)
                    .traceParent(traceParent != null ? traceParent : document.getTraceParent())
                    .published(false)
                    .build();
            outboxEventRepository.save(outboxEvent);
            log.info("Saved outbox event [{}] for topic [{}] aggregate [{}] (correlationId: {})",
                    outboxEvent.getId(), KafkaTopics.DOCUMENT_ANALYZED, documentId, correlationId);
        } catch (Exception e) {
            log.error("Failed to serialize and save outbox event for document [{}]: {}", documentId, e.getMessage());
            throw new RuntimeException("Failed to emit document.analyzed outbox event", e);
        }

        log.info("Successfully completed AI analysis for document [{}] (correlationId: {})", documentId, correlationId);
        return analysis;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markFailed(UUID documentId, String reason) {
        if (documentId == null) {
            log.warn("Cannot mark null documentId as FAILED (reason: {})", reason);
            return;
        }
        log.warn("Marking document [{}] as FAILED. Reason: {}", documentId, reason);
        documentRepository.findById(documentId).ifPresent(doc -> {
            doc.setStatus(DocumentStatus.FAILED);
            documentRepository.save(doc);

            ActivityLog failureLog = ActivityLog.builder()
                    .documentId(documentId)
                    .actionType(ActionType.FAILED)
                    .details("Document analysis failed: " + reason)
                    .timestamp(Instant.now())
                    .build();
            activityLogRepository.save(failureLog);
        });
    }
}
