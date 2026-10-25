package com.docpilot.common.service;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentAnalysis;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.ContentGeneratedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.CommunicationRepository;
import com.docpilot.common.repository.DocumentAnalysisRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.common.retry.RetryHelper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.prompt.PromptTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class ContentGenerationService {

    private final DocumentRepository documentRepository;
    private final DocumentAnalysisRepository documentAnalysisRepository;
    private final CommunicationRepository communicationRepository;
    private final ActivityLogRepository activityLogRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final DocumentEmbeddingService documentEmbeddingService;
    private final ChatClient chatClient;
    private final RetryHelper retryHelper;
    private final ObjectMapper objectMapper;
    private final org.springframework.beans.factory.ObjectProvider<com.docpilot.common.observability.DocPilotTracer> tracerProvider;

    private static final String EMAIL_SYSTEM_TEMPLATE = """
            You are an expert organizational communications officer for DocPilot.
            Your task is to generate a professional, ready-to-send email based strictly on the provided source document context and analysis.

            GUIDELINES:
            1. Adapt the tone to the document's category ({category}) and context ({context}). Tone guidance: {tone}.
            2. Produce a clear, compelling Subject line starting with "Subject: ".
            3. Produce a well-structured email body (greeting, key points, conclusion/next steps, professional sign-off).
            4. Grounding: All statements, facts, and figures MUST be strictly grounded in the provided source chunks. Do NOT invent facts or figures.
            5. Output format:
            Subject: <Subject Line>

            <Body>
            """;

    private static final String EMAIL_USER_TEMPLATE = """
            Please generate an organization-ready email based on the following document context:

            Category: {category}
            Context: {context}
            Summary: {summary}

            Source Document Grounding Information (Retrieved Chunks):
            {chunks}

            Generate the complete email with Subject and Body.
            """;

    private static final String SOCIAL_SYSTEM_TEMPLATE = """
            You are an expert social media communications specialist for DocPilot.
            Your task is to generate concise, public-facing copy for social media (target platform: {platform}).

            GUIDELINES:
            1. Target Platform Constraints: Strictly adhere to {platform} length constraints. The entire output MUST be under 280 characters.
            2. Content: Highlight the key announcement or core takeaway from the source document. Tone guidance: {tone}.
            3. Grounding: Every statement MUST be strictly grounded in the provided source document facts and chunks. Do NOT fabricate information.
            4. Include relevant hashtags (e.g. #DocPilot #Update).
            """;

    private static final String SOCIAL_USER_TEMPLATE = """
            Please generate a concise social media post for {platform} based on the following document context:

            Category: {category}
            Context: {context}
            Summary: {summary}

            Source Document Grounding Information (Retrieved Chunks):
            {chunks}

            Generate the public-facing copy adhering to the character limit (strictly <= 280 chars).
            """;

    public Communication generateContent(UUID documentId, CommunicationType type, String tone, String targetPlatform) {
        if (documentId == null) {
            throw new IllegalArgumentException("documentId cannot be null");
        }
        if (type == null) {
            throw new IllegalArgumentException("CommunicationType cannot be null");
        }

        Document document = documentRepository.findById(documentId)
                .orElseThrow(() -> new IllegalArgumentException("Document not found: " + documentId));

        if (document.getStatus() != DocumentStatus.ANALYZED
                && document.getStatus() != DocumentStatus.PENDING_APPROVAL
                && document.getStatus() != DocumentStatus.GENERATING) {
            markGenerationFailed(documentId, type, "Document is in status " + document.getStatus()
                    + ", but must be ANALYZED to generate communications");
            throw new IllegalStateException("Document [" + documentId + "] is in status " + document.getStatus()
                    + ", but must be ANALYZED to generate communications");
        }

        DocumentAnalysis analysis = documentAnalysisRepository.findByDocumentId(documentId).orElse(null);
        if (analysis == null) {
            markGenerationFailed(documentId, type, "Document has not been analyzed yet");
            throw new IllegalStateException("Document [" + documentId + "] has not been analyzed yet");
        }

        String correlationId = document.getCorrelationId();
        if (correlationId == null || correlationId.isBlank()) {
            correlationId = com.docpilot.common.observability.CorrelationContext.getOrGenerateCorrelationId(null);
            document.setCorrelationId(correlationId);
        } else {
            com.docpilot.common.observability.CorrelationContext.setCorrelationId(correlationId);
        }

        com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
        com.docpilot.common.observability.DocPilotTracer.TraceScope stageScope = null;
        if (tracer != null) {
            stageScope = tracer.startStageSpan("generation", correlationId, documentId.toString(), document.getTraceParent());
        }

        try {
            return doGenerateContent(document, analysis, type, tone, targetPlatform, stageScope);
        } catch (Exception e) {
            if (stageScope != null) {
                stageScope.markFailed(e);
            }
            log.error("Content generation failed for document [{}] (type: {}): {}", documentId, type, e.getMessage(), e);
            markGenerationFailed(documentId, type, e.getMessage());
            throw new RuntimeException("Content generation failed: " + e.getMessage(), e);
        } finally {
            if (stageScope != null) {
                stageScope.close();
            }
        }
    }

    @Transactional
    public Communication doGenerateContent(Document document, DocumentAnalysis analysis, CommunicationType type,
                                           String tone, String targetPlatform) throws JsonProcessingException {
        return doGenerateContent(document, analysis, type, tone, targetPlatform, null);
    }

    @Transactional
    public Communication doGenerateContent(Document document, DocumentAnalysis analysis, CommunicationType type,
                                           String tone, String targetPlatform,
                                           com.docpilot.common.observability.DocPilotTracer.TraceScope stageScope) throws JsonProcessingException {
        UUID documentId = document.getId();
        String correlationId = document.getCorrelationId();

        // 1. Record selection ActivityLog
        ActionType selectionAction = (type == CommunicationType.EMAIL) ? ActionType.EMAIL_SELECTED : ActionType.SOCIAL_SELECTED;
        ActivityLog selectionLog = ActivityLog.builder()
                .documentId(documentId)
                .actionType(selectionAction)
                .details("User selected communication channel: " + type + (targetPlatform != null ? " (" + targetPlatform + ")" : ""))
                .timestamp(Instant.now())
                .build();
        activityLogRepository.save(selectionLog);

        // 2. Transition document to GENERATING
        document.setStatus(DocumentStatus.GENERATING);
        document = documentRepository.save(document);

        // 3. RAG Retrieval from pgvector
        String query = (type == CommunicationType.EMAIL ? "Email communication details: " : "Social media highlights: ")
                + analysis.getSummary();
        List<org.springframework.ai.document.Document> chunks = documentEmbeddingService.similaritySearchByDocument(documentId, query, 4);

        StringBuilder chunksBuilder = new StringBuilder();
        if (chunks != null && !chunks.isEmpty()) {
            for (int i = 0; i < chunks.size(); i++) {
                chunksBuilder.append("Chunk ").append(i + 1).append(":\n")
                        .append(chunks.get(i).getText()).append("\n\n");
            }
        } else {
            // Fallback to analysis context/summary if vector store has no chunks yet
            chunksBuilder.append(analysis.getSummary()).append("\n");
            if (analysis.getExtractedContext() != null) {
                chunksBuilder.append(analysis.getExtractedContext()).append("\n");
            }
        }
        String chunksContext = chunksBuilder.toString().trim();

        // 4. Spring AI Prompt Template generation
        String effectiveTone = (tone != null && !tone.isBlank()) ? tone : "Adapted to " + analysis.getCategory() + " context";
        String effectivePlatform = (targetPlatform != null && !targetPlatform.isBlank()) ? targetPlatform : "X/Twitter";

        String generatedContent = executePrompt(type, analysis, chunksContext, effectiveTone, effectivePlatform);

        if (type == CommunicationType.SOCIAL && generatedContent.length() > 280) {
            generatedContent = generatedContent.substring(0, 277) + "...";
        }

        // 5. Persist Communication (status DRAFT)
        Communication communication = Communication.builder()
                .document(document)
                .type(type)
                .generatedContent(generatedContent)
                .status(CommunicationStatus.DRAFT)
                .correlationId(correlationId)
                .build();
        communication = communicationRepository.save(communication);

        // 6. Update Document status to PENDING_APPROVAL
        document.setStatus(DocumentStatus.PENDING_APPROVAL);
        documentRepository.save(document);

        // 7. Write ActivityLog for CONTENT_GENERATED
        ActivityLog generatedLog = ActivityLog.builder()
                .documentId(documentId)
                .actionType(ActionType.CONTENT_GENERATED)
                .details("Generated " + type + " communication (id: " + communication.getId() + ")")
                .timestamp(Instant.now())
                .build();
        activityLogRepository.save(generatedLog);

        // 8. Emit content.generated via Transactional Outbox
        ContentGeneratedEvent eventPayload = ContentGeneratedEvent.builder()
                .eventId(UUID.randomUUID())
                .documentId(documentId)
                .communicationId(communication.getId())
                .correlationId(correlationId)
                .type(type)
                .generatedContent(communication.getGeneratedContent())
                .status(communication.getStatus())
                .generatedAt(Instant.now())
                .build();

        String currentTraceParent = stageScope != null ? stageScope.getTraceParent() : document.getTraceParent();

        String payloadJson = objectMapper.writeValueAsString(eventPayload);
        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateType("COMMUNICATION")
                .aggregateId(communication.getId().toString())
                .eventType(KafkaTopics.CONTENT_GENERATED)
                .payload(payloadJson)
                .correlationId(correlationId)
                .traceParent(currentTraceParent)
                .published(false)
                .build();
        outboxEventRepository.save(outboxEvent);

        log.info("Content generation completed for document [{}] communication [{}] (type: {}, correlationId: {})",
                documentId, communication.getId(), type, correlationId);

        return communication;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markGenerationFailed(UUID documentId, CommunicationType type, String failureReason) {
        if (documentId == null) {
            return;
        }
        try {
            documentRepository.findById(documentId).ifPresent(doc -> {
                doc.setStatus(DocumentStatus.FAILED);
                documentRepository.save(doc);

                ActivityLog failureLog = ActivityLog.builder()
                        .documentId(documentId)
                        .actionType(ActionType.FAILED)
                        .details("Content generation failed" + (type != null ? " for " + type : "") + ": " + failureReason)
                        .timestamp(Instant.now())
                        .build();
                activityLogRepository.save(failureLog);
            });

            if (type != null) {
                List<Communication> pendingComms = communicationRepository.findByDocumentIdAndType(documentId, type);
                for (Communication comm : pendingComms) {
                    if (comm.getStatus() == CommunicationStatus.DRAFT) {
                        comm.setStatus(CommunicationStatus.FAILED);
                        communicationRepository.save(comm);
                    }
                }
            }
        } catch (Exception ex) {
            log.error("Failed to mark document as FAILED after generation error: {}", ex.getMessage(), ex);
        }
    }

    private String executePrompt(CommunicationType type, DocumentAnalysis analysis, String chunksContext,
                                 String tone, String platform) {
        Map<String, Object> templateModel = new HashMap<>();
        templateModel.put("category", analysis.getCategory() != null ? analysis.getCategory() : "GENERAL");
        templateModel.put("context", analysis.getExtractedContext() != null ? analysis.getExtractedContext() : "");
        templateModel.put("summary", analysis.getSummary() != null ? analysis.getSummary() : "");
        templateModel.put("chunks", chunksContext);
        templateModel.put("tone", tone);
        templateModel.put("platform", platform);

        PromptTemplate systemTemplate;
        PromptTemplate userTemplate;

        if (type == CommunicationType.EMAIL) {
            systemTemplate = new PromptTemplate(EMAIL_SYSTEM_TEMPLATE);
            userTemplate = new PromptTemplate(EMAIL_USER_TEMPLATE);
        } else {
            systemTemplate = new PromptTemplate(SOCIAL_SYSTEM_TEMPLATE);
            userTemplate = new PromptTemplate(SOCIAL_USER_TEMPLATE);
        }

        String systemPromptText = systemTemplate.render(templateModel);
        String userPromptText = userTemplate.render(templateModel);

        com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
        String result;
        if (tracer != null) {
            try {
                result = tracer.recordExternalCall("openai", "generate_content", () ->
                        retryHelper.executeWithRetry(context ->
                                chatClient.prompt()
                                        .system(systemPromptText)
                                        .user(userPromptText)
                                        .call()
                                        .content()
                        )
                );
            } catch (Exception e) {
                log.error("AI chat client call failed: {}", e.getMessage());
                throw new RuntimeException("AI chat call failed: " + e.getMessage(), e);
            }
        } else {
            result = retryHelper.executeWithRetry(context ->
                    chatClient.prompt()
                            .system(systemPromptText)
                            .user(userPromptText)
                            .call()
                            .content()
            );
        }

        if (result == null || result.isBlank()) {
            throw new IllegalStateException("ChatClient returned empty generated content");
        }

        return result.trim();
    }

    private void markGenerationFailed(Document document, CommunicationType type, String failureReason) {
        try {
            document.setStatus(DocumentStatus.FAILED);
            documentRepository.save(document);

            ActivityLog failureLog = ActivityLog.builder()
                    .documentId(document.getId())
                    .actionType(ActionType.FAILED)
                    .details("Content generation failed for " + type + ": " + failureReason)
                    .timestamp(Instant.now())
                    .build();
            activityLogRepository.save(failureLog);

            // Also check if any draft communication exists and mark it failed
            List<Communication> pendingComms = communicationRepository.findByDocumentIdAndType(document.getId(), type);
            for (Communication comm : pendingComms) {
                if (comm.getStatus() == CommunicationStatus.DRAFT) {
                    comm.setStatus(CommunicationStatus.FAILED);
                    communicationRepository.save(comm);
                }
            }
        } catch (Exception ex) {
            log.error("Failed to mark document as FAILED after generation error: {}", ex.getMessage(), ex);
        }
    }
}
