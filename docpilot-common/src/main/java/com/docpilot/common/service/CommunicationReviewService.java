package com.docpilot.common.service;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.ContentApprovedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.CommunicationRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class CommunicationReviewService {

    private final CommunicationRepository communicationRepository;
    private final DocumentRepository documentRepository;
    private final ActivityLogRepository activityLogRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final org.springframework.beans.factory.ObjectProvider<com.docpilot.common.observability.DocPilotTracer> tracerProvider;

    @Transactional
    public Communication editCommunication(UUID communicationId, String newContent) {
        if (communicationId == null) {
            throw new IllegalArgumentException("Communication ID cannot be null");
        }
        if (newContent == null || newContent.isBlank()) {
            throw new IllegalArgumentException("Edited content must not be blank");
        }

        Communication comm = communicationRepository.findById(communicationId)
                .orElseThrow(() -> new IllegalArgumentException("Communication not found: " + communicationId));

        if (comm.getEditCount() >= 1 || comm.isEdited() || comm.getStatus() == CommunicationStatus.EDITED) {
            log.warn("Rejected second edit attempt for communication [{}]. Current editCount: {}",
                    communicationId, comm.getEditCount());
            throw new IllegalStateException("Communication [" + communicationId +
                    "] has already been edited once. Only exactly one edit round is permitted.");
        }

        if (comm.getStatus() != CommunicationStatus.DRAFT) {
            log.warn("Cannot edit communication [{}] in status [{}]", communicationId, comm.getStatus());
            throw new IllegalStateException("Cannot edit communication in status: " + comm.getStatus() +
                    ". Only DRAFT communications can be edited.");
        }

        String trimmedContent = newContent.trim();
        if (comm.getType() == CommunicationType.SOCIAL && trimmedContent.length() > 280) {
            throw new IllegalArgumentException("Social media post content exceeds the maximum limit of 280 characters (length: "
                    + trimmedContent.length() + ")");
        }

        comm.setEditedContent(trimmedContent);
        comm.setEditCount(comm.getEditCount() + 1);
        comm.setEdited(true);
        comm.setStatus(CommunicationStatus.EDITED);
        comm = communicationRepository.save(comm);

        // Record ActivityLog entry for CONTENT_EDITED
        ActivityLog editLog = ActivityLog.builder()
                .documentId(comm.getDocument().getId())
                .actionType(ActionType.CONTENT_EDITED)
                .details("Edited content for " + comm.getType() + " communication (id: " + comm.getId() + ")")
                .timestamp(Instant.now())
                .build();
        activityLogRepository.save(editLog);

        log.info("Communication [{}] edited successfully (editCount: {})", comm.getId(), comm.getEditCount());
        return comm;
    }

    @Transactional
    public Communication approveCommunication(UUID communicationId) {
        if (communicationId == null) {
            throw new IllegalArgumentException("Communication ID cannot be null");
        }

        Communication comm = communicationRepository.findById(communicationId)
                .orElseThrow(() -> new IllegalArgumentException("Communication not found: " + communicationId));

        if (comm.getStatus() == CommunicationStatus.APPROVED) {
            throw new IllegalStateException("Communication [" + communicationId + "] has already been approved.");
        }
        if (comm.getStatus() == CommunicationStatus.REJECTED) {
            throw new IllegalStateException("Cannot approve rejected communication [" + communicationId + "].");
        }
        if (comm.getStatus() != CommunicationStatus.DRAFT && comm.getStatus() != CommunicationStatus.EDITED) {
            throw new IllegalStateException("Cannot approve communication in status: " + comm.getStatus() +
                    ". Only DRAFT or EDITED communications can be approved.");
        }

        Document document = comm.getDocument();
        String correlationId = document != null ? document.getCorrelationId() : comm.getCorrelationId();
        if (correlationId != null) {
            com.docpilot.common.observability.CorrelationContext.setCorrelationId(correlationId);
        }

        com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
        com.docpilot.common.observability.DocPilotTracer.TraceScope stageScope = null;
        if (tracer != null && document != null) {
            stageScope = tracer.startStageSpan("approval", correlationId, document.getId().toString(), document.getTraceParent());
        }

        try {
            // 1. Transition Communication to APPROVED
            comm.setStatus(CommunicationStatus.APPROVED);
            comm = communicationRepository.save(comm);

            // 2. Transition Document to APPROVED
            document.setStatus(DocumentStatus.APPROVED);
            documentRepository.save(document);

            // 3. Write ActivityLog for CONTENT_APPROVED
            ActivityLog approveLog = ActivityLog.builder()
                    .documentId(document.getId())
                    .actionType(ActionType.CONTENT_APPROVED)
                    .details("Approved " + comm.getType() + " communication (id: " + comm.getId() + ")")
                    .timestamp(Instant.now())
                    .build();
            activityLogRepository.save(approveLog);

            // 4. Emit content.approved via Transactional Outbox
            ContentApprovedEvent eventPayload = ContentApprovedEvent.builder()
                    .eventId(UUID.randomUUID())
                    .documentId(document.getId())
                    .communicationId(comm.getId())
                    .correlationId(correlationId)
                    .type(comm.getType())
                    .approvedContent(comm.getEffectiveContent())
                    .status(CommunicationStatus.APPROVED)
                    .approvedAt(Instant.now())
                    .build();

            String payloadJson;
            try {
                payloadJson = objectMapper.writeValueAsString(eventPayload);
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Failed to serialize content.approved event payload", e);
            }

            String currentTraceParent = stageScope != null ? stageScope.getTraceParent() : (document != null ? document.getTraceParent() : null);

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType("COMMUNICATION")
                    .aggregateId(comm.getId().toString())
                    .eventType(KafkaTopics.CONTENT_APPROVED)
                    .payload(payloadJson)
                    .correlationId(correlationId)
                    .traceParent(currentTraceParent)
                    .published(false)
                    .build();
            outboxEventRepository.save(outboxEvent);

            log.info("Communication [{}] approved and content.approved outbox event persisted (correlationId: {})",
                    comm.getId(), correlationId);
            return comm;
        } catch (Exception e) {
            if (stageScope != null) {
                stageScope.markFailed(e);
            }
            throw e;
        } finally {
            if (stageScope != null) {
                stageScope.close();
            }
        }
    }

    @Transactional
    public Communication rejectCommunication(UUID communicationId) {
        if (communicationId == null) {
            throw new IllegalArgumentException("Communication ID cannot be null");
        }

        Communication comm = communicationRepository.findById(communicationId)
                .orElseThrow(() -> new IllegalArgumentException("Communication not found: " + communicationId));

        if (comm.getStatus() == CommunicationStatus.REJECTED) {
            throw new IllegalStateException("Communication [" + communicationId + "] has already been rejected.");
        }
        if (comm.getStatus() == CommunicationStatus.APPROVED) {
            throw new IllegalStateException("Cannot reject already approved communication [" + communicationId + "].");
        }
        if (comm.getStatus() != CommunicationStatus.DRAFT && comm.getStatus() != CommunicationStatus.EDITED) {
            throw new IllegalStateException("Cannot reject communication in status: " + comm.getStatus() +
                    ". Only DRAFT or EDITED communications can be rejected.");
        }

        // 1. Transition Communication to REJECTED
        comm.setStatus(CommunicationStatus.REJECTED);
        comm = communicationRepository.save(comm);

        // 2. Reset Document to ANALYZED so user can try the other communication type
        Document document = comm.getDocument();
        document.setStatus(DocumentStatus.ANALYZED);
        documentRepository.save(document);

        // 3. Write ActivityLog for CONTENT_REJECTED
        ActivityLog rejectLog = ActivityLog.builder()
                .documentId(document.getId())
                .actionType(ActionType.CONTENT_REJECTED)
                .details("Rejected " + comm.getType() + " communication (id: " + comm.getId() + ")")
                .timestamp(Instant.now())
                .build();
        activityLogRepository.save(rejectLog);

        // 4. Pipeline stops for this communication — NO outbox event emitted
        log.info("Communication [{}] rejected; document [{}] reset to ANALYZED", comm.getId(), document.getId());
        return comm;
    }
}
