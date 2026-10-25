package com.docpilot.worker.service;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.CommunicationFailedEvent;
import com.docpilot.common.event.CommunicationSentEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.CommunicationRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.worker.delivery.email.EmailSenderClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.timelimiter.TimeLimiter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

@Slf4j
@Service
@RequiredArgsConstructor
public class EmailService {

    private final CommunicationRepository communicationRepository;
    private final ActivityLogRepository activityLogRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final EmailSenderClient emailSenderClient;
    private final ObjectMapper objectMapper;

    @Qualifier("emailCircuitBreaker")
    private final CircuitBreaker emailCircuitBreaker;

    @Qualifier("emailTimeLimiter")
    private final TimeLimiter emailTimeLimiter;

    @Qualifier("deliveryExecutorService")
    private final ExecutorService deliveryExecutorService;

    @Autowired(required = false)
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Value("${docpilot.delivery.email.default-recipient:team@docpilot.com}")
    private String defaultRecipient;

    /**
     * Executes email delivery for an approved communication.
     * The external SMTP/SendGrid call is protected by a Resilience4j Circuit Breaker + Timeout.
     * Updates Communication status to SENT or FAILED, writes ActivityLog, and emits events.
     *
     * @param communicationId the communication to deliver
     * @return true if successfully delivered, false if failed
     */
    public boolean deliver(UUID communicationId) {
        log.info("Starting email delivery processing for communication [{}]", communicationId);

        Communication comm = communicationRepository.findByIdWithDocument(communicationId).orElse(null);
        if (comm == null) {
            log.error("Communication [{}] not found for email delivery", communicationId);
            return false;
        }

        if (comm.getType() != CommunicationType.EMAIL) {
            log.warn("Communication [{}] is of type [{}], not EMAIL. Skipping.", communicationId, comm.getType());
            return false;
        }

        if (comm.getStatus() == CommunicationStatus.SENT) {
            log.info("Communication [{}] is already SENT. Skipping redundant delivery.", communicationId);
            return true;
        }

        if (comm.getStatus() != CommunicationStatus.APPROVED && comm.getStatus() != CommunicationStatus.FAILED) {
            log.error("Communication [{}] is in status [{}], but only APPROVED or FAILED communications can be delivered.",
                    communicationId, comm.getStatus());
            return false;
        }

        String content = comm.getEffectiveContent();
        String subject = parseSubject(content, comm.getDocument().getFilename());
        String body = parseBody(content);
        String recipient = defaultRecipient;

        try {
            // Protected external call: CircuitBreaker + TimeLimiter
            String messageId = executeWithProtection(() -> emailSenderClient.send(recipient, subject, body));

            handleSuccess(comm, recipient, messageId);
            return true;
        } catch (Exception ex) {
            String errorMsg = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getSimpleName();
            log.error("Email delivery failed for communication [{}]: {}", communicationId, errorMsg);
            handleFailure(comm, errorMsg);
            return false;
        }
    }

    private <T> T executeWithProtection(Callable<T> call) throws Exception {
        Supplier<CompletableFuture<T>> futureSupplier = () -> CompletableFuture.supplyAsync(() -> {
            try {
                return call.call();
            } catch (RuntimeException re) {
                throw re;
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }, deliveryExecutorService);

        Callable<T> timed = TimeLimiter.decorateFutureSupplier(emailTimeLimiter, futureSupplier);
        Callable<T> protectedCall = CircuitBreaker.decorateCallable(emailCircuitBreaker, timed);

        try {
            return protectedCall.call();
        } catch (CompletionException ce) {
            if (ce.getCause() instanceof Exception ex) {
                throw ex;
            }
            throw ce;
        }
    }

    @Transactional
    protected void handleSuccess(Communication comm, String recipient, String messageId) {
        comm.setStatus(CommunicationStatus.SENT);
        communicationRepository.save(comm);

        String correlationId = comm.getCorrelationId();
        if (correlationId == null && comm.getDocument() != null) {
            correlationId = comm.getDocument().getCorrelationId();
        }
        if (correlationId == null) {
            correlationId = com.docpilot.common.observability.CorrelationContext.getCorrelationId();
        }

        ActivityLog logEntry = ActivityLog.builder()
                .documentId(comm.getDocument().getId())
                .actionType(ActionType.EMAIL_SENT)
                .details("Email sent successfully to " + recipient + " (messageId: " + messageId + ")")
                .timestamp(Instant.now())
                .build();
        activityLogRepository.save(logEntry);

        CommunicationSentEvent sentEvent = CommunicationSentEvent.builder()
                .eventId(UUID.randomUUID())
                .documentId(comm.getDocument().getId())
                .communicationId(comm.getId())
                .correlationId(correlationId)
                .type(CommunicationType.EMAIL)
                .status(CommunicationStatus.SENT)
                .destination(recipient)
                .externalId(messageId)
                .sentAt(Instant.now())
                .build();

        emitOutboxAndKafka(comm, KafkaTopics.COMMUNICATION_SENT, sentEvent, correlationId);
        log.info("Email communication [{}] successfully transitioned to SENT (correlationId: {})", comm.getId(), correlationId);
    }

    @Transactional
    protected void handleFailure(Communication comm, String errorMessage) {
        comm.setStatus(CommunicationStatus.FAILED);
        communicationRepository.save(comm);

        String correlationId = comm.getCorrelationId();
        if (correlationId == null && comm.getDocument() != null) {
            correlationId = comm.getDocument().getCorrelationId();
        }
        if (correlationId == null) {
            correlationId = com.docpilot.common.observability.CorrelationContext.getCorrelationId();
        }

        ActivityLog logEntry = ActivityLog.builder()
                .documentId(comm.getDocument().getId())
                .actionType(ActionType.FAILED)
                .details("Email delivery failed: " + errorMessage)
                .timestamp(Instant.now())
                .build();
        activityLogRepository.save(logEntry);

        CommunicationFailedEvent failedEvent = CommunicationFailedEvent.builder()
                .eventId(UUID.randomUUID())
                .documentId(comm.getDocument().getId())
                .communicationId(comm.getId())
                .correlationId(correlationId)
                .type(CommunicationType.EMAIL)
                .status(CommunicationStatus.FAILED)
                .reason(errorMessage)
                .failedAt(Instant.now())
                .build();

        emitOutboxAndKafka(comm, KafkaTopics.COMMUNICATION_FAILED, failedEvent, correlationId);
        log.warn("Email communication [{}] transitioned to FAILED (reason: {}, correlationId: {})",
                comm.getId(), errorMessage, correlationId);
    }

    private void emitOutboxAndKafka(Communication comm, String topic, Object eventPayload, String correlationId) {
        String json;
        try {
            json = objectMapper.writeValueAsString(eventPayload);
        } catch (Exception e) {
            log.error("Failed to serialize event payload for topic [{}]: {}", topic, e.getMessage());
            json = "{}";
        }

        String traceParent = comm.getDocument() != null ? comm.getDocument().getTraceParent() : null;

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateType("COMMUNICATION")
                .aggregateId(comm.getId().toString())
                .eventType(topic)
                .payload(json)
                .correlationId(correlationId)
                .traceParent(traceParent)
                .published(false)
                .build();
        outboxEventRepository.save(outboxEvent);

        if (kafkaTemplate != null) {
            try {
                kafkaTemplate.send(topic, comm.getId().toString(), json);
            } catch (Exception e) {
                log.debug("Kafka direct send deferred to outbox relay: {}", e.getMessage());
            }
        }
    }

    private String parseSubject(String content, String fallbackFilename) {
        if (content == null || content.isBlank()) {
            return "DocPilot Notification: " + fallbackFilename;
        }
        for (String line : content.lines().toList()) {
            String trimmed = line.trim();
            if (trimmed.startsWith("Subject:")) {
                return trimmed.substring("Subject:".length()).trim();
            }
        }
        return "DocPilot Notification: " + fallbackFilename;
    }

    private String parseBody(String content) {
        if (content == null) {
            return "";
        }
        StringBuilder body = new StringBuilder();
        boolean foundSubject = false;
        for (String line : content.lines().toList()) {
            if (!foundSubject && line.trim().startsWith("Subject:")) {
                foundSubject = true;
                continue;
            }
            body.append(line).append("\n");
        }
        return body.toString().trim();
    }
}
