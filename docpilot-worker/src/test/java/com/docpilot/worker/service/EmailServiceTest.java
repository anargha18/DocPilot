package com.docpilot.worker.service;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.CommunicationRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.worker.delivery.email.EmailSenderClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.timelimiter.TimeLimiter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EmailServiceTest {

    @Mock
    private CommunicationRepository communicationRepository;

    @Mock
    private ActivityLogRepository activityLogRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private EmailSenderClient emailSenderClient;

    @Mock
    private KafkaTemplate<Object, Object> kafkaTemplate;

    private ObjectMapper objectMapper;
    private EmailService emailService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper().findAndRegisterModules();

        CircuitBreaker circuitBreaker = CircuitBreaker.ofDefaults("emailCircuitBreaker");
        TimeLimiter timeLimiter = TimeLimiter.of(Duration.ofSeconds(5));

        emailService = new EmailService(
                communicationRepository,
                activityLogRepository,
                outboxEventRepository,
                emailSenderClient,
                objectMapper,
                circuitBreaker,
                timeLimiter,
                Executors.newSingleThreadExecutor()
        );
        ReflectionTestUtils.setField(emailService, "defaultRecipient", "notifications@docpilot.internal");
    }

    @Test
    @DisplayName("deliver should return false if communication is not found")
    void deliver_NotFound_ReturnsFalse() {
        UUID commId = UUID.randomUUID();
        when(communicationRepository.findByIdWithDocument(commId)).thenReturn(Optional.empty());

        boolean result = emailService.deliver(commId);

        assertThat(result).isFalse();
    }

    @Test
    @DisplayName("deliver should return false if communication is not in APPROVED status")
    void deliver_NotApproved_ReturnsFalse() {
        UUID commId = UUID.randomUUID();
        Communication draft = Communication.builder()
                .type(CommunicationType.EMAIL)
                .status(CommunicationStatus.DRAFT)
                .build();
        draft.setId(commId);

        when(communicationRepository.findByIdWithDocument(commId)).thenReturn(Optional.of(draft));

        boolean result = emailService.deliver(commId);

        assertThat(result).isFalse();
        verify(emailSenderClient, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("deliver should skip and return true if communication is already SENT")
    void deliver_AlreadySent_ReturnsTrue() {
        UUID commId = UUID.randomUUID();
        Communication sent = Communication.builder()
                .type(CommunicationType.EMAIL)
                .status(CommunicationStatus.SENT)
                .build();
        sent.setId(commId);

        when(communicationRepository.findByIdWithDocument(commId)).thenReturn(Optional.of(sent));

        boolean result = emailService.deliver(commId);

        assertThat(result).isTrue();
        verify(emailSenderClient, never()).send(anyString(), anyString(), anyString());
    }

    @Test
    @DisplayName("deliver should send email, transition status to SENT, and persist outbox event")
    void deliver_Success() {
        UUID commId = UUID.randomUUID();
        Document doc = Document.builder().filename("update.txt").build();
        doc.setId(UUID.randomUUID());

        Communication comm = Communication.builder()
                .type(CommunicationType.EMAIL)
                .generatedContent("Subject: Security Notice\n\nPlease patch immediately.")
                .status(CommunicationStatus.APPROVED)
                .document(doc)
                .build();
        comm.setId(commId);

        when(communicationRepository.findByIdWithDocument(commId)).thenReturn(Optional.of(comm));
        when(emailSenderClient.send(anyString(), anyString(), anyString())).thenReturn("msg-12345");
        when(communicationRepository.save(any(Communication.class))).thenAnswer(i -> i.getArgument(0));

        boolean result = emailService.deliver(commId);

        assertThat(result).isTrue();
        verify(emailSenderClient).send(
                eq("notifications@docpilot.internal"),
                eq("Security Notice"),
                eq("Please patch immediately.")
        );

        assertThat(comm.getStatus()).isEqualTo(CommunicationStatus.SENT);

        ArgumentCaptor<ActivityLog> logCaptor = ArgumentCaptor.forClass(ActivityLog.class);
        verify(activityLogRepository).save(logCaptor.capture());
        assertThat(logCaptor.getValue().getActionType()).isEqualTo(ActionType.EMAIL_SENT.name());

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("communication.sent");
    }

    @Test
    @DisplayName("deliver should handle external client failure by marking FAILED and returning false")
    void deliver_ExternalFailure_MarksFailed() {
        UUID commId = UUID.randomUUID();
        Document doc = Document.builder().filename("update.txt").build();
        doc.setId(UUID.randomUUID());

        Communication comm = Communication.builder()
                .type(CommunicationType.EMAIL)
                .generatedContent("Subject: Test\n\nContent")
                .status(CommunicationStatus.APPROVED)
                .document(doc)
                .build();
        comm.setId(commId);

        when(communicationRepository.findByIdWithDocument(commId)).thenReturn(Optional.of(comm));
        when(communicationRepository.save(any(Communication.class))).thenAnswer(i -> i.getArgument(0));
        when(emailSenderClient.send(anyString(), anyString(), anyString()))
                .thenThrow(new RuntimeException("SMTP host unreachable"));

        boolean result = emailService.deliver(commId);

        assertThat(result).isFalse();
        assertThat(comm.getStatus()).isEqualTo(CommunicationStatus.FAILED);

        ArgumentCaptor<ActivityLog> logCaptor = ArgumentCaptor.forClass(ActivityLog.class);
        verify(activityLogRepository).save(logCaptor.capture());
        assertThat(logCaptor.getValue().getActionType()).isEqualTo(ActionType.FAILED.name());

        ArgumentCaptor<OutboxEvent> outboxCaptor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(outboxEventRepository).save(outboxCaptor.capture());
        assertThat(outboxCaptor.getValue().getEventType()).isEqualTo("communication.failed");
    }
}
