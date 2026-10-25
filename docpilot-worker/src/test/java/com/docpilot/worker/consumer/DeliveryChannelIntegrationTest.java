package com.docpilot.worker.consumer;

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
import com.docpilot.worker.delivery.social.XApiClient;
import com.docpilot.worker.service.EmailService;
import com.docpilot.worker.service.SocialMediaService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.GreenMailUtil;
import com.icegreen.greenmail.util.ServerSetup;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.mail.internet.MimeMessage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "docpilot.outbox.relay.enabled=false",
        "docpilot.dummy-consumer.enabled=false",
        "docpilot.analysis-consumer.enabled=false",
        "docpilot.email-consumer.enabled=true",
        "docpilot.social-consumer.enabled=true",
        "docpilot.kafka.consumer.email-group-id=test-email-delivery-group",
        "docpilot.kafka.consumer.social-group-id=test-social-delivery-group",
        "docpilot.delivery.email.default-recipient=recipient@example.com",
        "resilience4j.circuitbreaker.instances.emailDelivery.sliding-window-size=3",
        "resilience4j.circuitbreaker.instances.emailDelivery.minimum-number-of-calls=3",
        "resilience4j.circuitbreaker.instances.emailDelivery.failure-rate-threshold=50.0",
        "resilience4j.circuitbreaker.instances.emailDelivery.wait-duration-in-open-state=5000ms",
        "resilience4j.circuitbreaker.instances.socialDelivery.sliding-window-size=3",
        "resilience4j.circuitbreaker.instances.socialDelivery.minimum-number-of-calls=3",
        "resilience4j.circuitbreaker.instances.socialDelivery.failure-rate-threshold=50.0",
        "resilience4j.circuitbreaker.instances.socialDelivery.wait-duration-in-open-state=5000ms",
        "resilience4j.timelimiter.instances.emailDelivery.timeout-duration=2s",
        "resilience4j.timelimiter.instances.socialDelivery.timeout-duration=2s"
})
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = {
                KafkaTopics.CONTENT_APPROVED,
                KafkaTopics.COMMUNICATION_SENT,
                KafkaTopics.COMMUNICATION_FAILED
        }
)
class DeliveryChannelIntegrationTest {

    private static GreenMail greenMail;
    private static final int SMTP_PORT = 3025;

    @BeforeAll
    static void startGreenMail() {
        ServerSetup setup = new ServerSetup(SMTP_PORT, "127.0.0.1", ServerSetup.PROTOCOL_SMTP);
        greenMail = new GreenMail(setup);
        greenMail.start();
    }

    @AfterAll
    static void stopGreenMail() {
        if (greenMail != null) {
            greenMail.stop();
        }
    }

    @DynamicPropertySource
    static void overrideMailProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.mail.host", () -> "127.0.0.1");
        registry.add("spring.mail.port", () -> SMTP_PORT);
    }

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private DocumentRepository documentRepository;

    @Autowired
    private CommunicationRepository communicationRepository;

    @Autowired
    private ActivityLogRepository activityLogRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private EmailService emailService;

    @Autowired
    private SocialMediaService socialMediaService;

    @SpyBean
    private XApiClient xApiClient;

    @Autowired
    @Qualifier("emailCircuitBreaker")
    private CircuitBreaker emailCircuitBreaker;

    @Autowired
    @Qualifier("socialCircuitBreaker")
    private CircuitBreaker socialCircuitBreaker;

    @Autowired
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @Autowired
    private org.springframework.mail.javamail.JavaMailSender javaMailSender;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void resetState() {
        if (!greenMail.isRunning()) {
            greenMail.start();
        }
        greenMail.reset();
        emailCircuitBreaker.reset();
        socialCircuitBreaker.reset();
    }

    @org.junit.jupiter.api.AfterEach
    void ensureGreenMailRunning() {
        if (!greenMail.isRunning()) {
            greenMail.start();
        }
    }

    private Document createDocument(String filename) {
        Document doc = Document.builder()
                .filename(filename)
                .contentType("application/pdf")
                .storageKey("documents/" + UUID.randomUUID() + "/" + filename)
                .fileSize(1024L)
                .status(DocumentStatus.APPROVED)
                .build();
        return documentRepository.save(doc);
    }

    private Communication createApprovedCommunication(Document doc, CommunicationType type, String content) {
        Communication comm = Communication.builder()
                .document(doc)
                .type(type)
                .generatedContent(content)
                .status(CommunicationStatus.APPROVED)
                .build();
        return communicationRepository.save(comm);
    }

    @Test
    @DisplayName("DOD 1: Approved EMAIL communication is sent via SMTP (GreenMail) and status updates to SENT")
    void shouldDeliverApprovedEmailViaSmtpAndUpdateStatusToSent() throws Exception {
        Document doc = createDocument("q3-board-update.pdf");
        String emailContent = """
                Subject: Q3 Board Update - Record Growth
                
                Dear Board of Directors,
                We are pleased to report record performance in Q3 2026 across all business units.
                Best regards,
                Executive Leadership
                """;
        Communication comm = createApprovedCommunication(doc, CommunicationType.EMAIL, emailContent);

        ContentApprovedEvent event = ContentApprovedEvent.builder()
                .eventId(UUID.randomUUID())
                .documentId(doc.getId())
                .communicationId(comm.getId())
                .type(CommunicationType.EMAIL)
                .approvedContent(emailContent)
                .status(CommunicationStatus.APPROVED)
                .approvedAt(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(event);
        kafkaTemplate.send(KafkaTopics.CONTENT_APPROVED, comm.getId().toString(), payload);

        // Await delivery via EmailDeliveryConsumer
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Communication updated = communicationRepository.findById(comm.getId()).orElseThrow();
            assertThat(updated.getStatus()).isEqualTo(CommunicationStatus.SENT);
        });

        // Verify GreenMail received message
        MimeMessage[] receivedMessages = greenMail.getReceivedMessages();
        assertThat(receivedMessages).hasSize(1);
        MimeMessage msg = receivedMessages[0];
        assertThat(msg.getSubject()).isEqualTo("Q3 Board Update - Record Growth");
        assertThat(GreenMailUtil.getBody(msg)).contains("Dear Board of Directors");
        assertThat(msg.getAllRecipients()[0].toString()).isEqualTo("recipient@example.com");

        // Verify ActivityLog
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).anyMatch(l -> ActionType.EMAIL_SENT.name().equals(l.getActionType()));

        // Verify OutboxEvent
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).anyMatch(oe ->
                KafkaTopics.COMMUNICATION_SENT.equals(oe.getEventType()) &&
                        comm.getId().toString().equals(oe.getAggregateId())
        );
    }

    @Test
    @DisplayName("DOD 2: Approved SOCIAL communication posts via mocked X API client and status updates to SENT")
    void shouldPostApprovedSocialMediaViaXApiAndUpdateStatusToSent() throws Exception {
        Document doc = createDocument("product-launch.pdf");
        String tweetContent = "Excited to launch DocPilot 1.0! Empowering organizations with AI-assisted communications. #DocPilot #Launch";
        Communication comm = createApprovedCommunication(doc, CommunicationType.SOCIAL, tweetContent);

        ContentApprovedEvent event = ContentApprovedEvent.builder()
                .eventId(UUID.randomUUID())
                .documentId(doc.getId())
                .communicationId(comm.getId())
                .type(CommunicationType.SOCIAL)
                .approvedContent(tweetContent)
                .status(CommunicationStatus.APPROVED)
                .approvedAt(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(event);
        kafkaTemplate.send(KafkaTopics.CONTENT_APPROVED, comm.getId().toString(), payload);

        // Await delivery via SocialMediaDeliveryConsumer
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Communication updated = communicationRepository.findById(comm.getId()).orElseThrow();
            assertThat(updated.getStatus()).isEqualTo(CommunicationStatus.SENT);
        });

        // Verify X API client called
        verify(xApiClient, atLeastOnce()).postTweet(anyString());

        // Verify ActivityLog
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).anyMatch(l -> ActionType.SOCIAL_SENT.name().equals(l.getActionType()));

        // Verify OutboxEvent
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).anyMatch(oe ->
                KafkaTopics.COMMUNICATION_SENT.equals(oe.getEventType()) &&
                        comm.getId().toString().equals(oe.getAggregateId())
        );
    }

    @Test
    @DisplayName("DOD 3: Circuit breaker configured per external call with a test that forces a trip")
    void shouldForceCircuitBreakerTripOnEmailFailuresAndFastFail() {
        // Kill the GreenMail SMTP server mock to induce external network failures
        greenMail.stop();

        assertThat(emailCircuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

        Document doc = createDocument("failed-campaign.pdf");

        // Attempt 1: Fails due to SMTP down
        Communication comm1 = createApprovedCommunication(doc, CommunicationType.EMAIL, "Subject: F1\n\nBody1");
        boolean res1 = emailService.deliver(comm1.getId());
        assertThat(res1).isFalse();
        assertThat(communicationRepository.findById(comm1.getId()).orElseThrow().getStatus()).isEqualTo(CommunicationStatus.FAILED);
        assertThat(emailCircuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

        // Attempt 2: Fails due to SMTP down
        Communication comm2 = createApprovedCommunication(doc, CommunicationType.EMAIL, "Subject: F2\n\nBody2");
        boolean res2 = emailService.deliver(comm2.getId());
        assertThat(res2).isFalse();
        assertThat(communicationRepository.findById(comm2.getId()).orElseThrow().getStatus()).isEqualTo(CommunicationStatus.FAILED);
        assertThat(emailCircuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

        // Attempt 3: Fails due to SMTP down - reaches sliding window size 3, failure rate 100% >= 50%
        Communication comm3 = createApprovedCommunication(doc, CommunicationType.EMAIL, "Subject: F3\n\nBody3");
        boolean res3 = emailService.deliver(comm3.getId());
        assertThat(res3).isFalse();
        assertThat(communicationRepository.findById(comm3.getId()).orElseThrow().getStatus()).isEqualTo(CommunicationStatus.FAILED);

        // The Email Circuit Breaker MUST TRIP to OPEN!
        assertThat(emailCircuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // Attempt 4: Fast-fails immediately via Circuit Breaker without even attempting SMTP connection
        Communication comm4 = createApprovedCommunication(doc, CommunicationType.EMAIL, "Subject: F4\n\nBody4");
        boolean res4 = emailService.deliver(comm4.getId());
        assertThat(res4).isFalse();
        Communication failedComm4 = communicationRepository.findById(comm4.getId()).orElseThrow();
        assertThat(failedComm4.getStatus()).isEqualTo(CommunicationStatus.FAILED);

        // Check activity log records circuit breaker CallNotPermitted failure
        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).anyMatch(l -> ActionType.FAILED.name().equals(l.getActionType()) &&
                (l.getDetails().contains("CallNotPermittedException") || l.getDetails().contains("CircuitBreaker")));

        // Verify outbox has communication.failed event
        List<OutboxEvent> outboxEvents = outboxEventRepository.findAll();
        assertThat(outboxEvents).anyMatch(oe ->
                KafkaTopics.COMMUNICATION_FAILED.equals(oe.getEventType()) &&
                        comm4.getId().toString().equals(oe.getAggregateId())
        );
    }

    @Test
    @DisplayName("DOD 4: Fault Isolation — Killing the email provider mock trips email CB, but social posts still go through")
    void shouldProveFaultIsolationWhenEmailFailsSocialStillSucceeds() throws Exception {
        // 1. Kill email provider mock
        greenMail.stop();

        Document emailDoc = createDocument("email-doc.pdf");
        // Force Email circuit breaker to trip
        for (int i = 0; i < 3; i++) {
            Communication c = createApprovedCommunication(emailDoc, CommunicationType.EMAIL, "Subject: Err" + i + "\n\nErr");
            emailService.deliver(c.getId());
        }
        assertThat(emailCircuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        // 2. Verify Social Circuit Breaker is CLOSED and isolated
        assertThat(socialCircuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

        // 3. Send Social Media communication through Kafka event pipeline
        Document socialDoc = createDocument("social-doc.pdf");
        String tweetContent = "Channel isolation verified: Social post succeeded while Email CB is tripped!";
        Communication socialComm = createApprovedCommunication(socialDoc, CommunicationType.SOCIAL, tweetContent);

        ContentApprovedEvent event = ContentApprovedEvent.builder()
                .eventId(UUID.randomUUID())
                .documentId(socialDoc.getId())
                .communicationId(socialComm.getId())
                .type(CommunicationType.SOCIAL)
                .approvedContent(tweetContent)
                .status(CommunicationStatus.APPROVED)
                .approvedAt(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(event);
        kafkaTemplate.send(KafkaTopics.CONTENT_APPROVED, socialComm.getId().toString(), payload);

        // 4. Confirm Social post still goes through successfully
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Communication updatedSocial = communicationRepository.findById(socialComm.getId()).orElseThrow();
            assertThat(updatedSocial.getStatus()).isEqualTo(CommunicationStatus.SENT);
        });

        // 5. Confirm Social circuit breaker is still completely CLOSED
        assertThat(socialCircuitBreaker.getState()).isEqualTo(CircuitBreaker.State.CLOSED);

        // 6. Confirm ActivityLog has SOCIAL_SENT
        List<ActivityLog> socialLogs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(socialDoc.getId());
        assertThat(socialLogs).anyMatch(l -> ActionType.SOCIAL_SENT.name().equals(l.getActionType()));
    }

    @Test
    @DisplayName("DOD 3b: Timeout configured per external call — slow call triggers TimeLimiter timeout and marks FAILED")
    void shouldTriggerTimeoutWhenExternalCallExceedsConfiguredDuration() {
        Document doc = createDocument("timeout-test.pdf");
        Communication comm = createApprovedCommunication(doc, CommunicationType.SOCIAL, "Testing timeout");

        // Mock XApiClient to hang longer than configured 2s timeout
        org.mockito.Mockito.doAnswer(invocation -> {
            Thread.sleep(2500);
            return "tweet-late";
        }).when(xApiClient).postTweet(anyString());

        boolean result = socialMediaService.deliver(comm.getId());
        assertThat(result).isFalse();

        Communication updated = communicationRepository.findById(comm.getId()).orElseThrow();
        assertThat(updated.getStatus()).isEqualTo(CommunicationStatus.FAILED);

        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).anyMatch(l -> ActionType.FAILED.name().equals(l.getActionType()) &&
                (l.getDetails().contains("TimeoutException") || l.getDetails().contains("timeout")));
    }

    @Test
    @DisplayName("DOD 1b: Idempotent consumption — duplicate approved events do not produce duplicate sends")
    void shouldHandleDuplicateApprovedEventsIdempotently() throws Exception {
        Document doc = createDocument("idempotent-doc.pdf");
        String content = "Subject: Idempotency Check\n\nIdempotent body content";
        Communication comm = createApprovedCommunication(doc, CommunicationType.EMAIL, content);

        UUID eventId = UUID.randomUUID();
        ContentApprovedEvent event = ContentApprovedEvent.builder()
                .eventId(eventId)
                .documentId(doc.getId())
                .communicationId(comm.getId())
                .type(CommunicationType.EMAIL)
                .approvedContent(content)
                .status(CommunicationStatus.APPROVED)
                .approvedAt(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(event);

        // Send first event
        kafkaTemplate.send(KafkaTopics.CONTENT_APPROVED, comm.getId().toString(), payload);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Communication updated = communicationRepository.findById(comm.getId()).orElseThrow();
            assertThat(updated.getStatus()).isEqualTo(CommunicationStatus.SENT);
        });

        int receivedAfterFirst = greenMail.getReceivedMessages().length;
        assertThat(receivedAfterFirst).isEqualTo(1);

        // Re-send the exact same event
        kafkaTemplate.send(KafkaTopics.CONTENT_APPROVED, comm.getId().toString(), payload);

        // Give Kafka time to process duplicate
        Thread.sleep(1500);

        // Confirm GreenMail STILL has only 1 message (no duplicate send)
        assertThat(greenMail.getReceivedMessages()).hasSize(1);
    }
}
