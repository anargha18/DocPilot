package com.docpilot.worker.consumer;

import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentAnalysis;
import com.docpilot.common.domain.DocumentStatus;
import com.docpilot.common.domain.OutboxEvent;
import com.docpilot.common.event.ContentApprovedEvent;
import com.docpilot.common.event.DocumentUploadedEvent;
import com.docpilot.common.event.KafkaTopics;
import com.docpilot.common.idempotency.IdempotentConsumerService;
import com.docpilot.common.repository.ActivityLogRepository;
import com.docpilot.common.repository.CommunicationRepository;
import com.docpilot.common.repository.DocumentAnalysisRepository;
import com.docpilot.common.repository.DocumentRepository;
import com.docpilot.common.repository.OutboxEventRepository;
import com.docpilot.common.repository.ProcessedEventRepository;
import com.docpilot.common.storage.StorageService;
import com.docpilot.worker.delivery.social.XApiClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.icegreen.greenmail.util.GreenMail;
import com.icegreen.greenmail.util.ServerSetup;
import jakarta.mail.internet.MimeMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "docpilot.outbox.relay.enabled=false",
        "docpilot.dummy-consumer.enabled=false",
        "docpilot.analysis-consumer.enabled=true",
        "docpilot.email-consumer.enabled=true",
        "docpilot.social-consumer.enabled=true",
        "docpilot.ai.mock=true",
        "spring.ai.vectorstore.type=simple",
        "docpilot.delivery.email.default-recipient=recipient@example.com",
        "docpilot.kafka.retry.initial-interval-ms=100",
        "docpilot.kafka.retry.max-attempts=3",
        "docpilot.kafka.retry.multiplier=1.2",
        "docpilot.kafka.retry.max-interval-ms=300"
})
@ActiveProfiles("test")
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = {
                KafkaTopics.DOCUMENT_UPLOADED,
                KafkaTopics.CONTENT_APPROVED,
                KafkaTopics.COMMUNICATION_SENT,
                KafkaTopics.COMMUNICATION_FAILED
        }
)
class ConsumerIdempotencyIntegrationTest {

    private static GreenMail greenMail;
    private static final int SMTP_PORT = 3026;

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
    private DocumentAnalysisRepository documentAnalysisRepository;

    @Autowired
    private CommunicationRepository communicationRepository;

    @Autowired
    private ActivityLogRepository activityLogRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private ProcessedEventRepository processedEventRepository;

    @Autowired
    private StorageService storageService;

    @SpyBean
    private XApiClient xApiClient;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        if (!greenMail.isRunning()) {
            greenMail.start();
        }
        greenMail.reset();
    }

    @Test
    @DisplayName("DOD Idempotency 1: Replaying document.uploaded twice to DocumentAnalysisConsumer executes analysis once with no duplicate DB writes")
    void shouldEnforceIdempotencyForDocumentAnalysisConsumer() throws Exception {
        UUID eventId = UUID.randomUUID();
        String filename = "idempotency-doc-" + UUID.randomUUID() + ".txt";
        String content = "DocPilot Q4 Strategy: Scaling distributed processing and AI content automation.";
        String storageKey = storageService.upload(filename, content.getBytes(StandardCharsets.UTF_8), "text/plain");

        Document doc = Document.builder()
                .filename(filename)
                .contentType("text/plain")
                .fileSize((long) content.length())
                .storageKey(storageKey)
                .uploadTimestamp(Instant.now())
                .status(DocumentStatus.UPLOADED)
                .build();
        doc = documentRepository.save(doc);
        UUID docId = doc.getId();

        DocumentUploadedEvent event = DocumentUploadedEvent.builder()
                .documentId(docId)
                .filename(filename)
                .storageKey(storageKey)
                .contentType("text/plain")
                .fileSize(doc.getFileSize())
                .uploadTimestamp(doc.getUploadTimestamp())
                .eventId(eventId)
                .forceFailure(false)
                .build();

        String payload = objectMapper.writeValueAsString(event);
        ProducerRecord<Object, Object> record = new ProducerRecord<>(
                KafkaTopics.DOCUMENT_UPLOADED,
                docId.toString(),
                payload
        );
        record.headers().add(new RecordHeader("X-Event-Id", eventId.toString().getBytes(StandardCharsets.UTF_8)));

        // Send 1st Kafka message
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Await 1st processing
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Document refreshed = documentRepository.findById(docId).orElseThrow();
            assertThat(refreshed.getStatus()).isEqualTo(DocumentStatus.ANALYZED);
            assertThat(documentAnalysisRepository.findByDocumentId(docId)).isPresent();
        });

        long initialAnalysisCount = documentAnalysisRepository.count();
        long initialActivityLogCount = activityLogRepository.findByDocumentIdOrderByTimestampAsc(docId).size();
        long initialOutboxCount = outboxEventRepository.count();

        // Send 2nd duplicate Kafka message
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Wait to allow consumer to receive and process
        Thread.sleep(1500);

        // Verify NO duplicate DB writes
        assertThat(documentAnalysisRepository.count()).isEqualTo(initialAnalysisCount);
        assertThat(activityLogRepository.findByDocumentIdOrderByTimestampAsc(docId).size()).isEqualTo(initialActivityLogCount);
        assertThat(outboxEventRepository.count()).isEqualTo(initialOutboxCount);
        assertThat(processedEventRepository.existsByEventIdAndConsumerName(eventId.toString(), DocumentAnalysisConsumer.CONSUMER_NAME))
                .isTrue();
    }

    @Test
    @DisplayName("DOD Idempotency 2: Replaying content.approved twice for EMAIL to EmailDeliveryConsumer results in NO duplicate email and NO duplicate DB write")
    void shouldEnforceIdempotencyForEmailDeliveryConsumer() throws Exception {
        UUID eventId = UUID.randomUUID();
        Document doc = Document.builder()
                .filename("q4-email-announcement.pdf")
                .contentType("application/pdf")
                .fileSize(1024L)
                .storageKey("documents/" + UUID.randomUUID() + "/email.pdf")
                .status(DocumentStatus.APPROVED)
                .build();
        doc = documentRepository.save(doc);

        String emailContent = "Subject: Q4 Milestone Reached\n\nDear Team, we exceeded our Q4 milestones!";
        Communication comm = Communication.builder()
                .document(doc)
                .type(CommunicationType.EMAIL)
                .generatedContent(emailContent)
                .status(CommunicationStatus.APPROVED)
                .build();
        comm = communicationRepository.save(comm);
        UUID commId = comm.getId();

        ContentApprovedEvent event = ContentApprovedEvent.builder()
                .eventId(eventId)
                .documentId(doc.getId())
                .communicationId(commId)
                .type(CommunicationType.EMAIL)
                .approvedContent(emailContent)
                .status(CommunicationStatus.APPROVED)
                .approvedAt(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(event);
        ProducerRecord<Object, Object> record = new ProducerRecord<>(
                KafkaTopics.CONTENT_APPROVED,
                commId.toString(),
                payload
        );
        record.headers().add(new RecordHeader("X-Event-Id", eventId.toString().getBytes(StandardCharsets.UTF_8)));

        // Send 1st Kafka message
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Await delivery
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Communication updated = communicationRepository.findById(commId).orElseThrow();
            assertThat(updated.getStatus()).isEqualTo(CommunicationStatus.SENT);
        });

        // Exactly 1 email received by GreenMail
        MimeMessage[] messages = greenMail.getReceivedMessages();
        assertThat(messages).hasSize(1);

        long initialActivityLogs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId()).stream()
                .filter(l -> ActionType.EMAIL_SENT.name().equals(l.getActionType()))
                .count();
        long initialSentOutbox = outboxEventRepository.findAll().stream()
                .filter(oe -> KafkaTopics.COMMUNICATION_SENT.equals(oe.getEventType()) && commId.toString().equals(oe.getAggregateId()))
                .count();

        assertThat(initialActivityLogs).isEqualTo(1);
        assertThat(initialSentOutbox).isEqualTo(1);

        // Send 2nd duplicate Kafka message with same eventId
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Allow consumer time to process
        Thread.sleep(1500);

        // Crucial assertions: NO duplicate email sent, NO duplicate DB writes
        assertThat(greenMail.getReceivedMessages()).hasSize(1);

        long subsequentActivityLogs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId()).stream()
                .filter(l -> ActionType.EMAIL_SENT.name().equals(l.getActionType()))
                .count();
        long subsequentSentOutbox = outboxEventRepository.findAll().stream()
                .filter(oe -> KafkaTopics.COMMUNICATION_SENT.equals(oe.getEventType()) && commId.toString().equals(oe.getAggregateId()))
                .count();

        assertThat(subsequentActivityLogs).isEqualTo(1);
        assertThat(subsequentSentOutbox).isEqualTo(1);
        assertThat(processedEventRepository.existsByEventIdAndConsumerName(eventId.toString(), EmailDeliveryConsumer.CONSUMER_NAME))
                .isTrue();
    }

    @Test
    @DisplayName("DOD Idempotency 3: Replaying content.approved twice for SOCIAL to SocialMediaDeliveryConsumer results in NO duplicate tweet and NO duplicate DB write")
    void shouldEnforceIdempotencyForSocialMediaDeliveryConsumer() throws Exception {
        UUID eventId = UUID.randomUUID();
        Document doc = Document.builder()
                .filename("social-launch.pdf")
                .contentType("application/pdf")
                .fileSize(512L)
                .storageKey("documents/" + UUID.randomUUID() + "/social.pdf")
                .status(DocumentStatus.APPROVED)
                .build();
        doc = documentRepository.save(doc);

        String tweetContent = "Announcing DocPilot 2.0 with hardened distributed reliability! #DocPilot #AI";
        Communication comm = Communication.builder()
                .document(doc)
                .type(CommunicationType.SOCIAL)
                .generatedContent(tweetContent)
                .status(CommunicationStatus.APPROVED)
                .build();
        comm = communicationRepository.save(comm);
        UUID commId = comm.getId();

        ContentApprovedEvent event = ContentApprovedEvent.builder()
                .eventId(eventId)
                .documentId(doc.getId())
                .communicationId(commId)
                .type(CommunicationType.SOCIAL)
                .approvedContent(tweetContent)
                .status(CommunicationStatus.APPROVED)
                .approvedAt(Instant.now())
                .build();

        String payload = objectMapper.writeValueAsString(event);
        ProducerRecord<Object, Object> record = new ProducerRecord<>(
                KafkaTopics.CONTENT_APPROVED,
                commId.toString(),
                payload
        );
        record.headers().add(new RecordHeader("X-Event-Id", eventId.toString().getBytes(StandardCharsets.UTF_8)));

        // Send 1st Kafka message
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Await delivery
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            Communication updated = communicationRepository.findById(commId).orElseThrow();
            assertThat(updated.getStatus()).isEqualTo(CommunicationStatus.SENT);
        });

        verify(xApiClient, times(1)).postTweet(anyString());

        long initialActivityLogs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId()).stream()
                .filter(l -> ActionType.SOCIAL_SENT.name().equals(l.getActionType()))
                .count();
        long initialSentOutbox = outboxEventRepository.findAll().stream()
                .filter(oe -> KafkaTopics.COMMUNICATION_SENT.equals(oe.getEventType()) && commId.toString().equals(oe.getAggregateId()))
                .count();

        assertThat(initialActivityLogs).isEqualTo(1);
        assertThat(initialSentOutbox).isEqualTo(1);

        // Send 2nd duplicate Kafka message with same eventId
        kafkaTemplate.send(record).get(5, TimeUnit.SECONDS);

        // Allow consumer time to process
        Thread.sleep(1500);

        // Crucial assertions: NO duplicate tweet call, NO duplicate DB writes
        verify(xApiClient, times(1)).postTweet(anyString());

        long subsequentActivityLogs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId()).stream()
                .filter(l -> ActionType.SOCIAL_SENT.name().equals(l.getActionType()))
                .count();
        long subsequentSentOutbox = outboxEventRepository.findAll().stream()
                .filter(oe -> KafkaTopics.COMMUNICATION_SENT.equals(oe.getEventType()) && commId.toString().equals(oe.getAggregateId()))
                .count();

        assertThat(subsequentActivityLogs).isEqualTo(1);
        assertThat(subsequentSentOutbox).isEqualTo(1);
        assertThat(processedEventRepository.existsByEventIdAndConsumerName(eventId.toString(), SocialMediaDeliveryConsumer.CONSUMER_NAME))
                .isTrue();
    }
}
