package com.docpilot.worker;

import com.docpilot.common.model.DocumentAnalysisResult;
import com.docpilot.common.retry.RetryHelper;
import com.docpilot.worker.delivery.email.SmtpEmailSenderClient;
import com.docpilot.worker.delivery.social.DefaultXApiClient;
import com.docpilot.worker.service.AiAnalysisService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mail.MailSendException;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.ActiveProfiles;

import jakarta.mail.internet.MimeMessage;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(properties = {
        "docpilot.retry.max-attempts=3",
        "docpilot.retry.initial-interval-ms=50",
        "docpilot.retry.multiplier=1.5",
        "docpilot.retry.max-interval-ms=200",
        "docpilot.analysis-consumer.enabled=false",
        "docpilot.email-consumer.enabled=false",
        "docpilot.social-consumer.enabled=false",
        "docpilot.outbox.relay.enabled=false",
        "spring.kafka.bootstrap-servers=localhost:9092"
})
@ActiveProfiles("test")
class ExternalCallRetryPolicyIntegrationTest {

    @Autowired
    private RetryHelper retryHelper;

    @MockBean
    private JavaMailSender javaMailSender;

    @Autowired
    private SmtpEmailSenderClient smtpEmailSenderClient;

    @Test
    @DisplayName("DOD Retry: SMTP call retries with exponential backoff on transient failure and succeeds on 3rd attempt")
    void shouldRetrySmtpCallAndSucceedAfterTransientErrors() {
        AtomicInteger attempts = new AtomicInteger(0);

        when(javaMailSender.createMimeMessage()).thenReturn(mock(MimeMessage.class));

        doAnswer(invocation -> {
            int count = attempts.incrementAndGet();
            if (count < 3) {
                throw new MailSendException("Temporary SMTP connection timeout on attempt " + count);
            }
            return null;
        }).when(javaMailSender).send(any(MimeMessage.class));

        String messageId = smtpEmailSenderClient.send("test@docpilot.com", "Subject", "Body content");

        assertThat(messageId).isNotBlank();
        assertThat(attempts.get()).isEqualTo(3);
        verify(javaMailSender, times(3)).send(any(MimeMessage.class));
    }

    @Test
    @DisplayName("DOD Retry: RetryHelper exhausts after configured maxAttempts when failure is persistent")
    void shouldExhaustRetriesWhenPersistentFailureOccurs() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThatThrownBy(() -> {
            retryHelper.executeWithRetry(context -> {
                attempts.incrementAndGet();
                throw new RuntimeException("Persistent network failure");
            });
        }).hasMessageContaining("Persistent network failure");

        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("DOD Retry: RetryHelper retries AI call and recovers on transient error")
    void shouldRetryAiCallOnTransientError() {
        AtomicInteger attempts = new AtomicInteger(0);

        String result = retryHelper.executeWithRetry(context -> {
            int count = attempts.incrementAndGet();
            if (count < 2) {
                throw new RuntimeException("OpenAI rate limit / timeout on attempt " + count);
            }
            return "AI Analysis Output Success";
        });

        assertThat(result).isEqualTo("AI Analysis Output Success");
        assertThat(attempts.get()).isEqualTo(2);
    }
}
