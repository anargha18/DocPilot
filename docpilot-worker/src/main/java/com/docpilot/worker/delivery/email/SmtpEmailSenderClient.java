package com.docpilot.worker.delivery.email;

import com.docpilot.common.retry.RetryHelper;
import jakarta.mail.internet.MimeMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class SmtpEmailSenderClient implements EmailSenderClient {

    private final JavaMailSender javaMailSender;
    private final RetryHelper retryHelper;
    private final org.springframework.beans.factory.ObjectProvider<com.docpilot.common.observability.DocPilotTracer> tracerProvider;

    @Value("${docpilot.delivery.email.from:notifications@docpilot.com}")
    private String fromAddress;

    @Override
    public String send(String to, String subject, String body) {
        String messageId = "msg-" + UUID.randomUUID();
        log.info("Sending SMTP email to [{}] with subject [{}] (messageId: {})", to, subject, messageId);

        try {
            MimeMessage message = javaMailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, false, StandardCharsets.UTF_8.name());
            helper.setFrom(fromAddress);
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setText(body != null ? body : "", false);

            com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
            if (tracer != null) {
                tracer.recordExternalCall("smtp", "send_email", () -> {
                    retryHelper.executeWithRetry(context -> {
                        javaMailSender.send(message);
                        return null;
                    });
                    return null;
                });
            } else {
                retryHelper.executeWithRetry(context -> {
                    javaMailSender.send(message);
                    return null;
                });
            }
            log.info("Successfully sent SMTP email [messageId: {}] to [{}]", messageId, to);
            return messageId;
        } catch (Exception e) {
            log.error("Failed to send SMTP email to [{}]: {}", to, e.getMessage(), e);
            throw new RuntimeException("SMTP send failed: " + e.getMessage(), e);
        }
    }
}
