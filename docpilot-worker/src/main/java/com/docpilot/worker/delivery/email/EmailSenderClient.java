package com.docpilot.worker.delivery.email;

public interface EmailSenderClient {
    String send(String to, String subject, String body);
}
