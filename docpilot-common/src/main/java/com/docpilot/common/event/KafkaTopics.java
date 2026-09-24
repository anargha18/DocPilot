package com.docpilot.common.event;

import java.util.List;

public final class KafkaTopics {

    private KafkaTopics() {
        // utility class
    }

    public static final String DOCUMENT_UPLOADED = "document.uploaded";
    public static final String DOCUMENT_ANALYZED = "document.analyzed";
    public static final String CONTENT_GENERATED = "content.generated";
    public static final String CONTENT_APPROVED = "content.approved";
    public static final String COMMUNICATION_SENT = "communication.sent";
    public static final String COMMUNICATION_FAILED = "communication.failed";

    public static final String DLQ_SUFFIX = ".dlq";

    public static final List<String> ALL_MAIN_TOPICS = List.of(
            DOCUMENT_UPLOADED,
            DOCUMENT_ANALYZED,
            CONTENT_GENERATED,
            CONTENT_APPROVED,
            COMMUNICATION_SENT,
            COMMUNICATION_FAILED
    );

    public static String dlqFor(String topic) {
        if (topic == null) {
            return null;
        }
        return topic + DLQ_SUFFIX;
    }
}
