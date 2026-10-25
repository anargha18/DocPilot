package com.docpilot.worker.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DocumentTextExtractorTest {

    private DocumentTextExtractor textExtractor;

    @BeforeEach
    void setUp() {
        textExtractor = new DocumentTextExtractor();
    }

    @Test
    @DisplayName("extractText should throw IllegalArgumentException when InputStream is null")
    void extractText_NullStream_ThrowsException() {
        assertThatThrownBy(() -> textExtractor.extractText(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("InputStream cannot be null");
    }

    @Test
    @DisplayName("extractText should throw RuntimeException/IllegalStateException when stream is empty")
    void extractText_EmptyStream_ThrowsException() {
        ByteArrayInputStream emptyStream = new ByteArrayInputStream(new byte[0]);
        assertThatThrownBy(() -> textExtractor.extractText(emptyStream))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    @DisplayName("extractText should extract text from valid text stream")
    void extractText_ValidStream_ReturnsContent() {
        String original = "DocPilot Document Analysis Engine parses PDF, DOCX, and TXT files.";
        ByteArrayInputStream stream = new ByteArrayInputStream(original.getBytes(StandardCharsets.UTF_8));

        String extracted = textExtractor.extractText(stream);

        assertThat(extracted).contains("DocPilot Document Analysis Engine");
    }
}
