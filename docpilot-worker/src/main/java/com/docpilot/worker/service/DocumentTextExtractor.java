package com.docpilot.worker.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.tika.Tika;
import org.springframework.stereotype.Component;

import java.io.InputStream;

@Slf4j
@Component
public class DocumentTextExtractor {

    private final Tika tika = new Tika();

    public String extractText(InputStream inputStream) {
        if (inputStream == null) {
            throw new IllegalArgumentException("InputStream cannot be null for text extraction");
        }
        try {
            String extracted = tika.parseToString(inputStream);
            if (extracted == null || extracted.isBlank()) {
                throw new IllegalStateException("Extracted text is empty or blank");
            }
            return extracted.trim();
        } catch (Exception e) {
            log.error("Failed to extract text from document: {}", e.getMessage());
            throw new RuntimeException("Text extraction failed: " + e.getMessage(), e);
        }
    }
}
