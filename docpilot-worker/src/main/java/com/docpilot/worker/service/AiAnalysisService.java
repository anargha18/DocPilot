package com.docpilot.worker.service;

import com.docpilot.common.model.DocumentAnalysisResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AiAnalysisService {

    private final ChatClient chatClient;

    public DocumentAnalysisResult analyzeDocument(String text) {
        if (text == null || text.isBlank()) {
            throw new IllegalArgumentException("Cannot analyze empty document text");
        }

        log.info("Analyzing document with Spring AI ChatClient (input text length: {})", text.length());

        DocumentAnalysisResult result = chatClient.prompt()
                .system("""
                        You are an expert organizational document intelligence assistant.
                        Analyze the given document content and extract:
                        1. A concise, informative summary
                        2. The organizational context, background, and operational purpose
                        3. A category classification (e.g. REPORT, ANNOUNCEMENT, POLICY, MEMO, FINANCIAL, MARKETING, TECHNICAL, OTHER)
                        4. A list of key factual bullet points or metrics extracted from the source content
                        
                        Return your response strictly adhering to the requested structured schema.
                        """)
                .user(u -> u.text("Please analyze the following document content:\n\n{content}")
                        .param("content", text))
                .call()
                .entity(DocumentAnalysisResult.class);

        if (result == null) {
            throw new IllegalStateException("Spring AI ChatClient returned null analysis result");
        }

        log.info("Document analysis completed successfully: category='{}', summaryLength={}, factsCount={}",
                result.category(),
                result.summary() != null ? result.summary().length() : 0,
                result.keyFacts() != null ? result.keyFacts().size() : 0);

        return result;
    }
}
