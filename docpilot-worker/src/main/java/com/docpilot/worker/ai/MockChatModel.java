package com.docpilot.worker.ai;

import com.docpilot.common.model.DocumentAnalysisResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.chat.messages.AssistantMessage;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.model.Generation;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.ai.chat.prompt.Prompt;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MockChatModel implements ChatModel {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ChatResponse call(Prompt prompt) {
        String promptString = prompt.getContents();
        log.info("MockChatModel processing prompt (length: {})", promptString != null ? promptString.length() : 0);

        String lower = promptString != null ? promptString.toLowerCase() : "";

        String category = "REPORT";
        if (lower.contains("policy") || lower.contains("compliance") || lower.contains("security guideline")) {
            category = "POLICY";
        } else if (lower.contains("announcement") || lower.contains("launch") || lower.contains("press release")) {
            category = "ANNOUNCEMENT";
        } else if (lower.contains("quarter") || lower.contains("financial") || lower.contains("revenue") || lower.contains("budget")) {
            category = "FINANCIAL";
        } else if (lower.contains("technical") || lower.contains("architecture") || lower.contains("engineering") || lower.contains("system")) {
            category = "TECHNICAL";
        } else if (lower.contains("marketing") || lower.contains("campaign") || lower.contains("brand")) {
            category = "MARKETING";
        }

        List<String> keyFacts = new ArrayList<>();
        keyFacts.add("Document successfully ingested and parsed by DocPilot pipeline");
        keyFacts.add("Contextual categorization determined: " + category);
        keyFacts.add("Extracted core objectives and key organizational takeaways");

        String summary = "This document provides an overview of organizational operations, priorities, and strategic objectives within the "
                + category + " domain.";
        String context = "Internal organizational strategy and alignment communication for stakeholders.";

        DocumentAnalysisResult result = new DocumentAnalysisResult(summary, context, category, keyFacts);

        try {
            String json = objectMapper.writeValueAsString(result);
            return new ChatResponse(List.of(new Generation(new AssistantMessage(json))));
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize mock chat response", e);
        }
    }

    @Override
    public ChatOptions getDefaultOptions() {
        return null;
    }

    @Override
    public Flux<ChatResponse> stream(Prompt prompt) {
        return Flux.just(call(prompt));
    }
}
