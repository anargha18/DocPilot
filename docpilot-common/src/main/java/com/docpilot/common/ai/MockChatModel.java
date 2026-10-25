package com.docpilot.common.ai;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class MockChatModel implements ChatModel {

    public static volatile boolean FORCE_FAILURE = false;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public ChatResponse call(Prompt prompt) {
        String promptString = prompt.getContents();
        log.info("MockChatModel processing prompt (length: {})", promptString != null ? promptString.length() : 0);

        if (FORCE_FAILURE || (promptString != null && (promptString.contains("FORCE_AI_FAILURE")
                || promptString.contains("SIMULATE_FAILURE")
                || promptString.contains("SIMULATE_AI_FAILURE")))) {
            log.warn("MockChatModel throwing simulated failure");
            throw new RuntimeException("Simulated AI generation failure");
        }

        String lower = promptString != null ? promptString.toLowerCase() : "";

        // Check if this is a Content Generation request (Email or Social)
        boolean isSocialRequest = lower.contains("twitter") || lower.contains("x/twitter") || lower.contains("social media");
        boolean isEmailRequest = lower.contains("email") || lower.contains("subject:") || lower.contains("ready-to-send email");

        if (isEmailRequest && !lower.contains("expert organizational document intelligence assistant")) {
            return generateEmailResponse(promptString, lower);
        } else if (isSocialRequest && !lower.contains("expert organizational document intelligence assistant")) {
            return generateSocialResponse(promptString, lower);
        }

        // Otherwise default to DocumentAnalysisResult (Phase 4 analysis)
        return generateAnalysisResponse(promptString, lower);
    }

    private ChatResponse generateEmailResponse(String promptString, String lower) {
        String category = extractField(promptString, "Category", "REPORT");
        String context = extractField(promptString, "Context", "Organizational alignment and strategy update");
        String summary = extractField(promptString, "Summary", "Key organizational initiatives");
        String chunks = extractChunks(promptString);

        String subjectPrefix;
        String greetingPrefix;
        if (category.equalsIgnoreCase("POLICY")) {
            subjectPrefix = "Directive: Official Policy Compliance Update";
            greetingPrefix = "Please take note of the mandatory organizational compliance guidelines outlined below.";
        } else if (category.equalsIgnoreCase("ANNOUNCEMENT")) {
            subjectPrefix = "Announcement: Exciting Organizational Milestone";
            greetingPrefix = "We are thrilled to share an important milestone with our community and stakeholders.";
        } else if (category.equalsIgnoreCase("FINANCIAL")) {
            subjectPrefix = "Financial Briefing: Operations & Performance Review";
            greetingPrefix = "Please find the strategic performance highlights and operational figures detailed below.";
        } else {
            subjectPrefix = category + " Communication: Operational Update";
            greetingPrefix = "Here is the summary of recent updates and operational actions.";
        }

        StringBuilder bodyBuilder = new StringBuilder();
        bodyBuilder.append("Subject: ").append(subjectPrefix).append("\n\n");
        bodyBuilder.append("Dear Team,\n\n");
        bodyBuilder.append(greetingPrefix).append("\n\n");
        bodyBuilder.append("Context & Overview:\n");
        bodyBuilder.append(summary).append("\n\n");

        if (!chunks.isBlank()) {
            bodyBuilder.append("Source Document Grounding & Key Facts:\n");
            bodyBuilder.append(chunks).append("\n\n");
        }

        bodyBuilder.append("Please reach out if you have any questions or require further clarification.\n\n");
        bodyBuilder.append("Best regards,\n");
        bodyBuilder.append("DocPilot Communications Team");

        String content = bodyBuilder.toString();
        return new ChatResponse(List.of(new Generation(new AssistantMessage(content))));
    }

    private ChatResponse generateSocialResponse(String promptString, String lower) {
        String category = extractField(promptString, "Category", "UPDATE");
        String chunks = extractChunks(promptString);
        String summary = extractField(promptString, "Summary", "Exciting organizational update");

        // Extract key fact to ground the post
        String keyFact = "";
        if (!chunks.isBlank()) {
            String[] lines = chunks.split("\n");
            // First look for an explicit fact line
            for (String line : lines) {
                String trimmed = line.trim();
                if (trimmed.toLowerCase().contains("key fact:") || trimmed.toLowerCase().contains("fact:")) {
                    keyFact = trimmed.replaceAll("(?i)^.*(?:key\\s+)?fact:\\s*", "").trim();
                    break;
                }
            }
            // If not found, look for lines with specific numbers, codes, or quotes
            if (keyFact.isBlank()) {
                for (String line : lines) {
                    String clean = line.replaceAll("^[•\\-*\\d.]+\\s*", "").trim();
                    if (clean.matches(".*[0-9$'\"][^\\n]*") && clean.length() > 20) {
                        keyFact = clean;
                        break;
                    }
                }
            }
            // Fallback: pick the longest substantive line
            if (keyFact.isBlank()) {
                for (String line : lines) {
                    String clean = line.replaceAll("^[•\\-*\\d.]+\\s*", "").trim();
                    if (clean.length() > keyFact.length()) {
                        keyFact = clean;
                    }
                }
            }
        } else {
            keyFact = summary;
        }

        // Build Twitter-compliant copy (strictly <= 280 chars)
        String hashtags = " #DocPilot #" + category.replaceAll("[^a-zA-Z0-9]", "");
        int maxFactLength = 275 - "Exciting update: ".length() - hashtags.length();

        if (keyFact.length() > maxFactLength) {
            keyFact = keyFact.substring(0, Math.max(0, maxFactLength - 3)) + "...";
        }

        String socialCopy = "Exciting update: " + keyFact + hashtags;
        if (socialCopy.length() > 280) {
            socialCopy = socialCopy.substring(0, 277) + "...";
        }

        return new ChatResponse(List.of(new Generation(new AssistantMessage(socialCopy))));
    }

    private ChatResponse generateAnalysisResponse(String promptString, String lower) {
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

        // If there's a specific unusual fact in prompt, preserve it in key facts
        Pattern quotePattern = Pattern.compile("['\"]([^'\"]{10,120})['\"]");
        Matcher matcher = quotePattern.matcher(promptString != null ? promptString : "");
        if (matcher.find()) {
            keyFacts.add("Specific grounded fact: " + matcher.group(1));
        }

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

    private String extractField(String text, String fieldName, String fallback) {
        if (text == null) return fallback;
        Pattern p = Pattern.compile("(?i)" + fieldName + ":\\s*([^\\n]+)");
        Matcher m = p.matcher(text);
        if (m.find()) {
            return m.group(1).trim();
        }
        return fallback;
    }

    private String extractChunks(String text) {
        if (text == null) return "";
        int idx = text.indexOf("Source Document Grounding Information");
        if (idx != -1) {
            String after = text.substring(idx);
            int colonIdx = after.indexOf(":\n");
            if (colonIdx != -1) {
                after = after.substring(colonIdx + 2);
            } else {
                after = after.substring("Source Document Grounding Information".length());
            }
            int endIdx = after.indexOf("Generate ");
            if (endIdx != -1) {
                return after.substring(0, endIdx).trim();
            }
            return after.trim();
        }
        Pattern p = Pattern.compile("(?s)Chunk\\s*\\d*:(.*?)(?=(Chunk\\s*\\d*:|$))");
        Matcher m = p.matcher(text);
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            sb.append(m.group(1).trim()).append("\n");
        }
        return sb.toString().trim();
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
