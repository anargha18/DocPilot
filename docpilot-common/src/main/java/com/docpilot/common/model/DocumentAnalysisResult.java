package com.docpilot.common.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;

import java.util.List;

public record DocumentAnalysisResult(
        @JsonPropertyDescription("A concise, executive summary of the document contents")
        @JsonProperty(required = true)
        String summary,

        @JsonPropertyDescription("The organizational context, objective, or background of the document")
        @JsonProperty(required = true)
        String context,

        @JsonPropertyDescription("The classification category of the document, e.g. REPORT, ANNOUNCEMENT, POLICY, MEMO, FINANCIAL, MARKETING, TECHNICAL, OTHER")
        @JsonProperty(required = true)
        String category,

        @JsonPropertyDescription("A list of key factual statements, milestones, or bullet points extracted from the document")
        @JsonProperty(required = true)
        List<String> keyFacts
) {}
