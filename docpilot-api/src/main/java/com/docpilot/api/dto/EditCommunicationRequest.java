package com.docpilot.api.dto;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class EditCommunicationRequest {

    @JsonAlias({"content", "edited_content", "text"})
    private String editedContent;

    public String getEffectiveContent() {
        if (editedContent != null && !editedContent.isBlank()) {
            return editedContent.trim();
        }
        return null;
    }
}
