package com.docpilot.api.dto;

import com.docpilot.common.domain.CommunicationType;
import jakarta.validation.constraints.NotNull;
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
public class CreateCommunicationRequest {

    @NotNull(message = "Communication type is required (EMAIL or SOCIAL)")
    private CommunicationType type;

    private String tone;

    private String targetPlatform;
}
