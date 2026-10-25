package com.docpilot.common.service;

import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.repository.CommunicationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommunicationExecutionServiceTest {

    @Mock
    private CommunicationRepository communicationRepository;

    private CommunicationExecutionService executionService;

    @BeforeEach
    void setUp() {
        executionService = new CommunicationExecutionService(communicationRepository);
    }

    @Test
    @DisplayName("execute should throw IllegalArgumentException when communication is null or not found")
    void execute_Validation() {
        assertThatThrownBy(() -> executionService.execute((Communication) null))
                .isInstanceOf(IllegalArgumentException.class);

        UUID notFoundId = UUID.randomUUID();
        when(communicationRepository.findById(notFoundId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> executionService.execute(notFoundId))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Communication not found");
    }

    @Test
    @DisplayName("execute should throw IllegalStateException when communication is not APPROVED")
    void execute_NonApproved_ThrowsIllegalStateException() {
        UUID commId = UUID.randomUUID();
        Communication draftComm = Communication.builder()
                .type(CommunicationType.EMAIL)
                .status(CommunicationStatus.DRAFT)
                .build();
        draftComm.setId(commId);

        when(communicationRepository.findById(commId)).thenReturn(Optional.of(draftComm));

        assertThatThrownBy(() -> executionService.execute(commId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("APPROVED");
    }

    @Test
    @DisplayName("execute should return true when communication is in APPROVED status")
    void execute_Approved_ReturnsTrue() {
        UUID commId = UUID.randomUUID();
        Communication approvedComm = Communication.builder()
                .type(CommunicationType.SOCIAL)
                .status(CommunicationStatus.APPROVED)
                .build();
        approvedComm.setId(commId);

        when(communicationRepository.findById(commId)).thenReturn(Optional.of(approvedComm));

        boolean result = executionService.execute(commId);

        assertThat(result).isTrue();
    }
}
