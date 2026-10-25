package com.docpilot.common.service;

import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.repository.CommunicationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class CommunicationExecutionService {

    private final CommunicationRepository communicationRepository;

    /**
     * Attempts to execute a communication by ID.
     * Only APPROVED communications can reach the execution stage.
     *
     * @param communicationId the ID of the communication to execute
     * @return true if communication successfully entered the execution pipeline
     * @throws IllegalStateException if the communication is not in APPROVED status
     */
    public boolean execute(UUID communicationId) {
        Communication comm = communicationRepository.findById(communicationId)
                .orElseThrow(() -> new IllegalArgumentException("Communication not found: " + communicationId));
        return execute(comm);
    }

    /**
     * Attempts to execute a communication entity.
     * Only APPROVED communications can reach the execution stage.
     *
     * @param communication the communication entity to execute
     * @return true if communication successfully entered the execution pipeline
     * @throws IllegalStateException if the communication is not in APPROVED status
     */
    public boolean execute(Communication communication) {
        if (communication == null) {
            throw new IllegalArgumentException("Communication cannot be null");
        }
        if (communication.getStatus() != CommunicationStatus.APPROVED) {
            log.error("Execution blocked: Communication [{}] is in status [{}]. Only APPROVED communications can reach the execution stage.",
                    communication.getId(), communication.getStatus());
            throw new IllegalStateException("Execution blocked: only APPROVED communications can reach the execution stage. Communication ["
                    + communication.getId() + "] is in status [" + communication.getStatus() + "]");
        }

        log.info("Communication [{}] verified as APPROVED. Proceeding to execution stage (type: {}).",
                communication.getId(), communication.getType());
        return true;
    }
}
