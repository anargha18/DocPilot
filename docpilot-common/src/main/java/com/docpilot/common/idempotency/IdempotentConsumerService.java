package com.docpilot.common.idempotency;

import com.docpilot.common.domain.ProcessedEvent;
import com.docpilot.common.repository.ProcessedEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotentConsumerService {

    private final ProcessedEventRepository processedEventRepository;

    @Transactional(readOnly = true)
    public boolean isAlreadyProcessed(String eventId, String consumerName) {
        if (eventId == null || consumerName == null) {
            return false;
        }
        return processedEventRepository.existsByEventIdAndConsumerName(eventId, consumerName);
    }

    @Transactional
    public boolean markAsProcessed(String eventId, String consumerName, String payloadHash) {
        if (eventId == null || consumerName == null) {
            return false;
        }
        try {
            ProcessedEvent record = ProcessedEvent.builder()
                    .eventId(eventId)
                    .consumerName(consumerName)
                    .payloadHash(payloadHash)
                    .processedAt(Instant.now())
                    .build();
            processedEventRepository.saveAndFlush(record);
            return true;
        } catch (DataIntegrityViolationException e) {
            log.debug("Event {} already recorded as processed for consumer {}: {}",
                    eventId, consumerName, e.getMessage());
            return false;
        }
    }

    @Transactional
    public boolean executeIfNew(String eventId, String consumerName, Runnable action) {
        if (isAlreadyProcessed(eventId, consumerName)) {
            log.info("Skipping already processed event [{}] for consumer [{}]", eventId, consumerName);
            return false;
        }

        action.run();
        markAsProcessed(eventId, consumerName, null);
        return true;
    }

    @Transactional
    public void reset(String eventId, String consumerName) {
        processedEventRepository.deleteByEventIdAndConsumerName(eventId, consumerName);
    }
}
