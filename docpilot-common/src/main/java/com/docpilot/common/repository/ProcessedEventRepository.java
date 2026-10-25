package com.docpilot.common.repository;

import com.docpilot.common.domain.ProcessedEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, UUID> {
    boolean existsByEventIdAndConsumerName(String eventId, String consumerName);
    Optional<ProcessedEvent> findByEventIdAndConsumerName(String eventId, String consumerName);
    void deleteByEventIdAndConsumerName(String eventId, String consumerName);
    void deleteByEventId(String eventId);
}
