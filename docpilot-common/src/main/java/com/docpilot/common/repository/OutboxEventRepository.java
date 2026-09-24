package com.docpilot.common.repository;

import com.docpilot.common.domain.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
    List<OutboxEvent> findByPublishedFalseOrderByCreatedAtAsc();
    List<OutboxEvent> findByAggregateTypeAndAggregateId(String aggregateType, String aggregateId);

    @Query(value = "SELECT * FROM outbox_events WHERE published = false ORDER BY created_at ASC LIMIT :limit", nativeQuery = true)
    List<OutboxEvent> findUnpublishedEventsBatch(@Param("limit") int limit);
}
