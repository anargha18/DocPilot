package com.docpilot.common.repository;

import com.docpilot.common.domain.ActivityLog;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ActivityLogRepository extends JpaRepository<ActivityLog, UUID> {
    List<ActivityLog> findByDocumentIdOrderByTimestampAsc(UUID documentId);
    List<ActivityLog> findByDocumentIdOrderByTimestampDesc(UUID documentId);
}

