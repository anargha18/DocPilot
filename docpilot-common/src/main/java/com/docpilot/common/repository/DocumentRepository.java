package com.docpilot.common.repository;

import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface DocumentRepository extends JpaRepository<Document, UUID> {
    List<Document> findByStatus(DocumentStatus status);
    List<Document> findAllByOrderByUploadTimestampDesc();
}
