package com.docpilot.common.repository;

import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentAnalysis;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface DocumentAnalysisRepository extends JpaRepository<DocumentAnalysis, UUID> {
    Optional<DocumentAnalysis> findByDocumentId(UUID documentId);
    Optional<DocumentAnalysis> findByDocument(Document document);
}

