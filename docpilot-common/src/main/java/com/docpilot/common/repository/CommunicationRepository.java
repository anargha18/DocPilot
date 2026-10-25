package com.docpilot.common.repository;

import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface CommunicationRepository extends JpaRepository<Communication, UUID> {
    List<Communication> findByDocumentId(UUID documentId);
    List<Communication> findByDocumentIdAndType(UUID documentId, CommunicationType type);
    List<Communication> findByStatus(CommunicationStatus status);

    @Query("SELECT c FROM Communication c JOIN FETCH c.document WHERE c.id = :id")
    Optional<Communication> findByIdWithDocument(@Param("id") UUID id);
}

