package com.docpilot.common.repository;

import com.docpilot.common.AbstractPostgresRepositoryTest;
import com.docpilot.common.domain.Communication;
import com.docpilot.common.domain.CommunicationStatus;
import com.docpilot.common.domain.CommunicationType;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class CommunicationRepositoryTest extends AbstractPostgresRepositoryTest {

    @Autowired
    private DocumentRepository documentRepository;

    @Autowired
    private CommunicationRepository communicationRepository;

    @Test
    @DisplayName("Should persist communications and support content review workflow")
    void shouldPersistAndSupportWorkflow() {
        Document doc = documentRepository.save(Document.builder()
                .filename("product-launch.pdf")
                .contentType("application/pdf")
                .fileSize(524288L)
                .storageKey("storage/launch.pdf")
                .status(DocumentStatus.PENDING_APPROVAL)
                .build());

        // Create EMAIL communication
        Communication emailComm = communicationRepository.save(Communication.builder()
                .document(doc)
                .type(CommunicationType.EMAIL)
                .generatedContent("Subject: Launch Announcement\n\nWe are excited to unveil...")
                .status(CommunicationStatus.DRAFT)
                .build());

        // Create SOCIAL communication
        Communication socialComm = communicationRepository.save(Communication.builder()
                .document(doc)
                .type(CommunicationType.SOCIAL)
                .generatedContent("Big news! DocPilot is officially live 🚀 #AI #Productivity")
                .status(CommunicationStatus.DRAFT)
                .build());

        // Verify findByDocumentId
        List<Communication> allComms = communicationRepository.findByDocumentId(doc.getId());
        assertThat(allComms).hasSize(2);

        // Verify findByDocumentIdAndType
        List<Communication> emailOnly = communicationRepository.findByDocumentIdAndType(doc.getId(), CommunicationType.EMAIL);
        assertThat(emailOnly).hasSize(1);
        assertThat(emailOnly.get(0).getId()).isEqualTo(emailComm.getId());

        // Review and Edit workflow: DRAFT -> EDITED -> APPROVED -> SENT
        emailComm.setEditedContent("Subject: Exclusive Launch Announcement\n\nWe are thrilled to unveil...");
        emailComm.setStatus(CommunicationStatus.EDITED);
        emailComm = communicationRepository.save(emailComm);
        assertThat(emailComm.getStatus()).isEqualTo(CommunicationStatus.EDITED);
        assertThat(emailComm.getEditedContent()).contains("Exclusive Launch Announcement");

        emailComm.setStatus(CommunicationStatus.APPROVED);
        emailComm = communicationRepository.save(emailComm);
        assertThat(emailComm.getStatus()).isEqualTo(CommunicationStatus.APPROVED);

        emailComm.setStatus(CommunicationStatus.SENT);
        emailComm = communicationRepository.save(emailComm);
        assertThat(emailComm.getStatus()).isEqualTo(CommunicationStatus.SENT);

        List<Communication> sentComms = communicationRepository.findByStatus(CommunicationStatus.SENT);
        assertThat(sentComms).extracting(Communication::getId).contains(emailComm.getId());
    }
}

