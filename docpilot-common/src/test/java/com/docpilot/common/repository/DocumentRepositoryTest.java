package com.docpilot.common.repository;

import com.docpilot.common.AbstractPostgresRepositoryTest;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class DocumentRepositoryTest extends AbstractPostgresRepositoryTest {

    @Autowired
    private DocumentRepository documentRepository;

    @Test
    @DisplayName("Should persist and retrieve a document with timestamps and default status")
    void shouldPersistAndRetrieveDocument() {
        Document document = Document.builder()
                .filename("annual-report-2025.pdf")
                .contentType("application/pdf")
                .fileSize(2048576L)
                .storageKey("documents/2025/annual-report.pdf")
                .build();

        Document saved = documentRepository.save(document);

        assertThat(saved.getId()).isNotNull();
        assertThat(saved.getStatus()).isEqualTo(DocumentStatus.UPLOADED);
        assertThat(saved.getFilename()).isEqualTo("annual-report-2025.pdf");
        assertThat(saved.getFileName()).isEqualTo("annual-report-2025.pdf");
        assertThat(saved.getContentType()).isEqualTo("application/pdf");
        assertThat(saved.getFileSize()).isEqualTo(2048576L);
        assertThat(saved.getStorageKey()).isEqualTo("documents/2025/annual-report.pdf");
        assertThat(saved.getUploadTimestamp()).isNotNull();
        assertThat(saved.getCreatedAt()).isNotNull();
        assertThat(saved.getUpdatedAt()).isNotNull();

        Optional<Document> fetched = documentRepository.findById(saved.getId());
        assertThat(fetched).isPresent();
        assertThat(fetched.get().getFilename()).isEqualTo("annual-report-2025.pdf");
    }

    @Test
    @DisplayName("Should query documents by status and update through lifecycle states")
    void shouldQueryByStatusAndTransitionStates() {
        Document document = documentRepository.save(Document.builder()
                .filename("quarterly-update.docx")
                .contentType("application/vnd.openxmlformats-officedocument.wordprocessingml.document")
                .fileSize(102400L)
                .storageKey("documents/q3-update.docx")
                .build());

        List<Document> uploadedDocs = documentRepository.findByStatus(DocumentStatus.UPLOADED);
        assertThat(uploadedDocs).extracting(Document::getId).contains(document.getId());

        // Lifecycle transitions: UPLOADED -> ANALYZING -> ANALYZED -> GENERATING -> PENDING_APPROVAL -> APPROVED -> SENT
        DocumentStatus[] lifecycle = {
                DocumentStatus.ANALYZING,
                DocumentStatus.ANALYZED,
                DocumentStatus.GENERATING,
                DocumentStatus.PENDING_APPROVAL,
                DocumentStatus.APPROVED,
                DocumentStatus.SENT
        };

        for (DocumentStatus nextStatus : lifecycle) {
            document.setStatus(nextStatus);
            document = documentRepository.save(document);
            assertThat(document.getStatus()).isEqualTo(nextStatus);
        }

        List<Document> sentDocs = documentRepository.findByStatus(DocumentStatus.SENT);
        assertThat(sentDocs).extracting(Document::getId).contains(document.getId());
    }

    @Test
    @DisplayName("Should list documents ordered by upload timestamp descending")
    void shouldFindAllOrderedByUploadTimestampDesc() throws InterruptedException {
        Document doc1 = documentRepository.save(Document.builder()
                .filename("doc1.pdf")
                .contentType("application/pdf")
                .fileSize(5000L)
                .storageKey("key1")
                .uploadTimestamp(Instant.now().minusSeconds(60))
                .build());

        Document doc2 = documentRepository.save(Document.builder()
                .filename("doc2.pdf")
                .contentType("application/pdf")
                .fileSize(6000L)
                .storageKey("key2")
                .uploadTimestamp(Instant.now())
                .build());

        List<Document> ordered = documentRepository.findAllByOrderByUploadTimestampDesc();
        assertThat(ordered).isNotEmpty();

        int index1 = ordered.indexOf(doc1);
        int index2 = ordered.indexOf(doc2);
        assertThat(index2).isLessThan(index1);
    }
}

