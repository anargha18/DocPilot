package com.docpilot.common.repository;

import com.docpilot.common.AbstractPostgresRepositoryTest;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentAnalysis;
import com.docpilot.common.domain.DocumentStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class DocumentAnalysisRepositoryTest extends AbstractPostgresRepositoryTest {

    @Autowired
    private DocumentRepository documentRepository;

    @Autowired
    private DocumentAnalysisRepository documentAnalysisRepository;

    @Test
    @DisplayName("Should persist and retrieve document analysis linked to document")
    void shouldPersistAndRetrieveAnalysis() {
        Document doc = documentRepository.save(Document.builder()
                .filename("policy-handbook.pdf")
                .contentType("application/pdf")
                .fileSize(1048576L)
                .storageKey("storage/policy.pdf")
                .status(DocumentStatus.ANALYZED)
                .build());

        String rawJson = "{\"topics\": [\"Remote Work\", \"Leave Policy\"], \"sentiment\": \"formal\", \"version\": 2}";

        DocumentAnalysis analysis = DocumentAnalysis.builder()
                .document(doc)
                .summary("This handbook outlines company workplace standards and remote work policies.")
                .extractedContext("Internal HR Policy")
                .category("Human Resources")
                .rawExtractionJson(rawJson)
                .build();

        DocumentAnalysis savedAnalysis = documentAnalysisRepository.save(analysis);

        assertThat(savedAnalysis.getId()).isNotNull();
        assertThat(savedAnalysis.getSummary()).contains("workplace standards");
        assertThat(savedAnalysis.getExtractedContext()).isEqualTo("Internal HR Policy");
        assertThat(savedAnalysis.getCategory()).isEqualTo("Human Resources");
        assertThat(savedAnalysis.getRawExtractionJson()).isEqualTo(rawJson);

        Optional<DocumentAnalysis> byDocId = documentAnalysisRepository.findByDocumentId(doc.getId());
        assertThat(byDocId).isPresent();
        assertThat(byDocId.get().getId()).isEqualTo(savedAnalysis.getId());
        assertThat(byDocId.get().getDocument().getId()).isEqualTo(doc.getId());

        Optional<DocumentAnalysis> byDoc = documentAnalysisRepository.findByDocument(doc);
        assertThat(byDoc).isPresent();
        assertThat(byDoc.get().getId()).isEqualTo(savedAnalysis.getId());
    }
}

