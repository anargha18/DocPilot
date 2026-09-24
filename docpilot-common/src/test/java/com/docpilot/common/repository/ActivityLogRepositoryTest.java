package com.docpilot.common.repository;

import com.docpilot.common.AbstractPostgresRepositoryTest;
import com.docpilot.common.domain.ActionType;
import com.docpilot.common.domain.ActivityLog;
import com.docpilot.common.domain.Document;
import com.docpilot.common.domain.DocumentStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
class ActivityLogRepositoryTest extends AbstractPostgresRepositoryTest {

    @Autowired
    private DocumentRepository documentRepository;

    @Autowired
    private ActivityLogRepository activityLogRepository;

    @Test
    @DisplayName("Should maintain an ordered audit trail matching the full lifecycle from PROJECT.md Section 7")
    void shouldLogAndRetrieveFullAuditTrail() {
        Document doc = documentRepository.save(Document.builder()
                .filename("roadmap.pdf")
                .contentType("application/pdf")
                .fileSize(450000L)
                .storageKey("storage/roadmap.pdf")
                .status(DocumentStatus.UPLOADED)
                .build());

        // Document Uploaded -> Document Analyzed -> Email Selected -> Content Generated -> Content Edited -> Content Approved -> Email Sent
        Instant t0 = Instant.now().minusSeconds(120);

        ActionType[] trail = {
                ActionType.DOCUMENT_UPLOADED,
                ActionType.DOCUMENT_ANALYZED,
                ActionType.EMAIL_SELECTED,
                ActionType.CONTENT_GENERATED,
                ActionType.CONTENT_EDITED,
                ActionType.CONTENT_APPROVED,
                ActionType.EMAIL_SENT
        };

        for (int i = 0; i < trail.length; i++) {
            activityLogRepository.save(ActivityLog.builder()
                    .documentId(doc.getId())
                    .actionType(trail[i])
                    .timestamp(t0.plusSeconds(i * 10))
                    .details("Completed step: " + trail[i].name())
                    .build());
        }

        List<ActivityLog> logs = activityLogRepository.findByDocumentIdOrderByTimestampAsc(doc.getId());
        assertThat(logs).hasSize(7);

        for (int i = 0; i < trail.length; i++) {
            assertThat(logs.get(i).getActionType()).isEqualTo(trail[i].name());
            assertThat(logs.get(i).getDetails()).isEqualTo("Completed step: " + trail[i].name());
            assertThat(logs.get(i).getDocumentId()).isEqualTo(doc.getId());
        }

        List<ActivityLog> descLogs = activityLogRepository.findByDocumentIdOrderByTimestampDesc(doc.getId());
        assertThat(descLogs.get(0).getActionType()).isEqualTo(ActionType.EMAIL_SENT.name());
        assertThat(descLogs.get(6).getActionType()).isEqualTo(ActionType.DOCUMENT_UPLOADED.name());
    }
}

