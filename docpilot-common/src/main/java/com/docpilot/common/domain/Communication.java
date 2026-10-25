package com.docpilot.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@Entity
@Table(name = "communications")
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Communication extends BaseEntity {

    @Version
    @Column(name = "version")
    private Long version;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "document_id", nullable = false)
    private Document document;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false)
    private CommunicationType type;

    @Column(name = "generated_content", columnDefinition = "TEXT")
    private String generatedContent;

    @Column(name = "edited_content", columnDefinition = "TEXT")
    private String editedContent;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    @Builder.Default
    private CommunicationStatus status = CommunicationStatus.DRAFT;

    @Column(name = "edit_count", nullable = false)
    @Builder.Default
    private int editCount = 0;

    @Column(name = "edited", nullable = false)
    @Builder.Default
    private boolean edited = false;

    @Column(name = "correlation_id")
    private String correlationId;

    public String getEffectiveContent() {
        return (editedContent != null && !editedContent.isBlank()) ? editedContent : generatedContent;
    }
}

