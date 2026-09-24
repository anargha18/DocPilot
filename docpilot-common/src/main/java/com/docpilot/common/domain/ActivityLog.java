package com.docpilot.common.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
@Entity
@Table(name = "activity_logs")
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ActivityLog {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "id", updatable = false, nullable = false)
    private UUID id;

    @Column(name = "document_id", nullable = false)
    private UUID documentId;

    @Column(name = "action_type", nullable = false)
    private String actionType;

    @Column(name = "timestamp", nullable = false)
    @Builder.Default
    private Instant timestamp = Instant.now();

    @Column(name = "details", columnDefinition = "TEXT")
    private String details;

    @Column(name = "created_at", nullable = false, updatable = false)
    @Builder.Default
    private Instant createdAt = Instant.now();

    @PrePersist
    protected void onCreate() {
        Instant now = Instant.now();
        if (this.timestamp == null) {
            this.timestamp = now;
        }
        if (this.createdAt == null) {
            this.createdAt = now;
        }
    }

    public static class ActivityLogBuilder {
        public ActivityLogBuilder actionType(ActionType type) {
            this.actionType = type != null ? type.name() : null;
            return this;
        }

        public ActivityLogBuilder actionType(String type) {
            this.actionType = type;
            return this;
        }
    }

    public void setActionType(ActionType type) {
        this.actionType = type != null ? type.name() : null;
    }

    public void setActionType(String type) {
        this.actionType = type;
    }
}

