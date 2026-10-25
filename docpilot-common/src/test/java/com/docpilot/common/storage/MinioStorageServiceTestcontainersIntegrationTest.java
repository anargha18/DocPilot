package com.docpilot.common.storage;

import com.docpilot.common.AbstractPostgresRepositoryTest;
import com.docpilot.common.TestApplication;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class MinioStorageServiceTestcontainersIntegrationTest extends AbstractPostgresRepositoryTest {

    @Container
    static final GenericContainer<?> minio = new GenericContainer<>(DockerImageName.parse("minio/minio:latest"))
            .withEnv("MINIO_ROOT_USER", "minioadmin")
            .withEnv("MINIO_ROOT_PASSWORD", "minioadmin")
            .withCommand("server /data")
            .withExposedPorts(9000);

    @DynamicPropertySource
    static void configureMinio(DynamicPropertyRegistry registry) {
        registry.add("docpilot.storage.endpoint", () -> "http://" + minio.getHost() + ":" + minio.getFirstMappedPort());
        registry.add("docpilot.storage.access-key", () -> "minioadmin");
        registry.add("docpilot.storage.secret-key", () -> "minioadmin");
        registry.add("docpilot.storage.bucket", () -> "testcontainers-minio-bucket");
    }

    @Autowired
    private StorageService storageService;

    @Test
    @DisplayName("Should successfully initialize bucket, upload document to MinIO container, and retrieve bytes")
    void shouldUploadAndDownloadDocumentFromMinio() throws IOException {
        String filename = "executive_summary.pdf";
        byte[] testBytes = "Confidential Q3 Executive Summary Content for DocPilot".getBytes(StandardCharsets.UTF_8);
        String contentType = "application/pdf";

        // 1. Upload to Testcontainers MinIO instance
        String storageKey = storageService.upload(filename, testBytes, contentType);

        assertThat(storageKey).isNotBlank();
        assertThat(storageKey).startsWith("documents/");
        assertThat(storageKey).endsWith(filename);

        // 2. Download from MinIO and verify contents match byte-for-byte
        try (InputStream in = storageService.download(storageKey)) {
            assertThat(in).isNotNull();
            byte[] downloaded = in.readAllBytes();
            assertThat(downloaded).isEqualTo(testBytes);
        }
    }
}
