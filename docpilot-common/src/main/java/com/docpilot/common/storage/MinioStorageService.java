package com.docpilot.common.storage;

import com.docpilot.common.retry.RetryHelper;
import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class MinioStorageService implements StorageService {

    @Value("${docpilot.storage.endpoint:http://localhost:9000}")
    private String endpoint;

    @Value("${docpilot.storage.access-key:minioadmin}")
    private String accessKey;

    @Value("${docpilot.storage.secret-key:minioadmin}")
    private String secretKey;

    @Value("${docpilot.storage.bucket:docpilot-documents}")
    private String bucket;

    private final RetryHelper retryHelper;
    private final org.springframework.beans.factory.ObjectProvider<com.docpilot.common.observability.DocPilotTracer> tracerProvider;

    private MinioClient minioClient;
    private final Map<String, byte[]> fallbackStore = new ConcurrentHashMap<>();
    private boolean minioAvailable = false;

    @PostConstruct
    public void init() {
        try {
            minioClient = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(accessKey, secretKey)
                    .build();

            boolean exists = minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
            if (!exists) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
                log.info("Created MinIO bucket: {}", bucket);
            }
            minioAvailable = true;
            log.info("Successfully connected to MinIO at [{}] bucket [{}]", endpoint, bucket);
        } catch (Exception e) {
            log.warn("MinIO is unavailable at [{}] ({}), falling back to in-memory storage for development/tests",
                    endpoint, e.getMessage());
            minioAvailable = false;
        }
    }

    @Override
    public String upload(String filename, byte[] data, String contentType) {
        String storageKey = "documents/" + UUID.randomUUID() + "/" + filename;

        if (minioAvailable && minioClient != null) {
            try {
                com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
                if (tracer != null) {
                    tracer.recordExternalCall("minio", "upload", () -> {
                        retryHelper.executeCallable(() -> {
                            minioClient.putObject(
                                    PutObjectArgs.builder()
                                            .bucket(bucket)
                                            .object(storageKey)
                                            .stream(new ByteArrayInputStream(data), data.length, -1)
                                            .contentType(contentType)
                                            .build()
                            );
                            return null;
                        });
                        return null;
                    });
                } else {
                    retryHelper.executeCallable(() -> {
                        minioClient.putObject(
                                PutObjectArgs.builder()
                                        .bucket(bucket)
                                        .object(storageKey)
                                        .stream(new ByteArrayInputStream(data), data.length, -1)
                                        .contentType(contentType)
                                        .build()
                        );
                        return null;
                    });
                }
                log.debug("Uploaded {} bytes to MinIO object [{}]", data.length, storageKey);
                return storageKey;
            } catch (Exception e) {
                log.warn("Failed to upload to MinIO ({}), storing in fallback store", e.getMessage());
            }
        }

        fallbackStore.put(storageKey, data);
        return storageKey;
    }

    @Override
    public InputStream download(String storageKey) {
        if (minioAvailable && minioClient != null) {
            try {
                com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
                if (tracer != null) {
                    return tracer.recordExternalCall("minio", "download", () ->
                            retryHelper.executeCallable(() -> minioClient.getObject(
                                    GetObjectArgs.builder()
                                            .bucket(bucket)
                                            .object(storageKey)
                                            .build()
                            ))
                    );
                } else {
                    return retryHelper.executeCallable(() -> minioClient.getObject(
                            GetObjectArgs.builder()
                                    .bucket(bucket)
                                    .object(storageKey)
                                    .build()
                    ));
                }
            } catch (Exception e) {
                log.warn("Failed to download from MinIO ({}), checking fallback", e.getMessage());
            }
        }

        byte[] data = fallbackStore.get(storageKey);
        if (data != null) {
            return new ByteArrayInputStream(data);
        }
        throw new IllegalArgumentException("File not found for storageKey: " + storageKey);
    }
}
