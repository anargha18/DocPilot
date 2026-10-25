package com.docpilot.common.service;

import com.docpilot.common.retry.RetryHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.transformer.splitter.TokenTextSplitter;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DocumentEmbeddingService {

    private final VectorStore vectorStore;
    private final RetryHelper retryHelper;
    private final org.springframework.beans.factory.ObjectProvider<com.docpilot.common.observability.DocPilotTracer> tracerProvider;
    private final TokenTextSplitter tokenTextSplitter = new TokenTextSplitter();

    public List<Document> chunkAndEmbed(UUID documentId, String filename, String text) {
        if (text == null || text.isBlank()) {
            return List.of();
        }

        Map<String, Object> metadata = new HashMap<>();
        metadata.put("documentId", documentId.toString());
        if (filename != null) {
            metadata.put("filename", filename);
        }

        Document sourceDoc = new Document(text, metadata);
        List<Document> chunks = tokenTextSplitter.apply(List.of(sourceDoc));

        for (Document chunk : chunks) {
            chunk.getMetadata().put("documentId", documentId.toString());
            if (filename != null) {
                chunk.getMetadata().put("filename", filename);
            }
        }

        log.info("Embedding and persisting {} chunks into vector store for document [{}]", chunks.size(), documentId);
        com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
        if (tracer != null) {
            tracer.recordExternalCall("embedding", "chunk_and_embed", (Runnable) () -> {
                retryHelper.executeWithRetry(context -> {
                    vectorStore.add(chunks);
                    return null;
                });
            });
        } else {
            retryHelper.executeWithRetry(context -> {
                vectorStore.add(chunks);
                return null;
            });
        }
        return chunks;
    }

    public List<Document> similaritySearch(String query, int topK) {
        SearchRequest searchRequest = SearchRequest.builder()
                .query(query)
                .topK(topK)
                .similarityThresholdAll()
                .build();
        com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
        if (tracer != null) {
            try {
                return tracer.recordExternalCall("embedding", "similarity_search", () -> vectorStore.similaritySearch(searchRequest));
            } catch (Exception e) {
                log.warn("Similarity search failed: {}", e.getMessage());
                return List.of();
            }
        }
        return vectorStore.similaritySearch(searchRequest);
    }

    public List<Document> similaritySearchByDocument(UUID documentId, String query, int topK) {
        try {
            SearchRequest searchRequest = SearchRequest.builder()
                    .query(query)
                    .topK(topK)
                    .filterExpression("documentId == '" + documentId.toString() + "'")
                    .similarityThresholdAll()
                    .build();
            List<Document> results = vectorStore.similaritySearch(searchRequest);
            if (results != null && !results.isEmpty()) {
                return results;
            }
        } catch (Exception e) {
            log.warn("Vector store filter expression search failed for document [{}]: {}", documentId, e.getMessage());
        }

        // Fallback: search and filter in memory by metadata
        try {
            List<Document> generalResults = similaritySearch(query, topK * 4);
            List<Document> filtered = generalResults.stream()
                    .filter(d -> d.getMetadata() != null && documentId.toString().equals(String.valueOf(d.getMetadata().get("documentId"))))
                    .limit(topK)
                    .toList();
            if (!filtered.isEmpty()) {
                return filtered;
            }
        } catch (Exception e) {
            log.warn("Fallback similarity search failed for document [{}]: {}", documentId, e.getMessage());
        }

        return List.of();
    }
}
