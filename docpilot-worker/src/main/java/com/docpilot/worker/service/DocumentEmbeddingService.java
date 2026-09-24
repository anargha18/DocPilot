package com.docpilot.worker.service;

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
        vectorStore.add(chunks);
        return chunks;
    }

    public List<Document> similaritySearch(String query, int topK) {
        SearchRequest searchRequest = SearchRequest.builder()
                .query(query)
                .topK(topK)
                .similarityThresholdAll()
                .build();
        return vectorStore.similaritySearch(searchRequest);
    }

    public List<Document> similaritySearchByDocument(UUID documentId, String query, int topK) {
        SearchRequest searchRequest = SearchRequest.builder()
                .query(query)
                .topK(topK)
                .filterExpression("documentId == '" + documentId.toString() + "'")
                .similarityThresholdAll()
                .build();
        return vectorStore.similaritySearch(searchRequest);
    }
}
