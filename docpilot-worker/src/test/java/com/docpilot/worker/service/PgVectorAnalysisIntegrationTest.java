package com.docpilot.worker.service;

import com.docpilot.common.service.DocumentEmbeddingService;
import com.docpilot.worker.ai.MockEmbeddingModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.ai.document.Document;
import org.springframework.ai.vectorstore.SearchRequest;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:postgresql://localhost:5432/docpilot",
        "spring.datasource.username=docpilot",
        "spring.datasource.password=docpilot_secret",
        "spring.datasource.driver-class-name=org.postgresql.Driver",
        "spring.jpa.hibernate.ddl-auto=none",
        "spring.flyway.enabled=true",
        "spring.flyway.validate-on-migrate=false",
        "spring.ai.vectorstore.type=pgvector",
        "spring.ai.vectorstore.pgvector.initialize-schema=false",
        "spring.ai.vectorstore.pgvector.schema-validation=false",
        "docpilot.ai.mock=true",
        "docpilot.dummy-consumer.enabled=false",
        "docpilot.analysis-consumer.enabled=false",
        "docpilot.outbox.relay.enabled=false"
})
@ActiveProfiles("local")
class PgVectorAnalysisIntegrationTest {

    @Autowired
    private DocumentEmbeddingService documentEmbeddingService;

    @Autowired
    private VectorStore vectorStore;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("DOD: Document content is chunked and embedded into pgvector, and similarity search returns relevant chunks")
    void shouldChunkEmbedIntoPgVectorAndPerformSimilaritySearch() {
        UUID docId = UUID.randomUUID();
        String filename = "docpilot-architecture.txt";
        String content = """
                DocPilot is an event-driven AI platform built with Spring Boot 3, Apache Kafka,
                PostgreSQL with pgvector for semantic retrieval, Redis for rate limiting, and MinIO for object storage.
                The system analyzes uploaded organizational documents and auto-generates ready-to-send communications.
                The RAG pipeline retrieves relevant document chunks based on vector cosine similarity stored in PostgreSQL.
                """;

        // 1. Chunk and embed into pgvector
        List<Document> chunks = documentEmbeddingService.chunkAndEmbed(docId, filename, content);
        assertThat(chunks).isNotEmpty();

        // 2. Verify rows in PostgreSQL vector_store table
        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM vector_store WHERE metadata->>'documentId' = ?",
                Integer.class,
                docId.toString()
        );
        assertThat(count).isNotNull();
        assertThat(count).isGreaterThanOrEqualTo(1);

        // Verify the embedding vector exists and is not null
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(
                "SELECT id, content, metadata, embedding IS NOT NULL as has_vector FROM vector_store WHERE metadata->>'documentId' = ?",
                docId.toString()
        );
        assertThat(rows).isNotEmpty();
        assertThat(rows.get(0).get("has_vector")).isEqualTo(true);
        assertThat((String) rows.get(0).get("content")).contains("DocPilot");

        // 3. Perform basic similarity search against pgvector
        String query = "event-driven AI platform with Kafka and pgvector";
        List<Document> searchResults = documentEmbeddingService.similaritySearch(query, 3);

        assertThat(searchResults).isNotEmpty();
        Document topResult = searchResults.get(0);
        assertThat(topResult.getText()).isNotBlank();
        assertThat(topResult.getMetadata()).containsKey("documentId");

        // 4. Perform filtered similarity search tagged by documentId
        List<Document> docFilteredResults = documentEmbeddingService.similaritySearchByDocument(docId, query, 3);
        assertThat(docFilteredResults).isNotEmpty();
        for (Document d : docFilteredResults) {
            assertThat(d.getMetadata().get("documentId")).isEqualTo(docId.toString());
        }
    }
}
