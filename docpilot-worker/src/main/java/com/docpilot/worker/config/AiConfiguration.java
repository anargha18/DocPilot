package com.docpilot.worker.config;

import com.docpilot.worker.ai.MockChatModel;
import com.docpilot.worker.ai.MockEmbeddingModel;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.vectorstore.SimpleVectorStore;
import org.springframework.ai.vectorstore.VectorStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AiConfiguration {

    @Bean
    @ConditionalOnProperty(name = "docpilot.ai.mock", havingValue = "true", matchIfMissing = true)
    public ChatModel chatModel() {
        return new MockChatModel();
    }

    @Bean
    @ConditionalOnProperty(name = "docpilot.ai.mock", havingValue = "true", matchIfMissing = true)
    public EmbeddingModel embeddingModel() {
        return new MockEmbeddingModel();
    }

    @Bean
    @ConditionalOnMissingBean
    public ChatClient chatClient(ChatClient.Builder chatClientBuilder) {
        return chatClientBuilder.build();
    }

    @Bean
    @ConditionalOnProperty(name = "spring.ai.vectorstore.type", havingValue = "simple")
    @ConditionalOnMissingBean(VectorStore.class)
    public VectorStore simpleVectorStore(EmbeddingModel embeddingModel) {
        return SimpleVectorStore.builder(embeddingModel).build();
    }
}
