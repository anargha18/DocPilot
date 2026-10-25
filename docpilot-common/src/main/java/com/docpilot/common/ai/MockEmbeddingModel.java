package com.docpilot.common.ai;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.ai.embedding.Embedding;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.embedding.EmbeddingRequest;
import org.springframework.ai.embedding.EmbeddingResponse;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class MockEmbeddingModel implements EmbeddingModel {

    public static final int DIMENSIONS = 1536;

    @Override
    public EmbeddingResponse call(EmbeddingRequest request) {
        List<String> instructions = request.getInstructions();
        List<Embedding> embeddings = new ArrayList<>(instructions.size());

        for (int i = 0; i < instructions.size(); i++) {
            float[] vector = generateDeterministicVector(instructions.get(i));
            embeddings.add(new Embedding(vector, i));
        }

        return new EmbeddingResponse(embeddings);
    }

    @Override
    public float[] embed(Document document) {
        return generateDeterministicVector(document.getText());
    }

    @Override
    public int dimensions() {
        return DIMENSIONS;
    }

    public static float[] generateDeterministicVector(String text) {
        float[] vector = new float[DIMENSIONS];
        if (text == null || text.isBlank()) {
            return vector;
        }

        String[] tokens = text.toLowerCase().split("[^a-z0-9]+");
        for (String token : tokens) {
            if (token.isBlank()) {
                continue;
            }
            int h = Math.abs(token.hashCode());
            int idx1 = h % DIMENSIONS;
            int idx2 = (h * 31 + 17) % DIMENSIONS;
            if (idx2 < 0) idx2 += DIMENSIONS;
            int idx3 = (h * 67 + 37) % DIMENSIONS;
            if (idx3 < 0) idx3 += DIMENSIONS;

            vector[idx1] += 1.0f;
            vector[idx2] += 0.5f;
            vector[idx3] += 0.25f;
        }

        double sumSq = 0.0;
        for (float v : vector) {
            sumSq += (double) v * v;
        }
        double norm = Math.sqrt(sumSq);
        if (norm > 0) {
            for (int i = 0; i < DIMENSIONS; i++) {
                vector[i] = (float) (vector[i] / norm);
            }
        }

        return vector;
    }
}
