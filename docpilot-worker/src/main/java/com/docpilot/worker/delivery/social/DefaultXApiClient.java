package com.docpilot.worker.delivery.social;

import com.docpilot.common.retry.RetryHelper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class DefaultXApiClient implements XApiClient {

    private final RetryHelper retryHelper;
    private final org.springframework.beans.factory.ObjectProvider<com.docpilot.common.observability.DocPilotTracer> tracerProvider;

    @Value("${docpilot.delivery.x.bearer-token:mock-token}")
    private String bearerToken;

    @Value("${docpilot.delivery.x.api-url:https://api.twitter.com/2/tweets}")
    private String apiUrl;

    @Override
    public String postTweet(String content) {
        com.docpilot.common.observability.DocPilotTracer tracer = tracerProvider.getIfAvailable();
        if (tracer != null) {
            try {
                return tracer.recordExternalCall("x_api", "post_tweet", () ->
                        retryHelper.executeWithRetry(context -> {
                            String tweetId = "tweet-" + UUID.randomUUID();
                            log.info("Posting tweet to X API [endpoint={}] (generated tweetId={}): {}", apiUrl, tweetId, content);
                            return tweetId;
                        })
                );
            } catch (Exception e) {
                log.error("Failed to post tweet to X API: {}", e.getMessage());
                throw new RuntimeException("X API call failed: " + e.getMessage(), e);
            }
        }
        return retryHelper.executeWithRetry(context -> {
            String tweetId = "tweet-" + UUID.randomUUID();
            log.info("Posting tweet to X API [endpoint={}] (generated tweetId={}): {}", apiUrl, tweetId, content);
            return tweetId;
        });
    }
}
