package com.docpilot.api.config;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class OpenApiConfig {

    public static final String BEARER_AUTH = "BearerAuth";

    @Bean
    public OpenAPI docPilotOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("DocPilot REST API")
                        .version("1.0.0")
                        .description("""
                                ### DocPilot — AI-Assisted Organizational Communication Platform
                                
                                DocPilot is an event-driven platform that transforms organizational documents into ready-to-use communications:
                                - **AI Document Analysis & RAG**: Extracts key facts, identifies organizational context, and indexes embeddings into PostgreSQL with pgvector.
                                - **AI Content Generation**: Generates contextual Emails and Social Media posts grounded in source documents.
                                - **Review & Approval**: Supports exactly one round of user edits and requires explicit approval before dispatch.
                                - **Reliable Execution**: Asynchronous dispatch via Kafka with Transactional Outbox, idempotency, and Dead Letter Queue management.
                                """)
                        .contact(new Contact()
                                .name("DocPilot Engineering Team")
                                .email("engineering@docpilot.internal"))
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0")))
                .servers(List.of(
                        new Server().url("http://localhost:8080").description("Local Development Server"),
                        new Server().url("/").description("Current Server Context")
                ))
                .components(new Components()
                        .addSecuritySchemes(BEARER_AUTH, new SecurityScheme()
                                .name(BEARER_AUTH)
                                .type(SecurityScheme.Type.HTTP)
                                .scheme("bearer")
                                .bearerFormat("JWT")
                                .description("Enter JWT Bearer token obtained from `/api/auth/login`")
                        ))
                .addSecurityItem(new SecurityRequirement().addList(BEARER_AUTH));
    }
}
