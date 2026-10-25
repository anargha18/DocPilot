# Multi-stage Dockerfile for DocPilot services (docpilot-api, docpilot-worker)
ARG APP_MODULE=docpilot-api

FROM eclipse-temurin:21-jdk-alpine AS builder
WORKDIR /workspace

# Install build dependencies
RUN apk add --no-cache bash

# Copy gradle wrapper and configurations for dependency caching
COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
COPY docpilot-common/build.gradle ./docpilot-common/
COPY docpilot-api/build.gradle ./docpilot-api/
COPY docpilot-worker/build.gradle ./docpilot-worker/

# Pre-fetch dependencies
RUN ./gradlew dependencies --no-daemon || true

# Copy all source files
COPY docpilot-common/src ./docpilot-common/src
COPY docpilot-api/src ./docpilot-api/src
COPY docpilot-worker/src ./docpilot-worker/src

# Build the requested module's bootJar
ARG APP_MODULE=docpilot-api
RUN ./gradlew :${APP_MODULE}:bootJar --no-daemon -x test

# Runtime base stage (small Alpine JRE footprint)
FROM eclipse-temurin:21-jre-alpine AS runtime
RUN apk add --no-cache curl \
    && addgroup -S docpilot && adduser -S docpilot -G docpilot \
    && mkdir -p /app/logs \
    && chown -R docpilot:docpilot /app

WORKDIR /app

ARG APP_MODULE=docpilot-api
COPY --from=builder --chown=docpilot:docpilot /workspace/${APP_MODULE}/build/libs/${APP_MODULE}-*.jar /app/app.jar

USER docpilot

ENV SPRING_PROFILES_ACTIVE=docker \
    JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"

EXPOSE 8080 8081

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -Djava.security.egd=file:/dev/./urandom -jar /app/app.jar"]
