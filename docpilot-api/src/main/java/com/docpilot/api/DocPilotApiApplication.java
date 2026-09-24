package com.docpilot.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.docpilot")
@EntityScan(basePackages = "com.docpilot.common.domain")
@EnableJpaRepositories(basePackages = "com.docpilot.common.repository")
@EnableScheduling
public class DocPilotApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(DocPilotApiApplication.class, args);
    }
}

