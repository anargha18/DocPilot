package com.docpilot.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(scanBasePackages = "com.docpilot")
@EntityScan(basePackages = "com.docpilot.common.domain")
@EnableJpaRepositories(basePackages = "com.docpilot.common.repository")
public class DocPilotWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(DocPilotWorkerApplication.class, args);
    }
}

