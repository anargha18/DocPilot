package com.docpilot.api.security;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "docpilot.security")
public class SecurityUserProperties {

    private List<UserDefinition> users = new ArrayList<>();

    @Getter
    @Setter
    public static class UserDefinition {
        private String username;
        private String password;
        private List<String> roles = new ArrayList<>();
    }
}
