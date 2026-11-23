package com.fyp.config;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Loads {@link AppConfig} from a YAML resource on the classpath, replacing
 * {@code legacy/src/fyp_attribution/core/config.py}.
 */
public final class ConfigLoader {

    public static final String DEFAULT_RESOURCE = "config.yaml";

    private ConfigLoader() {
    }

    public static AppConfig load() {
        return load(DEFAULT_RESOURCE);
    }

    public static AppConfig load(String resource) {
        try (InputStream in = ConfigLoader.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IllegalStateException("Config resource not found on classpath: " + resource);
            }
            AppConfig config = yamlMapper().readValue(in, AppConfig.class);
            // Here rather than in the job so both routes refuse a bad pair of ticks the same way: this
            // is the only config reader the API bean and AttributionJob.main share.
            config.pipeline().validateTicks();
            return config;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read config resource: " + resource, e);
        }
    }

    /** The JSON stored in {@code runs.config_json}, so a run records the config that produced it. */
    public static String toJson(AppConfig config) {
        try {
            return jsonMapper().writeValueAsString(config);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialise config", e);
        }
    }

    private static ObjectMapper yamlMapper() {
        return new ObjectMapper(new YAMLFactory())
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                // Extra keys such as a future kafka block must not break loading.
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    private static ObjectMapper jsonMapper() {
        return new ObjectMapper().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }
}
