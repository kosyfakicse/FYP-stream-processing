package com.fyp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.fyp.config.AppConfig;
import com.fyp.config.ConfigLoader;
import com.fyp.service.AttributionService;
import com.fyp.service.DatasetService;

/**
 * Starts the HTTP surface over the pipeline: {@code POST /runs} names a stored dataset and returns the
 * run id the results are stored under.
 *
 * <p>{@code AttributionService} is built here rather than annotated, so that no Spring type appears
 * under model/, pipeline/ or storage/. The pipeline stays runnable from {@code AttributionJob.main}
 * with no container, and its tests never load one.
 */
@SpringBootApplication
public class ApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(ApiApplication.class, args);
    }

    /**
     * One service for the process: it owns the DuckDB connection, and {@code close()} releases it
     * when the context shuts down.
     *
     * <p>{@link AppConfig} is a bean of its own so a test can point the pipeline at a throwaway
     * database by replacing one small bean, rather than replacing the service that reads it.
     */
    @Bean
    public AppConfig appConfig() {
        return ConfigLoader.load();
    }

    /**
     * The pipeline's service, built on the config's database file.
     *
     * <p>The schema is applied here, before the first request, so a read against a database with
     * nothing in it answers with an empty list. Left to the write paths alone, a freshly reset
     * database had no tables at all and {@code GET /runs} failed with "Table with name runs does not
     * exist".
     */
    @Bean(destroyMethod = "close")
    public AttributionService attributionService(AppConfig config) {
        AttributionService service = AttributionService.fromConfig(config);
        service.ensureSchema();
        return service;
    }

    /**
     * Uploads get their own connection to the same database file, so the pipeline's lock and the
     * dataset lock never have to be held at the same time. The controller sees only the service.
     */
    @Bean(destroyMethod = "close")
    public DatasetService datasetService(AppConfig config) {
        return DatasetService.fromConfig(config);
    }
}
