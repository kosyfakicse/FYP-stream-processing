package com.fyp.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.HttpStatus.OK;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fyp.config.AppConfig;
import com.fyp.config.DatasetConfig;
import com.fyp.config.DatasetEntry;
import com.fyp.config.FlinkConfig;
import com.fyp.config.PipelineConfig;

/**
 * The first two requests a server ever gets, over a database file that was created by this start and
 * has never been written to.
 *
 * <p>That is the state {@code run.bat reset} leaves behind, and it is the one case
 * {@link ApiRoundTripTest} cannot show: every test there writes before it reads, so the schema the
 * upload applied is already in place by the time a {@code GET} happens.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class EmptyDatabaseReadTest {

    @TempDir
    static Path tempDir;

    @LocalServerPort
    private int port;

    /**
     * A read is not a reason to fail. Before {@code ApiApplication} applied the schema while building
     * the pipeline service, these two calls answered 500 with DuckDB's "Table with name runs does not
     * exist", because only the upload and the run paths ever created the tables.
     */
    @Test
    void listingDatasetsAndRunsOnAFreshDatabaseAnswersAnEmptyArray() throws IOException {
        JsonNode runs = new ObjectMapper().readTree(get("/runs").getBody());
        JsonNode datasets = new ObjectMapper().readTree(get("/datasets").getBody());

        assertThat(get("/runs").getStatusCode()).isEqualTo(OK);
        assertThat(runs.isArray()).isTrue();
        assertThat(runs.size()).isZero();
        assertThat(datasets.isArray()).isTrue();
        assertThat(datasets.size()).isZero();
    }

    private ResponseEntity<String> get(String path) {
        return RestClient.create("http://localhost:" + port).get().uri(path).retrieve()
                .onStatus(status -> true, (request, response) -> {
                })
                .toEntity(String.class);
    }

    @TestConfiguration
    static class FreshStorageConfig {

        /**
         * Nothing is written here on purpose: the database file is named so that opening the services
         * creates it, and the empty {@code uploads} directory is what {@code GET /datasets} reads.
         */
        @Bean
        @Primary
        AppConfig freshTestAppConfig() throws IOException {
            Path dataset = tempDir.resolve("tweets.csv");
            Files.writeString(dataset, "user_id,text,timestamp\nuser_1,#AI,1000\n",
                    StandardCharsets.UTF_8);
            Files.createDirectories(tempDir.resolve("uploads"));
            return new AppConfig(
                    new DatasetConfig("tweet_hashtag", tempDir.resolve("uploads").toString(), 0,
                            new DatasetEntry("nothing uploaded yet", dataset.toString()), null, null),
                    new FlinkConfig(1),
                    new PipelineConfig(1, 100, tempDir.resolve("attribution.duckdb").toString()));
        }
    }
}
