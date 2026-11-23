package com.fyp.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.CREATED;
import static org.springframework.http.HttpStatus.NO_CONTENT;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.http.HttpStatus.OK;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fyp.RunContext;
import com.fyp.config.AppConfig;
import com.fyp.config.DatasetConfig;
import com.fyp.config.DatasetEntry;
import com.fyp.config.FlinkConfig;
import com.fyp.config.PipelineConfig;
import com.fyp.model.WindowOutput;
import com.fyp.storage.DuckDbExecutor;
import com.fyp.storage.Row;
import com.fyp.storage.RunRepository;
import com.fyp.storage.WindowOutputRepository;

/**
 * The endpoints over real HTTP against the real pipeline and a real DuckDB file, which is the only
 * test here that would notice a broken bean wiring or a converter that cannot serialise the record.
 *
 * <p>{@code AttributionJob} and {@link RunControllerTest} both sidestep the container; this is the
 * one that runs inside it.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ApiRoundTripTest {

    private static final String CSV = "user_id,text,timestamp\n"
            + "user_1,Loving #python today,1000\n"
            + "user_2,#python is awesome,1200\n"
            + "user_3,Check out #java,1400\n";

    /**
     * Static because Spring builds the context, and so asks for the storage config below, before
     * JUnit hands out a per-instance directory.
     */
    @TempDir
    static Path tempDir;

    @LocalServerPort
    private int port;

    private RestClient client() {
        return RestClient.create("http://localhost:" + port);
    }

    /**
     * The two endpoints together: the upload is what a run names, and the run row is what proves the
     * id reached storage rather than stopping at the controller.
     */
    @Test
    void anUploadedDatasetIsAttributedAndStoredUnderTheReturnedRunId() throws Exception {
        String datasetId = upload("python-and-java.csv");

        ResponseEntity<String> response = run(datasetId);
        assertThat(response.getStatusCode()).isEqualTo(OK);
        String runId = new ObjectMapper().readTree(response.getBody()).path("runId").asText();
        assertThat(runId).matches("run-\\d{8}-\\d{4}-[0-9a-f]{4}");

        List<WindowOutput> stored;
        try (DuckDbExecutor reader = new DuckDbExecutor(dbFile())) {
            stored = new WindowOutputRepository(reader).findByRunId(runId);
            Optional<RunContext> row = new RunRepository(reader).findByRunId(runId);
            assertThat(row).isPresent();
            assertThat(row.get().dataset()).isEqualTo(datasetId);
        }
        assertThat(stored)
                .extracting(WindowOutput::category)
                .containsExactly("JAVA", "PYTHON");
        assertThat(stored).allMatch(output -> output.windowEnd() - output.windowStart() == 1_000L);

        JsonNode detail = new ObjectMapper().readTree(get("/runs/" + runId).getBody());
        assertThat(detail.path("runId").asText()).isEqualTo(runId);
        assertThat(detail.path("dataset").asText()).isEqualTo(datasetId);
        assertThat(detail.path("startedAt").asText()).endsWith("Z");
        assertThat(detail.path("finishedAt").asText()).endsWith("Z");
        assertThat(detail.path("windows").size()).isEqualTo(2);
        assertThat(detail.path("windows").get(0).path("category").asText()).isEqualTo("JAVA");
        assertThat(detail.path("windows").get(0).path("outputCount").asLong()).isEqualTo(1);
        assertThat(detail.path("windows").get(0).path("attribution").has("user_3")).isTrue();
        assertThat(detail.path("windows").get(1).path("category").asText()).isEqualTo("PYTHON");
        assertThat(detail.path("windows").get(1).path("outputCount").asLong()).isEqualTo(2);
        JsonNode pythonAttribution = detail.path("windows").get(1).path("attribution");
        assertThat(pythonAttribution.path("user_1").asLong()).isEqualTo(1);
        assertThat(pythonAttribution.path("user_2").asLong()).isEqualTo(1);

        JsonNode graph = new ObjectMapper().readTree(get("/runs/" + runId + "/graph").getBody());
        assertThat(graph.path("vertices").findValuesAsText("vertexId"))
                .containsExactly("K1", "M1", "S1", "W1");
        assertThat(graph.path("edges").size()).isEqualTo(3);
        assertThat(graph.path("states").size()).isEqualTo(4);
        assertThat(graph.path("interactions").size()).isEqualTo(5);

        // The read orders by (time_start, vertex_id, category) and this run is one interval long, so the
        // two boxes arrive before the windows, with no category because neither operator picked one.
        assertThat(graph.path("states").findValuesAsText("vertexId"))
                .containsExactly("K1", "M1", "W1", "W1");
        assertThat(graph.path("states").get(0).path("category").isNull()).isTrue();
        assertThat(graph.path("states").get(0).path("quantity").asLong()).isEqualTo(3);
        assertThat(graph.path("states").get(0).path("attribution").isEmpty()).isTrue();
        assertThat(graph.path("states").findValues("category").stream()
                .filter(node -> !node.isNull())
                .map(JsonNode::asText))
                .containsExactly("JAVA", "PYTHON");

        JsonNode javaState = graph.path("states").get(2);
        assertThat(javaState.path("vertexId").asText()).isEqualTo("W1");
        assertThat(javaState.path("timeEnd").asLong() - javaState.path("timeStart").asLong())
                .isEqualTo(1_000L);
        assertThat(javaState.path("firedAt").asLong()).isEqualTo(2_000L);
        assertThat(javaState.path("attribution").path("user_3").asLong()).isEqualTo(1);

        JsonNode firstInteraction = graph.path("interactions").get(0);
        assertThat(firstInteraction.path("sourceVertex").asText()).isEqualTo("K1");
        assertThat(firstInteraction.path("targetVertex").asText()).isEqualTo("M1");
        assertThat(firstInteraction.path("category").isNull()).isTrue();
        assertThat(firstInteraction.path("quantity").asLong()).isEqualTo(3);

        JsonNode listed = new ObjectMapper().readTree(get("/runs").getBody());
        assertThat(listed.findValuesAsText("runId")).contains(runId);
    }

    @Test
    void anIdNothingWasUploadedUnderIsNotFound() throws Exception {
        ResponseEntity<String> response = run("ds-20260101-0000-ffff");

        assertThat(response.getStatusCode()).isEqualTo(NOT_FOUND);
        assertThat(new ObjectMapper().readTree(response.getBody()).path("error").asText())
                .isEqualTo("no dataset with id ds-20260101-0000-ffff");
    }

    /**
     * The run row is written before the dataset is even read, so a run that cannot read its file
     * leaves an unfinished run with no results behind. Asserting the unfinished count rather than the
     * total keeps this independent of whether the successful run above has happened yet.
     */
    @Test
    void aDatasetWhoseFileWentBadAfterUploadStoresNoResults() throws Exception {
        String datasetId = upload("edited-later.csv");
        Files.writeString(uploadDir().resolve(datasetId + ".csv"), "a,b,c\n1,2,3\n");

        ResponseEntity<String> response = run(datasetId);
        assertThat(response.getStatusCode()).isEqualTo(BAD_REQUEST);
        assertThat(new ObjectMapper().readTree(response.getBody()).path("error").asText())
                .isEqualTo("Expected header user_id,text,timestamp but found: a,b,c");

        try (DuckDbExecutor reader = new DuckDbExecutor(dbFile())) {
            List<Row> unfinished = reader.query(
                    "SELECT run_id FROM runs WHERE finished_at IS NULL", List.of());
            assertThat(unfinished).hasSize(1);
            String unfinishedRun = unfinished.get(0).getString(0);
            assertThat(new WindowOutputRepository(reader).countByRunId(unfinishedRun)).isZero();

            // The read is how a client sees this state: a row with no finish time and no windows.
            JsonNode detail = new ObjectMapper().readTree(get("/runs/" + unfinishedRun).getBody());
            assertThat(detail.path("finishedAt").isNull()).isTrue();
            assertThat(detail.path("windows").size()).isZero();
        }
    }

    /**
     * The upload path end to end: its own DuckDB connection, its own directory, a file whose bytes are
     * exactly what was sent. {@link DatasetControllerTest} stubs the service, so this is the test that
     * would notice the bean missing from {@code ApiApplication} or the retention read wrong.
     */
    @Test
    void anUploadBecomesAFileTheApiCanListAndForget() throws Exception {
        ResponseEntity<String> uploaded = client().post()
                .uri("/datasets?name=october.csv")
                .contentType(MediaType.valueOf("text/csv"))
                .body(CSV.getBytes(StandardCharsets.UTF_8))
                .retrieve()
                .toEntity(String.class);

        assertThat(uploaded.getStatusCode()).isEqualTo(CREATED);
        JsonNode stored = new ObjectMapper().readTree(uploaded.getBody());
        String datasetId = stored.path("datasetId").asText();
        assertThat(datasetId).matches("ds-\\d{8}-\\d{4}-[0-9a-f]{4}");
        assertThat(stored.path("originalName").asText()).isEqualTo("october.csv");
        assertThat(stored.path("rowCount").asLong()).isEqualTo(3);
        assertThat(uploaded.getHeaders().getFirst("Location")).isEqualTo("/datasets/" + datasetId);

        Path file = uploadDir().resolve(datasetId + ".csv");
        assertThat(Files.readAllBytes(file)).isEqualTo(CSV.getBytes(StandardCharsets.UTF_8));

        JsonNode listed = new ObjectMapper().readTree(get("/datasets").getBody());
        assertThat(listed.findValuesAsText("datasetId")).contains(datasetId);

        assertThat(client().delete().uri("/datasets/" + datasetId).retrieve()
                .onStatus(status -> true, (request, response) -> {
                }).toEntity(String.class).getStatusCode()).isEqualTo(NO_CONTENT);
        assertThat(file).doesNotExist();
        assertThat(get("/datasets/" + datasetId).getStatusCode()).isEqualTo(NOT_FOUND);
    }

    private String upload(String name) throws Exception {
        ResponseEntity<String> uploaded = client().post()
                .uri("/datasets?name=" + name)
                .contentType(MediaType.valueOf("text/csv"))
                .body(CSV.getBytes(StandardCharsets.UTF_8))
                .retrieve()
                .toEntity(String.class);
        assertThat(uploaded.getStatusCode()).isEqualTo(CREATED);
        return new ObjectMapper().readTree(uploaded.getBody()).path("datasetId").asText();
    }

    /** Errors are asserted on rather than thrown, so the status and the body are both in hand. */
    private ResponseEntity<String> run(String datasetId) {
        return client().post()
                .uri("/runs")
                .contentType(MediaType.APPLICATION_JSON)
                .body("{\"datasetId\":\"" + datasetId + "\"}")
                .retrieve()
                .onStatus(status -> true, (request, response) -> {
                })
                .toEntity(String.class);
    }

    private ResponseEntity<String> get(String path) {
        return client().get().uri(path).retrieve()
                .onStatus(status -> true, (request, response) -> {
                })
                .toEntity(String.class);
    }

    private static Path dbFile() {
        return tempDir.resolve("attribution.duckdb");
    }

    private static Path uploadDir() {
        return tempDir.resolve("uploads");
    }

    @TestConfiguration
    static class StorageConfig {

        /**
         * Replacing {@link AppConfig} is enough to move the whole pipeline into {@code #tempDir}:
         * {@code ApiApplication} builds both services from this bean, so the real wiring still runs.
         * A run over HTTP records the dataset id it was given, so the {@code target_dataset} below is
         * only what the command line job would store.
         *
         * <p>The method is not called {@code appConfig}: two definitions of one bean name collide,
         * because Boot forbids overriding a bean whether or not the replacement is {@code @Primary}.
         */
        @Bean
        @Primary
        AppConfig testAppConfig() throws IOException {
            Path dataset = tempDir.resolve("tweets.csv");
            Files.writeString(dataset, CSV, StandardCharsets.UTF_8);
            Path db = tempDir.resolve("attribution.duckdb");
            return new AppConfig(
                    new DatasetConfig("tweet_hashtag", uploadDir().toString(), 0,
                            new DatasetEntry("api test data", dataset.toString()), null, null),
                    new FlinkConfig(1),
                    new PipelineConfig(1, 1, 1, 100, db.toString()));
        }
    }
}
