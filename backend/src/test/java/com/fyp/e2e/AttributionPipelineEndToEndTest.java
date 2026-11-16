package com.fyp.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fyp.RunContext;
import com.fyp.config.AppConfig;
import com.fyp.config.DatasetConfig;
import com.fyp.config.DatasetEntry;
import com.fyp.config.FlinkConfig;
import com.fyp.config.PipelineConfig;
import com.fyp.model.Attribution;
import com.fyp.model.WindowOutput;
import com.fyp.service.AttributionService;
import com.fyp.storage.DuckDbExecutor;
import com.fyp.storage.RunRepository;
import com.fyp.storage.RunResults;
import com.fyp.storage.WindowOutputRepository;

/**
 * CSV in, DuckDB file out, results read back by run id. This is the test that would notice a
 * regression anywhere between the loader and the sink.
 */
class AttributionPipelineEndToEndTest {

    private static final String CSV = "user_id,text,timestamp\n"
            + "user_1,Loving #python today,1000\n"
            + "user_2,#python is awesome,1200\n"
            + "user_3,Check out #java,1400\n"
            + "user_4,#python and #streaming,1600\n";

    @TempDir
    Path tempDir;

    private Path datasetCsv;
    private AppConfig config;
    private String jdbcUrl;
    private DuckDbExecutor executor;

    @BeforeEach
    void setUp() throws IOException {
        datasetCsv = tempDir.resolve("tweets.csv");
        Files.writeString(datasetCsv, CSV, StandardCharsets.UTF_8);
        Path dbFile = tempDir.resolve("attribution.duckdb");
        jdbcUrl = DuckDbExecutor.urlFor(dbFile);

        config = new AppConfig(
                new DatasetConfig("tweet_hashtag", tempDir.resolve("uploads").toString(), 0,
                        new DatasetEntry("test data", datasetCsv.toString()), null, null),
                new FlinkConfig(1),
                new PipelineConfig(1, 100, dbFile.toString()));

        executor = new DuckDbExecutor(jdbcUrl);
    }


    @Test
    void runReturnsAnIdThatRetrievesExactlyThatRunsResults() throws Exception {
        try (AttributionService service = new AttributionService(config, executor, jdbcUrl)) {
            String runId = service.run(datasetCsv);
            RunResults results = service.fetchAll(runId);

            assertThat(runId).matches("run-\\d{8}-\\d{4}-[0-9a-f]{4}");
            assertThat(results.runId()).isEqualTo(runId);
            assertThat(results.outputs())
                    .extracting(WindowOutput::category)
                    .containsExactly("JAVA", "PYTHON", "STREAMING");
            assertThat(results.totalCounted()).isEqualTo(5);

            WindowOutput python = results.outputs().stream()
                    .filter(output -> output.category().equals("PYTHON"))
                    .findFirst()
                    .orElseThrow();
            assertThat(python.windowStart()).isEqualTo(1_000L);
            assertThat(python.windowEnd()).isEqualTo(2_000L);
            assertThat(python.outputCount()).isEqualTo(3);
            assertThat(python.attribution())
                    .isEqualTo(new Attribution(Map.of("user_1", 1L, "user_2", 1L, "user_4", 1L)));
        }
    }

    @Test
    void aSecondRunIntoTheSameFileKeepsItsOwnResultsAndItsOwnKeys() throws Exception {
        try (AttributionService service = new AttributionService(config, executor, jdbcUrl)) {
            String first = service.run(datasetCsv);
            String second = service.run(datasetCsv);

            assertThat(second).isNotEqualTo(first);
            assertThat(new WindowOutputRepository(executor).countByRunId(first)).isEqualTo(3);
            assertThat(new WindowOutputRepository(executor).countByRunId(second)).isEqualTo(3);
            assertThat(new WindowOutputRepository(executor).findByRunId(first))
                    .extracting(WindowOutput::category)
                    .containsExactly("JAVA", "PYTHON", "STREAMING");
        }
    }

    @Test
    void recordsTheRunRowWithItsDatasetAndConfigSnapshot() throws Exception {
        try (AttributionService service = new AttributionService(config, executor, jdbcUrl)) {
            String runId = service.run(datasetCsv);

            RunRepository runs = new RunRepository(executor);
            RunContext recorded = runs.findByRunId(runId).orElseThrow();

            assertThat(recorded.runId()).isEqualTo(runId);
            assertThat(recorded.dataset()).isEqualTo("tweet_hashtag");
            assertThat(recorded.configJson()).contains("\"target_dataset\":\"tweet_hashtag\"");
            assertThat(executor.query("SELECT finished_at FROM runs WHERE run_id = ?", List.of(runId))
                    .get(0).getObject(0)).isNotNull();
        }
    }
}
